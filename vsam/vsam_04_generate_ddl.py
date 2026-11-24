#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_04_generate_ddl.py - Step 04: PostgreSQL DDL Generation
=============================================================================
Generates valid PostgreSQL DDL for all VSAM target tables using:
  - Column metadata from Step 01 (copybook_extracts.csv) or Step 03 profile
  - Table/schema mapping from Step 02 (load_matrix.csv)
  - Profile (including schema_col & key columns) from Step 03 (type_gate.json)

For AGILETIC source company:
  - Column names and datatypes are directly inferred from S3 CSV headers & data.
  - Key columns are taken from mapped copybook or auto-detected in positional order.
For other source companies:
  - Copybook column specs with dynamic date regex and varchar length auto-widening.

Generates inside outputs/vsam/gen_ddl/:
  - outputs/vsam/gen_ddl/<schema>.sql      (CREATE SCHEMA + CREATE TABLE)
  - outputs/vsam/gen_ddl/all_tables.sql    (all tables combined)
  - outputs/vsam/gen_ddl/<schema>_pk.sql  (ALTER TABLE ADD PRIMARY KEY)
  - outputs/vsam/gen_ddl/all_pk.sql       (all pk constraints combined)
  - reports/vsam/<db>_vsam_04_generate_ddl_report_<ts>.xlsx
=============================================================================
"""
import sys
import re
import json
import shutil
import argparse
from pathlib import Path
import pandas as pd

from sweep_common import (
    log_summary,
    VSAM_OUTPUTS_DIR,
    VSAM_GEN_DDL_DIR,
    VSAM_REPORTS_DIR,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    run_psql,
    q,
    norm_col,
)
from vsam.common_colmap import effective_columns_meta
from sweep_common import layer_dir
from sweep_ddlfrag import FragmentStore
from sweep_merge import write_text_file
from sweep_migrate import CTL_ALL_SQL
from sweep_targeting import add_targeting_args, selector_or_exit, validate_or_exit
from sweep_layers import add_layer_arg, ensure_layer
from vsam.vsam_common import get_all_active_schemas, get_korg_override_pkey, load_csv_safe, OUT

logger = get_logger("vsam_04_generate_ddl", "vsam_04_generate_ddl")

AUDIT_COLS = [
    '  "createddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "createdby" character varying(40) DEFAULT CURRENT_USER',
    '  "modifieddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "modifiedby" character varying(40) DEFAULT CURRENT_USER',
    '  "createddate_m" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP',
    '  "createdby_m" character varying(40) NOT NULL DEFAULT CURRENT_USER',
]

CTL_DDL = CTL_ALL_SQL      # control schema objects (oracle_load_log / vsam_load_log, rejects, delta registry ...)


AUDIT_COL_NAMES = {
    "createddate", "createdby", "modifieddate", "modifiedby",
    "createddate_m", "createdby_m"
}


def build_table_ddl(schema, table, columns, key_cols):
    """
    Returns (pre_load_sql, post_load_sql) tuple.
    pre_load: CREATE TABLE without PK constraint.
    post_load: ALTER TABLE ADD CONSTRAINT pk.
    Deduplicates column names (case-insensitive) — first occurrence wins.
    Skips data columns whose names clash with reserved audit column names.
    """
    col_lines = []
    seen_cols = set()  # normalised names already emitted
    for c in columns:
        if c.get("is_filler") == "YES":
            continue
        col_name_raw = norm_col(c.get("col_name", ""))
        if not col_name_raw:
            continue
        if col_name_raw in seen_cols:
            logger.warning(
                "Duplicate column '%s' in %s.%s — keeping first occurrence, skipping duplicate.",
                col_name_raw, schema, table
            )
            continue
        # Reserve audit column names for the audit block below
        if col_name_raw in AUDIT_COL_NAMES:
            logger.warning(
                "Data column '%s' in %s.%s clashes with audit column name — skipping data column.",
                col_name_raw, schema, table
            )
            continue
        seen_cols.add(col_name_raw)
        col_type = c.get("sql_type") or "text"
        col_lines.append(f"  {q(col_name_raw)} {col_type}")

    col_lines.extend(AUDIT_COLS)

    pre_sql = (
        f"CREATE TABLE IF NOT EXISTS {q(schema)}.{q(table)} (\n"
        + ",\n".join(col_lines)
        + "\n);"
    )

    post_sql = ""
    if key_cols:
        pk_name = q(f"pk_{table}")
        pk_def = ", ".join(q(k) for k in key_cols)
        post_sql = (
            f"ALTER TABLE {q(schema)}.{q(table)}\n"
            f"  ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_def});"
        )

    return pre_sql, post_sql


def _add_fragment(bucket, schema, table, sql):
    """Collects the distinct statements generated for one (schema, table) during the run."""
    stmts = bucket.setdefault((schema, table), [])
    if sql not in stmts:
        stmts.append(sql)


def main():
    parser = argparse.ArgumentParser(description="Step 04: Generate PostgreSQL DDL for VSAM tables.")
    parser.add_argument("--execute", action="store_true", help="Apply pre-load DDL to PostgreSQL immediately")
    parser.add_argument("--bg", action="store_true", help="Run in background")
    add_targeting_args(parser, company=True)
    add_layer_arg(parser)
    args = parser.parse_args()
    sel = selector_or_exit(args, logger)
    ensure_layer(args, logger, required=bool(args.execute), db=bool(args.execute), ddl_what="Step 04 (generate DDL)")
    store = FragmentStore(layer_dir(VSAM_OUTPUTS_DIR / "ddl_fragments"))

    logger.info("=" * 75)
    logger.info("SWEEP VSAM MIGRATION - STEP 04: TARGET DDL GENERATION")
    logger.info("=" * 75)

    # Load Step 02 load matrix
    lm_file = OUT / "vsam_02_load_matrix.csv" if (OUT / "vsam_02_load_matrix.csv").exists() else OUT / "load_matrix.csv"
    lm = load_csv_safe(lm_file)
    if sel.filtered and not lm.empty:
        validate_or_exit(sel.validate_companies, lm["source_company"].astype(str).unique(), what="the load matrix", logger=logger)
        validate_or_exit(sel.validate_tables, list(zip(lm["target_schema"], lm["target_table"])), what="the load matrix", logger=logger)

    # Load Step 01 copybook extracts
    cb_csv = OUT / "vsam_01_copybook_extracts.csv"
    cb_meta = {}  # copybook_name_upper -> [field_dict]
    if cb_csv.exists():
        cb_df = pd.read_csv(cb_csv, dtype=str).fillna("")
        for cb_name, grp in cb_df.groupby("copybook_file"):
            cb_key = cb_name.upper().split(".")[0]
            cb_meta[cb_key] = grp.to_dict(orient="records")

    # Load Step 03 profile (for schema_col and pk_valid info)
    tg_file = OUT / "vsam_03_type_gate.json"
    tg_prof = {}
    if tg_file.exists():
        tg_prof = json.loads(tg_file.read_text(encoding="utf-8"))

    # Prepare output directories (nothing is cleaned: a targeted run must not destroy the DDL of other tables)
    VSAM_GEN_DDL_DIR.mkdir(parents=True, exist_ok=True)
    full = not sel.filtered
    if full:      # legacy folders of older versions
        for obsolete_dir in [VSAM_OUTPUTS_DIR / "pre_load", VSAM_OUTPUTS_DIR / "post_load"]:
            if obsolete_dir.exists():
                try:
                    shutil.rmtree(obsolete_dir)
                    logger.info(f"Removed legacy directory: {obsolete_dir}")
                except OSError as e:
                    logger.debug(f"Could not remove {obsolete_dir}: {e}")

    src_col = "source_filename" if "source_filename" in lm.columns else "source_file"
    targets = lm.drop_duplicates(["target_schema", "target_table"])
    if sel.filtered:
        companies_of = lm.groupby(["target_schema", "target_table"])["source_company"].apply(lambda x: {str(v).upper() for v in x})
        targets = targets[[sel.matches(r.target_schema, r.target_table)
                           and (not sel.companies or bool(companies_of[(r.target_schema, r.target_table)] & sel.companies))
                           for r in targets.itertuples()]]
    if sel.dry_run:
        for r in targets.itertuples():
            logger.info(f"[dry-run] would generate DDL for {r.target_schema}.{r.target_table}")
        logger.info(f"[dry-run] {sel.describe()}: DDL of {len(targets)} table(s); nothing written")
        return True
    per_schema_pre = {}   # schema -> [pre_sql, ...]   (this run's tables)
    per_schema_post = {}  # schema -> [post_sql, ...]
    frag_pre, frag_post = {}, {}       # (schema, table) -> [distinct statements]
    manifest_entries = {}
    report = []

    for r in targets.itertuples():
        schema = r.target_schema
        table = r.target_table

        # Find the source company and source file for this target
        src_rows = lm[(lm["target_schema"] == schema) & (lm["target_table"] == table)]
        if src_rows.empty:
            report.append({
                "schema": schema, "table": table, "status": "SKIPPED",
                "columns_count": 0, "pk_cols": "", "schema_col": "", "detail": "No source rows in load matrix"
            })
            continue

        # Get copybook name
        cb_name_raw = src_rows["copybook_map"].iloc[0] if "copybook_map" in src_rows.columns else ""
        cb_key = cb_name_raw.upper().split(".")[0] if cb_name_raw else ""
        columns_meta = cb_meta.get(cb_key, [])

        # Get key columns from load matrix
        key_cols_raw = src_rows["key_columns"].iloc[0] if "key_columns" in src_rows.columns else ""
        key_cols = [norm_col(c.strip()) for c in re.split(r"[,;+]", key_cols_raw) if c.strip()] if key_cols_raw else []

        # Get schema_col and dynamic type_overrides from Step 03 profile
        co = src_rows["source_company"].iloc[0]
        src = src_rows[src_col].iloc[0]
        tg = tg_prof.get(f"{co}|{src}", {})
        schema_col = tg.get("schema_col")
        type_overrides = tg.get("type_overrides", {})

        cb_display = cb_name_raw if (cb_name_raw and cb_name_raw.upper() != "NO_COPYBOOK") else "NO_COPYBOOK"
        sc_display = schema_col if (schema_col and schema_col.upper() != "NO_KORG") else "NO_KORG"

        # Check for AGILETIC source company
        is_agiletic = "AGILETIC" in str(co).upper()

        # A key is applied ONLY when it comes from the copybook KEY group (Step 02: every key column an ACCEPTED match,
        # already in the load matrix) or from korg_overrides. Nothing is guessed and Step 03's profile is never a source
        # of keys; a source without a trusted key is created (and loaded) without a primary key.
        if not key_cols:
            first_fn = src_rows["s3_filename"].iloc[0] if "s3_filename" in src_rows.columns else ""
            key_cols = get_korg_override_pkey(co, src, first_fn)

        if is_agiletic:
            # For AGILETIC: directly create column names and datatype inferred from CSV in Step 03
            columns_meta = tg.get("columns", [])
        elif columns_meta:
            # Step 02 column map: columns named per --tbl-col, datatype from the mapped copybook column.
            # Sources that share this table contribute their columns too (union, first occurrence wins).
            merged, seen = [], set()
            src_pairs = src_rows[["source_company", src_col]].drop_duplicates().itertuples(index=False)
            for co_i, src_i in src_pairs:
                for c in effective_columns_meta(co_i, src_i, cb_meta.get(cb_key, [])):
                    if c["col_name"] not in seen:
                        seen.add(c["col_name"])
                        merged.append(c)
            columns_meta = merged
        elif not columns_meta:
            # When copybook is missing: retrieve columns inferred from file header in Step 03
            columns_meta = tg.get("columns", [])

        if not columns_meta:
            report.append({
                "schema": schema, "table": table, "status": "NO_COLUMNS",
                "columns_count": 0, "pk_cols": ", ".join(key_cols),
                "copybook": cb_display,
                "schema_col": sc_display,
                "detail": f"No copybook metadata or file header columns found for '{cb_display}'"
            })
            continue

        # Apply dynamic type overrides (regex date types DATE / VARCHAR(10) & widened varchars)
        effective_cols = []
        for c in columns_meta:
            c_eff = dict(c)
            cn = c_eff.get("col_name", "")
            if cn in type_overrides:
                c_eff["sql_type"] = type_overrides[cn]
            effective_cols.append(c_eff)

        pre_sql, post_sql = build_table_ddl(schema, table, effective_cols, key_cols)
        per_schema_pre.setdefault(schema, []).append(pre_sql)
        _add_fragment(frag_pre, schema, table, pre_sql)
        if post_sql:
            per_schema_post.setdefault(schema, []).append(post_sql)
            _add_fragment(frag_post, schema, table, post_sql)

        # Provision table across ALL active schemas and 'bcs' fallback schema
        # (Ensures cross-company active schema records e.g. 110 in FDIC file have destination table ready)
        for act_s in get_all_active_schemas():
            act_tables = per_schema_pre.setdefault(act_s, [])
            act_pre_sql, act_post_sql = build_table_ddl(act_s, table, effective_cols, key_cols)
            if act_pre_sql not in act_tables:
                act_tables.append(act_pre_sql)
                _add_fragment(frag_pre, act_s, table, act_pre_sql)
                if act_post_sql:
                    per_schema_post.setdefault(act_s, []).append(act_post_sql)
                    _add_fragment(frag_post, act_s, table, act_post_sql)

        data_cols = [c for c in effective_cols if c.get("is_filler") != "YES"]
        pk_desc = f"pk={','.join(key_cols)}" if key_cols else "NO_PK"
        status_desc = "DDL_OK" if key_cols else "DDL_OK (NO_PK)"
        report.append({
            "schema": schema, "table": table, "status": status_desc,
            "columns_count": len(data_cols),
            "pk_cols": ", ".join(key_cols) if key_cols else "NONE",
            "copybook": cb_display,
            "schema_col": sc_display,
            "detail": f"{pk_desc} | korg={sc_display} | copybook={cb_display}"
        })

    # Per-table fragments (only this run's tables), then the per-schema scripts assembled from ALL fragments
    for (sch, tbl), stmts in frag_pre.items():
        kinds = ["pre"] if store.write(sch, tbl, "pre", "\n\n".join(stmts) + "\n") else []
        if store.write(sch, tbl, "pk", "\n\n".join(frag_post.get((sch, tbl), [])) + "\n" if frag_post.get((sch, tbl)) else ""):
            kinds.append("pk")
        manifest_entries[f"{sch}.{tbl}"] = {"schema": sch, "table": tbl, "kinds": kinds}
    if full:
        pruned = store.prune(set(frag_pre))
        if pruned:
            logger.info(f"full run: removed {pruned} stale DDL fragment(s)")
    store.record(manifest_entries, full=full)
    touched = sorted({s_ for s_, _ in frag_pre}) if not full else store.schemas()
    for s_ in touched:
        store.assemble(s_, "pre", f"CREATE SCHEMA IF NOT EXISTS {q(s_)};\n\n", VSAM_GEN_DDL_DIR / f"{s_}.sql")
        store.assemble(s_, "pk", "", VSAM_GEN_DDL_DIR / f"{s_}_pk.sql")
    all_pre_sql = [CTL_DDL] + [(VSAM_GEN_DDL_DIR / f"{s_}.sql").read_text(encoding="utf-8") for s_ in store.schemas()
                               if (VSAM_GEN_DDL_DIR / f"{s_}.sql").exists()]
    all_post_sql = [t for t in ((VSAM_GEN_DDL_DIR / f"{s_}_pk.sql").read_text(encoding="utf-8") for s_ in store.schemas()
                                if (VSAM_GEN_DDL_DIR / f"{s_}_pk.sql").exists()) if t.strip()]

    all_pre_file = VSAM_GEN_DDL_DIR / "all_tables.sql"
    write_text_file(all_pre_file, "\n".join(all_pre_sql))
    write_text_file(VSAM_GEN_DDL_DIR / "vsam_04_all.sql", "\n".join(all_pre_sql))

    all_post_file = VSAM_GEN_DDL_DIR / "all_pk.sql"
    if all_post_sql:
        write_text_file(all_post_file, "\n".join(all_post_sql))
        write_text_file(VSAM_GEN_DDL_DIR / "vsam_04_all_pk.sql", "\n".join(all_post_sql))
    else:
        empty_note = "-- No primary keys: tables without a copybook / override key load keyless (snapshot replace).\n"
        write_text_file(all_post_file, empty_note)
        write_text_file(VSAM_GEN_DDL_DIR / "vsam_04_all_pk.sql", empty_note)

    ts = get_timestamp()
    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_04_generate_ddl_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generate_excel_report(report_file, {
        "DDL_Summary": report,
        "GeneratedSchemas": [
            {"schema": s, "table_count": len(t), "has_pk": s in per_schema_post}
            for s, t in per_schema_pre.items()
        ]
    })

    logger.info(f"DDL generated: {len(per_schema_pre)} schemas, {sum(len(v) for v in per_schema_pre.values())} tables.")
    logger.info(f"Tables DDL file : {all_pre_file}")
    logger.info(f"PK DDL file     : {all_post_file}")
    logger.info(f"Excel Report    : {report_file}")

    if args.execute:
        logger.info("Executing pre-load DDL in PostgreSQL...")
        if full:
            run_psql(all_pre_file, is_file=True)
        else:      # a targeted run applies only its own tables
            run_psql(CTL_DDL + "\n" + "\n".join(store.script_for(s_, "pre", sorted(t_ for ss_, t_ in frag_pre if ss_ == s_)) for s_ in touched))
        logger.info("[+] Pre-load DDL executed successfully.")
    else:
        logger.info("Dry run complete. Use --execute or 'sweep.py vsam apply-ddl' to provision tables.")

    log_summary(logger, Path(__file__).name, [
        ('schemas generated', len(per_schema_pre)),
        ('tables generated', sum(len(v) for v in per_schema_pre.values())),
        ('tables with PK constraint', sum(len(v) for v in per_schema_post.values())),
        ('tables skipped', sum(1 for r in report if r['status'] != 'DDL_OK')),
        ('execution mode', 'LIVE EXECUTE' if args.execute else 'DRY RUN'),
    ])


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
