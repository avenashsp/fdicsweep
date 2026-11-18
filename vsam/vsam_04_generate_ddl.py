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

_ROOT = Path(__file__).resolve().parent.parent
_TRACK = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_TRACK) not in sys.path:
    sys.path.insert(0, str(_TRACK))

from sweep_common import (
    VSAM_OUTPUTS_DIR, VSAM_GEN_DDL_DIR, VSAM_REPORTS_DIR,
    get_logger, get_timestamp, get_db_name, generate_excel_report, run_psql
)
from vsam_common import (
    OUT, q, norm_col, load_csv_safe, get_korg_override_pkey,
    get_all_active_schemas
)

logger = get_logger("vsam_04_generate_ddl", "vsam_04_generate_ddl")

AUDIT_COLS = [
    '  "createddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "createdby" character varying(40) DEFAULT CURRENT_USER',
    '  "modifieddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "modifiedby" character varying(40) DEFAULT CURRENT_USER',
    '  "createddate_m" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP',
    '  "createdby_m" character varying(40) NOT NULL DEFAULT CURRENT_USER',
]

CTL_DDL = """CREATE SCHEMA IF NOT EXISTS sweep_ctl;
CREATE TABLE IF NOT EXISTS sweep_ctl.load_log (
    uri             text PRIMARY KEY,
    target_schema   text NOT NULL,
    target_table    text NOT NULL,
    size_bytes      bigint DEFAULT 0,
    loaded_rows     bigint DEFAULT 0,
    seconds         numeric DEFAULT 0,
    file_format     text DEFAULT 'csv',
    worker_id       int DEFAULT 1,
    status          text DEFAULT 'SUCCESS',
    message         text,
    loaded_at       timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sweep_vsam_target ON sweep_ctl.load_log (target_schema, target_table);
"""


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


def main():
    parser = argparse.ArgumentParser(description="Step 04: Generate PostgreSQL DDL for VSAM tables.")
    parser.add_argument("--execute", action="store_true", help="Apply pre-load DDL to PostgreSQL immediately")
    parser.add_argument("--company", help="Comma-separated company filter")
    parser.add_argument("--bg", action="store_true", help="Run in background")
    args = parser.parse_args()

    logger.info("=" * 75)
    logger.info("SWEEP VSAM MIGRATION - STEP 04: TARGET DDL GENERATION")
    logger.info("=" * 75)

    # Load Step 02 load matrix
    lm_file = OUT / "vsam_02_load_matrix.csv" if (OUT / "vsam_02_load_matrix.csv").exists() else OUT / "load_matrix.csv"
    lm = load_csv_safe(lm_file)
    if args.company:
        cos = [c.strip().upper() for c in args.company.split(",")]
        lm = lm[lm["source_company"].isin(cos)]

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

    # Prepare output directories
    VSAM_GEN_DDL_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old SQL files in gen_ddl
    for old in VSAM_GEN_DDL_DIR.glob("*.sql"):
        try:
            old.unlink()
        except Exception:
            pass

    # Remove obsolete pre_load and post_load directories if they exist
    for obsolete_dir in [VSAM_OUTPUTS_DIR / "pre_load", VSAM_OUTPUTS_DIR / "post_load"]:
        if obsolete_dir.exists():
            try:
                shutil.rmtree(obsolete_dir)
                logger.info(f"Removed legacy directory: {obsolete_dir}")
            except Exception as e:
                logger.debug(f"Could not remove {obsolete_dir}: {e}")

    src_col = "source_filename" if "source_filename" in lm.columns else "source_file"
    targets = lm.drop_duplicates(["target_schema", "target_table"])
    per_schema_pre = {}   # schema -> [pre_sql, ...]
    per_schema_post = {}  # schema -> [post_sql, ...]
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

        # Fallback to Step 03 profile or korg_overrides if key_cols was blank
        if is_agiletic:
            # For AGILETIC: key columns from mapped copybook or positional auto-discovery in Step 03
            key_cols = [norm_col(c) for c in tg.get("pk_cols", []) if c] if tg.get("pk_valid") else []
        else:
            if not key_cols and tg.get("pk_cols"):
                key_cols = [norm_col(c) for c in tg["pk_cols"] if c]
            if not key_cols:
                first_fn = src_rows["s3_filename"].iloc[0] if "s3_filename" in src_rows.columns else ""
                key_cols = get_korg_override_pkey(co, src, first_fn)

        if is_agiletic:
            # For AGILETIC: directly create column names and datatype inferred from CSV in Step 03
            columns_meta = tg.get("columns", [])
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
        if post_sql:
            per_schema_post.setdefault(schema, []).append(post_sql)

        # Provision table across ALL active schemas and 'bcs' fallback schema
        # (Ensures cross-company active schema records e.g. 110 in FDIC file have destination table ready)
        for act_s in get_all_active_schemas():
            act_tables = per_schema_pre.setdefault(act_s, [])
            act_pre_sql, act_post_sql = build_table_ddl(act_s, table, effective_cols, key_cols)
            if act_pre_sql not in act_tables:
                act_tables.append(act_pre_sql)
                if act_post_sql:
                    per_schema_post.setdefault(act_s, []).append(act_post_sql)

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

    # Write per-schema table SQL files inside gen_ddl
    all_pre_sql = [CTL_DDL]
    for s, stmts in sorted(per_schema_pre.items()):
        body = f"CREATE SCHEMA IF NOT EXISTS {q(s)};\n\n" + "\n\n".join(stmts) + "\n"
        (VSAM_GEN_DDL_DIR / f"{s}.sql").write_text(body, encoding="utf-8")
        all_pre_sql.append(body)

    all_pre_file = VSAM_GEN_DDL_DIR / "all_tables.sql"
    all_pre_file.write_text("\n".join(all_pre_sql), encoding="utf-8")
    (VSAM_GEN_DDL_DIR / "vsam_04_all.sql").write_text("\n".join(all_pre_sql), encoding="utf-8")

    # Write per-schema PK SQL files inside gen_ddl
    all_post_sql = []
    for s, stmts in sorted(per_schema_post.items()):
        body = "\n\n".join(stmts) + "\n"
        (VSAM_GEN_DDL_DIR / f"{s}_pk.sql").write_text(body, encoding="utf-8")
        all_post_sql.append(body)

    all_post_file = VSAM_GEN_DDL_DIR / "all_pk.sql"
    if all_post_sql:
        all_post_file.write_text("\n".join(all_post_sql), encoding="utf-8")
        (VSAM_GEN_DDL_DIR / "vsam_04_all_pk.sql").write_text("\n".join(all_post_sql), encoding="utf-8")
    else:
        empty_note = "-- Mode DATA / No PKs: Primary key constraints bypassed for raw truncate-and-load ingestion.\n"
        all_post_file.write_text(empty_note, encoding="utf-8")
        (VSAM_GEN_DDL_DIR / "vsam_04_all_pk.sql").write_text(empty_note, encoding="utf-8")

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
        run_psql(all_pre_file, is_file=True)
        logger.info("[+] Pre-load DDL executed successfully.")
    else:
        logger.info("Dry run complete. Use --execute or 'sweep.py vsam apply-ddl' to provision tables.")

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  schemas generated          : {len(per_schema_pre):>10,}",
        f"  tables generated           : {sum(len(v) for v in per_schema_pre.values()):>10,}",
        f"  tables with PK constraint  : {sum(len(v) for v in per_schema_post.values()):>10,}",
        f"  tables skipped             : {sum(1 for r in report if r['status'] != 'DDL_OK'):>10,}",
        f"  execution mode             : {'LIVE EXECUTE' if args.execute else 'DRY RUN':>10}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)


if __name__ == "__main__":
    main()
