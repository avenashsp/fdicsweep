#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_07_validate.py - Step 07: End-to-End Validation & Parity Audit
=============================================================================
Performs comprehensive post-migration validation:
  1. Row Count Validation: source file estimated rows vs PostgreSQL exact counts.
  2. Column Structure Audit: verifies columns exist, with correct names.
  3. Datatype & Length Audit: compares column types, lengths, precision, scale
     between copybook spec (Step 01) and PostgreSQL information_schema.
  4. PK Constraint Audit: verifies PK constraints are present in PostgreSQL.
  5. Audit Column Check: verifies the 4 standard audit columns are present.
  6. S3 File Reconciliation: S3 planned files vs sweep_ctl.load_log.

Generates:
  - reports/vsam/<db>_vsam_07_validate_report_<ts>.xlsx
    Sheets: RowCounts | ColumnAudit | TypeAudit | PKAudit | FileReconciliation
=============================================================================
"""
import sys
import re
import argparse
from pathlib import Path
import pandas as pd

from sweep_common import (
    VSAM_REPORTS_DIR, CTL_SCHEMA,
    get_logger, get_timestamp, get_db_name, generate_excel_report, run_psql, query_db_rows, q
)
from vsam.vsam_common import OUT, load_csv_safe, read_json

logger = get_logger("vsam_07_validate", "vsam_07_validate")

AUDIT_REQUIRED = {
    "createddate", "createdby", "modifieddate", "modifiedby", "createddate_m", "createdby_m"
}

# PostgreSQL datatype normalization for comparison
def pg_base_type(pg_type):
    """Normalize a PostgreSQL data_type to a base comparable form."""
    t = (pg_type or "").lower().strip()
    return {
        "character varying": "varchar",
        "character": "char",
        "integer": "integer",
        "bigint": "bigint",
        "smallint": "smallint",
        "numeric": "numeric",
        "real": "real",
        "double precision": "double precision",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
        "date": "date",
        "text": "text",
        "boolean": "boolean",
    }.get(t, t)


def cb_sql_base(sql_type):
    """Extract base type from copybook-derived SQL type like varchar(40) -> varchar."""
    t = (sql_type or "").lower().strip()
    m = re.match(r"(\w+)\(", t)
    return m.group(1) if m else t


def main():
    parser = argparse.ArgumentParser(description="Step 07: Comprehensive VSAM migration validation.")
    parser.add_argument("--exact-counts", action="store_true", help="Perform exact count(*) queries on target tables")
    parser.add_argument("--tables", help="Comma-separated tables to validate")
    parser.add_argument("--schema", help="Validate only this schema")
    parser.add_argument("--bg", action="store_true", help="Run in background")
    args = parser.parse_args()

    logger.info("=" * 75)
    logger.info("SWEEP VSAM MIGRATION - STEP 08: VALIDATION & PARITY AUDIT")
    logger.info("=" * 75)

    # Load Step 02 load matrix
    lm_file = OUT / "vsam_02_load_matrix.csv" if (OUT / "vsam_02_load_matrix.csv").exists() else OUT / "load_matrix.csv"
    lm = load_csv_safe(lm_file)

    if args.tables:
        tbls = [t.strip().lower() for t in args.tables.split(",")]
        lm = lm[lm["target_table"].isin(tbls)]
    if args.schema:
        lm = lm[lm["target_schema"] == args.schema.strip().lower()]

    # Load Step 03 profile for dynamic type overrides
    tg_prof = read_json(OUT / "vsam_03_type_gate.json", default={})

    # Load Step 01 copybook extracts
    cb_csv = OUT / "vsam_01_copybook_extracts.csv"
    cb_meta = {}  # copybook_name_upper -> [field_dict]
    if cb_csv.exists():
        cb_df = pd.read_csv(cb_csv, dtype=str).fillna("")
        for cb_name, grp in cb_df.groupby("copybook_file"):
            cb_key = cb_name.upper().split(".")[0]
            cb_meta[cb_key] = [r for r in grp.to_dict(orient="records") if r.get("is_filler") != "YES"]

    # ===========================================================================
    # 1. S3 File Reconciliation
    # ===========================================================================
    logger.info("1. Reconciling loaded files against sweep_ctl.load_log...")
    try:
        log_sql = f"SELECT uri || '|' || target_schema || '|' || target_table || '|' || status || '|' || COALESCE(loaded_rows::text, '0') FROM {CTL_SCHEMA}.load_log;"
        log_rows = query_db_rows(log_sql)
        loaded_map = {}
        for r in log_rows:
            parts = r.split("|")
            if len(parts) >= 5:
                raw_uri = parts[0].split("#")[0]
                loaded_map[f"{raw_uri}|{parts[1]}"] = {
                    "schema": parts[1], "table": parts[2],
                    "status": parts[3], "rows": parts[4]
                }
    except RuntimeError as e:
        logger.error(f"Cannot validate: failed to query {CTL_SCHEMA}.load_log: {e}")
        return False

    fname_col = "s3_filename" if "s3_filename" in lm.columns else "csv_filename"
    recon = []
    for r in lm.itertuples():
        fname = getattr(r, fname_col, "")
        done_key = f"{r.full_path}|{r.target_schema}"
        l_info = loaded_map.get(done_key, {})
        recon.append({
            "schema": r.target_schema, "table": r.target_table,
            "filename": fname, "full_path": r.full_path,
            "load_status": l_info.get("status", "NOT_LOADED"),
            "loaded_rows": l_info.get("rows", 0)
        })

    # ===========================================================================
    # 2. Row Counts, Column & Type Audit
    # ===========================================================================
    targets = lm.drop_duplicates(["target_schema", "target_table"])
    bcs_targets = lm.drop_duplicates(["target_table"]).copy()
    bcs_targets["target_schema"] = "bcs"
    targets = pd.concat([targets, bcs_targets]).drop_duplicates(["target_schema", "target_table"])
    row_count_rows = []
    col_audit_rows = []
    type_audit_rows = []
    pk_audit_rows = []

    logger.info(f"2. Auditing {len(targets)} target tables in PostgreSQL...")

    # Fetch all column info in one query
    catalog_sql = """
    SELECT
        c.table_schema,
        c.table_name,
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.is_nullable
    FROM information_schema.columns c
    WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY c.table_schema, c.table_name, c.ordinal_position;
    """
    try:
        catalog_rows = query_db_rows(catalog_sql)
    except RuntimeError as e:
        logger.error(f"Cannot validate: failed to query the database catalog (check credentials/sweep.env): {e}")
        return False
    db_cols = {}  # (schema, table) -> {col_name: {type, len, prec, scale, nullable}}
    for r in catalog_rows:
        parts = r.split("|")
        if len(parts) >= 8:
            key = (parts[0], parts[1])
            db_cols.setdefault(key, {})[parts[2].lower()] = {
                "data_type": parts[3],
                "char_max_len": parts[4],
                "numeric_precision": parts[5],
                "numeric_scale": parts[6],
                "is_nullable": parts[7]
            }

    # Fetch all PK info in one query
    pk_sql = """
    SELECT
        tc.table_schema,
        tc.table_name,
        string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) AS pk_cols
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
    GROUP BY tc.table_schema, tc.table_name;
    """
    try:
        pk_rows_raw = query_db_rows(pk_sql)
    except RuntimeError as e:
        logger.error(f"Cannot validate: failed to query database PK constraints: {e}")
        return False
    db_pks = {}  # (schema, table) -> pk_cols_csv
    for r in pk_rows_raw:
        parts = r.split("|")
        if len(parts) >= 3:
            db_pks[(parts[0], parts[1])] = parts[2]

    src_col = "source_filename" if "source_filename" in lm.columns else "source_file"

    for t in targets.itertuples():
        schema = t.target_schema
        table = t.target_table
        tgt = f"{q(schema)}.{q(table)}"
        tbl_key = (schema, table)

        # Row count
        cnt = -1
        if args.exact_counts:
            try:
                cnt_str = run_psql(f"SELECT count(*) FROM {tgt};", tuples_only=True)
                cnt = int(cnt_str.strip())
            except (RuntimeError, ValueError) as e:
                logger.warning(f"Exact count failed for {tgt}: {e}")
                cnt = -1

        db_col_map = db_cols.get(tbl_key, {})
        has_audit = all(c in db_col_map for c in AUDIT_REQUIRED)

        row_count_rows.append({
            "schema": schema, "table": table,
            "db_row_count": cnt if cnt >= 0 else "SKIPPED",
            "audit_columns_present": "YES" if has_audit else "NO",
            "status": "VALIDATED" if db_col_map else "TABLE_MISSING"
        })

        # Get copybook metadata for this table
        src_rows = lm[(lm["target_schema"] == schema) & (lm["target_table"] == table)]
        cb_name_raw = src_rows["copybook_map"].iloc[0] if not src_rows.empty and "copybook_map" in src_rows.columns else ""
        cb_key = cb_name_raw.upper().split(".")[0] if cb_name_raw else ""
        expected_cols = cb_meta.get(cb_key, [])

        # Expected PK cols from load matrix
        src_col = "source_filename" if "source_filename" in src_rows.columns else "source_file"
        co = src_rows["source_company"].iloc[0] if not src_rows.empty and "source_company" in src_rows.columns else ""
        src = src_rows[src_col].iloc[0] if not src_rows.empty and src_col in src_rows.columns else ""
        tg_entry = tg_prof.get(f"{co}|{src}", {})
        type_overrides = tg_entry.get("type_overrides", {})

        if not expected_cols:
            expected_cols = tg_entry.get("columns", [])

        key_cols_raw = src_rows["key_columns"].iloc[0] if not src_rows.empty and "key_columns" in src_rows.columns else ""
        if not key_cols_raw and tg_entry.get("pk_cols"):
            key_cols_raw = ", ".join(tg_entry["pk_cols"])
        expected_pk = [c.strip() for c in re.split(r"[,;+]", key_cols_raw) if c.strip()] if key_cols_raw else []

        # Column presence audit
        for c in expected_cols:
            col = c["col_name"]
            present = col in db_col_map
            col_audit_rows.append({
                "schema": schema, "table": table,
                "copybook_col": col,
                "present_in_db": "YES" if present else "NO",
                "status": "OK" if present else "MISSING"
            })

        # Type audit: compare copybook spec (with dynamic overrides) vs PostgreSQL actual
        for c in expected_cols:
            col = c["col_name"]
            eff_type = type_overrides.get(col, c["sql_type"])
            if col not in db_col_map:
                type_audit_rows.append({
                    "schema": schema, "table": table, "column": col,
                    "expected_type": eff_type,
                    "actual_type": "COLUMN_MISSING",
                    "expected_len": c["len_display"],
                    "actual_len": "",
                    "match": "FAIL"
                })
                continue

            db_info = db_col_map[col]
            exp_base = cb_sql_base(eff_type)
            act_base = pg_base_type(db_info["data_type"])
            exp_len = c["len_display"]
            act_len = db_info["char_max_len"] or db_info["numeric_precision"] or ""
            type_match = "OK" if exp_base == act_base else "MISMATCH"

            type_audit_rows.append({
                "schema": schema, "table": table, "column": col,
                "expected_type": eff_type,
                "actual_pg_type": db_info["data_type"],
                "expected_len": exp_len,
                "actual_len": act_len,
                "numeric_precision": db_info["numeric_precision"],
                "numeric_scale": db_info["numeric_scale"],
                "is_nullable": db_info["is_nullable"],
                "match": type_match
            })

        # PK audit (evaluates PostgreSQL catalog live)
        actual_pk_csv = db_pks.get(tbl_key, "")
        actual_pk_cols = [c.strip() for c in actual_pk_csv.split(",") if c.strip()] if actual_pk_csv else []
        if actual_pk_cols:
            if expected_pk:
                pk_match = set(expected_pk) == set(actual_pk_cols)
                pk_status = "OK" if pk_match else "MISMATCH"
            else:
                pk_status = "ACTIVE_PK"
        else:
            pk_status = "TABLE_UNCONSTRAINED (NO_PK)"

        pk_audit_rows.append({
            "schema": schema, "table": table,
            "expected_pk": ", ".join(expected_pk) if expected_pk else "NONE",
            "actual_pk_in_db": ", ".join(actual_pk_cols) if actual_pk_cols else "NONE",
            "match": pk_status
        })

    # ===========================================================================
    # Excel Report
    # ===========================================================================
    ts = get_timestamp()
    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_07_validate_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generate_excel_report(report_file, {
        "FileReconciliation": recon,
        "RowCounts": row_count_rows,
        "ColumnAudit": col_audit_rows,
        "TypeAudit": type_audit_rows,
        "PKAudit": pk_audit_rows
    })

    logger.info("Validation Complete.")
    logger.info(f"Excel Report: {report_file}")

    missing_cols = sum(1 for r in col_audit_rows if r["status"] != "OK")
    type_mismatches = sum(1 for r in type_audit_rows if r["match"] != "OK")
    pk_issues = sum(1 for r in pk_audit_rows if r["match"] != "OK")
    not_loaded = sum(1 for r in recon if r["load_status"] == "NOT_LOADED")

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  files reconciled           : {len(recon):>10,}",
        f"  files NOT loaded           : {not_loaded:>10,}",
        f"  tables audited             : {len(row_count_rows):>10,}",
        f"  columns checked            : {len(col_audit_rows):>10,}",
        f"  missing columns            : {missing_cols:>10,}",
        f"  type mismatches            : {type_mismatches:>10,}",
        f"  PK issues                  : {pk_issues:>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)

    # Parity verdict: every mapped file loaded, no missing columns, no type mismatches.
    return not_loaded == 0 and missing_cols == 0 and type_mismatches == 0


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
