#!/usr/bin/env python3
"""
05_applyDDL.py - Target DDL Deployment & Partition Verification
Applies sweep_meta.sql control schemas and executes outputs/pre_load/*_tables.sql
to provision clean bare tables and child partitions in AWS RDS PostgreSQL.

Generates: reports/05_applyDDL_report_<timestamp>.xlsx
"""
import sys
import argparse
from pathlib import Path
from sweep_common import (
    log_summary,
    BASE_DIR,
    SWEEP_META_SQL,
    ORACLE_PRE_LOAD_DIR,
    ORACLE_REPORTS_DIR,
    clip_error,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    run_psql,
    query_db_dicts,
    PGHOST,
    PGPORT,
    PGDATABASE,
    PGUSER,
    has_db_auth,
    AUTH_HELP,
    DB_ERRORS,
)

logger = get_logger("oracle_05_apply_ddl", "oracle_05_apply_ddl")


def deploy_ddl(schemas_filter=None, skip_meta=False):
    ts = get_timestamp()
    if not has_db_auth():
        logger.error(AUTH_HELP)
        return False
    meta_sql_file = SWEEP_META_SQL if SWEEP_META_SQL.exists() else (BASE_DIR / "sweep_meta.sql")

    logger.info(f"Target Database: {PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE}")

    # 1. Apply Migration Control Catalog
    if not skip_meta and meta_sql_file.exists():
        logger.info("Applying migration control schemas (sweep_ctl, sweep_meta)...")
        try:
            run_psql(meta_sql_file, is_file=True)
            logger.info("Control schemas applied successfully.")
        except RuntimeError as e:
            logger.error(f"Failed to apply sweep_meta.sql: {e}")
            return False

    # 2. Find and apply table DDL scripts
    ddl_files = list(ORACLE_PRE_LOAD_DIR.glob("*_tables.sql"))
    if not ddl_files:
        logger.error(f"No table DDL scripts found in {ORACLE_PRE_LOAD_DIR}. Run 04_generateDDL.py first.")
        return False

    deployed_files = []
    for ddl_f in ddl_files:
        schema_name = ddl_f.stem.replace("_tables", "")
        if schemas_filter and schema_name.lower() not in schemas_filter:
            continue

        logger.info(f"Applying target DDL: {ddl_f.name}...")
        try:
            run_psql(ddl_f, is_file=True)
            deployed_files.append({"schema": schema_name, "file": ddl_f.name, "status": "SUCCESS"})
        except RuntimeError as e:
            logger.error(f"Failed to execute {ddl_f.name}: {e}")
            deployed_files.append({"schema": schema_name, "file": ddl_f.name, "status": clip_error(f"FAILED: {e}")})

    # 3. Verify deployed tables and partitions via PostgreSQL Catalog
    logger.info("Verifying tables and declarative partitions in PostgreSQL...")
    verified_tables = []
    try:
        sql_tables = ("SELECT table_schema, table_name FROM information_schema.tables "
                      "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta') "
                      "ORDER BY table_schema, table_name")
        tbl_rows = query_db_dicts(sql_tables)

        # Query partition inheritance counts
        sql_parts = ("SELECT n.nspname AS schema_name, p.relname AS parent_table, count(*) AS partition_count "
                     "FROM pg_inherits i "
                     "JOIN pg_class p ON p.oid = i.inhparent "
                     "JOIN pg_class c ON c.oid = i.inhrelid "
                     "JOIN pg_namespace n ON n.oid = p.relnamespace "
                     "GROUP BY n.nspname, p.relname "
                     "ORDER BY 1, 2")
        part_counts = {}
        for r in query_db_dicts(sql_parts):
            part_counts[(r["schema_name"].lower(), r["parent_table"].lower())] = int(r["partition_count"])

        for t in tbl_rows:
            s = t["table_schema"].lower()
            tb = t["table_name"].lower()
            p_cnt = part_counts.get((s, tb), 0)
            verified_tables.append({
                "schema_name": s,
                "table_name": tb,
                "is_partitioned": p_cnt > 0,
                "child_partitions": p_cnt
            })
    except DB_ERRORS as e:
        logger.warning(f"Could not verify catalog tables: {e}")

    # Generate Excel Report
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_05_apply_ddl_report_{ts}.xlsx"
    total_parts = sum(r["child_partitions"] for r in verified_tables)

    summary_sheet = [
        {"Metric": "Deployment Timestamp", "Value": ts},
        {"Metric": "Target PostgreSQL Host", "Value": f"{PGHOST}:{PGPORT}"},
        {"Metric": "Target Database", "Value": PGDATABASE},
        {"Metric": "DDL Files Executed", "Value": len(deployed_files)},
        {"Metric": "Total Tables Verified", "Value": len(verified_tables)},
        {"Metric": "Total Declarative Partitions", "Value": total_parts},
        {"Metric": "Table State", "Value": "Bare tables provisioned; ready for streaming bulk load"}
    ]

    sheets = {
        "Summary": summary_sheet,
        "Executed_Scripts": deployed_files,
        "Catalog_Verification": verified_tables
    }
    generate_excel_report(report_file, sheets)
    logger.info(f"Excel report generated: {report_file}")
    
    log_summary(logger, Path(__file__).name, [
        ('ddl scripts executed', len(deployed_files)),
        ('tables deployed in postgres', len(verified_tables)),
        ('declarative partitions', total_parts),
        ('target database', PGDATABASE),
    ])
        
    failed = [d for d in deployed_files if d["status"] != "SUCCESS"]
    if failed:
        logger.error(f"{len(failed)} DDL script(s) failed: {', '.join(d['file'] for d in failed)}")
    return not failed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="05_applyDDL.py - Apply Target DDL to PostgreSQL")
    parser.add_argument("--schemas", help="Comma-separated schemas to apply (e.g. bss01001,bss01104)")
    parser.add_argument("--skip-meta", action="store_true", help="Skip re-applying sweep_meta.sql")
    args = parser.parse_args()

    schemas = {s.strip().lower() for s in args.schemas.split(",")} if args.schemas else None
    ok = deploy_ddl(schemas_filter=schemas, skip_meta=args.skip_meta)
    sys.exit(0 if ok else 1)
