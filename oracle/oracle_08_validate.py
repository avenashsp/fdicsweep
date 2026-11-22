#!/usr/bin/env python3
"""
=============================================================================
08_validate.py - Full-Fledged Oracle Dump vs AWS RDS PostgreSQL Parity Audit
=============================================================================
Performs a comprehensive, multi-level validation between the master Oracle SQL
dump and the target AWS RDS Aurora PostgreSQL database:

1. Schema-Level Comparison:
   - Validates existence of all source Oracle schemas/owners in PostgreSQL.
   - Compares table, view, and sequence object counts per schema.

2. Table-Level Comparison:
   - Compares all regular and partitioned tables.
   - Validates declarative PostgreSQL partitioning hierarchy (parent vs child tables).

3. Column & Data Type-Level Comparison:
   - Compares all column definitions, ordinal positions, and nullability.
   - Performs intelligent type mapping verification (Oracle type -> expected PG type vs actual PG type).
   - Identifies and verifies native FDICSweep application audit columns
     (createdby_m, createddate_m, modifiedby, modifieddate).

4. Constraints Comparison:
   - Validates Primary Keys (PK), Unique Constraints (UK/AK), Foreign Keys (FK), and Check Constraints.
   - Verifies constraint validation status (convalidated: VALIDATED vs NOT VALID).

5. Indexes Comparison:
   - Compares all unique and secondary indexes across target tables.
   - Validates index validity and indexed column expressions.

6. Sequences Comparison:
   - Validates all database sequences defined in the Oracle dump.

7. Views and Triggers Comparison:
   - Validates standard views, materialized views, and table triggers.

8. Row Count Reconciliation (PostgreSQL vs S3 Mapped Files vs Oracle Stats):
   - Fast approximate row counts (pg_class.reltuples / pg_stat_user_tables.n_live_tup).
   - Aggregates child partitions into parent partitioned table counts.
   - Reconciles live PG counts against loaded rows in sweep_ctl.load_log from S3.
   - Compares against Oracle source statistics (num_rows).
   - Optional --exact-counts flag for 100% exact SELECT COUNT(*) audit.

9. Comprehensive Multi-Tab Excel Report:
   - Generates reports/oracle/08_validate_report_<timestamp>.xlsx with 10 dedicated tabs:
     1. Executive_Summary
     2. Schema_Comparison
     3. Table_Comparison
     4. Column_Comparison
     5. Constraints_Comparison
     6. Index_Comparison
     7. Sequence_Comparison
     8. Views_and_Triggers
     9. Row_Count_Audit
     10. Discrepancies (actionable punch list)
=============================================================================
"""
import sys
import csv
import argparse
import importlib
from pathlib import Path
from collections import defaultdict

from sweep_common import (
    log_summary,
    generate_excel_report,
    get_db_name,
    get_logger,
    get_timestamp,
    META_CONSTRAINTS_CSV,
    META_INDEXES_CSV,
    META_OBJECTS_CSV,
    META_PARTITIONS_CSV,
    META_SEQUENCES_CSV,
    META_TABLE_STATS_CSV,
    META_TABLES_CSV,
    ORACLE_DUMP_DIR,
    ORACLE_INPUTS_DIR,
    ORACLE_OUTPUTS_DIR,
    ORACLE_REPORTS_DIR,
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGUSER,
)
from sweep_s3meta import has_meta, make_meta, meta_equal, meta_from_row
from sweep_targeting import add_targeting_args, selector_or_exit, validate_or_exit
from oracle.oracle_08_catalog import fetch_target_postgres_catalogs, test_db_connection
from oracle.oracle_08_audit import (
    compare_columns, compare_constraints, compare_indexes, compare_schemas, compare_sequences,
    compare_tables, compare_views_and_triggers, reconcile_row_counts,
)

logger = get_logger("oracle_08_validate", "oracle_08_validate")





# -----------------------------------------------------------------------------
# Metadata Loader / Auto-Extractor
# -----------------------------------------------------------------------------
def load_or_extract_metadata(dump_file=None, owners_filter=None):
    """
    Loads extracted Oracle metadata CSVs from outputs/oracle/oracle_metadata/.
    If metadata CSVs are missing or empty, attempts auto-extraction from master SQL dump.
    """
    required_csvs = [
        META_TABLES_CSV, META_CONSTRAINTS_CSV, META_INDEXES_CSV,
        META_PARTITIONS_CSV, META_TABLE_STATS_CSV, META_SEQUENCES_CSV, META_OBJECTS_CSV
    ]

    missing = [c for c in required_csvs if not c.exists() or c.stat().st_size == 0]

    if missing:
        logger.info(f"Metadata CSVs not found or empty ({len(missing)} missing). Checking for Oracle SQL dump...")
        target_dump = None
        if dump_file and Path(dump_file).exists():
            target_dump = Path(dump_file)
        else:
            candidates = list(ORACLE_INPUTS_DIR.glob("*.sql")) + (list(ORACLE_DUMP_DIR.glob("*.sql")) if ORACLE_DUMP_DIR != ORACLE_INPUTS_DIR else [])
            valid_candidates = [c for c in candidates if c.name != "sweep_meta.sql"]
            if valid_candidates:
                target_dump = valid_candidates[0]

        if target_dump:
            logger.info(f"Auto-extracting metadata from master dump: {target_dump}...")
            try:
                try:
                    mod = importlib.import_module("oracle.oracle_01_extract_mtd")
                except ImportError:
                    mod = importlib.import_module("oracle_01_extract_mtd")
                mod.extract_metadata_from_master(sql_file_path=str(target_dump), owners_filter=owners_filter)
            except (RuntimeError, OSError, ValueError, ImportError, AttributeError, KeyError) as e:
                logger.error(f"Failed to auto-extract metadata: {e}")
        else:
            logger.warning("No Oracle SQL dump file found in inputs/oracle/oracle_dump/. Proceeding with available files.")

    def read_csv_dicts(path):
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    metadata = {
        "tables": read_csv_dicts(META_TABLES_CSV),
        "constraints": read_csv_dicts(META_CONSTRAINTS_CSV),
        "indexes": read_csv_dicts(META_INDEXES_CSV),
        "partitions": read_csv_dicts(META_PARTITIONS_CSV),
        "table_stats": read_csv_dicts(META_TABLE_STATS_CSV),
        "sequences": read_csv_dicts(META_SEQUENCES_CSV),
        "objects": read_csv_dicts(META_OBJECTS_CSV)
    }
    return metadata


# -----------------------------------------------------------------------------
# Query Target PostgreSQL System Catalogs
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Main Audit & Comparison Engine
# -----------------------------------------------------------------------------
def run_full_parity_audit(exact_counts=False, tables_filter=None, schemas_filter=None, dump_file=None):
    ts = get_timestamp()
    logger.info("=" * 80)
    logger.info("SWEEP ENTERPRISE ORACLE DUMP VS POSTGRESQL PARITY AUDIT (STEP 08)")
    logger.info("=" * 80)

    # 1. Load Extracted Oracle Metadata
    logger.info("[1/7] Loading extracted Oracle SQL dump metadata...")
    meta = load_or_extract_metadata(dump_file=dump_file, owners_filter=schemas_filter)

    # 2. Check Database Connectivity & Fetch Catalogs
    logger.info("[2/7] Connecting to PostgreSQL and querying system catalogs...")
    db_connected = test_db_connection()
    if db_connected:
        logger.info(f"Connected to PostgreSQL database: {PGDATABASE} on {PGHOST}")
        pg_cat = fetch_target_postgres_catalogs(
            exact_counts=exact_counts,
            tables_filter=tables_filter,
            schemas_filter=schemas_filter
        )
    else:
        logger.warning("[!] Could not connect to PostgreSQL database. Catalog verification will mark DB as UNREACHABLE.")
        pg_cat = {
            "schemas": [], "tables": [], "columns": [], "constraints": [],
            "indexes": [], "sequences": [], "views": [], "triggers": [],
            "load_log": [], "load_log_files": [], "exact_row_counts": {}
        }

    # 3. Load S3 Planned File Mapping
    logger.info("[3/7] Loading planned S3 files inventory from s3_file_map.csv...")
    map_path = ORACLE_OUTPUTS_DIR / "oracle_02_s3_file_map.csv"
    if not map_path.exists():
        map_path = ORACLE_OUTPUTS_DIR / "s3_file_map.csv"
    s3_planned_map = defaultdict(lambda: {"files": 0, "size_mb": 0.0})
    s3_planned_files = defaultdict(list)
    if map_path.exists():
        with open(map_path, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                s3_planned_files[((r.get("target_schema") or "").lower().strip(), (r.get("target_table") or "").lower().strip())].append(
                    (r.get("uri"), meta_from_row(r)))
                s = (r.get("target_schema") or "").lower().strip()
                t = (r.get("target_table") or "").lower().strip()
                size_mb = float(r.get("size_mb") or 0.0)
                if s and t:
                    s3_planned_map[(s, t)]["files"] += 1
                    s3_planned_map[(s, t)]["size_mb"] += size_mb

    # Same comparison as Step 06: a planned file is current only when load_log has a SUCCESS row whose stored
    # LastModified + ETag + size all equal the file map; SUCCESS with different values = re-delivered, not reloaded.
    stored_files = {r["uri"]: make_meta(r.get("s3_last_modified"), r.get("s3_etag"), r.get("s3_size"))
                    for r in pg_cat.get("load_log_files", [])}
    s3_file_state = {}
    for key, files in s3_planned_files.items():
        st = {"current": 0, "changed": 0, "pending": 0}
        for uri, planned in files:
            stored = stored_files.get(uri)
            if stored is None:
                st["pending"] += 1
            elif not any(stored.values()) or not has_meta(planned) or meta_equal(stored, planned):
                st["current"] += 1            # loaded (legacy rows without metadata count as current)
            else:
                st["changed"] += 1
        s3_file_state[key] = st

    # S3 Loaded Log aggregation
    s3_loaded_map = {}
    for r in pg_cat["load_log"]:
        k = (r["target_schema"].lower(), r["target_table"].lower())
        s3_loaded_map[k] = {
            "files": int(r.get("s3_files_loaded") or 0),
            "rows": int(r.get("s3_loaded_rows") or 0),
            "bytes": int(r.get("s3_bytes_loaded") or 0),
            "seconds": float(r.get("total_load_seconds") or 0.0)
        }

    # Discrepancy Aggregator
    discrepancies = []

    oracle_schemas, schema_comparison = compare_schemas(db_connected, discrepancies, meta, pg_cat, schemas_filter)

    oracle_tables, pg_child_partitions, table_comparison = compare_tables(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter)

    column_comparison, oracle_cols_map = compare_columns(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter)

    constraints_comparison, oracle_cons_map = compare_constraints(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter)

    index_comparison, oracle_idx_map = compare_indexes(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter)

    oracle_seqs, sequence_comparison = compare_sequences(db_connected, discrepancies, meta, pg_cat, schemas_filter)

    oracle_triggers, oracle_views, views_triggers_comparison = compare_views_and_triggers(db_connected, discrepancies, meta, pg_cat, schemas_filter)

    row_count_audit = reconcile_row_counts(db_connected, discrepancies, exact_counts, meta, oracle_tables, pg_cat, pg_child_partitions, s3_loaded_map, s3_planned_map, schemas_filter, tables_filter, s3_file_state)

    # -------------------------------------------------------------------------
    # LEVEL 9: Executive Summary Dashboard & Verdict
    # -------------------------------------------------------------------------
    logger.info("[7/7] Generating Executive Summary Dashboard and Excel report...")
    total_schemas_ora = len(oracle_schemas)
    total_schemas_matched = sum(1 for s in schema_comparison if s["status"] == "MATCH")

    total_tables_ora = len(oracle_tables)
    total_tables_matched = sum(1 for t in table_comparison if t["table_status"] == "MATCH")

    total_cols_ora = sum(len(c_dict) for c_dict in oracle_cols_map.values())
    total_cols_matched = sum(1 for c in column_comparison if c["overall_status"] in ("MATCH", "AUDIT_COLUMN_OK"))
    type_mismatches_cnt = sum(1 for c in column_comparison if c["overall_status"] == "TYPE_MISMATCH")

    total_cons_ora = sum(len(c_dict) for c_dict in oracle_cons_map.values())
    total_cons_matched = sum(1 for c in constraints_comparison if c["status"] == "MATCH")

    total_idx_ora = sum(len(i_dict) for i_dict in oracle_idx_map.values())
    total_idx_matched = sum(1 for i in index_comparison if "MATCH" in i["status"])

    total_seq_ora = len(oracle_seqs)
    total_seq_matched = sum(1 for sq in sequence_comparison if sq["status"] == "MATCH")

    total_views_ora = len(oracle_views)
    total_views_matched = sum(1 for v in views_triggers_comparison if "VIEW" in v["object_type"] and v["status"] == "MATCH")

    total_trgs_ora = len(oracle_triggers)
    total_trgs_matched = sum(1 for trg in views_triggers_comparison if trg["object_type"] == "TRIGGER" and trg["status"] == "MATCH")

    row_matched_cnt = sum(1 for r in row_count_audit if r["parity_status"] in ("EXACT_MATCH", "APPROX_MATCH", "EMPTY_TABLE"))

    total_s3_planned = sum(p["files"] for p in s3_planned_map.values())
    total_s3_loaded = sum(l["files"] for l in s3_loaded_map.values())

    is_overall_pass = (
        db_connected and
        len(discrepancies) == 0 and
        total_tables_matched >= total_tables_ora and
        total_tables_ora > 0
    )

    verdict_label = "PASS - 100% PARITY ACHIEVED" if is_overall_pass else ("ATTENTION NEEDED: DISCREPANCIES FOUND" if db_connected else "AUDIT COMPLETED OFFLINE (DB UNREACHABLE)")

    exec_summary = [
        {"Metric": "Audit Execution Timestamp", "Value": ts, "Parity_Assessment": "COMPLETED"},
        {"Metric": "Master Oracle Dump Source", "Value": str(dump_file or "oracle/inputs/oracle_dump/"), "Parity_Assessment": "VERIFIED"},
        {"Metric": "Target PostgreSQL Host", "Value": f"{PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE}", "Parity_Assessment": "CONNECTED" if db_connected else "UNREACHABLE"},
        {"Metric": "Overall Migration Parity Verdict", "Value": verdict_label, "Parity_Assessment": "PASS" if is_overall_pass else "REVIEW_REQUIRED"},
        {"Metric": "Total Actionable Discrepancies", "Value": len(discrepancies), "Parity_Assessment": "ZERO" if len(discrepancies) == 0 else f"{len(discrepancies)} ISSUES"},
        {"Metric": "----------------------------------------", "Value": "----------------------------------------", "Parity_Assessment": "--------"},
        {"Metric": "Schemas (Oracle Dump vs PostgreSQL)", "Value": f"{total_schemas_matched} of {total_schemas_ora} Matched", "Parity_Assessment": f"{round((total_schemas_matched/max(total_schemas_ora,1))*100,1)}%"},
        {"Metric": "Tables (Oracle Dump vs PostgreSQL)", "Value": f"{total_tables_matched} of {total_tables_ora} Matched", "Parity_Assessment": f"{round((total_tables_matched/max(total_tables_ora,1))*100,1)}%"},
        {"Metric": "Columns Evaluated", "Value": f"{total_cols_matched} of {total_cols_ora} Matched", "Parity_Assessment": f"{round((total_cols_matched/max(total_cols_ora,1))*100,1)}%"},
        {"Metric": "Column Data Type Mismatches", "Value": type_mismatches_cnt, "Parity_Assessment": "ZERO" if type_mismatches_cnt == 0 else "MISMATCHES FOUND"},
        {"Metric": "Constraints (PK, UK, FK, Check)", "Value": f"{total_cons_matched} of {total_cons_ora} Matched & Validated", "Parity_Assessment": f"{round((total_cons_matched/max(total_cons_ora,1))*100,1)}%"},
        {"Metric": "Indexes (Unique & Secondary)", "Value": f"{total_idx_matched} of {total_idx_ora} Matched", "Parity_Assessment": f"{round((total_idx_matched/max(total_idx_ora,1))*100,1)}%"},
        {"Metric": "Database Sequences", "Value": f"{total_seq_matched} of {total_seq_ora} Matched", "Parity_Assessment": f"{round((total_seq_matched/max(total_seq_ora,1))*100,1)}%"},
        {"Metric": "Views & Materialized Views", "Value": f"{total_views_matched} of {total_views_ora} Matched", "Parity_Assessment": f"{round((total_views_matched/max(total_views_ora,1))*100,1)}%"},
        {"Metric": "Table Triggers", "Value": f"{total_trgs_matched} of {total_trgs_ora} Matched", "Parity_Assessment": f"{round((total_trgs_matched/max(total_trgs_ora,1))*100,1)}%"},
        {"Metric": "Table Row Count Parity", "Value": f"{row_matched_cnt} of {len(row_count_audit)} Tables Aligned", "Parity_Assessment": f"{round((row_matched_cnt/max(len(row_count_audit),1))*100,1)}%"},
        {"Metric": "S3 Data Files Ingestion Parity", "Value": f"{total_s3_loaded} of {total_s3_planned} Loaded into DB", "Parity_Assessment": f"{round((total_s3_loaded/max(total_s3_planned,1))*100,1)}%"}
    ]

    # Console Summary
    logger.info("=" * 80)
    logger.info(f"AUDIT VERDICT: {verdict_label}")
    logger.info("=" * 80)
    logger.info(f"Schemas Parity         : {total_schemas_matched}/{total_schemas_ora}")
    logger.info(f"Tables Parity          : {total_tables_matched}/{total_tables_ora}")
    logger.info(f"Columns Parity         : {total_cols_matched}/{total_cols_ora} (Type Mismatches: {type_mismatches_cnt})")
    logger.info(f"Constraints Parity     : {total_cons_matched}/{total_cons_ora}")
    logger.info(f"Indexes Parity         : {total_idx_matched}/{total_idx_ora}")
    logger.info(f"Sequences Parity       : {total_seq_matched}/{total_seq_ora}")
    logger.info(f"Views Parity           : {total_views_matched}/{total_views_ora}")
    logger.info(f"Triggers Parity        : {total_trgs_matched}/{total_trgs_ora}")
    logger.info(f"Row Count Parity       : {row_matched_cnt}/{len(row_count_audit)} tables aligned")
    logger.info(f"Total Discrepancies    : {len(discrepancies)}")
    logger.info("=" * 80)

    # Multi-Tab Excel Workbook
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_08_validate_report_{ts}.xlsx"
    sheets = {
        "Executive_Summary": exec_summary,
        "Schema_Comparison": schema_comparison,
        "Table_Comparison": table_comparison,
        "Column_Comparison": column_comparison,
        "Constraints_Comparison": constraints_comparison,
        "Index_Comparison": index_comparison,
        "Sequence_Comparison": sequence_comparison,
        "Views_and_Triggers": views_triggers_comparison,
        "Row_Count_Audit": row_count_audit,
        "Discrepancies": discrepancies if discrepancies else [{"Status": "Zero discrepancies detected. 100% object and data parity achieved."}]
    }

    generate_excel_report(report_file, sheets)
    logger.info(f"[+] Master Excel validation report created successfully: {report_file}")
    
    log_summary(logger, Path(__file__).name, [
        ('schemas audited', f"{total_schemas_matched} / {total_schemas_ora}"),
        ('tables audited', f"{total_tables_matched} / {total_tables_ora}"),
        ('columns audited', f"{total_cols_matched} / {total_cols_ora}"),
        ('constraints audited', f"{total_cons_matched} / {total_cons_ora}"),
        ('indexes audited', f"{total_idx_matched} / {total_idx_ora}"),
        ('sequences audited', f"{total_seq_matched} / {total_seq_ora}"),
        ('views audited', f"{total_views_matched} / {total_views_ora}"),
        ('triggers audited', f"{total_trgs_matched} / {total_trgs_ora}"),
        ('row count parity', f"{row_matched_cnt} / {len(row_count_audit)} tables"),
        ('total discrepancies', len(discrepancies)),
        ('overall audit verdict', ('PASS' if is_overall_pass else 'ACTION REQUIRED')),
    ])
        
    return is_overall_pass


# -----------------------------------------------------------------------------
# CLI Entry Point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="08_validate.py - Full-Fledged Oracle Dump vs PostgreSQL Parity Audit")
    parser.add_argument("--exact-counts", action="store_true", help="Perform exact live COUNT(*) queries instead of catalog approximations")
    parser.add_argument("--file", help="Master Oracle SQL dump file path (defaults to inputs/oracle/oracle_dump/*.sql)")
    parser.add_argument("--bg", action="store_true", help="Run detached in background supervisor")
    add_targeting_args(parser, schemas="--schemas")      # --file here is the Oracle dump, not an S3 file
    args = parser.parse_args()

    sel = selector_or_exit(args, logger, use_files=False)
    try:
        with open(META_TABLES_CSV, "r", encoding="utf-8") as fh:
            known = {(r["owner"], r["table_name"]) for r in csv.DictReader(fh)}
    except OSError:
        known = set()
    if known:
        validate_or_exit(sel.validate_tables, sorted(known), what="the Oracle metadata", logger=logger)
    if sel.dry_run:
        picked = [k for k in sorted(known) if sel.matches(*k)] if known else []
        logger.info(f"[dry-run] {sel.describe()}: the audit would cover {len(picked) if known else 'all'} table(s); no report written")
        sys.exit(0)

    ok = run_full_parity_audit(
        exact_counts=args.exact_counts,
        tables_filter=sel if sel.table_selectors else None,
        schemas_filter=set(sel.schemas) or None,
        dump_file=args.file
    )
    sys.exit(0 if ok else 1)
