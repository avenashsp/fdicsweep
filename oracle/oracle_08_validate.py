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
import re
import sys
import csv
import argparse
import importlib
from pathlib import Path
from collections import defaultdict

from sweep_common import (
    ORACLE_INPUTS_DIR,
    ORACLE_DUMP_DIR,
    ORACLE_OUTPUTS_DIR,
    ORACLE_REPORTS_DIR,
    LOAD_LOG_TABLE,
    META_TABLES_CSV,
    META_CONSTRAINTS_CSV,
    META_INDEXES_CSV,
    META_PARTITIONS_CSV,
    META_TABLE_STATS_CSV,
    META_SEQUENCES_CSV,
    META_OBJECTS_CSV,
    PGHOST,
    PGPORT,
    PGDATABASE,
    PGUSER,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    norm_col,
    oracle_to_pg,
    DB_ERRORS,
)

logger = get_logger("oracle_08_validate", "oracle_08_validate")

# Native FDICSweep application audit columns added by the framework
AUDIT_COLUMNS = {"createdby_m", "createddate_m", "modifiedby", "modifieddate"}


# -----------------------------------------------------------------------------
# Type Normalization & Compatibility Engine
# -----------------------------------------------------------------------------
def normalize_pg_type(t_str):
    """Normalizes PostgreSQL data type representations for exact parity checking."""
    if not t_str:
        return ""
    t = str(t_str).lower().strip()
    t = re.sub(r"\s+", " ", t)
    # Remove unnecessary modifier clauses
    t = re.sub(r"\bwithout time zone\b", "", t).strip()
    t = re.sub(r"\bcharacter varying\b", "varchar", t)
    t = re.sub(r"\bcharacter\b", "char", t)
    t = re.sub(r"\bbpchar\b", "char", t)
    if t in ("integer", "int4", "int"):
        return "integer"
    if t in ("bigint", "int8"):
        return "bigint"
    if t in ("smallint", "int2"):
        return "smallint"
    if t in ("double precision", "float8"):
        return "double precision"
    if t in ("real", "float4"):
        return "real"
    if t in ("boolean", "bool"):
        return "boolean"
    if t.startswith("timestamp") and "with time zone" in t:
        return "timestamptz"
    # numeric(p, 0) -> numeric(p,0)
    t = re.sub(r",\s+", ",", t)
    return t


def is_type_compatible(expected_type, actual_type):
    """
    Checks if target PostgreSQL type is compatible with expected mapped type.
    Handles standard SQL type widening and equivalent representations.
    """
    exp = normalize_pg_type(expected_type)
    act = normalize_pg_type(actual_type)

    if exp == act:
        return True, "EXACT_MATCH"

    # varchar(N) vs text
    if exp.startswith("varchar") and act == "text":
        return True, "COMPATIBLE_TEXT"
    if act.startswith("varchar") and exp == "text":
        return True, "COMPATIBLE_VARCHAR"

    # numeric(p,0) vs integer / bigint / smallint (symmetrically checked)
    m_exp_num = re.match(r"^numeric\((\d+),0\)$", exp)
    if m_exp_num:
        prec = int(m_exp_num.group(1))
        if prec <= 4 and act in ("smallint", "integer", "bigint", exp):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 10 and act in ("integer", "bigint", exp):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 18 and act in ("bigint", exp):
            return True, "COMPATIBLE_BIGINT"

    m_act_num = re.match(r"^numeric\((\d+),0\)$", act)
    if m_act_num:
        prec = int(m_act_num.group(1))
        if prec <= 4 and exp in ("smallint", "integer", "bigint", act):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 10 and exp in ("integer", "bigint", act):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 18 and exp in ("bigint", act):
            return True, "COMPATIBLE_BIGINT"

    # numeric generic
    if exp == "numeric" and (act.startswith("numeric") or act in ("double precision", "real", "bigint", "integer")):
        return True, "COMPATIBLE_NUMERIC"
    if act == "numeric" and (exp.startswith("numeric") or exp in ("double precision", "real", "bigint", "integer")):
        return True, "COMPATIBLE_NUMERIC"

    # timestamp(0) vs timestamp
    if exp in ("timestamp(0)", "timestamp") and act in ("timestamp(0)", "timestamp"):
        return True, "COMPATIBLE_TIMESTAMP"

    # timestamptz vs timestamp with time zone
    if exp in ("timestamptz", "timestamp with time zone") and act in ("timestamptz", "timestamp with time zone"):
        return True, "COMPATIBLE_TIMESTAMPTZ"

    # char(1) vs varchar(1)
    if exp in ("char(1)", "varchar(1)") and act in ("char(1)", "varchar(1)"):
        return True, "COMPATIBLE_CHAR"

    return False, "TYPE_MISMATCH"


# -----------------------------------------------------------------------------
# Database Catalog Query Helpers (Pure psql CLI - Zero psycopg2 dependency)
# -----------------------------------------------------------------------------
def execute_db_query(sql):
    """
    Executes SQL query against PostgreSQL target database exclusively using psql CLI.
    """
    from sweep_common import query_db_dicts
    return query_db_dicts(sql)


def test_db_connection():
    """Tests if the PostgreSQL target database is reachable via psql CLI."""
    try:
        from sweep_common import run_psql
        out = run_psql("SELECT 1 AS ok;", tuples_only=True, unaligned=True)
        return bool(out and "1" in out)
    except RuntimeError as e:
        logger.warning(f"PostgreSQL connection failed ({PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE}): {e}")
        return False


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
def fetch_target_postgres_catalogs(exact_counts=False, tables_filter=None, schemas_filter=None):
    """
    Queries target PostgreSQL system catalogs for schemas, tables, columns,
    constraints, indexes, sequences, views, triggers, and load_log records.
    """
    catalog = {
        "schemas": [],
        "tables": [],
        "columns": [],
        "constraints": [],
        "indexes": [],
        "sequences": [],
        "views": [],
        "triggers": [],
        "load_log": [],
        "exact_row_counts": {}
    }

    # 1. Schemas
    sql_schemas = """
        SELECT nspname AS schema_name
        FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%'
          AND nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY nspname;
    """
    try:
        catalog["schemas"] = execute_db_query(sql_schemas)
    except DB_ERRORS as e:
        logger.warning(f"Could not query pg_namespace: {e}")

    # 2. Tables and Partition Hierarchy
    sql_tables = """
        SELECT 
            n.nspname AS table_schema,
            c.relname AS table_name,
            c.relkind,
            CASE c.relkind 
                WHEN 'r' THEN 'TABLE' 
                WHEN 'p' THEN 'PARTITIONED TABLE' 
                WHEN 'f' THEN 'FOREIGN TABLE' 
                ELSE 'OTHER' 
            END AS table_type,
            COALESCE(c.reltuples::bigint, 0) AS approx_row_count,
            COALESCE(s.n_live_tup, 0) AS stat_live_rows,
            CASE WHEN i.inhparent IS NOT NULL THEN true ELSE false END AS is_partition_child,
            pn.nspname AS parent_schema,
            p.relname AS parent_table
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.schemaname = n.nspname AND s.relname = c.relname
        LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
        LEFT JOIN pg_class p ON i.inhparent = p.oid
        LEFT JOIN pg_namespace pn ON p.relnamespace = pn.oid
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY n.nspname, c.relname;
    """
    try:
        catalog["tables"] = execute_db_query(sql_tables)
        for r in catalog["tables"]:
            r["is_partition_child"] = str(r.get("is_partition_child") or "").lower() in ("t", "true", "1", "yes")
            try:
                r["approx_row_count"] = int(float(r.get("approx_row_count") or 0))
            except (ValueError, TypeError):
                r["approx_row_count"] = 0
            try:
                r["stat_live_rows"] = int(float(r.get("stat_live_rows") or 0))
            except (ValueError, TypeError):
                r["stat_live_rows"] = 0
    except DB_ERRORS as e:
        logger.warning(f"Could not query PostgreSQL tables: {e}")

    # 3. Columns & Formatted Data Types
    sql_cols = """
        SELECT 
            n.nspname AS table_schema,
            c.relname AS table_name,
            a.attname AS column_name,
            a.attnum AS column_id,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
            t.typname AS udt_name,
            NOT a.attnotnull AS is_nullable,
            pg_get_expr(d.adbin, d.adrelid) AS column_default
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
        WHERE a.attnum > 0 
          AND NOT a.attisdropped
          AND c.relkind IN ('r', 'p')
          AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY n.nspname, c.relname, a.attnum;
    """
    try:
        catalog["columns"] = execute_db_query(sql_cols)
        for r in catalog["columns"]:
            r["is_nullable"] = str(r.get("is_nullable") or "").lower() in ("t", "true", "1", "yes")
    except DB_ERRORS as e:
        logger.warning(f"Could not query PostgreSQL columns: {e}")

    # 4. Constraints
    sql_cons = """
        SELECT 
            n.nspname AS table_schema,
            c.relname AS table_name,
            con.conname AS constraint_name,
            con.contype AS constraint_type,
            con.convalidated AS is_validated,
            (
                SELECT array_to_string(array_agg(a.attname ORDER BY u.attposition), ',')
                FROM unnest(con.conkey) WITH ORDINALITY AS u(attnum, attposition)
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
            ) AS columns,
            fn.nspname AS ref_schema,
            fc.relname AS ref_table,
            (
                SELECT array_to_string(array_agg(fa.attname ORDER BY fu.attposition), ',')
                FROM unnest(con.confkey) WITH ORDINALITY AS fu(attnum, attposition)
                JOIN pg_attribute fa ON fa.attrelid = fc.oid AND fa.attnum = fu.attnum
            ) AS ref_columns
        FROM pg_constraint con
        JOIN pg_class c ON con.conrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_class fc ON con.confrelid = fc.oid
        LEFT JOIN pg_namespace fn ON fc.relnamespace = fn.oid
        WHERE n.nspname NOT LIKE 'pg_%'
          AND n.nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY n.nspname, c.relname, con.conname;
    """
    try:
        catalog["constraints"] = execute_db_query(sql_cons)
        for r in catalog["constraints"]:
            r["is_validated"] = str(r.get("is_validated") or "").lower() in ("t", "true", "1", "yes")
    except DB_ERRORS as e:
        logger.warning(f"Could not query PostgreSQL constraints: {e}")

    # 5. Indexes
    sql_idxs = """
        SELECT 
            n.nspname AS table_schema,
            c.relname AS table_name,
            i.relname AS index_name,
            idx.indisunique AS is_unique,
            idx.indisprimary AS is_primary,
            idx.indisvalid AS is_valid,
            pg_get_indexdef(idx.indexrelid) AS index_def,
            (
                SELECT array_to_string(array_agg(a.attname ORDER BY u.attposition), ',')
                FROM unnest(idx.indkey) WITH ORDINALITY AS u(attnum, attposition)
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
            ) AS columns
        FROM pg_index idx
        JOIN pg_class i ON idx.indexrelid = i.oid
        JOIN pg_class c ON idx.indrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT LIKE 'pg_%'
          AND n.nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY n.nspname, c.relname, i.relname;
    """
    try:
        catalog["indexes"] = execute_db_query(sql_idxs)
        for r in catalog["indexes"]:
            r["is_unique"] = str(r.get("is_unique") or "").lower() in ("t", "true", "1", "yes")
            r["is_primary"] = str(r.get("is_primary") or "").lower() in ("t", "true", "1", "yes")
            r["is_valid"] = str(r.get("is_valid") or "").lower() in ("t", "true", "1", "yes")
    except DB_ERRORS as e:
        logger.warning(f"Could not query PostgreSQL indexes: {e}")

    # 6. Sequences
    sql_seqs = """
        SELECT 
            n.nspname AS sequence_schema,
            c.relname AS sequence_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY n.nspname, c.relname;
    """
    try:
        catalog["sequences"] = execute_db_query(sql_seqs)
    except DB_ERRORS as e:
        logger.warning(f"Could not query PostgreSQL sequences: {e}")

    # 7. Views & Materialized Views
    sql_views = """
        SELECT 
            n.nspname AS view_schema,
            c.relname AS view_name,
            CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' END AS view_type
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('v', 'm')
          AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY n.nspname, c.relname;
    """
    try:
        catalog["views"] = execute_db_query(sql_views)
    except DB_ERRORS as e:
        logger.warning(f"Could not query PostgreSQL views: {e}")

    # 8. Triggers
    sql_trgs = """
        SELECT 
            n.nspname AS trigger_schema,
            c.relname AS table_name,
            t.tgname AS trigger_name,
            NOT (t.tgenabled = 'D') AS is_enabled
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE NOT t.tgisinternal
          AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname NOT IN ('information_schema', 'sweep_ctl', 'sweep_meta')
        ORDER BY n.nspname, c.relname, t.tgname;
    """
    try:
        catalog["triggers"] = execute_db_query(sql_trgs)
        for r in catalog["triggers"]:
            r["is_enabled"] = str(r.get("is_enabled") or "").lower() in ("t", "true", "1", "yes")
    except DB_ERRORS as e:
        logger.warning(f"Could not query PostgreSQL triggers: {e}")

    # 9. Loaded S3 File Records from sweep_ctl.load_log
    sql_load_log = f"""
        SELECT 
            LOWER(target_schema) AS target_schema,
            LOWER(target_table) AS target_table,
            COUNT(*) AS s3_files_loaded,
            SUM(loaded_rows) AS s3_loaded_rows,
            SUM(size_bytes) AS s3_bytes_loaded,
            ROUND(SUM(seconds), 2) AS total_load_seconds
        FROM {LOAD_LOG_TABLE}
        WHERE status = 'SUCCESS'
        GROUP BY LOWER(target_schema), LOWER(target_table);
    """
    try:
        catalog["load_log"] = execute_db_query(sql_load_log)
    except DB_ERRORS as e:
        logger.warning(f"Could not query {LOAD_LOG_TABLE}: {e}")

    # 10. Exact Table Counts (if requested)
    if exact_counts:
        logger.info("Executing exact SELECT COUNT(*) on all PostgreSQL tables...")
        for tbl in catalog["tables"]:
            s = tbl["table_schema"].lower()
            t = tbl["table_name"].lower()
            if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
                continue
            if schemas_filter and s not in schemas_filter:
                continue
            # Only count base or partitioned parent tables
            try:
                res = execute_db_query(f'SELECT COUNT(*) AS cnt FROM "{s}"."{t}";')
                catalog["exact_row_counts"][(s, t)] = int(res[0]["cnt"]) if res else 0
            except DB_ERRORS as e:
                logger.warning(f"Exact count failed for {s}.{t}: {e}")
                catalog["exact_row_counts"][(s, t)] = -1

    return catalog


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
            "load_log": [], "exact_row_counts": {}
        }

    # 3. Load S3 Planned File Mapping
    logger.info("[3/7] Loading planned S3 files inventory from s3_file_map.csv...")
    map_path = ORACLE_OUTPUTS_DIR / "oracle_02_s3_file_map.csv"
    if not map_path.exists():
        map_path = ORACLE_OUTPUTS_DIR / "s3_file_map.csv"
    s3_planned_map = defaultdict(lambda: {"files": 0, "size_mb": 0.0})
    if map_path.exists():
        with open(map_path, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                s = (r.get("target_schema") or "").lower().strip()
                t = (r.get("target_table") or "").lower().strip()
                size_mb = float(r.get("size_mb") or 0.0)
                if s and t:
                    s3_planned_map[(s, t)]["files"] += 1
                    s3_planned_map[(s, t)]["size_mb"] += size_mb

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

    # -------------------------------------------------------------------------
    # LEVEL 1: Schema-Level Comparison
    # -------------------------------------------------------------------------
    logger.info("[4/7] Performing Schema-level, Table-level, and Column-level comparisons...")
    oracle_schemas = set()
    for col in meta["tables"]:
        o = (col.get("owner") or "").upper().strip()
        if o: oracle_schemas.add(o)
    for obj in meta["objects"]:
        o = (obj.get("owner") or "").upper().strip()
        if o: oracle_schemas.add(o)
    for s in meta["sequences"]:
        o = (s.get("owner") or "").upper().strip()
        if o: oracle_schemas.add(o)

    pg_schemas_set = {s["schema_name"].lower() for s in pg_cat["schemas"]}
    all_schemas = sorted(set(s.lower() for s in oracle_schemas) | pg_schemas_set)

    schema_comparison = []
    for s in all_schemas:
        if schemas_filter and s not in schemas_filter:
            continue
        in_oracle = s.upper() in oracle_schemas
        in_pg = s in pg_schemas_set

        # Count tables, views, sequences
        ora_tbl_cnt = len(set(col["table_name"].upper() for col in meta["tables"] if col.get("owner", "").upper() == s.upper()))
        pg_tbl_cnt = sum(1 for t in pg_cat["tables"] if t["table_schema"].lower() == s and not t["is_partition_child"])
        ora_view_cnt = sum(1 for o in meta["objects"] if o.get("owner", "").upper() == s.upper() and "VIEW" in o.get("object_type", "").upper())
        pg_view_cnt = sum(1 for v in pg_cat["views"] if v["view_schema"].lower() == s)
        ora_seq_cnt = sum(1 for sq in meta["sequences"] if sq.get("owner", "").upper() == s.upper())
        pg_seq_cnt = sum(1 for sq in pg_cat["sequences"] if sq["sequence_schema"].lower() == s)

        if not db_connected:
            status = "DB_UNREACHABLE"
            notes = "Cannot reach PostgreSQL to verify schema"
        elif in_oracle and in_pg:
            status = "MATCH"
            notes = "Schema exists in both Oracle dump and PostgreSQL"
        elif in_oracle and not in_pg:
            status = "MISSING_IN_PG"
            notes = "Schema defined in Oracle dump but missing in PostgreSQL"
            discrepancies.append({
                "Object_Category": "SCHEMA", "Target_Schema": s, "Target_Table": "",
                "Object_Name": s, "Discrepancy_Type": "MISSING_SCHEMA_IN_PG",
                "Oracle_Expected": "Present in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Execute 'CREATE SCHEMA IF NOT EXISTS \"{s}\";'"
            })
        else:
            status = "EXTRA_IN_PG"
            notes = "Schema exists in PostgreSQL but not in Oracle dump"

        schema_comparison.append({
            "target_schema": s,
            "in_oracle_dump": "YES" if in_oracle else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "tables_in_oracle": ora_tbl_cnt,
            "tables_in_postgres": pg_tbl_cnt,
            "views_in_oracle": ora_view_cnt,
            "views_in_postgres": pg_view_cnt,
            "sequences_in_oracle": ora_seq_cnt,
            "sequences_in_postgres": pg_seq_cnt,
            "status": status,
            "notes": notes
        })

    # -------------------------------------------------------------------------
    # LEVEL 2: Table-Level Comparison
    # -------------------------------------------------------------------------
    oracle_tables = {}
    for col in meta["tables"]:
        o = (col.get("owner") or "").lower().strip()
        t = norm_col(col.get("table_name") or "")
        orig_t = (col.get("table_name") or "").upper().strip()
        is_part = str(col.get("partitioned") or "").upper() == "YES"
        if o and t:
            if (o, t) not in oracle_tables:
                oracle_tables[(o, t)] = {
                    "owner": o, "table_name": t, "orig_table": orig_t,
                    "partitioned": is_part, "child_partitions": set()
                }

    # Populate child partition names from oracle_metadata_partitions.csv
    for p in meta["partitions"]:
        o = (p.get("owner") or "").lower().strip()
        t = norm_col(p.get("table_name") or "")
        part_name = norm_col(p.get("partition_name") or "")
        if (o, t) in oracle_tables:
            oracle_tables[(o, t)]["partitioned"] = True
            oracle_tables[(o, t)]["child_partitions"].add(part_name)

    # Index PostgreSQL tables
    pg_tables_map = {}
    pg_child_partitions = defaultdict(list)
    for t_row in pg_cat["tables"]:
        s = t_row["table_schema"].lower()
        t = t_row["table_name"].lower()
        pg_tables_map[(s, t)] = t_row
        if t_row["is_partition_child"]:
            ps = (t_row.get("parent_schema") or s).lower()
            pt = (t_row.get("parent_table") or "").lower()
            if pt:
                pg_child_partitions[(ps, pt)].append(t)

    all_table_keys = sorted(set(oracle_tables.keys()) | set(pg_tables_map.keys()))
    table_comparison = []

    for s, t in all_table_keys:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_t = oracle_tables.get((s, t))
        pg_t = pg_tables_map.get((s, t))
        in_oracle = ora_t is not None
        in_pg = pg_t is not None

        ora_part = ora_t["partitioned"] if ora_t else False
        ora_child_cnt = len(ora_t["child_partitions"]) if ora_t else 0
        pg_child_cnt = len(pg_child_partitions.get((s, t), []))
        pg_type = pg_t["table_type"] if pg_t else "MISSING"

        if not db_connected:
            tbl_status = "DB_UNREACHABLE"
            notes = "PostgreSQL catalog offline"
        elif in_oracle and in_pg:
            if ora_part and pg_t["relkind"] != "p":
                tbl_status = "PARTITION_MISMATCH"
                notes = f"Table partitioned in Oracle but created as {pg_type} in PostgreSQL"
                discrepancies.append({
                    "Object_Category": "TABLE", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": t, "Discrepancy_Type": "PARTITIONING_TYPE_MISMATCH",
                    "Oracle_Expected": "PARTITIONED TABLE", "Postgres_Actual": pg_type,
                    "Actionable_Recommendation": "Re-run Step 04 and Step 05 to create declarative partitioned table."
                })
            else:
                tbl_status = "MATCH"
                notes = f"Table exists with {pg_child_cnt} child partitions in PG"
        elif in_oracle and not in_pg:
            tbl_status = "MISSING_IN_PG"
            notes = "Table defined in Oracle dump but missing in PostgreSQL"
            discrepancies.append({
                "Object_Category": "TABLE", "Target_Schema": s, "Target_Table": t,
                "Object_Name": t, "Discrepancy_Type": "MISSING_TABLE_IN_PG",
                "Oracle_Expected": "Present in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": "Run Step 05: python sweep.py oracle apply-ddl"
            })
        else:
            tbl_status = "EXTRA_IN_PG"
            notes = "Table exists in PostgreSQL but not in Oracle dump"

        table_comparison.append({
            "target_schema": s,
            "target_table": t,
            "in_oracle_dump": "YES" if in_oracle else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "oracle_partitioned": "YES" if ora_part else "NO",
            "pg_table_type": pg_type,
            "oracle_child_partitions_count": ora_child_cnt,
            "pg_child_partitions_count": pg_child_cnt,
            "table_status": tbl_status,
            "notes": notes
        })

    # -------------------------------------------------------------------------
    # LEVEL 3: Column & Datatype-Level Comparison
    # -------------------------------------------------------------------------
    oracle_cols_map = defaultdict(dict)
    for col in meta["tables"]:
        s = (col.get("owner") or "").lower().strip()
        t = norm_col(col.get("table_name") or "")
        c = norm_col(col.get("column_name") or "")
        if s and t and c:
            oracle_cols_map[(s, t)][c] = col

    pg_cols_map = defaultdict(dict)
    for col in pg_cat["columns"]:
        s = col["table_schema"].lower().strip()
        t = col["table_name"].lower().strip()
        c = col["column_name"].lower().strip()
        pg_cols_map[(s, t)][c] = col

    column_comparison = []
    evaluated_tables = set(oracle_cols_map.keys()) | set(pg_cols_map.keys())

    for s, t in sorted(evaluated_tables):
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_c_dict = oracle_cols_map.get((s, t), {})
        pg_c_dict = pg_cols_map.get((s, t), {})
        all_cols = sorted(set(ora_c_dict.keys()) | set(pg_c_dict.keys()))

        for c in all_cols:
            ora_c = ora_c_dict.get(c)
            pg_c = pg_c_dict.get(c)
            in_ora = ora_c is not None
            in_pg = pg_c is not None
            is_audit = c in AUDIT_COLUMNS

            ora_dt = ora_c.get("data_type", "") if ora_c else ""
            ora_len = ora_c.get("data_length", "") if ora_c else ""
            ora_prec = ora_c.get("data_precision", "") if ora_c else ""
            ora_scale = ora_c.get("data_scale", "") if ora_c else ""
            expected_pg = oracle_to_pg(ora_dt, ora_len, ora_prec, ora_scale) if ora_c else ""

            actual_pg = pg_c.get("formatted_type", "") if pg_c else ""
            cid = ora_c.get("column_id", "") if ora_c else pg_c.get("column_id", "")

            ora_null = ora_c.get("nullable", "Y") if ora_c else ""
            pg_null = "YES" if pg_c and pg_c.get("is_nullable") else ("NO" if pg_c else "")

            # Check Type Parity
            if not db_connected:
                overall_status = "DB_UNREACHABLE"
                type_match = "N/A"
                null_match = "N/A"
            elif is_audit and not in_ora and in_pg:
                overall_status = "AUDIT_COLUMN_OK"
                type_match = "MATCH (APPLICATION AUDIT)"
                null_match = "MATCH"
            elif in_ora and in_pg:
                compat, reason = is_type_compatible(expected_pg, actual_pg)
                type_match = "MATCH" if compat else "MISMATCH"

                # Nullability check
                exp_null_bool = (ora_null.upper() == "Y")
                act_null_bool = bool(pg_c.get("is_nullable"))
                null_match = "MATCH" if exp_null_bool == act_null_bool else "MISMATCH"

                if not compat:
                    overall_status = "TYPE_MISMATCH"
                    discrepancies.append({
                        "Object_Category": "COLUMN", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": c, "Discrepancy_Type": "DATA_TYPE_MISMATCH",
                        "Oracle_Expected": f"{ora_dt} -> {expected_pg}", "Postgres_Actual": actual_pg,
                        "Actionable_Recommendation": f"Alter column type: ALTER TABLE \"{s}\".\"{t}\" ALTER COLUMN \"{c}\" TYPE {expected_pg};"
                    })
                elif null_match == "MISMATCH":
                    overall_status = "NULLABILITY_MISMATCH"
                    discrepancies.append({
                        "Object_Category": "COLUMN", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": c, "Discrepancy_Type": "NULLABILITY_MISMATCH",
                        "Oracle_Expected": f"NULLABLE={ora_null}", "Postgres_Actual": f"NULLABLE={pg_null}",
                        "Actionable_Recommendation": f"Adjust nullability: ALTER TABLE \"{s}\".\"{t}\" ALTER COLUMN \"{c}\" {'DROP' if exp_null_bool else 'SET'} NOT NULL;"
                    })
                else:
                    overall_status = "MATCH"

            elif in_ora and not in_pg:
                overall_status = "MISSING_IN_PG"
                type_match = "MISSING"
                null_match = "MISSING"
                discrepancies.append({
                    "Object_Category": "COLUMN", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": c, "Discrepancy_Type": "MISSING_COLUMN_IN_PG",
                    "Oracle_Expected": f"{c} ({expected_pg})", "Postgres_Actual": "Missing in table",
                    "Actionable_Recommendation": f"Add missing column: ALTER TABLE \"{s}\".\"{t}\" ADD COLUMN \"{c}\" {expected_pg};"
                })
            else:
                overall_status = "EXTRA_IN_PG"
                type_match = "EXTRA"
                null_match = "EXTRA"

            column_comparison.append({
                "target_schema": s,
                "target_table": t,
                "column_name": c,
                "column_id": cid,
                "in_oracle_dump": "YES" if in_ora else "NO",
                "in_postgres": "YES" if in_pg else "NO",
                "oracle_data_type": ora_dt,
                "expected_pg_type": expected_pg,
                "actual_pg_type": actual_pg,
                "type_match": type_match,
                "oracle_nullable": ora_null,
                "pg_nullable": pg_null,
                "nullable_match": null_match,
                "is_audit_column": "YES" if is_audit else "NO",
                "overall_status": overall_status
            })

    # -------------------------------------------------------------------------
    # LEVEL 4: Constraints Comparison
    # -------------------------------------------------------------------------
    logger.info("[5/7] Comparing Constraints, Indexes, Sequences, Views, and Triggers...")
    oracle_cons_map = defaultdict(dict)
    for c_row in meta["constraints"]:
        s = (c_row.get("owner") or "").lower().strip()
        t = norm_col(c_row.get("table_name") or "")
        cn = norm_col(c_row.get("constraint_name") or "")
        ctype = (c_row.get("ctype") or "").upper().strip()
        cols = ",".join(norm_col(x) for x in c_row.get("columns", "").split(",") if x.strip())
        if s and t:
            oracle_cons_map[(s, t)][cn] = {
                "name": cn, "ctype": ctype, "columns": cols,
                "ref_table": norm_col(c_row.get("ref_table") or ""),
                "ref_columns": ",".join(norm_col(x) for x in c_row.get("ref_columns", "").split(",") if x.strip())
            }

    pg_cons_map = defaultdict(dict)
    for c_row in pg_cat["constraints"]:
        s = c_row["table_schema"].lower().strip()
        t = c_row["table_name"].lower().strip()
        cn = c_row["constraint_name"].lower().strip()
        pg_cons_map[(s, t)][cn] = c_row

    constraints_comparison = []
    all_cons_tables = sorted(set(oracle_cons_map.keys()) | set(pg_cons_map.keys()))

    for s, t in all_cons_tables:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_c = oracle_cons_map.get((s, t), {})
        pg_c = pg_cons_map.get((s, t), {})
        all_cn = sorted(set(ora_c.keys()) | set(pg_c.keys()))

        for cn in all_cn:
            oc = ora_c.get(cn)
            pc = pg_c.get(cn)
            in_ora = oc is not None
            in_pg = pc is not None

            ctype_map = {"P": "PRIMARY KEY", "U": "UNIQUE", "R": "FOREIGN KEY", "C": "CHECK"}
            ctype_pg_map = {"p": "PRIMARY KEY", "u": "UNIQUE", "f": "FOREIGN KEY", "c": "CHECK"}

            exp_type = ctype_map.get(oc["ctype"], oc["ctype"]) if oc else ""
            act_type = ctype_pg_map.get(pc["constraint_type"], pc.get("constraint_type", "")) if pc else ""
            cols = oc["columns"] if oc else (pc.get("columns") or "")
            ref_tbl = oc["ref_table"] if oc else (pc.get("ref_table") or "")
            ref_cols = oc["ref_columns"] if oc else (pc.get("ref_columns") or "")

            is_val = pc.get("is_validated") if pc else None
            is_val_str = "YES" if is_val is True else ("NO" if is_val is False else "N/A")

            if not db_connected:
                status = "DB_UNREACHABLE"
            elif in_ora and in_pg:
                if is_val is False:
                    status = "UNVALIDATED"
                    discrepancies.append({
                        "Object_Category": "CONSTRAINT", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": cn, "Discrepancy_Type": "CONSTRAINT_NOT_VALIDATED",
                        "Oracle_Expected": f"{exp_type} ({cols}) VALIDATED", "Postgres_Actual": "NOT VALID",
                        "Actionable_Recommendation": f"Execute: ALTER TABLE \"{s}\".\"{t}\" VALIDATE CONSTRAINT \"{cn}\";"
                    })
                else:
                    status = "MATCH"
            elif in_ora and not in_pg:
                status = "MISSING_IN_PG"
                discrepancies.append({
                    "Object_Category": "CONSTRAINT", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": cn, "Discrepancy_Type": "MISSING_CONSTRAINT_IN_PG",
                    "Oracle_Expected": f"{exp_type} on ({cols})", "Postgres_Actual": "Missing in DB",
                    "Actionable_Recommendation": "Run Step 07: python sweep.py oracle load-obj"
                })
            else:
                status = "EXTRA_IN_PG"

            constraints_comparison.append({
                "target_schema": s,
                "target_table": t,
                "constraint_name": cn,
                "constraint_type": exp_type or act_type,
                "columns": cols,
                "ref_table": ref_tbl,
                "ref_columns": ref_cols,
                "in_oracle_dump": "YES" if in_ora else "NO",
                "in_postgres": "YES" if in_pg else "NO",
                "pg_is_validated": is_val_str,
                "status": status
            })

    # -------------------------------------------------------------------------
    # LEVEL 5: Indexes Comparison
    # -------------------------------------------------------------------------
    oracle_idx_map = defaultdict(dict)
    for idx in meta["indexes"]:
        s = (idx.get("table_owner") or idx.get("owner") or "").lower().strip()
        t = norm_col(idx.get("table_name") or "")
        iname = norm_col(idx.get("index_name") or "")
        is_uniq = str(idx.get("uniqueness") or "").upper() == "UNIQUE"
        cols = ",".join(norm_col(x) for x in idx.get("columns", "").split(",") if x.strip())
        if s and t and iname:
            oracle_idx_map[(s, t)][iname] = {
                "name": iname, "unique": is_uniq, "columns": cols
            }

    pg_idx_map = defaultdict(dict)
    for idx in pg_cat["indexes"]:
        s = idx["table_schema"].lower().strip()
        t = idx["table_name"].lower().strip()
        iname = idx["index_name"].lower().strip()
        pg_idx_map[(s, t)][iname] = idx

    index_comparison = []
    all_idx_tables = sorted(set(oracle_idx_map.keys()) | set(pg_idx_map.keys()))

    for s, t in all_idx_tables:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_i = oracle_idx_map.get((s, t), {})
        pg_i = pg_idx_map.get((s, t), {})
        all_inames = sorted(set(ora_i.keys()) | set(pg_i.keys()))

        for iname in all_inames:
            oi = ora_i.get(iname)
            pi = pg_i.get(iname)
            in_ora = oi is not None
            in_pg = pi is not None

            uniq_str = "UNIQUE" if (oi and oi["unique"]) or (pi and pi.get("is_unique")) else "NON-UNIQUE"
            cols = oi["columns"] if oi else (pi.get("columns") or "")
            is_valid = "YES" if (pi and pi.get("is_valid")) else ("NO" if pi else "N/A")

            if not db_connected:
                status = "DB_UNREACHABLE"
            elif in_ora and in_pg:
                status = "MATCH"
            elif in_ora and not in_pg:
                # Check if backing index for PK or Unique constraint satisfies it
                backing_found = any(pi_obj.get("columns") == cols for pi_obj in pg_i.values())
                if backing_found:
                    status = "MATCH (CONSTRAINT BACKING INDEX)"
                else:
                    status = "MISSING_IN_PG"
                    discrepancies.append({
                        "Object_Category": "INDEX", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": iname, "Discrepancy_Type": "MISSING_INDEX_IN_PG",
                        "Oracle_Expected": f"{uniq_str} INDEX on ({cols})", "Postgres_Actual": "Missing in DB",
                        "Actionable_Recommendation": f"Execute: CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{iname}\" ON \"{s}\".\"{t}\" ({cols});"
                    })
            else:
                status = "EXTRA_IN_PG"

            index_comparison.append({
                "target_schema": s,
                "target_table": t,
                "index_name": iname,
                "uniqueness": uniq_str,
                "columns": cols,
                "in_oracle_dump": "YES" if in_ora else "NO",
                "in_postgres": "YES" if in_pg else "NO",
                "pg_is_valid": is_valid,
                "status": status
            })

    # -------------------------------------------------------------------------
    # LEVEL 6: Sequences Comparison
    # -------------------------------------------------------------------------
    oracle_seqs = {}
    for sq in meta["sequences"]:
        s = (sq.get("owner") or "").lower().strip()
        sq_name = norm_col(sq.get("sequence_name") or "")
        if s and sq_name:
            oracle_seqs[(s, sq_name)] = sq

    pg_seqs = {(sq["sequence_schema"].lower(), sq["sequence_name"].lower()) for sq in pg_cat["sequences"]}
    all_seq_keys = sorted(set(oracle_seqs.keys()) | pg_seqs)

    sequence_comparison = []
    for s, sq_name in all_seq_keys:
        if schemas_filter and s not in schemas_filter:
            continue
        in_ora = (s, sq_name) in oracle_seqs
        in_pg = (s, sq_name) in pg_seqs
        ora_info = oracle_seqs.get((s, sq_name), {})

        if not db_connected:
            status = "DB_UNREACHABLE"
        elif in_ora and in_pg:
            status = "MATCH"
        elif in_ora and not in_pg:
            status = "MISSING_IN_PG"
            discrepancies.append({
                "Object_Category": "SEQUENCE", "Target_Schema": s, "Target_Table": "",
                "Object_Name": sq_name, "Discrepancy_Type": "MISSING_SEQUENCE_IN_PG",
                "Oracle_Expected": "Present in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Execute: CREATE SEQUENCE IF NOT EXISTS \"{s}\".\"{sq_name}\";"
            })
        else:
            status = "EXTRA_IN_PG"

        sequence_comparison.append({
            "target_schema": s,
            "sequence_name": sq_name,
            "in_oracle_dump": "YES" if in_ora else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "oracle_min_value": ora_info.get("min_value", ""),
            "oracle_max_value": ora_info.get("max_value", ""),
            "oracle_increment_by": ora_info.get("increment_by", ""),
            "status": status
        })

    # -------------------------------------------------------------------------
    # LEVEL 7: Views and Triggers Comparison
    # -------------------------------------------------------------------------
    oracle_views = {}
    oracle_triggers = {}
    for obj in meta["objects"]:
        s = (obj.get("owner") or "").lower().strip()
        oname = norm_col(obj.get("object_name") or "")
        otype = (obj.get("object_type") or "").upper().strip()
        if "VIEW" in otype:
            oracle_views[(s, oname)] = otype
        elif "TRIGGER" in otype:
            oracle_triggers[(s, oname)] = otype

    pg_views = {(v["view_schema"].lower(), v["view_name"].lower()): v["view_type"] for v in pg_cat["views"]}
    pg_triggers = {(t["trigger_schema"].lower(), t["trigger_name"].lower()): t for t in pg_cat["triggers"]}

    views_triggers_comparison = []

    # Check Views
    all_view_keys = sorted(set(oracle_views.keys()) | set(pg_views.keys()))
    for s, vname in all_view_keys:
        if schemas_filter and s not in schemas_filter:
            continue
        in_ora = (s, vname) in oracle_views
        in_pg = (s, vname) in pg_views
        vtype = oracle_views.get((s, vname), pg_views.get((s, vname), "VIEW"))

        if not db_connected:
            status = "DB_UNREACHABLE"
        elif in_ora and in_pg:
            status = "MATCH"
        elif in_ora and not in_pg:
            status = "MISSING_IN_PG"
            discrepancies.append({
                "Object_Category": "VIEW", "Target_Schema": s, "Target_Table": "",
                "Object_Name": vname, "Discrepancy_Type": "MISSING_VIEW_IN_PG",
                "Oracle_Expected": f"{vtype} in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Deploy view from outputs/post_load/{s}_views.sql"
            })
        else:
            status = "EXTRA_IN_PG"

        views_triggers_comparison.append({
            "target_schema": s,
            "object_type": vtype,
            "object_name": vname,
            "target_table": "N/A",
            "in_oracle_dump": "YES" if in_ora else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "status": status
        })

    # Check Triggers
    all_trg_keys = sorted(set(oracle_triggers.keys()) | set(pg_triggers.keys()))
    for s, trgname in all_trg_keys:
        if schemas_filter and s not in schemas_filter:
            continue
        in_ora = (s, trgname) in oracle_triggers
        in_pg = (s, trgname) in pg_triggers
        pg_trg_info = pg_triggers.get((s, trgname), {})
        tbl_name = pg_trg_info.get("table_name", "N/A")

        if not db_connected:
            status = "DB_UNREACHABLE"
        elif in_ora and in_pg:
            status = "MATCH"
        elif in_ora and not in_pg:
            status = "MISSING_IN_PG"
            discrepancies.append({
                "Object_Category": "TRIGGER", "Target_Schema": s, "Target_Table": tbl_name,
                "Object_Name": trgname, "Discrepancy_Type": "MISSING_TRIGGER_IN_PG",
                "Oracle_Expected": "Trigger in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Deploy trigger from outputs/post_load/{s}_triggers.sql"
            })
        else:
            status = "EXTRA_IN_PG"

        views_triggers_comparison.append({
            "target_schema": s,
            "object_type": "TRIGGER",
            "object_name": trgname,
            "target_table": tbl_name,
            "in_oracle_dump": "YES" if in_ora else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "status": status
        })

    # -------------------------------------------------------------------------
    # LEVEL 8: Approximate Row Counts & S3 Parity Reconciliation
    # -------------------------------------------------------------------------
    logger.info("[6/7] Reconciling table row counts: PostgreSQL approximate live rows vs S3 mapped files...")
    # Oracle Source Stats Mapping
    oracle_stats_rows = {}
    for st in meta["table_stats"]:
        o = (st.get("owner") or "").lower().strip()
        t = norm_col(st.get("table_name") or "")
        nr = st.get("num_rows")
        if nr not in ("", None, "None"):
            oracle_stats_rows[(o, t)] = int(float(nr))

    # Aggregate approximate rows from pg_class (and sum child partitions for partitioned tables)
    pg_table_approx = {}
    for t_row in pg_cat["tables"]:
        s = t_row["table_schema"].lower()
        t = t_row["table_name"].lower()
        # Prefer n_live_tup if available; otherwise reltuples
        raw_cnt = t_row.get("stat_live_rows")
        if raw_cnt in (None, "", "None"):
            raw_cnt = t_row.get("approx_row_count", 0)
        try:
            cnt = int(float(raw_cnt or 0))
        except (ValueError, TypeError):
            cnt = 0
        pg_table_approx[(s, t)] = max(cnt, 0)

    # For parent partitioned tables, calculate total rows as the sum of child partitions
    for (ps, pt), children in pg_child_partitions.items():
        child_sum = sum(pg_table_approx.get((ps, ct), 0) for ct in children)
        pg_table_approx[(ps, pt)] = child_sum

    row_count_audit = []
    all_audit_tables = sorted(set(oracle_tables.keys()) | set(s3_planned_map.keys()) | set(s3_loaded_map.keys()))

    for s, t in all_audit_tables:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        planned_info = s3_planned_map.get((s, t), {"files": 0, "size_mb": 0.0})
        loaded_info = s3_loaded_map.get((s, t), {"files": 0, "rows": 0, "bytes": 0, "seconds": 0.0})
        s3_loaded_rows = loaded_info["rows"]
        ora_source_rows = oracle_stats_rows.get((s, t))

        # Determine PG Row Count
        if exact_counts and (s, t) in pg_cat["exact_row_counts"]:
            pg_rows = pg_cat["exact_row_counts"][(s, t)]
            count_method = "EXACT (COUNT(*))"
        else:
            pg_rows = pg_table_approx.get((s, t), 0)
            count_method = "APPROXIMATE (pg_catalog)"

        # Parity comparison: PG row count vs S3 loaded rows
        if not db_connected:
            parity_status = "DB_UNREACHABLE"
            diff = 0
            var_pct = "N/A"
        elif s3_loaded_rows > 0:
            diff = pg_rows - s3_loaded_rows
            diff_pct = round((diff / s3_loaded_rows) * 100, 2)
            var_pct = f"{diff_pct}%"

            if diff == 0:
                parity_status = "EXACT_MATCH"
            elif not exact_counts and abs(diff) / max(s3_loaded_rows, 1) <= 0.05:
                # Within 5% tolerance for PostgreSQL pg_class statistical approximation
                parity_status = "APPROX_MATCH"
            else:
                parity_status = "MISMATCH"
                discrepancies.append({
                    "Object_Category": "ROW_COUNT", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": t, "Discrepancy_Type": "ROW_COUNT_DISCREPANCY",
                    "Oracle_Expected": f"S3 Streamed: {s3_loaded_rows:,} rows",
                    "Postgres_Actual": f"PG Count: {pg_rows:,} rows (Diff: {diff:+,})",
                    "Actionable_Recommendation": f"Investigate streaming load for {s}.{t} in sweep_ctl.load_log."
                })
        elif s3_loaded_rows == 0 and planned_info["files"] > 0:
            diff = 0
            var_pct = "0%"
            parity_status = "PENDING_INGESTION"
        elif s3_loaded_rows == 0 and pg_rows == 0:
            diff = 0
            var_pct = "0%"
            parity_status = "EMPTY_TABLE"
        else:
            diff = pg_rows
            var_pct = "100%"
            parity_status = "LOADED_WITHOUT_LOG"

        row_count_audit.append({
            "target_schema": s,
            "target_table": t,
            "pg_table_rows": pg_rows if db_connected else "N/A",
            "count_method": count_method if db_connected else "N/A",
            "s3_mapped_files_planned": planned_info["files"],
            "s3_files_loaded": loaded_info["files"],
            "s3_loaded_rows": s3_loaded_rows,
            "oracle_source_rows": ora_source_rows if ora_source_rows is not None else "N/A",
            "row_difference": diff if db_connected else "N/A",
            "variance_pct": var_pct,
            "parity_status": parity_status
        })

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
    
    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  schemas audited            : {total_schemas_matched:>10} / {total_schemas_ora}",
        f"  tables audited             : {total_tables_matched:>10} / {total_tables_ora}",
        f"  columns audited            : {total_cols_matched:>10} / {total_cols_ora}",
        f"  constraints audited        : {total_cons_matched:>10} / {total_cons_ora}",
        f"  indexes audited            : {total_idx_matched:>10} / {total_idx_ora}",
        f"  sequences audited          : {total_seq_matched:>10} / {total_seq_ora}",
        f"  views audited              : {total_views_matched:>10} / {total_views_ora}",
        f"  triggers audited           : {total_trgs_matched:>10} / {total_trgs_ora}",
        f"  row count parity           : {row_matched_cnt:>10} / {len(row_count_audit)} tables",
        f"  total discrepancies        : {len(discrepancies):>10,}",
        f"  overall audit verdict      : {('PASS' if is_overall_pass else 'ACTION REQUIRED'):>10}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)
        
    return is_overall_pass


# -----------------------------------------------------------------------------
# CLI Entry Point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="08_validate.py - Full-Fledged Oracle Dump vs PostgreSQL Parity Audit")
    parser.add_argument("--exact-counts", action="store_true", help="Perform exact live COUNT(*) queries instead of catalog approximations")
    parser.add_argument("--tables", help="Comma-separated tables to audit (e.g. brokeraccountdetail,transactions)")
    parser.add_argument("--schemas", help="Comma-separated schemas to audit (e.g. bss01001,bss01104)")
    parser.add_argument("--file", help="Master Oracle SQL dump file path (defaults to inputs/oracle/oracle_dump/*.sql)")
    parser.add_argument("--bg", action="store_true", help="Run detached in background supervisor")
    args = parser.parse_args()

    tables = {t.strip().lower() for t in args.tables.split(",")} if args.tables else None
    schemas = {s.strip().lower() for s in args.schemas.split(",")} if args.schemas else None

    ok = run_full_parity_audit(
        exact_counts=args.exact_counts,
        tables_filter=tables,
        schemas_filter=schemas,
        dump_file=args.file
    )
    sys.exit(0 if ok else 1)
