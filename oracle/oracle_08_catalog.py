#!/usr/bin/env python3
"""
oracle_08_catalog.py - PostgreSQL catalog access for the Step 08 parity audit.

One small function per catalog (schemas, tables, columns, constraints, indexes, sequences,
views, triggers, load log, exact counts) feeding `fetch_target_postgres_catalogs`.
"""

from sweep_common import DB_ERRORS, get_logger, LOAD_LOG_TABLE, PGDATABASE, PGHOST, PGPORT, PGUSER

logger = get_logger("oracle_08_validate", "oracle_08_validate")


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


def fetch_schemas(catalog):
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


def fetch_tables(catalog):
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


def fetch_columns(catalog):
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


def fetch_constraints(catalog):
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


def fetch_indexes(catalog):
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


def fetch_sequences(catalog):
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


def fetch_views(catalog):
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


def fetch_triggers(catalog):
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


def fetch_load_log(catalog):
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
    # per-file S3 metadata (LastModified / ETag / size) as loaded, for the re-delivered-file comparison
    try:
        catalog["load_log_files"] = execute_db_query(
            f"SELECT uri, s3_last_modified, s3_etag, s3_size FROM {LOAD_LOG_TABLE} WHERE status = 'SUCCESS'")
    except DB_ERRORS as e:
        logger.warning(f"Could not read S3 metadata from {LOAD_LOG_TABLE} ({e}); re-delivered files cannot be detected")
        catalog["load_log_files"] = []


def fetch_exact_counts(catalog, exact_counts, schemas_filter, tables_filter):
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
        "load_log_files": [],
        "exact_row_counts": {}
    }

    fetch_schemas(catalog)

    fetch_tables(catalog)

    fetch_columns(catalog)

    fetch_constraints(catalog)

    fetch_indexes(catalog)

    fetch_sequences(catalog)

    fetch_views(catalog)

    fetch_triggers(catalog)

    fetch_load_log(catalog)

    fetch_exact_counts(catalog, exact_counts, schemas_filter, tables_filter)

    return catalog
