#!/usr/bin/env python3
"""
vsam/common_schema.py - Target table DDL building and live PostgreSQL inspection / load-strategy decisions.
"""
from sweep_common import DB_ERRORS, norm_col, q, query_db_rows, run_psql
from vsam.common_config import logger


AUDIT_COLS = [
    '  "createddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "createdby" character varying(40) DEFAULT CURRENT_USER',
    '  "modifieddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "modifiedby" character varying(40) DEFAULT CURRENT_USER',
    '  "createddate_m" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP',
    '  "createdby_m" character varying(40) NOT NULL DEFAULT CURRENT_USER',
]


AUDIT_COL_NAMES = {
    "createddate", "createdby", "modifieddate", "modifiedby",
    "createddate_m", "createdby_m"
}


def build_table_ddl(schema: str, table: str, columns: list, key_cols: list):
    """
    Returns (pre_load_sql, post_load_sql) tuple.
    pre_load: CREATE TABLE without PK constraint.
    post_load: ALTER TABLE ADD CONSTRAINT pk.
    Deduplicates column names (case-insensitive) — first occurrence wins.
    Skips data columns whose names clash with reserved audit column names.
    """
    col_lines = []
    seen_cols = set()
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


def check_table_exists(schema: str, table: str) -> bool:
    """Checks if a table exists in PostgreSQL live catalog."""
    sql = f"""
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = '{schema.lower()}' AND table_name = '{table.lower()}';
    """
    try:
        rows = query_db_rows(sql)
        return len(rows) > 0
    except DB_ERRORS as e:
        logger.debug(f"Could not check existence of table {schema}.{table}: {e}")
        return False


def get_table_pk_columns_live(schema: str, table: str) -> list:
    """
    Queries PostgreSQL live catalog to check if schema.table has an active PRIMARY KEY constraint.
    Returns list of primary key column names (in ordinal order), or [] if no PK exists.
    """
    sql = f"""
    SELECT a.attname
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
    WHERE n.nspname = '{schema.lower()}'
      AND c.relname = '{table.lower()}'
      AND i.indisprimary
    ORDER BY array_position(i.indkey, a.attnum);
    """
    try:
        rows = query_db_rows(sql)
        return [r.strip() for r in rows if r.strip()]
    except DB_ERRORS as e:
        logger.warning(f"Could not check PK for {schema}.{table}: e")
        logger.debug(f"Could not check PK for {schema}.{table}: {e}")
        return []


def table_has_existing_data(schema: str, table: str) -> bool:
    """
    Checks if schema.table currently has at least 1 row in PostgreSQL.
    """
    sql = f"SELECT 1 FROM {q(schema)}.{q(table)} LIMIT 1;"
    try:
        rows = query_db_rows(sql)
        return len(rows) > 0
    except DB_ERRORS as e:
        logger.debug(f"Could not check row existence for {schema}.{table}: {e}")
        return False


def check_table_load_strategy(schema: str, table: str, tg_prof_pk: list = None, mode: str = "default") -> dict:
    """
    Inspects PostgreSQL live for schema.table to determine load strategy:
    Mode: 'default'
      - If table is fresh (no existing data):
        * If table has PK defined, apply PK constraint to table in PostgreSQL!
        * Set action = 'INSERT_FRESH', has_pk = True, pk_cols = live_pk_cols
      - If table has existing data and YES PK constraint -> action = 'UPSERT', has_pk = True
      - If table has existing data and NO PK constraint -> action = 'TRUNCATE_AND_LOAD', has_pk = False

    Mode: 'data'
      - If table is fresh (no existing data):
        * Do NOT apply PK constraints. Load data only.
        * Set action = 'DIRECT_INSERT', has_pk = False, pk_cols = []
      - If table has existing data and YES PK constraint -> action = 'UPSERT', has_pk = True
      - If table has existing data and NO PK constraint -> action = 'TRUNCATE_AND_LOAD', has_pk = False
    """
    has_data = table_has_existing_data(schema, table)
    live_pk_cols = get_table_pk_columns_live(schema, table)
    has_pk = bool(live_pk_cols)
    candidate_pks = [norm_col(c) for c in (tg_prof_pk or []) if c]
    mode_clean = (mode or "default").lower().strip()

    if not has_data:
        # Table is fresh (0 rows)
        if mode_clean != "data" and candidate_pks and not has_pk:
            # Default mode: apply PK constraint to the fresh table before data load
            try:
                pk_name = q(f"pk_{table}")
                pk_def = ", ".join(q(k) for k in candidate_pks)
                alter_sql = f"ALTER TABLE {q(schema)}.{q(table)} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_def});"
                run_psql(alter_sql)
                logger.info(f"Applied PK constraint on fresh table {schema}.{table} ({', '.join(candidate_pks)})")
                live_pk_cols = get_table_pk_columns_live(schema, table)
                has_pk = bool(live_pk_cols)
            except DB_ERRORS as e:
                logger.warning(f"Could not apply PK constraint to {schema}.{table}: {e}")
                has_pk = False

        if has_pk:
            action = "INSERT_FRESH"
        else:
            action = "DIRECT_INSERT"
    elif has_pk:
        # Table has existing data and has PK constraint in DB
        action = "UPSERT"
    else:
        # Table has existing data and NO PK constraint in DB
        action = "TRUNCATE_AND_LOAD"

    return {
        "has_data": has_data,
        "has_pk": has_pk,
        "pk_cols": live_pk_cols if has_pk else [],
        "action": action
    }


# -----------------------------------------------------------------------------
# DDL Generation with Native Application Audit Columns
# -----------------------------------------------------------------------------
def table_ddl(schema, table, p):
    """
    CREATE TABLE IF NOT EXISTS for target table.
    Natively embeds the FDICSweep application audit columns:
      createdby_m   varchar(40) NOT NULL DEFAULT CURRENT_USER
      createddate_m timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
      modifiedby    varchar(40) DEFAULT CURRENT_USER
      modifieddate  timestamp DEFAULT CURRENT_TIMESTAMP
    """
    key = p.get("key", [])
    lines = [f'  {q(c["name"])} {c["sql_type"]}' + (" NOT NULL" if c["name"] in key else "") for c in p["columns"]]

    # FDICSweep Standard Application Audit Columns (All 6 native audit columns)
    lines.extend([
        "  \"createddate\" timestamp without time zone DEFAULT CURRENT_TIMESTAMP",
        "  \"createdby\" character varying(40) DEFAULT CURRENT_USER",
        "  \"modifieddate\" timestamp without time zone DEFAULT CURRENT_TIMESTAMP",
        "  \"modifiedby\" character varying(40) DEFAULT CURRENT_USER",
        "  \"createddate_m\" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "  \"createdby_m\" character varying(40) NOT NULL DEFAULT CURRENT_USER"
    ])

    if key:
        lines.append(f"  CONSTRAINT {q('pk_' + table)} PRIMARY KEY (" + ", ".join(q(k) for k in key) + ")")

    return f"CREATE TABLE IF NOT EXISTS {q(schema)}.{q(table)} (\n" + ",\n".join(lines) + "\n);"
