#!/usr/bin/env python3
"""
vsam/common_schema.py - Target table DDL building and live PostgreSQL inspection / load-strategy decisions.
"""
import sweep_audit
from sweep_common import DB_ERRORS, norm_col, q, query_db_rows
from vsam.common_config import logger


AUDIT_COLS = sweep_audit.create_table_lines()      # the six canonical audit columns (sweep_audit)


AUDIT_COL_NAMES = set(sweep_audit.AUDIT_NAMES)


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
        + "\n);\n"
        + sweep_audit.trigger_sql(schema, table)
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


# -----------------------------------------------------------------------------
# DDL Generation with Native Application Audit Columns
# -----------------------------------------------------------------------------
def table_ddl(schema, table, p):
    """
    CREATE TABLE IF NOT EXISTS for target table.
    Natively embeds the six canonical audit columns (see sweep_audit / docs/audit_columns.md).
    """
    key = p.get("key", [])
    lines = [f'  {q(c["name"])} {c["sql_type"]}' + (" NOT NULL" if c["name"] in key else "") for c in p["columns"]]

    lines.extend(sweep_audit.create_table_lines())      # the six canonical audit columns

    if key:
        lines.append(f"  CONSTRAINT {q('pk_' + table)} PRIMARY KEY (" + ", ".join(q(k) for k in key) + ")")

    return f"CREATE TABLE IF NOT EXISTS {q(schema)}.{q(table)} (\n" + ",\n".join(lines) + "\n);"
