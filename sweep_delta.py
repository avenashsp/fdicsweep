#!/usr/bin/env python3
"""
sweep_delta.py - capturing the APPLIED delta of a load, and the registry that orders deltas along the layer chain.

While bronze loads a file, the rows that were really applied (inserted or changed, after the "only if different" filter)
are copied into an UNLOGGED table of the sweep_delta schema of the same database: the table's columns + `_seq` (the delta's
sequence number), `_op` ('I' / 'U') and `_row_no`. The delta is registered in sweep_ctl.delta_registry with a strictly
increasing sequence number per table. `sweep.py promote` moves it to the next layer, which applies it in sequence order and
records its own delta for the hop after it.

If the target table was EMPTY before the load (a Day-1 style load) or `--full` is given, nothing is copied: the registry row
says `full_copy` and the promote hop copies the whole source table (no doubled storage).
"""
from sweep_common import CTL_SCHEMA, lit, q, query_db_dicts, run_psql

DELTA_SCHEMA = "sweep_delta"
REGISTRY = f"{CTL_SCHEMA}.delta_registry"
# audit columns the TOOL sets itself (never copied between layers); createddate / createdby / modifieddate / modifiedby can be
# source data, so they travel with the rows when a file or delta carries them
from sweep_audit import AUDIT_SET
TOOL_AUDIT_COLS = AUDIT_SET      # the six canonical audit columns (never compared by delta / self-compare)


def delta_table_name(schema, table):
    """sweep_delta."<schema>__<table>" (quoted)."""
    return f"{q(DELTA_SCHEMA)}.{q(f'{schema}__{table}')}"


def ensure_delta_table(schema, table, dbname=None):
    """Creates / extends the unlogged delta table of schema.table (columns follow the target table, all nullable)."""
    d = delta_table_name(schema, table)
    short = f"{schema}__{table}"
    tgt = f"{q(schema)}.{q(table)}"
    run_psql(
        f"CREATE SCHEMA IF NOT EXISTS {q(DELTA_SCHEMA)};\n"
        f"CREATE UNLOGGED TABLE IF NOT EXISTS {d} AS SELECT * FROM {tgt} WITH NO DATA;\n"
        f"ALTER TABLE {d} ADD COLUMN IF NOT EXISTS _seq bigint, ADD COLUMN IF NOT EXISTS _op text, ADD COLUMN IF NOT EXISTS _row_no bigint;\n"
        f"CREATE INDEX IF NOT EXISTS {q('ix_' + short[:55] + '_seq')} ON {d} (_seq);\n"
        # columns added to the table later (controlled DDL) are added to the delta table too
        "DO $d$ DECLARE c record; BEGIN FOR c IN SELECT a.attname AS n, format_type(a.atttypid, a.atttypmod) AS t "
        f"FROM pg_attribute a WHERE a.attrelid = {lit(tgt)}::regclass AND a.attnum > 0 AND NOT a.attisdropped "
        f"AND NOT EXISTS (SELECT 1 FROM pg_attribute b WHERE b.attrelid = {lit(d)}::regclass AND b.attname = a.attname AND NOT b.attisdropped) "
        f"LOOP EXECUTE format('ALTER TABLE {d} ADD COLUMN %I %s', c.n, c.t); END LOOP; END $d$;",
        dbname=dbname)
    return d


def capture_setup_sql(schema, table, full_flag=False, name="_cap", seq=None, strict=True):
    """
    Statements run inside the load transaction BEFORE the upsert: serialize the table's deltas (the advisory lock is held until
    COMMIT, so sequence numbers follow the order the data was applied in) and decide full copy vs captured delta.
    Creates the temp table `name` (default _cap) with the columns (full_copy, seq).
    `seq` fixes the sequence number (a promote hop keeps the number the delta has in the source layer); with `strict` the
    transaction fails unless it is exactly the next number of the table (deltas are applied in strict sequence order).
    """
    tgt = f"{q(schema)}.{q(table)}"
    last = (f"COALESCE((SELECT max(seq) FROM {REGISTRY} WHERE target_schema = {lit(schema)} AND target_table = {lit(table)}), 0)")
    seq_expr = f"{int(seq)}" if seq is not None else f"{last} + 1"
    check = ""
    if seq is not None and strict:
        check = (f"DO $chk$ BEGIN IF {int(seq)} <> {last} + 1 THEN RAISE EXCEPTION "
                 f"'sequence gap: delta {int(seq)} of {schema}.{table} cannot be applied after delta %', {last}; END IF; END $chk$;\n")
    return (
        f"SELECT pg_advisory_xact_lock(hashtextextended({lit('sweep_delta:' + schema + '.' + table)}, 0));\n"
        + check +
        f"CREATE TEMP TABLE {name} ON COMMIT DROP AS SELECT ({'true' if full_flag else 'false'} OR NOT EXISTS (SELECT 1 FROM {tgt})) AS full_copy, "
        f"{seq_expr} AS seq;\n")


def capture_spec(schema, table, name="_cap"):
    """The `capture` argument of sweep_loadpipe.upsert_statements for schema.table."""
    return {"delta": delta_table_name(schema, table), "seq": f"(SELECT seq FROM {name})", "skip": f"(SELECT full_copy FROM {name})"}


def registry_sql(schema, table, run_id, uris, columns, origin_layer, inserted_sql, updated_sql, seq_sql="(SELECT seq FROM _cap)",
                 full_sql="(SELECT full_copy FROM _cap)", keep_delta=True, message=None):
    """
    The delta_registry row of the applied delta, written in the same transaction as the data. Skipped when nothing was applied
    (no inserted and no updated rows: there is nothing to promote). `inserted_sql` / `updated_sql` are SQL expressions.
    """
    delta = delta_table_name(schema, table) if keep_delta else None
    delta_expr = f"CASE WHEN {full_sql} THEN NULL ELSE {lit(delta)} END" if delta else "NULL"
    return (
        f"INSERT INTO {REGISTRY} (run_id, seq, target_schema, target_table, origin_layer, source_uris, rows_inserted, rows_updated, "
        f"rows_total, full_copy, delta_table, columns, status, message) "
        f"SELECT {lit(run_id)}, {seq_sql}, {lit(schema)}, {lit(table)}, {lit(origin_layer)}, {lit(uris)}, {inserted_sql}, {updated_sql}, "
        f"{inserted_sql} + {updated_sql}, {full_sql}, {delta_expr}, {lit(','.join(columns))}, 'SUCCESS', {lit(message)} "
        f"WHERE {inserted_sql} + {updated_sql} > 0;\n")


def registry_rows(schema=None, table=None, dbname=None):
    """Registry rows (dicts) of the connected database, optionally of one table, ordered by table and sequence."""
    where = ""
    if schema and table:
        where = f"WHERE target_schema = {lit(schema)} AND target_table = {lit(table)}"
    return query_db_dicts(f"SELECT * FROM {REGISTRY} {where} ORDER BY target_schema, target_table, seq", dbname=dbname)


def last_applied_seq(schema, table, dbname=None):
    rows = query_db_dicts(
        f"SELECT COALESCE(max(seq), 0) AS n FROM {REGISTRY} WHERE target_schema = {lit(schema)} AND target_table = {lit(table)} "
        f"AND status = 'SUCCESS'", dbname=dbname)
    return int(rows[0]["n"]) if rows else 0


def fast_registry_sql(schema, table, run_id, uri, columns, origin_layer, rows_sql=":n"):
    """
    Registry row for the --fast-bulk path (direct COPY into a bare table, empty or not): the file is ALWAYS registered as a FULL COPY
    (no delta is captured; the promote hop copies the source table), rows_inserted = the rows this file copied, rows_updated = 0.
    `rows_sql` is the number of rows COPIED by the file (the psql variable :n, set from ROW_COUNT right after the \\copy) - the target
    table is never counted. The sequence number is taken under the same advisory lock the staged path uses, right before the insert,
    so part files of one table that load at the same time copy in parallel and only queue for their sequence numbers.
    """
    key = lit('sweep_delta:' + schema + '.' + table)
    return (
        f"DO $lk$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended({key}, 0)); END $lk$;\n"
        f"INSERT INTO {REGISTRY} (run_id, seq, target_schema, target_table, origin_layer, source_uris, rows_inserted, rows_updated, "
        f"rows_total, full_copy, delta_table, columns, status) "
        f"SELECT {lit(run_id)}, COALESCE((SELECT max(seq) FROM {REGISTRY} WHERE target_schema = {lit(schema)} "
        f"AND target_table = {lit(table)}), 0) + 1, {lit(schema)}, {lit(table)}, {lit(origin_layer)}, {lit(uri)}, {rows_sql}, 0, {rows_sql}, true, NULL, "
        f"{lit(','.join(columns))}, 'SUCCESS' WHERE {rows_sql} > 0;\n")
