#!/usr/bin/env python3
"""
oracle/oracle_06_pipeline.py - SQL pipeline of the Oracle Step 06 loader (one transaction per file).

Every file takes the same path, whatever the table looks like (the source is a relational database, so its
rows can break the same rules everywhere):

    BEGIN
      triggers off for this transaction only (session_replication_role = replica, or DISABLE TRIGGER fallback)
      \\copy the file into an all-text staging table (identity column = record number)
      validate values with pg_input_is_valid (PostgreSQL 16+)           -> BAD_VALUE
      typed staging table WITHOUT NOT NULL / CHECK / defaults
      NOT NULL and CHECK constraints evaluated on the staged rows       -> NOT_NULL / CHECK
      foreign keys checked against the parent tables                    -> FK_MISSING
      duplicates inside the file on the primary key AND every alternate unique key (last row wins)
                                                                        -> DUP_IN_FILE (not a failure)
      rows that collide on an alternate key with a different existing row -> ALT_KEY_CONFLICT
      too many real rejects? -> the file fails and the whole transaction rolls back
      INSERT ... ON CONFLICT (primary or first unique key) DO UPDATE
      row-count check: read = inserted + updated + unchanged + rejected + duplicates, else roll back
      rejected rows -> sweep_ctl.load_rejects, load_log row, triggers back on, COMMIT
    END

Rejected rows never fail the file unless their number reaches `max_rejects` (default 0 = never).
`fast_bulk` keeps the old direct COPY for an empty, constraint-free table (no bad-row isolation).
"""
from sweep_common import ORACLE_LOAD_LOG_TABLE, lit, q
from sweep_loadpipe import (  # re-exported: the Oracle loader and its tests import these from here
    MAX_DUP_ROWS_PRINTED, MAX_REJECT_ROWS_PRINTED, REJECT_TABLE, REJECT_TABLE_DDL, TEXT_TYPES, _cmp,
    ensure_reject_table, load_table_meta, parse_output, persist_failed_file_rejects, probe_trigger_mode, upsert_statements,
)
from sweep_delta import capture_setup_sql, capture_spec, registry_sql
from sweep_s3meta import meta_sql_values

__all__ = [
    "MAX_DUP_ROWS_PRINTED", "MAX_REJECT_ROWS_PRINTED", "REJECT_TABLE", "REJECT_TABLE_DDL", "TEXT_TYPES", "_cmp",
    "ensure_reject_table", "load_table_meta", "parse_output", "persist_failed_file_rejects", "probe_trigger_mode", "upsert_statements",
    "build_fast_script", "build_staged_script", "has_constraints", "load_log_insert", "load_table_meta", "plan_load",
]

# -----------------------------------------------------------------------------
# Catalog metadata (cached per table by the caller)
# -----------------------------------------------------------------------------
def has_constraints(meta):
    return bool(meta["keys"] or meta["checks"] or meta["fks"]
                or any(c["notnull"] for c in meta["columns"].values()))


# -----------------------------------------------------------------------------
# Script builders
# -----------------------------------------------------------------------------
def _is_text(type_name):
    return type_name in TEXT_TYPES


def _trigger_off_on(schema, table, meta, mode):
    tgt = f"{q(schema)}.{q(table)}"
    if mode == "replica":
        return "SET LOCAL session_replication_role = replica;\n", ""
    off = "".join(f"ALTER TABLE {tgt} DISABLE TRIGGER {q(t)};\n" for t in meta["triggers"])
    on = "".join(f"ALTER TABLE {tgt} ENABLE TRIGGER {q(t)};\n" for t in meta["triggers"])
    return off, on


def plan_load(meta, header_cols):
    """Decides keys / validations for this file. Returns (plan, problem) - problem is a message or ''."""
    cols = meta["columns"]
    missing = [c for c in header_cols if c not in cols]
    if missing:
        return None, f"Columns in S3 file not found in table: {', '.join(missing[:5])}"
    generated = [c for c in header_cols if cols[c]["generated"]]
    if generated:
        return None, f"File contains generated column(s) that cannot be loaded: {', '.join(generated)}"
    absent_required = [n for n, c in cols.items()
                       if c["notnull"] and not c["hasdef"] and not c["identity"] and n not in header_cols]
    if absent_required:
        return None, ("NOT NULL column(s) without a default are absent from the file: "
                      + ", ".join(absent_required[:5]))
    present = set(header_cols)
    keys = [k for k in meta["keys"] if set(k["cols"]) <= present]
    arbiter = keys[0] if keys else None
    return {
        "present": present,
        "arbiter": arbiter,
        "alt_keys": [k for k in keys if k is not arbiter],
        "not_null": [c for c in header_cols if cols[c]["notnull"]],
        "checks": [c for c in meta["checks"] if set(c["cols"]) <= present],
        "skipped_checks": [c["name"] for c in meta["checks"] if not set(c["cols"]) <= present],
        "fks": [f for f in meta["fks"] if set(f["cols"]) <= present],
        "identity": any(cols[c]["identity"] for c in header_cols),
    }, ""


def build_fast_script(schema, table, header_cols, stream_cmd, meta, trigger_mode, log_sql):
    """Old behaviour for an empty, constraint-free table: one direct \\copy in one transaction."""
    tgt = f"{q(schema)}.{q(table)}"
    off, on = _trigger_off_on(schema, table, meta, trigger_mode)
    col_list = ", ".join(q(c) for c in header_cols)
    return (
        "\\set ON_ERROR_STOP on\nBEGIN;\n" + off +
        f"\\copy {tgt} ({col_list}) FROM PROGRAM '{stream_cmd}' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n"
        + on + log_sql + "\nCOMMIT;\n")


def build_staged_script(schema, table, header_cols, stream_cmd, meta, plan, trigger_mode, max_rejects, uri, log_sql, *,
                        track="oracle", copy_from=None, keep_target=False, capture=None, pre_sql="", apply_pre_sql="", post_sql="",
                        dry_run_sql=None, raw_cols=None, col_map=None, defaults=None):
    """
    The staged one-transaction script (see the module docstring). Besides Step 06 it is the apply script of `sweep.py promote`:
      copy_from      replaces the `\\copy _raw ... FROM PROGRAM` line (e.g. FROM STDIN for the streamed transport)
      keep_target    conflict policy keep_target: existing keys are not updated
      capture        {run_id, layer, full, uris, downstream}: copy the applied delta to sweep_delta and register it
      pre_sql        right after BEGIN (lock / statement timeouts), apply_pre_sql right before the apply (e.g. TRUNCATE)
      post_sql       after the apply (sequences) ; dry_run_sql: report counts and ROLLBACK instead of applying
      raw_cols       the columns of the FILE in file order (raw staging); col_map {table column: file column} and defaults
                     {table column: SQL expression} come from the FE loader rules (sweep_fwd_rules): header_cols are TABLE columns
    """
    col_map, defaults = col_map or {}, defaults or {}
    rawname = lambda c: col_map.get(c, c)
    tgt = f"{q(schema)}.{q(table)}"
    cols = meta["columns"]
    off, on = _trigger_off_on(schema, table, meta, trigger_mode)
    raw_cols = raw_cols or [rawname(c) for c in header_cols if c not in defaults]
    col_list = ", ".join(q(c) for c in raw_cols)
    raw_defs = ", ".join(f"{q(c)} text" for c in raw_cols)
    s = []
    add = s.append
    add("\\set ON_ERROR_STOP on\n\\pset tuples_only on\n\\pset format unaligned\nBEGIN;\n")
    add(pre_sql)
    add(off)
    add(f"CREATE TEMP TABLE _raw (_row_no bigint GENERATED ALWAYS AS IDENTITY, {raw_defs}) ON COMMIT DROP;\n")
    add(copy_from or f"\\copy _raw ({col_list}) FROM PROGRAM '{stream_cmd}' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n")
    add("CREATE TEMP TABLE _c (k text PRIMARY KEY, n bigint NOT NULL DEFAULT 0) ON COMMIT DROP;\n"
        "CREATE TEMP TABLE _rej (row_no bigint, reason text, detail text, row_data jsonb) ON COMMIT DROP;\n"
        "INSERT INTO _c SELECT 'raw', count(*) FROM _raw;\n")

    # 1. malformed values (type / length) - one pass over the raw rows
    typed = [c for c in header_cols if not _is_text(cols[c]["type"]) and c not in defaults]
    if typed:
        tests = ", ".join(
            f"CASE WHEN NOT pg_input_is_valid(NULLIF({q(rawname(c))}, ''), {lit(cols[c]['type'])}) THEN {lit(c)} END" for c in typed)
        add("INSERT INTO _rej SELECT _row_no, 'BAD_VALUE', array_to_string(bad, ', '), to_jsonb(x) - '_row_no' - 'bad' "
            f"FROM (SELECT r.*, array_remove(ARRAY[{tests}], NULL) AS bad FROM _raw r) x WHERE cardinality(bad) > 0;\n")

    # 2. typed staging table: no NOT NULL, no CHECK, no defaults
    sel = []
    for c in header_cols:
        t = cols[c]["type"]
        r = q(rawname(c))
        if c in defaults:
            sel.append(f"({defaults[c]})::{t} AS {q(c)}")
        else:
            sel.append(f"{r}::text AS {q(c)}" if _is_text(t) and t == "text"
                       else f"{r}::{t} AS {q(c)}" if _is_text(t)
                       else f"NULLIF({r}, '')::{t} AS {q(c)}")
    add(f"CREATE TEMP TABLE _stg ON COMMIT DROP AS SELECT _row_no, {', '.join(sel)} FROM _raw r "
        "WHERE NOT EXISTS (SELECT 1 FROM _rej b WHERE b.row_no = r._row_no);\nDROP TABLE _raw;\n")

    # 3. NOT NULL + CHECK in one pass
    tests = [f"CASE WHEN {q(c)} IS NULL THEN {lit('NOT_NULL:' + c)} END" for c in plan["not_null"]]
    tests += [f"CASE WHEN (({ck['expr']})) IS FALSE THEN {lit('CHECK:' + ck['name'])} END" for ck in plan["checks"]]
    if tests:
        add("INSERT INTO _rej SELECT x._row_no, split_part(r, ':', 1), substr(r, strpos(r, ':') + 1), x.js FROM "
            f"(SELECT s._row_no, to_jsonb(s) - '_row_no' AS js, array_remove(ARRAY[{', '.join(tests)}], NULL) AS reasons "
            "FROM _stg s) x CROSS JOIN LATERAL unnest(x.reasons) AS r;\n")

    # 4. foreign keys
    for fk in plan["fks"]:
        child = " AND ".join(f"s.{q(c)} IS NOT NULL" for c in fk["cols"])
        match = " AND ".join(f"p.{q(pc)} = s.{q(c)}" for c, pc in zip(fk["cols"], fk["parent_cols"]))
        extra = ""
        if fk["self_ref"]:
            extra = (" AND NOT EXISTS (SELECT 1 FROM _stg p WHERE "
                     + " AND ".join(f"p.{q(pc)} = s.{q(c)}" for c, pc in zip(fk["cols"], fk["parent_cols"])) + ")")
        add("INSERT INTO _rej SELECT s._row_no, 'FK_MISSING', "
            f"{lit(fk['name'] + ' -> ' + fk['parent'] + '(' + ','.join(fk['parent_cols']) + ')')}, to_jsonb(s) - '_row_no' "
            f"FROM _stg s WHERE {child} AND NOT EXISTS (SELECT 1 FROM {fk['parent']} p WHERE {match}){extra};\n")

    # drop everything rejected so far from the staging table
    add("DELETE FROM _stg s WHERE EXISTS (SELECT 1 FROM _rej r WHERE r.row_no = s._row_no);\n")

    # 5. duplicates inside the file: primary key first, then every alternate unique key (last row wins)
    for key in ([plan["arbiter"]] if plan["arbiter"] else []) + plan["alt_keys"]:
        kc = ", ".join(q(c) for c in key["cols"])
        notnull = " AND ".join(f"{q(c)} IS NOT NULL" for c in key["cols"])
        add("WITH d AS (DELETE FROM _stg s USING (SELECT _row_no FROM (SELECT _row_no, row_number() OVER "
            f"(PARTITION BY {kc} ORDER BY _row_no DESC) AS rn FROM _stg WHERE {notnull}) w WHERE rn > 1) l "
            "WHERE s._row_no = l._row_no RETURNING s._row_no, to_jsonb(s) - '_row_no' AS js) "
            f"INSERT INTO _rej SELECT _row_no, 'DUP_IN_FILE', {lit('key ' + key['name'] + ' (' + ','.join(key['cols']) + ')')}, js FROM d;\n")

    # 6. alternate keys already used by a DIFFERENT row of the table
    if plan["alt_keys"] and plan["arbiter"]:
        a_cols = plan["arbiter"]["cols"]
        for key in plan["alt_keys"]:
            join = " AND ".join(f"t.{q(c)} = s.{q(c)}" for c in key["cols"])
            diff = (f"({', '.join('t.' + q(c) for c in a_cols)}) IS DISTINCT FROM "
                    f"({', '.join('s.' + q(c) for c in a_cols)})")
            add("INSERT INTO _rej SELECT s._row_no, 'ALT_KEY_CONFLICT', "
                f"{lit('key ' + key['name'] + ' (' + ','.join(key['cols']) + ') is used by another row')}, to_jsonb(s) - '_row_no' "
                f"FROM _stg s JOIN {tgt} t ON {join} WHERE {diff};\n")
        add("DELETE FROM _stg s WHERE EXISTS (SELECT 1 FROM _rej r WHERE r.row_no = s._row_no);\n")

    # 7. report what was found (echoed to the client even if the file then fails), then the threshold
    add("INSERT INTO _c SELECT 'rejected', count(*) FROM _rej WHERE reason <> 'DUP_IN_FILE';\n"
        "INSERT INTO _c SELECT 'dup', count(*) FROM _rej WHERE reason = 'DUP_IN_FILE';\n"
        "INSERT INTO _c SELECT 'staged', count(*) FROM _stg;\n"
        "SELECT '@@COUNTS@@' || json_object_agg(k, n) FROM _c;\n"
        "SELECT '@@REJ_SUMMARY@@' || json_build_object('reason', reason, 'detail', detail, 'n', n) "
        "FROM (SELECT reason, detail, count(*) AS n FROM _rej GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 200) g;\n"
        "SELECT '@@REJ_ROW@@' || json_build_object('row_no', row_no, 'reason', reason, 'detail', detail, 'row', row_data) "
        f"FROM (SELECT * FROM _rej WHERE reason <> 'DUP_IN_FILE' ORDER BY row_no LIMIT {MAX_REJECT_ROWS_PRINTED}) a;\n"
        "SELECT '@@REJ_ROW@@' || json_build_object('row_no', row_no, 'reason', reason, 'detail', detail, 'row', row_data) "
        f"FROM (SELECT * FROM _rej WHERE reason = 'DUP_IN_FILE' ORDER BY row_no LIMIT {MAX_DUP_ROWS_PRINTED}) a;\n")
    if max_rejects and max_rejects > 0:
        add("DO $$ DECLARE n bigint; msg text; BEGIN "
            "SELECT c.n INTO n FROM _c c WHERE c.k = 'rejected'; "
            f"IF n >= {int(max_rejects)} THEN "
            "SELECT string_agg('record ' || row_no || ' ' || reason || ' ' || coalesce(detail, ''), '; ') INTO msg "
            "FROM (SELECT * FROM _rej WHERE reason <> 'DUP_IN_FILE' ORDER BY row_no LIMIT 5) f; "
            f"RAISE EXCEPTION 'Too many rejected rows: % (limit {int(max_rejects)}). First: %', n, msg; END IF; END $$;\n")

    if dry_run_sql is not None:                       # promote --dry-run: counts only, nothing is applied
        add(dry_run_sql)
        add("ROLLBACK;\n")
        return "".join(s)

    # 8. apply: INSERT ... ON CONFLICT on the arbiter key, updating only rows that differ
    cap_spec = None
    if capture:
        add(capture_setup_sql(schema, table, capture.get("full", False), seq=capture.get("seq"), strict=capture.get("strict", True)))
        cap_spec = capture_spec(schema, table) if capture.get("downstream", True) else None
    add(apply_pre_sql)
    for stmt in upsert_statements(tgt, header_cols, cols, plan["arbiter"], plan["present"], plan["identity"],
                                  keep_target=keep_target, capture=cap_spec, no_update=set(defaults)):
        add(stmt)

    # 9. nothing may disappear silently
    add("DO $$ DECLARE raw_n bigint; acc bigint; BEGIN "
        "SELECT n INTO raw_n FROM _c WHERE k = 'raw'; "
        "SELECT sum(n) INTO acc FROM _c WHERE k IN ('inserted', 'updated', 'unchanged', 'rejected', 'dup'); "
        "IF raw_n IS DISTINCT FROM acc THEN RAISE EXCEPTION 'Row count check failed: % rows read but % accounted for "
        "(inserted + updated + unchanged + rejected + duplicates)', raw_n, acc; END IF; END $$;\n"
        "SELECT '@@COUNTS@@' || json_object_agg(k, n) FROM _c;\n")

    # 10. persist rejects + log, restore triggers, commit
    add(f"DELETE FROM {REJECT_TABLE} WHERE uri = {lit(uri)} AND target_schema = {lit(schema)} AND target_table = {lit(table)};\n"
        f"INSERT INTO {REJECT_TABLE} (track, uri, target_schema, target_table, row_no, reason, detail, row_data, file_status) "
        f"SELECT {lit(track)}, {lit(uri)}, {lit(schema)}, {lit(table)}, row_no, reason, detail, row_data, 'LOADED' FROM "
        f"(SELECT * FROM _rej WHERE reason <> 'DUP_IN_FILE' UNION ALL "
        f"(SELECT * FROM _rej WHERE reason = 'DUP_IN_FILE' ORDER BY row_no LIMIT {MAX_REJECT_ROWS_PRINTED})) z;\n")
    if capture:
        add(registry_sql(schema, table, capture["run_id"], capture.get("uris", uri), header_cols, capture.get("layer"),
                         "(SELECT n FROM _c WHERE k = 'inserted')", "(SELECT n FROM _c WHERE k = 'updated')",
                         keep_delta=capture.get("downstream", True), message=capture.get("message")))
    add(post_sql)
    add(on)
    add(log_sql + "\nCOMMIT;\n")
    return "".join(s)


def load_log_insert(uri, schema, table, size_bytes, file_format, worker_id, s3_meta=None):
    """load_log row of the file (one row per URI, refreshed on a reload) incl. the S3 metadata it was loaded with."""
    lm, etag, size = meta_sql_values(s3_meta)
    return (f"INSERT INTO {ORACLE_LOAD_LOG_TABLE} (uri, target_schema, target_table, size_bytes, file_format, worker_id, status, "
            f"s3_last_modified, s3_etag, s3_size) "
            f"VALUES ({lit(uri)}, {lit(schema)}, {lit(table)}, {size_bytes}, {lit(file_format)}, {worker_id}, 'SUCCESS', "
            f"{lm}, {etag}, {size}) "
            f"ON CONFLICT (uri) DO UPDATE SET loaded_at = now(), status = 'SUCCESS', size_bytes = EXCLUDED.size_bytes, "
            f"s3_last_modified = EXCLUDED.s3_last_modified, s3_etag = EXCLUDED.s3_etag, s3_size = EXCLUDED.s3_size;")

