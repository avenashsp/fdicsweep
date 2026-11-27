#!/usr/bin/env python3
"""
sweep_loadpipe.py - building blocks shared by the Oracle (Step 06) and VSAM (Step 06) staged loaders.

Both tracks load a file in ONE transaction through temporary tables (raw text -> validated -> typed -> upsert),
report bad rows in sweep_ctl.load_rejects and reconcile the row counts. This module holds the parts that are
identical: the reject table, trigger handling probe, output markers, and the "update only rows that differ" upsert.
"""
import json
import subprocess
import tempfile

from sweep_common import CTL_SCHEMA, lit, q, query_db_dicts, run_psql
from sweep_platform import group_popen_kwargs, kill_process_group

REJECT_TABLE = f"{CTL_SCHEMA}.load_rejects"
MAX_REJECT_ROWS_PRINTED = 1000       # rejected rows echoed back per file (and stored when the file fails)
MAX_DUP_ROWS_PRINTED = 200
TEXT_TYPES = ("text", "character varying")                  # unbounded text needs no validation / cast

REJECT_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {REJECT_TABLE} (
    id            bigserial PRIMARY KEY,
    track         text,
    uri           text NOT NULL,
    target_schema text,
    target_table  text,
    row_no        bigint,
    reason        text NOT NULL,
    detail        text,
    row_data      jsonb,
    file_status   text DEFAULT 'LOADED',
    rejected_at   timestamptz DEFAULT now()
);
ALTER TABLE {REJECT_TABLE} ADD COLUMN IF NOT EXISTS track text;
CREATE INDEX IF NOT EXISTS idx_sweep_load_rejects_uri ON {REJECT_TABLE} (uri);
"""


def ensure_reject_table():
    run_psql(REJECT_TABLE_DDL)


# -----------------------------------------------------------------------------
# Catalog metadata of a target table
# -----------------------------------------------------------------------------
def load_table_meta(schema, table):
    """Columns, unique keys, CHECK / FK constraints and enabled user triggers of schema.table."""
    reg = f"to_regclass({lit(q(schema) + '.' + q(table))})"
    cols = query_db_dicts(
        f"SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull AS notnull, "
        f"a.atthasdef AS hasdef, a.attidentity AS identity, a.attgenerated AS generated "
        f"FROM pg_attribute a WHERE a.attrelid = {reg} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum")
    keys = query_db_dicts(
        f"SELECT c.relname AS name, i.indisprimary AS is_primary, "
        f"(SELECT string_agg(a.attname, ',' ORDER BY k.ord) FROM unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, ord) "
        f" JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum) AS cols "
        f"FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
        f"WHERE i.indrelid = {reg} AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL "
        f"ORDER BY i.indisprimary DESC, c.relname")
    checks = query_db_dicts(
        f"SELECT conname AS name, pg_get_expr(conbin, conrelid) AS expr, "
        f"(SELECT string_agg(a.attname, ',') FROM pg_attribute a WHERE a.attrelid = conrelid AND a.attnum = ANY(conkey)) AS cols "
        f"FROM pg_constraint WHERE conrelid = {reg} AND contype = 'c' ORDER BY conname")
    fks = query_db_dicts(
        f"SELECT conname AS name, confrelid::regclass::text AS parent, confrelid = conrelid AS self_ref, "
        f"(SELECT string_agg(a.attname, ',' ORDER BY u.ord) FROM unnest(conkey) WITH ORDINALITY u(attnum, ord) "
        f" JOIN pg_attribute a ON a.attrelid = conrelid AND a.attnum = u.attnum) AS cols, "
        f"(SELECT string_agg(a.attname, ',' ORDER BY u.ord) FROM unnest(confkey) WITH ORDINALITY u(attnum, ord) "
        f" JOIN pg_attribute a ON a.attrelid = confrelid AND a.attnum = u.attnum) AS parent_cols "
        f"FROM pg_constraint WHERE conrelid = {reg} AND contype = 'f' ORDER BY conname")
    triggers = query_db_dicts(
        f"SELECT tgname AS name FROM pg_trigger WHERE tgrelid = {reg} AND NOT tgisinternal AND tgenabled = 'O' ORDER BY tgname")
    # everything that makes a table NOT bare (the direct-COPY path needs a bare table): ALL indexes and ALL non-internal triggers of the
    # table and of its partitions (any depth), whatever their state - unique or not, partial, expression, valid or invalid; enabled or disabled
    family = (f"WITH RECURSIVE rel(oid, name) AS (SELECT {reg}::oid, {reg}::text WHERE {reg} IS NOT NULL UNION ALL "
              f"SELECT i.inhrelid, i.inhrelid::regclass::text FROM pg_inherits i JOIN rel ON i.inhparent = rel.oid) ")
    indexes = query_db_dicts(
        family + "SELECT c.relname AS name, rel.name AS on_rel, ix.indisunique AS uniq, ix.indpred IS NOT NULL AS partial, "
                 "ix.indexprs IS NOT NULL AS expr, ix.indisvalid AS valid FROM pg_index ix JOIN pg_class c ON c.oid = ix.indexrelid "
                 "JOIN rel ON rel.oid = ix.indrelid ORDER BY c.relname")
    all_triggers = query_db_dicts(
        family + "SELECT t.tgname AS name, rel.name AS on_rel, t.tgenabled AS state FROM pg_trigger t JOIN rel ON rel.oid = t.tgrelid "
                 "WHERE NOT t.tgisinternal ORDER BY t.tgname")
    flag = lambda v: str(v).strip().lower() in ("t", "true", "1")
    return {
        "columns": {c["name"]: {"type": c["type"], "notnull": flag(c["notnull"]), "hasdef": flag(c["hasdef"]),
                                "identity": c["identity"], "generated": c["generated"]} for c in cols},
        "keys": [{"name": k["name"], "primary": flag(k["is_primary"]), "cols": k["cols"].split(",")} for k in keys if k["cols"]],
        "checks": [{"name": c["name"], "expr": c["expr"], "cols": (c["cols"] or "").split(",") if c["cols"] else []} for c in checks],
        "fks": [{"name": f["name"], "parent": f["parent"], "self_ref": flag(f["self_ref"]),
                 "cols": f["cols"].split(","), "parent_cols": f["parent_cols"].split(",")} for f in fks if f["cols"]],
        "triggers": [t["name"] for t in triggers],
        "indexes": [{"name": i["name"], "on": i["on_rel"], "unique": flag(i["uniq"]), "partial": flag(i["partial"]), "expr": flag(i["expr"]),
                     "valid": flag(i["valid"])} for i in indexes],
        "all_triggers": [{"name": t["name"], "on": t["on_rel"], "enabled": t["state"] != "D"} for t in all_triggers],
    }


def tables_with_rows(pairs, chunk=150):
    """
    {(schema, table)} of the given tables that hold at least one row - one query per `chunk` tables. A table that does not
    exist counts as empty. Used by the "already loaded" rule: a file only counts as loaded while its target is not empty
    (a table somebody truncated by hand must be reloaded).
    """
    pairs = list(dict.fromkeys((s.lower(), t.lower()) for s, t in pairs))
    found = set()
    for i in range(0, len(pairs), chunk):
        part = pairs[i:i + chunk]
        sql = " UNION ALL ".join(
            f"SELECT {lit(s)} AS s, {lit(t)} AS t WHERE EXISTS (SELECT 1 FROM {q(s)}.{q(t)})" for s, t in part)
        try:
            found |= {(r["s"], r["t"]) for r in query_db_dicts(sql)}
        except RuntimeError:                         # one missing table must not hide the others: ask table by table
            for s, t in part:
                try:
                    if query_db_dicts(f"SELECT 1 AS x FROM {q(s)}.{q(t)} LIMIT 1"):
                        found.add((s, t))
                except RuntimeError:
                    pass
    return found


def probe_trigger_mode():
    """'replica' when this database user may SET session_replication_role, otherwise 'disable'."""
    try:
        run_psql("BEGIN;\nSET LOCAL session_replication_role = replica;\nROLLBACK;")
        return "replica"
    except RuntimeError:
        return "disable"


def _cmp(expr, type_name):
    """Comparison operand: types without an equality operator (json, xml) are compared as text."""
    return f"{expr}::text" if type_name in ("json", "xml") or type_name.startswith("point") else expr


def upsert_statements(tgt, header_cols, cols, arbiter, present, identity, stg="_stg", where="", sfx="", existing="_existing",
                      keep_target=False, capture=None, no_update=()):
    """
    SQL statements that apply the staged rows `stg` to `tgt`:
      INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE (target columns) IS DISTINCT FROM (file columns)
    and record 'inserted' / 'updated' / 'unchanged' (keys suffixed with `sfx`) in the temp table _c(k, n).
    `where` restricts the staged rows (e.g. one destination schema). Columns that come FROM the file (modifieddate /
    modifiedby included) are source data and count as a change; audit values the tool sets itself are not compared.
    Inserts and updates are told apart by which staged keys existed BEFORE the statement (RETURNING xmax would not
    work on partitioned tables).

    keep_target=True (conflict policy keep_target): rows whose key already exists are left alone (ON CONFLICT DO NOTHING).
    no_update: columns that are inserted but never overwritten on a conflict (FE default-column values).
    capture = {"delta": '"sweep_delta"."s__t"', "seq": "<sql>", "skip": "<sql boolean>"}: the rows that were really applied
    (inserted or changed, after the only-if-different filter) are also written to the delta table with _op 'I' / 'U',
    unless `skip` is true (empty target / full copy: the next hop copies the whole table instead). An optional "row_base" (SQL
    expression) is added to the delta's _row_no: a chunked load applies the file in several statements, and promote reads the delta
    ORDER BY _row_no, so the numbers must keep increasing from chunk to chunk (a key in two chunks: the later row wins).
    """
    out = []
    head = ", ".join(q(c) for c in header_cols)
    ins_cols = f"INSERT INTO {tgt} AS t ({head})" + (" OVERRIDING SYSTEM VALUE" if identity else "")
    select = f"SELECT {head} FROM {stg} AS s {where}"
    on_conflict = ""
    cap_cols = ", ".join(q(c) for c in header_cols)
    rown = f"row_number() OVER () + {capture['row_base']}" if capture and capture.get("row_base") else "row_number() OVER ()"
    if arbiter:
        a_set = set(arbiter["cols"])
        upd = [c for c in header_cols if c not in a_set and cols[c]["identity"] != "a" and c not in no_update]
        sets = [f"{q(c)} = EXCLUDED.{q(c)}" for c in upd]
        # triggers are off, so set the audit columns the audit trigger would have set on an update
        if "modifiedby" in cols and "modifiedby" not in present:
            sets.append('"modifiedby" = CURRENT_USER')
        if "modifieddate" in cols and "modifieddate" not in present:
            sets.append('"modifieddate" = CURRENT_TIMESTAMP')
        arb_list = ", ".join(q(c) for c in arbiter["cols"])
        if keep_target or not upd:
            on_conflict = f" ON CONFLICT ({arb_list}) DO NOTHING"
        else:
            old = ", ".join(_cmp(f"t.{q(c)}", cols[c]["type"]) for c in upd)
            new = ", ".join(_cmp(f"EXCLUDED.{q(c)}", cols[c]["type"]) for c in upd)
            on_conflict = (f" ON CONFLICT ({arb_list}) DO UPDATE SET {', '.join(sets)} "
                           f"WHERE ({old}) IS DISTINCT FROM ({new})")
        key_join = " AND ".join(f"t.{q(c)} = s.{q(c)}" for c in arbiter["cols"])
        out.append(f"CREATE TEMP TABLE {existing} ON COMMIT DROP AS SELECT {', '.join('s.' + q(c) for c in arbiter['cols'])} "
                   f"FROM {stg} s {where} {'AND' if where else 'WHERE'} EXISTS (SELECT 1 FROM {tgt} t WHERE {key_join});\n")
        ret = cap_cols if capture else ", ".join(q(c) for c in arbiter["cols"])
        e_join = " AND ".join(f"i.{q(c)} = e.{q(c)}" for c in arbiter["cols"])
        e_first = q(arbiter["cols"][0])
        delta_cte = ""
        if capture:
            icols = ", ".join(f"i.{q(c)}" for c in header_cols)
            delta_cte = (f", d AS (INSERT INTO {capture['delta']} ({cap_cols}, _seq, _op, _row_no) SELECT {icols}, {capture['seq']}, "
                         f"CASE WHEN e.{e_first} IS NULL THEN 'I' ELSE 'U' END, {rown} "
                         f"FROM ins i LEFT JOIN {existing} e ON {e_join} WHERE NOT {capture['skip']} RETURNING 1)")
        out.append(f"WITH ins AS ({ins_cols} {select}{on_conflict} RETURNING {ret}), "
                   f"x AS (SELECT count(*) AS tot, count(e.{e_first}) AS upd FROM ins i LEFT JOIN {existing} e ON {e_join}){delta_cte} "
                   f"INSERT INTO _c SELECT 'inserted{sfx}', tot - upd FROM x UNION ALL SELECT 'updated{sfx}', upd FROM x;\n")
        out.append(f"DROP TABLE {existing};\n")
    else:
        delta_cte = ""
        if capture:
            delta_cte = (f", d AS (INSERT INTO {capture['delta']} ({cap_cols}, _seq, _op, _row_no) SELECT {cap_cols}, {capture['seq']}, 'I', "
                         f"{rown} FROM ins WHERE NOT {capture['skip']} RETURNING 1)")
        ret = cap_cols if capture else "1"
        out.append(f"WITH ins AS ({ins_cols} {select} RETURNING {ret}){delta_cte} "
                   f"INSERT INTO _c SELECT 'inserted{sfx}', count(*) FROM ins UNION ALL SELECT 'updated{sfx}', 0;\n")
    out.append(f"INSERT INTO _c SELECT 'unchanged{sfx}', (SELECT n FROM _c WHERE k = 'staged{sfx}') - "
               f"(SELECT sum(n) FROM _c WHERE k IN ('inserted{sfx}', 'updated{sfx}'));\n")
    return out


def parse_output(stdout):
    """
    Counts (last @@COUNTS@@ wins), reject summary and sample rows echoed by the staged script. A chunked load echoes after every
    chunk: @@REJ_BEGIN@@ starts a fresh (cumulative) summary and a sample row echoed again is kept once.
    """
    counts, summary, rows, seen = {}, [], [], set()
    for line in (stdout or "").splitlines():
        if line.startswith("@@COUNTS@@"):
            counts = {k: int(v) for k, v in json.loads(line[10:]).items()}
        elif line.startswith("@@REJ_BEGIN@@"):
            summary = []
        elif line.startswith("@@REJ_SUMMARY@@"):
            summary.append(json.loads(line[15:]))
        elif line.startswith("@@REJ_ROW@@"):
            r = json.loads(line[11:])
            key = (r.get("f"), r.get("row_no"), r.get("reason"), r.get("detail"))
            if key not in seen:
                seen.add(key)
                rows.append(r)
    return counts, summary, rows


def persist_failed_file_rejects(uri, schema, table, rows, track="oracle"):
    """When a file fails (e.g. too many rejects) its transaction rolled back; keep the reject sample for review."""
    if not rows:
        return
    values = ",\n".join(
        f"({lit(track)}, {lit(uri)}, {lit(schema)}, {lit(table)}, {int(r['row_no'])}, {lit(r['reason'])}, {lit(r.get('detail'))}, "
        f"{lit(json.dumps(r.get('row')))}::jsonb, 'FILE_FAILED')" for r in rows)
    run_psql(f"DELETE FROM {REJECT_TABLE} WHERE uri = {lit(uri)} AND target_schema = {lit(schema)} AND target_table = {lit(table)};\n"
             f"INSERT INTO {REJECT_TABLE} (track, uri, target_schema, target_table, row_no, reason, detail, row_data, file_status) "
             f"VALUES {values};")


# -----------------------------------------------------------------------------
# load_history: the append-only ledger of every load attempt (migration 2)
# -----------------------------------------------------------------------------
HISTORY_TABLE = f"{CTL_SCHEMA}.load_history"


def history_insert_sql(track, layer, source, run_id, uri, schema, table, *, read, inserted, updated, unchanged, rejected, dup,
                       s3_meta=None, reset=False, status="SUCCESS", message=None, count_target=None, where="true", before_sql=None):
    """
    One load_history row. The counts are SQL scalar expressions evaluated INSIDE the load transaction, so the row exists only if
    the data does. rows_before (the ledger baseline) is stored on the first row of a table - the rows it already held - or as 0 when
    the load replaced the table (`reset`). `before_sql` overrides the baseline with an SQL expression (the fast-bulk path, which never counts
    the target table).
    """
    from sweep_s3meta import meta_sql_values
    tgt = f"{q(schema)}.{q(table)}"
    lm, etag, size = meta_sql_values(s3_meta)
    ins = f"COALESCE({inserted}, 0)"
    if before_sql is not None:
        before = before_sql
    elif reset:
        before = "0"
    else:
        before = (f"CASE WHEN NOT EXISTS (SELECT 1 FROM {HISTORY_TABLE} h WHERE h.target_schema = {lit(schema)} "
                  f"AND h.target_table = {lit(table)} AND h.status = 'SUCCESS') THEN (SELECT count(*) FROM {count_target or tgt}) - {ins} END")
    n = lambda e: f"COALESCE({e}, 0)"
    return (f"INSERT INTO {HISTORY_TABLE} (run_id, track, layer, source, uri, target_schema, target_table, status, rows_read, "
            f"rows_inserted, rows_updated, rows_unchanged, rows_rejected, rows_duplicates, rows_before, s3_last_modified, s3_etag, "
            f"s3_size, message, started_at, finished_at) SELECT {lit(run_id)}, {lit(track)}, {lit(layer)}, {lit(source)}, {lit(uri)}, "
            f"{lit(schema)}, {lit(table)}, {lit(status)}, {n(read)}, {ins}, {n(updated)}, {n(unchanged)}, {n(rejected)}, {n(dup)}, "
            f"{before}, {lm}, {etag}, {size}, {lit(message) if message else 'NULL'}, NULL, now() WHERE {where};\n")


def record_history_failure(track, layer, source, run_id, uri, schema, table, message, read=0):
    """FAILED row (autocommit): the data transaction rolled back, the attempt stays on record. Never raises."""
    try:
        run_psql(f"INSERT INTO {HISTORY_TABLE} (run_id, track, layer, source, uri, target_schema, target_table, status, rows_read, "
                 f"message, finished_at) VALUES ({lit(run_id)}, {lit(track)}, {lit(layer)}, {lit(source)}, {lit(uri)}, {lit(schema)}, "
                 f"{lit(table)}, 'FAILED', {int(read or 0)}, {lit(str(message)[:2000])}, now());")
    except Exception:       # noqa: BLE001 - the failure itself is already reported; a lost history row must not hide it
        pass


# -----------------------------------------------------------------------------
# Chunked loads: the file is fed through the psql script in blocks of N records
# -----------------------------------------------------------------------------
def csv_chunks(stream, chunk_rows, flush_bytes=4 << 20):
    """
    Splits a CSV byte stream (header record first) into chunks of `chunk_rows` records. Yields ("data", bytes) pieces and
    ("end", records) when a chunk is complete. A record may span lines (quoted newlines): a line ends a record only when the
    number of double quotes since the record started is even. The header record is not data and is dropped.
    """
    pieces, size, records, quotes, header_seen = [], 0, 0, 0, False

    def take():
        nonlocal pieces, size
        data = b"".join(pieces)
        pieces, size = [], 0
        return data

    for line in iter(stream.readline, b""):
        quotes += line.count(b'"')
        if header_seen:
            pieces.append(line)
            size += len(line)
        if quotes % 2:                                    # inside a quoted field: the record goes on
            if size >= flush_bytes:
                yield "data", take()
            continue
        quotes = 0
        if not header_seen:
            header_seen = True
            continue
        records += 1
        if records >= chunk_rows:
            yield "data", take()
            yield "end", records
            records = 0
        elif size >= flush_bytes:
            yield "data", take()
    if pieces:
        data = take()
        yield "data", data if data.endswith(b"\n") else data + b"\n"
    if records or pieces:
        yield "end", records


def chunked_script(prologue, copy_stdin_sql, chunk_sql, epilogue, argv, chunk_rows, on_chunk=None):
    """
    Generator of script pieces for run_psql_script_stream: the prologue, then for every chunk the \\copy command, its rows, the end
    marker and the SQL of the chunk (`chunk_sql(n)`), finally the epilogue. If the source stream fails it raises: the transaction
    rolls back and nothing is committed.
    """
    yield prologue
    with tempfile.TemporaryFile() as errf:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=errf, **group_popen_kwargs())
        try:
            n, copy_open = 0, False
            for kind, payload in csv_chunks(proc.stdout, chunk_rows):
                if kind == "data":
                    if not copy_open:
                        yield copy_stdin_sql
                        copy_open = True
                    yield payload
                else:
                    n += 1
                    yield "\\.\n"
                    copy_open = False
                    yield chunk_sql(n)
                    if on_chunk:
                        on_chunk(n, payload)
            rc = proc.wait()
            if rc != 0:
                errf.seek(0)
                raise RuntimeError(f"source stream failed (exit {rc}): {errf.read().decode('utf-8', 'replace').strip()[:500]}")
        finally:
            if proc.poll() is None:
                kill_process_group(proc)
                proc.wait()
    yield epilogue
