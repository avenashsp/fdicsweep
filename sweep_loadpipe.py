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
    flag = lambda v: str(v).strip().lower() in ("t", "true", "1")
    return {
        "columns": {c["name"]: {"type": c["type"], "notnull": flag(c["notnull"]), "hasdef": flag(c["hasdef"]),
                                "identity": c["identity"], "generated": c["generated"]} for c in cols},
        "keys": [{"name": k["name"], "primary": flag(k["is_primary"]), "cols": k["cols"].split(",")} for k in keys if k["cols"]],
        "checks": [{"name": c["name"], "expr": c["expr"], "cols": (c["cols"] or "").split(",") if c["cols"] else []} for c in checks],
        "fks": [{"name": f["name"], "parent": f["parent"], "self_ref": flag(f["self_ref"]),
                 "cols": f["cols"].split(","), "parent_cols": f["parent_cols"].split(",")} for f in fks if f["cols"]],
        "triggers": [t["name"] for t in triggers],
    }


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


def upsert_statements(tgt, header_cols, cols, arbiter, present, identity, stg="_stg", where="", sfx="", existing="_existing"):
    """
    SQL statements that apply the staged rows `stg` to `tgt`:
      INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE (target columns) IS DISTINCT FROM (file columns)
    and record 'inserted' / 'updated' / 'unchanged' (keys suffixed with `sfx`) in the temp table _c(k, n).
    `where` restricts the staged rows (e.g. one destination schema). Columns that come FROM the file (modifieddate /
    modifiedby included) are source data and count as a change; audit values the tool sets itself are not compared.
    Inserts and updates are told apart by which staged keys existed BEFORE the statement (RETURNING xmax would not
    work on partitioned tables).
    """
    out = []
    head = ", ".join(q(c) for c in header_cols)
    ins_cols = f"INSERT INTO {tgt} AS t ({head})" + (" OVERRIDING SYSTEM VALUE" if identity else "")
    select = f"SELECT {head} FROM {stg} AS s {where}"
    on_conflict = ""
    if arbiter:
        a_set = set(arbiter["cols"])
        upd = [c for c in header_cols if c not in a_set and cols[c]["identity"] != "a"]
        sets = [f"{q(c)} = EXCLUDED.{q(c)}" for c in upd]
        # triggers are off, so set the audit columns the audit trigger would have set on an update
        if "modifiedby" in cols and "modifiedby" not in present:
            sets.append('"modifiedby" = CURRENT_USER')
        if "modifieddate" in cols and "modifieddate" not in present:
            sets.append('"modifieddate" = CURRENT_TIMESTAMP')
        arb_list = ", ".join(q(c) for c in arbiter["cols"])
        if upd:
            old = ", ".join(_cmp(f"t.{q(c)}", cols[c]["type"]) for c in upd)
            new = ", ".join(_cmp(f"EXCLUDED.{q(c)}", cols[c]["type"]) for c in upd)
            on_conflict = (f" ON CONFLICT ({arb_list}) DO UPDATE SET {', '.join(sets)} "
                           f"WHERE ({old}) IS DISTINCT FROM ({new})")
        else:
            on_conflict = f" ON CONFLICT ({arb_list}) DO NOTHING"
        key_join = " AND ".join(f"t.{q(c)} = s.{q(c)}" for c in arbiter["cols"])
        out.append(f"CREATE TEMP TABLE {existing} ON COMMIT DROP AS SELECT {', '.join('s.' + q(c) for c in arbiter['cols'])} "
                   f"FROM {stg} s {where} {'AND' if where else 'WHERE'} EXISTS (SELECT 1 FROM {tgt} t WHERE {key_join});\n")
        ret = ", ".join(q(c) for c in arbiter["cols"])
        e_join = " AND ".join(f"i.{q(c)} = e.{q(c)}" for c in arbiter["cols"])
        e_first = q(arbiter["cols"][0])
        out.append(f"WITH ins AS ({ins_cols} {select}{on_conflict} RETURNING {ret}), "
                   f"x AS (SELECT count(*) AS tot, count(e.{e_first}) AS upd FROM ins i LEFT JOIN {existing} e ON {e_join}) "
                   f"INSERT INTO _c SELECT 'inserted{sfx}', tot - upd FROM x UNION ALL SELECT 'updated{sfx}', upd FROM x;\n")
        out.append(f"DROP TABLE {existing};\n")
    else:
        out.append(f"WITH ins AS ({ins_cols} {select} RETURNING 1) "
                   f"INSERT INTO _c SELECT 'inserted{sfx}', count(*) FROM ins UNION ALL SELECT 'updated{sfx}', 0;\n")
    out.append(f"INSERT INTO _c SELECT 'unchanged{sfx}', (SELECT n FROM _c WHERE k = 'staged{sfx}') - "
               f"(SELECT sum(n) FROM _c WHERE k IN ('inserted{sfx}', 'updated{sfx}'));\n")
    return out


def parse_output(stdout):
    """
    Counts (last @@COUNTS@@ wins), reject summary and sample rows echoed by the staged script. A chunked load echoes
    after every chunk: @@REJ_BEGIN@@ starts a fresh (cumulative) summary and repeated sample rows are kept once.
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


def persist_failed_file_rejects(uri, schema, table, rows):
    """When a file fails (e.g. too many rejects) its transaction rolled back; keep the reject sample for review."""
    if not rows:
        return
    values = ",\n".join(
        f"({lit(uri)}, {lit(schema)}, {lit(table)}, {int(r['row_no'])}, {lit(r['reason'])}, {lit(r.get('detail'))}, "
        f"{lit(json.dumps(r.get('row')))}::jsonb, 'FILE_FAILED')" for r in rows)
    run_psql(f"DELETE FROM {REJECT_TABLE} WHERE uri = {lit(uri)} AND target_schema = {lit(schema)} AND target_table = {lit(table)};\n"
             f"INSERT INTO {REJECT_TABLE} (uri, target_schema, target_table, row_no, reason, detail, row_data, file_status) "
             f"VALUES {values};")


# -----------------------------------------------------------------------------
# Chunked loads: feed the file through the psql script in blocks of N records
# -----------------------------------------------------------------------------
def csv_chunks(stream, chunk_rows, flush_bytes=4 << 20):
    """
    Splits a CSV byte stream (header first) into chunks of `chunk_rows` records. Yields ("data", bytes) pieces and
    ("end", records) when a chunk is complete. A record may span lines (quoted newlines): a line ends a record
    only when the number of double quotes seen since the record started is even.
    """
    buf, size, rows, quotes, header_done = [], 0, 0, 0, False

    def take():
        nonlocal buf, size
        data = b"".join(buf)
        buf, size = [], 0
        return data

    for line in iter(stream.readline, b""):
        quotes += line.count(b'"')
        if header_done:
            buf.append(line)
            size += len(line)
        if quotes & 1:                                     # inside a quoted field: the record continues
            if size >= flush_bytes:
                yield "data", take()
            continue
        quotes = 0
        if not header_done:                                # the header record is not data
            header_done = True
            continue
        rows += 1
        if rows >= chunk_rows:
            yield "data", take()
            yield "end", rows
            rows = 0
        elif size >= flush_bytes:
            yield "data", take()
    if buf:
        data = take()
        yield "data", data if data.endswith(b"\n") else data + b"\n"
    if rows or buf:
        yield "end", rows


def chunked_script(prologue, copy_stdin_sql, chunk_sql, epilogue, argv, chunk_rows, on_chunk=None):
    """
    Generator of script pieces for run_psql_script_stream: prologue, then for every chunk the \\copy command, its
    rows, the end marker and the chunk's SQL, finally the epilogue. If the source stream fails, it raises (the
    transaction rolls back and nothing is committed).
    """
    yield prologue
    with tempfile.TemporaryFile() as errf:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=errf, **group_popen_kwargs())
        try:
            chunk, open_copy = 0, False
            for kind, payload in csv_chunks(proc.stdout, chunk_rows):
                if kind == "data":
                    if not open_copy:
                        yield copy_stdin_sql
                        open_copy = True
                    yield payload
                else:
                    chunk += 1
                    yield "\\.\n"
                    open_copy = False
                    yield chunk_sql(chunk)
                    if on_chunk:
                        on_chunk(chunk, payload)
            rc = proc.wait()
            if rc != 0:
                errf.seek(0)
                raise RuntimeError(f"source stream failed (exit {rc}): {errf.read().decode('utf-8', 'replace').strip()[:500]}")
        finally:
            if proc.poll() is None:
                kill_process_group(proc)
                proc.wait()
    yield epilogue
