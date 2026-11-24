#!/usr/bin/env python3
"""
sweep_promote.py - `sweep.py promote`, `chain`: moving data ONE hop along bronze -> silver -> gold_backup -> gold.

S3 is read once, into bronze. Every other layer is filled from the previous one over the cluster network:

  * transport: the target psql runs `\\copy _raw FROM PROGRAM 'psql -d <source> -c "COPY (...) TO STDOUT"'` (client side, no
    extension needed). If that is not possible, `--transport stream` (or SWEEP_PROMOTE_TRANSPORT=stream, also picked
    automatically when the program transport cannot start) pipes the source psql into the target psql's stdin from Python.
    postgres_fdw was considered and not used: it needs an extension, a server object and user mappings in every database
    (CREATE EXTENSION is DDL the Database team would have to grant on gold), while the pipe needs nothing but psql.
  * the target applies the rows with the SAME staged script as Oracle Step 06 (validation, rejects, only-if-different upsert,
    row-count check, ONE transaction per table), writes sweep_ctl.promote_log and records its own delta for the next hop.
  * deltas are applied per table in strict sequence order; a layer only takes a table when the previous layer applied that
    delta successfully; a failure stops that table's chain, other tables continue; re-running is idempotent and resumable.
  * nothing here deletes rows. `replace` (atomic TRUNCATE + INSERT) is for VSAM keyless tables in silver and is REFUSED for
    gold and gold_backup. Model output tables (track model_output) are upserted on their key, changed rows only.
"""
import json
import os
import shlex
import subprocess
import time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import sweep_common as c
from sweep_common import CTL_SCHEMA, clip_error, get_logger, lit, psql_base_args, psql_env, q, query_db_dicts, run_psql, run_psql_script
from sweep_delta import REGISTRY, TOOL_AUDIT_COLS, ensure_delta_table
from sweep_layers import (LayerError, chain, get_layer, layer_conn, read_manifest, same_credentials, use_layer, validate_manifest_rows)
from sweep_loadpipe import (ensure_reject_table, load_table_meta, parse_output, persist_failed_file_rejects, probe_trigger_mode)
from sweep_merge import current_run_id
from sweep_s3meta import canon_time
from sweep_platform import group_popen_kwargs, kill_process_group

logger = get_logger("sweep_promote", "sweep_promote")

RETENTION_DAYS = int(os.getenv("SWEEP_DELTA_RETENTION_DAYS", "14"))
STREAM_MARK = "-- @@STREAM_DATA@@"
LOCK_RETRIES = 5
EXIT_FAILED = 1


class PromoteError(RuntimeError):
    """A table cannot be promoted (message says why); other tables are unaffected."""


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def sequence_sql(schema, table, meta):
    """
    Advances the identity / serial sequences of a touched table to GREATEST(current value, max(column)). A sequence is never
    moved backwards (the application's own inserts into gold depend on that).
    """
    out = []
    for col, info in meta["columns"].items():
        if info["type"] not in ("integer", "bigint", "smallint"):
            continue
        if not (info["identity"] in ("a", "d") or info["hasdef"]):
            continue
        out.append(
            f"DO $seq$ DECLARE s text; m bigint; lv bigint; ic boolean; BEGIN "
            f"s := pg_get_serial_sequence({lit(q(schema) + '.' + q(table))}, {lit(col)}); "
            f"IF s IS NOT NULL THEN "
            f"EXECUTE format('SELECT max(%I) FROM %I.%I', {lit(col)}, {lit(schema)}, {lit(table)}) INTO m; "
            f"EXECUTE format('SELECT last_value, is_called FROM %s', s) INTO lv, ic; "
            f"IF m IS NOT NULL AND (m > lv OR (m = lv AND NOT ic)) THEN PERFORM setval(s, m, true); END IF; "
            f"END IF; END $seq$;\n")
    return "".join(out)


def timeout_sql(layer):
    """SET LOCAL lock / statement timeouts of the layer (one small transaction per table, never held across tables)."""
    sql = ""
    if layer.lock_timeout and str(layer.lock_timeout) != "0":
        sql += f"SET LOCAL lock_timeout = {lit(layer.lock_timeout)};\n"
    if layer.statement_timeout and str(layer.statement_timeout) != "0":
        sql += f"SET LOCAL statement_timeout = {lit(layer.statement_timeout)};\n"
    return sql


def source_copy_sql(cols, schema, table, seq=None, delta_table=None):
    """COPY (...) TO STDOUT that reads the delta rows of `seq` (or the whole source table) in the source database."""
    col_list = ", ".join(q(x) for x in cols)
    if delta_table:
        inner = f"SELECT {col_list} FROM {delta_table} WHERE _seq = {int(seq)} ORDER BY _row_no"
    else:
        inner = f"SELECT {col_list} FROM {q(schema)}.{q(table)}"
    return f"COPY ({inner}) TO STDOUT WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"


def source_argv(src_conn, copy_sql):
    return [c.PSQL_BIN, "-X", "-w", "-h", src_conn.host, "-p", str(src_conn.port), "-U", src_conn.user, "-d", src_conn.dbname,
            "-v", "ON_ERROR_STOP=1", "-c", copy_sql]


def program_literal(argv):
    """Shell command for psql `\\copy ... FROM PROGRAM '<this>'`: shell-quoted, single quotes doubled for psql."""
    return shlex.join(argv).replace("'", "''")


def dry_run_counts_sql(tgt, header_cols, cols, plan, keep_target, replace):
    """Read-only counts for `promote --dry-run`: how many staged rows would be inserted, updated and left alone."""
    if plan["arbiter"] and not replace:
        a = plan["arbiter"]["cols"]
        join = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in a)
        upd = [x for x in header_cols if x not in set(a) and cols[x]["identity"] != "a"]
        if keep_target or not upd:
            changed = "false"
        else:
            old = ", ".join(f"t.{q(x)}::text" for x in upd)
            new = ", ".join(f"s.{q(x)}::text" for x in upd)
            changed = f"({old}) IS DISTINCT FROM ({new})"
        first = q(a[0])
        return (f"SELECT '@@DRY@@' || json_build_object('inserted', count(*) FILTER (WHERE t.{first} IS NULL), "
                f"'updated', count(*) FILTER (WHERE t.{first} IS NOT NULL AND {changed}), "
                f"'unchanged', count(*) FILTER (WHERE t.{first} IS NOT NULL AND NOT ({changed})), "
                f"'rejected', (SELECT n FROM _c WHERE k = 'rejected')) FROM _stg s LEFT JOIN {tgt} t ON {join};\n")
    return ("SELECT '@@DRY@@' || json_build_object('inserted', (SELECT count(*) FROM _stg), 'updated', 0, 'unchanged', 0, "
            "'rejected', (SELECT n FROM _c WHERE k = 'rejected'));\n")


def _parse_dry(stdout):
    for line in (stdout or "").splitlines():
        if line.startswith("@@DRY@@"):
            return {k: int(v or 0) for k, v in json.loads(line[7:]).items()}
    return {}


def _is_lock_timeout(text):
    return "lock timeout" in (text or "").lower() or "could not obtain lock" in (text or "").lower()


# -----------------------------------------------------------------------------
# running a script with one of the two transports
# -----------------------------------------------------------------------------
def run_with_stream(script, src_argv, options, timeout):
    """
    Fallback transport: the script's `\\copy _raw ... FROM STDIN` is fed by the source psql through a pipe made here, so no
    `FROM PROGRAM` is needed. `script` contains STREAM_MARK where the data goes. The target transaction only commits when the
    source finished cleanly (otherwise psql sees EOF in the middle of COPY and the transaction rolls back).
    """
    head, _, tail = script.partition(STREAM_MARK + "\n")
    env = psql_env(options=options)
    tgt = subprocess.Popen(psql_base_args() + ["-v", "ON_ERROR_STOP=1", "-f", "-"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, text=False, env=env, **group_popen_kwargs())
    src = subprocess.Popen(src_argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=psql_env(), **group_popen_kwargs())
    deadline = time.time() + timeout if timeout else None
    src_ok = False
    try:
        tgt.stdin.write(head.encode("utf-8"))
        while True:
            chunk = src.stdout.read(1 << 20)
            if not chunk:
                break
            tgt.stdin.write(chunk)
            if deadline and time.time() > deadline:
                raise RuntimeError(f"promote transport timed out after {timeout}s")
        src.wait()
        src_ok = src.returncode == 0
        if src_ok:
            tgt.stdin.write(b"\\.\n" + tail.encode("utf-8"))
        out, err = tgt.communicate(timeout=timeout or None)       # closes the pipe (an explicit close() breaks communicate on 3.9)
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
        kill_process_group(tgt)
        kill_process_group(src)
        raise RuntimeError(f"stream transport failed: {e}")
    if not src_ok:
        src_err = (src.stderr.read() or b"").decode("utf-8", "replace")
        return subprocess.CompletedProcess(tgt.args, 1, out.decode("utf-8", "replace"), f"source read failed: {src_err}")
    return subprocess.CompletedProcess(tgt.args, tgt.returncode, out.decode("utf-8", "replace"), err.decode("utf-8", "replace"))


def run_apply_script(script, transport, src_argv, options, timeout=None):
    """Runs the apply script, retrying on a lock timeout (short lock_timeout on gold) with exponential backoff."""
    last = None
    for attempt in range(LOCK_RETRIES):
        if transport == "stream":
            res = run_with_stream(script, src_argv, options, timeout or c.LOAD_TIMEOUT)
        else:
            res = run_psql_script(script, options=options, timeout=timeout)
        if res.returncode == 0 or not _is_lock_timeout(res.stderr or res.stdout):
            return res
        last = res
        wait = min(2 ** attempt, 30)
        logger.warning(f"lock timeout, retry {attempt + 1}/{LOCK_RETRIES - 1} in {wait}s")
        time.sleep(wait)
    return last


def with_lock_retry(fn):
    """Runs fn, retrying with exponential backoff when it fails on a lock timeout (short lock_timeout on gold)."""
    for attempt in range(LOCK_RETRIES):
        try:
            return fn()
        except RuntimeError as e:
            if not _is_lock_timeout(str(e)) or attempt == LOCK_RETRIES - 1:
                raise
            wait = min(2 ** attempt, 30)
            logger.warning(f"lock timeout, retry {attempt + 1}/{LOCK_RETRIES - 1} in {wait}s")
            time.sleep(wait)


def pick_transport(requested):
    """program (default) | stream. `auto` falls back to stream when `\\copy ... FROM PROGRAM` cannot run."""
    requested = (requested or os.environ.get("SWEEP_PROMOTE_TRANSPORT") or "auto").lower()
    if requested in ("program", "stream"):
        return requested
    try:
        res = run_psql_script("\\copy (SELECT 1) TO PROGRAM 'cat > /dev/null'\n", timeout=60, retries=0)
        return "program" if res.returncode == 0 else "stream"
    except RuntimeError:
        return "stream"


# -----------------------------------------------------------------------------
# registry / planning
# -----------------------------------------------------------------------------
def source_deltas(src, schema, table, after_seq):
    """Registry rows of the source layer with seq > after_seq (SUCCESS only), oldest first."""
    rows = query_db_dicts(
        f"SELECT seq, run_id, columns, delta_table, full_copy, purged_at, source_uris, origin_layer FROM {REGISTRY} "
        f"WHERE target_schema = {lit(schema)} AND target_table = {lit(table)} AND status = 'SUCCESS' AND seq > {int(after_seq)} "
        f"ORDER BY seq", dbname=src.database)
    for r in rows:
        r["seq"] = int(r["seq"])
        r["full_copy"] = str(r["full_copy"]).lower() in ("t", "true")
    return rows


def target_last_seq(schema, table):
    rows = query_db_dicts(f"SELECT COALESCE(max(seq), 0) AS n FROM {REGISTRY} WHERE target_schema = {lit(schema)} "
                          f"AND target_table = {lit(table)} AND status = 'SUCCESS'")
    return int(rows[0]["n"])


def table_columns(schema, table, dbname):
    """[(name, generated)] of a table in `dbname` (empty when the table does not exist)."""
    rows = query_db_dicts(
        f"SELECT a.attname AS name, a.attgenerated AS generated FROM pg_attribute a WHERE a.attrelid = "
        f"to_regclass({lit(q(schema) + '.' + q(table))}) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", dbname=dbname)
    return [(r["name"], r["generated"]) for r in rows]


def has_rows(schema, table, dbname):
    return bool(query_db_dicts(f"SELECT 1 AS x FROM {q(schema)}.{q(table)} LIMIT 1", dbname=dbname))


# -----------------------------------------------------------------------------
# one delta of one table
# -----------------------------------------------------------------------------
def apply_one(src, tgt, mrow, delta, opts, meta_cache):
    """
    Applies one delta (or a full copy) of schema.table to the target layer in ONE transaction.
    delta: {"seq": int | None, "full": bool, "columns": [..], "delta_table": str | None, "origin_layer": str}
    Returns {"inserted", "updated", "unchanged", "rejected"}; raises PromoteError.
    """
    from oracle.oracle_06_pipeline import build_staged_script, plan_load
    schema, table, track = mrow["schema"], mrow["table"], mrow["track"]
    mode, policy = mrow["promote_mode"], mrow["conflict_policy"]
    if mode == "replace" and not tgt.allow_ddl:
        raise PromoteError(f"promote_mode=replace is refused on layer {tgt.name}: nothing is ever truncated or replaced there")
    tmeta = meta_cache.get((schema, table)) or load_table_meta(schema, table)
    meta_cache[(schema, table)] = tmeta
    if not tmeta["columns"]:
        raise PromoteError(f"table {schema}.{table} does not exist in {tgt.database}: tables are created by Steps 04/05 or a "
                           f"controlled migration, never by promote")

    # ---- columns to move and the schema drift check (BEFORE any data is read)
    if delta["full"]:
        src_cols = table_columns(schema, table, src.database)
        if not src_cols:
            raise PromoteError(f"table {schema}.{table} does not exist in the source layer {src.name}")
        header_cols = [n for n, g in src_cols if not g and n not in TOOL_AUDIT_COLS]
    else:
        header_cols = [x for x in delta["columns"] if x]
    missing = [x for x in header_cols if x not in tmeta["columns"]]
    if missing:
        raise PromoteError(f"schema drift: column(s) {', '.join(missing)} of {src.name}.{schema}.{table} are missing in {tgt.name}; "
                           f"no automatic ALTER is ever made - the Database team adds them with a controlled migration")
    plan, problem = plan_load(tmeta, header_cols)
    if problem:
        raise PromoteError(f"schema drift: {problem}" if "NOT NULL" in problem else problem)

    keyed = plan["arbiter"] is not None
    if mode == "upsert" and not keyed:
        if track == "model_output":
            raise PromoteError("SKIP keyless model output table: no primary key to upsert on (set promote_mode=insert_only in the manifest)")
        raise PromoteError(f"{schema}.{table} has no key in {tgt.name} but promote_mode is upsert: use insert_only or replace")
    keep_target = policy == "keep_target" or (mode == "insert_only" and keyed)
    replace = mode == "replace"
    tgt_sql = f"{q(schema)}.{q(table)}"
    apply_pre = ""
    if replace:
        apply_pre = f"TRUNCATE TABLE {tgt_sql};\n"
    elif mode == "insert_only" and not keyed and delta["full"]:      # keyless: only rows the target does not have yet
        cl = ", ".join(q(x) for x in header_cols)
        apply_pre = (f"CREATE TEMP TABLE _novel ON COMMIT DROP AS SELECT {cl} FROM _stg EXCEPT ALL SELECT {cl} FROM {tgt_sql};\n"
                     f"DELETE FROM _stg;\nINSERT INTO _stg (_row_no, {cl}) SELECT row_number() OVER (), {cl} FROM _novel;\n")

    seq = delta["seq"]
    uri = f"promote://{src.name}/{schema}.{table}#{seq if seq is not None else 'full'}"
    capture = {"run_id": opts["run_id"], "layer": delta.get("origin_layer") or src.name,
               "full": (delta["full"] and track != "model_output") or replace, "uris": uri,
               "downstream": tgt.downstream is not None, "seq": seq, "strict": delta.get("strict", True)}
    if seq is None and track != "model_output":
        capture = None                     # a full copy that covers no registered delta leaves only the promote_log row
    log_sql = (f"INSERT INTO {CTL_SCHEMA}.promote_log (run_id, source_layer, target_layer, target_schema, target_table, track, seq, mode, "
               f"inserted, updated, unchanged, rejected, status, finished_at) VALUES ({lit(opts['run_id'])}, {lit(src.name)}, {lit(tgt.name)}, "
               f"{lit(schema)}, {lit(table)}, {lit(track)}, {'NULL' if seq is None else int(seq)}, {lit(mode)}, "
               f"(SELECT n FROM _c WHERE k = 'inserted'), (SELECT n FROM _c WHERE k = 'updated'), (SELECT n FROM _c WHERE k = 'unchanged'), "
               f"(SELECT n FROM _c WHERE k = 'rejected'), 'SUCCESS', now());")
    argv = source_argv(layer_conn(src), source_copy_sql(header_cols, schema, table, seq, delta["delta_table"]))
    transport = opts["transport"]
    if transport == "stream":
        col_list = ", ".join(q(x) for x in header_cols)
        copy_from, stream_cmd = f"\\copy _raw ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n{STREAM_MARK}\n", ""
    else:
        copy_from, stream_cmd = None, program_literal(argv)
    trig = probe_trigger_mode() if tgt.trigger_mode == "auto" else tgt.trigger_mode
    dry = opts["dry_run"]
    if capture and capture["downstream"] and not dry:
        ensure_delta_table(schema, table)          # this layer records its own delta for the next hop
    script = build_staged_script(
        schema, table, header_cols, stream_cmd, tmeta, plan, trig, 0, uri, log_sql, track=track, copy_from=copy_from,
        keep_target=keep_target, capture=None if dry else capture, pre_sql=timeout_sql(tgt), apply_pre_sql=apply_pre,
        post_sql=sequence_sql(schema, table, tmeta),
        dry_run_sql=dry_run_counts_sql(tgt_sql, header_cols, tmeta["columns"], plan, keep_target, replace) if dry else None)
    res = run_apply_script(script, transport, argv, "-c synchronous_commit=off" if not dry else "-c statement_timeout=0")
    if os.environ.get("SWEEP_PROMOTE_DEBUG_DIR"):          # keeps the generated script and its output for troubleshooting
        d = Path(os.environ["SWEEP_PROMOTE_DEBUG_DIR"])
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{tgt.name}_{schema}.{table}_{seq if seq is not None else 'full'}.sql").write_text(
            script + "\n-- STDOUT\n" + (res.stdout or "") + "\n-- STDERR\n" + (res.stderr or ""))
    counts, summary, reject_rows = parse_output(res.stdout)
    if res.returncode != 0:
        if reject_rows and not dry:
            try:
                persist_failed_file_rejects(uri, schema, table, reject_rows, track=track)
            except RuntimeError as e:
                logger.warning(f"could not store the rejected rows of {uri}: {e}")
        raise PromoteError(clip_error(res.stderr or res.stdout or "psql failed"))
    if dry:
        counts = _parse_dry(res.stdout)
    return {k: int(counts.get(k, 0)) for k in ("inserted", "updated", "unchanged", "rejected")}


# -----------------------------------------------------------------------------
# planning the deltas of one table
# -----------------------------------------------------------------------------
def plan_deltas(src, tgt, mrow, opts):
    """
    The deltas to apply to schema.table in order. Raises PromoteError for a gap (a delta that cannot be applied yet), a purged
    delta or a missing baseline. [] means the table is up to date.
    """
    schema, table, track = mrow["schema"], mrow["table"], mrow["track"]
    last = target_last_seq(schema, table)
    pending = source_deltas(src, schema, table, last)
    everything = query_db_dicts(f"SELECT 1 AS x FROM {REGISTRY} WHERE target_schema = {lit(schema)} AND target_table = {lit(table)} LIMIT 1",
                                dbname=src.database)
    if track == "model_output" and not everything:
        # the data-model scripts write these tables directly: compare the whole source table, apply only what differs
        return [{"seq": None, "full": True, "columns": [], "delta_table": None, "origin_layer": src.name}]
    if opts["full"]:
        seq = pending[-1]["seq"] if pending else None
        return [{"seq": seq, "full": True, "columns": [], "delta_table": None, "origin_layer": src.name, "strict": False}] \
            if (pending or opts["force_full_without_delta"]) else []
    if not pending:
        return []
    expect = last + 1
    out = []
    for d in pending:
        if d["seq"] != expect:
            raise PromoteError(f"sequence gap: expected delta {expect} of {schema}.{table} but the source offers {d['seq']}; the "
                               f"table has no baseline in {tgt.name} yet - run `promote --full` for it first")
        if d["purged_at"]:
            raise PromoteError(f"delta {d['seq']} of {schema}.{table} was purged from {src.name}; run `promote --full` to resynchronize")
        out.append({"seq": d["seq"], "full": d["full_copy"], "columns": (d["columns"] or "").split(","),
                    "delta_table": d["delta_table"] or None, "origin_layer": d["origin_layer"] or src.name})
        expect += 1
    if last == 0 and not out[0]["full"] and not has_rows(schema, table, tgt.database) and has_rows(schema, table, src.database):
        raise PromoteError(f"{schema}.{table} is empty in {tgt.name}: it needs a baseline (`promote --full`) before deltas can be applied")
    return out


def record_failure(src, tgt, mrow, delta, message, opts):
    """FAILED row in promote_log (autocommit, the data transaction rolled back)."""
    try:
        run_psql(f"INSERT INTO {CTL_SCHEMA}.promote_log (run_id, source_layer, target_layer, target_schema, target_table, track, seq, mode, "
                 f"status, message, finished_at) VALUES ({lit(opts['run_id'])}, {lit(src.name)}, {lit(tgt.name)}, {lit(mrow['schema'])}, "
                 f"{lit(mrow['table'])}, {lit(mrow['track'])}, {'NULL' if delta.get('seq') is None else int(delta['seq'])}, "
                 f"{lit(mrow['promote_mode'])}, 'FAILED', {lit(message)}, now());")
    except RuntimeError as e:
        logger.error(f"could not record the failure of {mrow['schema']}.{mrow['table']}: {e}")


def promote_table(src, tgt, mrow, opts, meta_cache):
    """Applies every pending delta of one table in sequence order; stops at the first failure. Returns a result dict."""
    name = f"{mrow['schema']}.{mrow['table']}"
    res = {"table": name, "track": mrow["track"], "mode": mrow["promote_mode"], "inserted": 0, "updated": 0, "unchanged": 0,
           "rejected": 0, "deltas": 0, "status": "UP_TO_DATE", "message": ""}
    try:
        deltas = with_lock_retry(lambda: plan_deltas(src, tgt, mrow, opts))
    except (PromoteError, RuntimeError) as e:
        res.update(status="FAILED", message=clip_error(e))
        return res
    for d in deltas:
        t0 = time.time()
        try:
            r = apply_one(src, tgt, mrow, d, opts, meta_cache)
        except PromoteError as e:
            if str(e).startswith("SKIP"):
                res.update(status="SKIPPED", message=str(e)[5:].strip())
                logger.warning(f"{name}: {res['message']}")
            else:
                res.update(status="FAILED", message=clip_error(e))
                logger.error(f"{name}: delta {d['seq']} failed - {res['message']}")
                if not opts["dry_run"]:
                    record_failure(src, tgt, mrow, d, res["message"], opts)
            return res
        except RuntimeError as e:
            res.update(status="FAILED", message=clip_error(e))
            logger.error(f"{name}: delta {d['seq']} failed - {res['message']}")
            if not opts["dry_run"]:
                record_failure(src, tgt, mrow, d, res["message"], opts)
            return res
        for k in ("inserted", "updated", "unchanged", "rejected"):
            res[k] += r[k]
        res["deltas"] += 1
        res["status"] = "DRY_RUN" if opts["dry_run"] else "SUCCESS"
        logger.info(f"{name}: delta {d['seq'] if d['seq'] is not None else 'full'} -> {tgt.name}: inserted {r['inserted']}, updated {r['updated']}, "
                    f"unchanged {r['unchanged']}, rejected {r['rejected']} ({time.time() - t0:.1f}s)")
    return res


# -----------------------------------------------------------------------------
# one hop
# -----------------------------------------------------------------------------
def check_hop(from_layer, to_layer):
    src, tgt = get_layer(from_layer), get_layer(to_layer)
    if tgt.upstream != src.name:
        raise LayerError(f"promote moves data ONE hop: {tgt.name!r} is filled from {tgt.upstream!r}, not from {src.name!r} "
                         f"(use --from {tgt.upstream} --to {tgt.name})" if tgt.upstream else f"{tgt.name!r} is the first layer; it is loaded from S3")
    return src, tgt


def select_rows(src, tgt, tables, logger_=logger):
    """Manifest rows of the target (enabled) that the source layer also owns, narrowed by --tables."""
    from sweep_targeting import Selector, SelectorError, parse_table_ref
    tgt_rows = read_manifest(tgt)
    if not tgt_rows:
        raise LayerError(f"layer {tgt.name} has no enabled tables: {tgt.manifest_path} is missing or empty "
                         f"(python3 sweep.py layers init-manifests)")
    validate_manifest_rows(tgt_rows, str(tgt.manifest_path))
    src_keys = {(r["schema"], r["table"]) for r in read_manifest(src)}
    sel = Selector(tables=[parse_table_ref(t) for t in (tables or [])])
    try:
        sel.validate_tables([(r["schema"], r["table"]) for r in tgt_rows], what=f"the {tgt.name} manifest")
    except SelectorError as e:
        raise LayerError(str(e))
    rows = []
    for r in tgt_rows:
        if not sel.matches(r["schema"], r["table"]):
            continue
        if (r["schema"], r["table"]) not in src_keys:
            logger_.warning(f"{r['schema']}.{r['table']} is in the {tgt.name} manifest but not in the {src.name} manifest: skipped")
            continue
        rows.append(r)
    return rows


def fwd_pending_between(src, tgt):
    """Names of the FE files the source layer applied that the target layer has not applied yet (None when a log cannot be read)."""
    from sweep_fwd_ddl import layer_log
    a, b = layer_log(src), layer_log(tgt)
    if a is None or b is None:
        return None
    done = lambda log: {n for n, r in log.items() if r["status"] == "APPLIED"}
    return sorted(done(a) - done(b))


def promote(from_layer, to_layer, tables=None, full=False, dry_run=False, workers=4, transport=None, force_full_without_delta=False,
            ignore_pending_fwd=False):
    """
    Moves data one hop. Returns (results, exit_code). Tables are processed parent-first, level by level (all tables of a level
    finish before the next level starts, workers in parallel inside a level); every table is its own transaction.
    """
    from oracle.oracle_06_order import plan_levels
    from sweep_migrate import LATEST_VERSION
    src, tgt = check_hop(from_layer, to_layer)
    rows = select_rows(src, tgt, tables)
    if not ignore_pending_fwd:       # schema first, then data: the FE files of the source layer must be in the target layer
        pending = fwd_pending_between(src, tgt)
        if pending:
            raise LayerError(f"{tgt.name} has {len(pending)} pending FE file(s) that {src.name} already applied: {', '.join(pending[:5])}"
                             f"{' ...' if len(pending) > 5 else ''}. Run `python3 sweep.py fwd-ddl promote --from {src.name} --to {tgt.name}"
                             f"{' --allow-ddl' if not tgt.allow_ddl else ''}` first (--ignore-pending-fwd overrides).")
    sconn, tconn = layer_conn(src), layer_conn(tgt)
    if not same_credentials(sconn, tconn):
        raise LayerError("source and target layer must use the same host, port, user and password for the cross-database transfer "
                         "(the source psql inherits the target's credentials); use ~/.pgpass or aligned sweep.<layer>.env files")
    results = []
    opts = {"run_id": current_run_id(), "full": full, "dry_run": dry_run, "transport": None, "force_full_without_delta": force_full_without_delta}
    with use_layer(tgt):
        from sweep_layers import verify_database
        verify_database(tgt)
        try:
            c.check_postgres_version()      # the staged script validates values with pg_input_is_valid (PostgreSQL 16+)
        except RuntimeError as e:
            raise LayerError(str(e))
        for lay, dbname in ((src, src.database), (tgt, tgt.database)):
            done = {int(r["version"]) for r in query_db_dicts("SELECT version FROM sweep_ctl.schema_version", dbname=dbname)} \
                if _has_version_table(dbname) else set()
            if LATEST_VERSION not in done:
                raise LayerError(f"{dbname} (layer {lay.name}) is not migrated: run `python3 sweep.py migrate --layer {lay.name}` first")
        opts["transport"] = pick_transport(transport)
        ensure_reject_table()
        logger.info(f"promote {src.name} -> {tgt.name}: {len(rows)} table(s), transport {opts['transport']}, run {opts['run_id']}"
                    + (" [DRY RUN]" if dry_run else "") + (" [FULL]" if full else ""))
        jobs = [{"target_schema": r["schema"], "target_table": r["table"], "size_bytes": 0, "filename": f"{r['schema']}.{r['table']}", "row": r}
                for r in rows]
        levels = plan_levels(jobs) if jobs else []
        meta_cache = {}
        failed_tables = set()
        for level, files in levels:
            logger.info(f"level {level}: {len(files)} table(s)")
            with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
                futures = [pool.submit(promote_table, src, tgt, j["row"], opts, meta_cache) for j in files]
                for fut in futures:
                    r = fut.result()
                    results.append(r)
                    if r["status"] == "FAILED":
                        failed_tables.add(r["table"])
    failed = [r for r in results if r["status"] == "FAILED"]
    ok = sum(1 for r in results if r["status"] in ("SUCCESS", "DRY_RUN"))
    logger.info(f"promote {src.name} -> {tgt.name} finished: {ok} table(s) applied, {sum(1 for r in results if r['status'] == 'UP_TO_DATE')} up to date, "
                f"{sum(1 for r in results if r['status'] == 'SKIPPED')} skipped, {len(failed)} failed")
    for r in failed:
        logger.error(f"  FAILED {r['table']}: {r['message']}")
    return results, (EXIT_FAILED if failed else 0)


def _has_version_table(dbname):
    rows = query_db_dicts("SELECT to_regclass('sweep_ctl.schema_version') IS NOT NULL AS ok", dbname=dbname)
    return bool(rows) and rows[0]["ok"] in ("t", "true")


def write_promote_report(results, src, tgt, dry_run):
    from sweep_common import ORACLE_REPORTS_DIR, generate_excel_report, get_timestamp
    report = ORACLE_REPORTS_DIR / f"{tgt.database.lower()}_promote_{src.name}_to_{tgt.name}_{get_timestamp()}.xlsx"
    summary = [{"Metric": "Hop", "Value": f"{src.name} -> {tgt.name}"}, {"Metric": "Dry run", "Value": dry_run},
               {"Metric": "Tables", "Value": len(results)},
               {"Metric": "Failed", "Value": sum(1 for r in results if r["status"] == "FAILED")}]
    generate_excel_report(report, {"Summary": summary, "Tables": results or [{"status": "nothing to do"}]})
    return report


# -----------------------------------------------------------------------------
# status matrix, purge, chain
# -----------------------------------------------------------------------------
def promote_status(tables=None, limit=10):
    """
    The delta x layer status matrix: for every table the latest `limit` deltas with their state in each layer
    (SUCCESS = applied, FAILED = last attempt failed, BLOCKED = previous layer has not applied it, PENDING = can be promoted).
    Returns (rows, layers).
    """
    layers = chain()
    reg = {}
    failures = {}
    for lay in layers:
        try:
            rows = query_db_dicts(f"SELECT target_schema AS s, target_table AS t, seq, status FROM {REGISTRY}", dbname=lay.database)
            fails = query_db_dicts(f"SELECT DISTINCT ON (target_schema, target_table, seq) target_schema AS s, target_table AS t, seq, status "
                                   f"FROM {CTL_SCHEMA}.promote_log WHERE target_layer = {lit(lay.name)} ORDER BY target_schema, target_table, seq, id DESC",
                                   dbname=lay.database)
        except RuntimeError as e:
            logger.warning(f"layer {lay.name}: registry not readable ({clip_error(e)})")
            rows, fails = [], []
        for r in rows:
            reg[(lay.name, r["s"], r["t"], int(r["seq"]))] = r["status"]
        for r in fails:
            if r["seq"] not in ("", None) and r["status"] == "FAILED":
                failures[(lay.name, r["s"], r["t"], int(r["seq"]))] = "FAILED"
    keys = sorted({(s, t, sq) for (_, s, t, sq) in reg})
    from sweep_targeting import Selector, parse_table_ref
    sel = Selector(tables=[parse_table_ref(t) for t in (tables or [])])
    per_table = defaultdict(list)
    for s, t, sq in keys:
        if sel.matches(s, t):
            per_table[(s, t)].append(sq)
    out = []
    for (s, t), seqs in sorted(per_table.items()):
        for sq in sorted(seqs, reverse=True)[:limit]:
            row = {"table": f"{s}.{t}", "seq": sq}
            prev_ok = True
            for lay in layers:
                st = reg.get((lay.name, s, t, sq)) or failures.get((lay.name, s, t, sq))
                if st:
                    row[lay.name] = st
                else:
                    row[lay.name] = "PENDING" if prev_ok else "BLOCKED"
                prev_ok = prev_ok and row[lay.name] == "SUCCESS"
            out.append(row)
    return out, [l.name for l in layers]


def format_status(rows, layers):
    if not rows:
        return "No deltas registered yet."
    w = max(len(r["table"]) for r in rows)
    lines = [f"{'table'.ljust(w)}  {'seq':>5}  " + "  ".join(l.ljust(11) for l in layers)]
    for r in rows:
        lines.append(f"{r['table'].ljust(w)}  {r['seq']:>5}  " + "  ".join(str(r[l]).ljust(11) for l in layers))
    return "\n".join(lines)


def purge_deltas(dry_run=False, retention_days=None):
    """
    Removes delta rows once EVERY later layer applied them and SWEEP_DELTA_RETENTION_DAYS have passed since the last
    application. The registry row stays (purged_at is set); the data tables themselves are never touched.
    """
    days = RETENTION_DAYS if retention_days is None else int(retention_days)
    layers = chain()
    regs = {}
    for lay in layers:
        regs[lay.name] = {(r["target_schema"], r["target_table"], int(r["seq"])): r for r in query_db_dicts(
            f"SELECT target_schema, target_table, seq, delta_table, purged_at, status, applied_at FROM {REGISTRY}", dbname=lay.database)}
    now = datetime.now(timezone.utc)
    purged = []
    for i, lay in enumerate(layers[:-1]):
        later = layers[i + 1:]
        todo = []
        for key, r in regs[lay.name].items():
            if not r["delta_table"] or r["purged_at"]:
                continue
            applied = [regs[l.name].get(key) for l in later]
            if any(a is None or a["status"] != "SUCCESS" for a in applied):
                continue
            last = max(datetime.strptime(canon_time(a["applied_at"]), "%Y-%m-%dT%H:%M:%SZ") if a["applied_at"] else now for a in applied)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if (now - last).days >= days:
                todo.append((key, r["delta_table"]))
        if not todo:
            continue
        logger.info(f"layer {lay.name}: {len(todo)} delta(s) can be purged" + (" [dry run]" if dry_run else ""))
        purged.extend((lay.name, k[0], k[1], k[2]) for k, _ in todo)
        if dry_run:
            continue
        with use_layer(lay):
            for (s, t, sq), table in todo:
                run_psql(f"DELETE FROM {table} WHERE _seq = {sq};\n"
                         f"UPDATE {REGISTRY} SET purged_at = now() WHERE target_schema = {lit(s)} AND target_table = {lit(t)} AND seq = {sq};")
    return purged


CHAIN_STAGES = ("bronze", "silver", "models", "gold_backup", "gold")


def _fwd_args(layer=None, **kw):
    import argparse
    ns = dict(layer=layer, dry_run=False, allow_ddl=False, allow_data=False, force=False, file=None, filename=None, tables=None, schemas=None,
              exclude_tables=None, from_layer=None, to_layer=None)
    ns.update(kw)
    return argparse.Namespace(**ns)


def run_chain(until=None, load_cmds=None, workers=4, transport=None, tables=None, dry_run=False, skip_models=False, allow_ddl=False,
              models_args=None):
    """
    The order of a Day-2 run (schema first, then data, per layer):
      load bronze from S3 -> fwd-ddl apply (bronze) -> fwd-ddl promote + data promote (silver) -> RUN THE DATA MODELS in silver ->
      fwd-ddl promote + data promote (gold_backup: Oracle deltas AND model outputs) -> fwd-ddl promote + data promote (gold)
    It stops at the first failure. `until` is one of bronze, silver, models, gold_backup, gold. It never continues past silver while silver
    has pending FE files (the models read the FE-changed tables). `skip_models` promotes without running the models.
    FE files for gold_backup / gold are applied only with `allow_ddl` (they are DDL on live layers).
    Returns the exit code.
    """
    import sweep_fwd_ddl as fd
    layers = chain()
    names = [l.name for l in layers]
    if until and until not in CHAIN_STAGES:
        raise LayerError(f"unknown --until {until!r}: choose one of {', '.join(CHAIN_STAGES)}")
    stop_after = CHAIN_STAGES.index(until) if until else len(CHAIN_STAGES)

    def reached(stage):
        return CHAIN_STAGES.index(stage) <= stop_after

    for cmd in (load_cmds or []):
        logger.info(f"chain: loading {names[0]}: {' '.join(cmd)}")
        if subprocess.run(cmd, cwd=str(c.BASE_DIR), env=os.environ.copy()).returncode != 0:
            logger.error(f"chain stopped: the {names[0]} load failed")
            return EXIT_FAILED
    logger.info("chain: FE (fwd-ddl) files of bronze")
    if fd.cmd_apply(_fwd_args("bronze", dry_run=dry_run)) != 0:
        logger.error("chain stopped: fwd-ddl apply on bronze failed (nothing was promoted)")
        return EXIT_FAILED
    if not reached("silver"):
        return 0
    hops = list(zip(layers, layers[1:]))                      # bronze->silver, silver->gold_backup, gold_backup->gold
    for src, tgt in hops:
        if tgt.name == "gold_backup":
            if not skip_models and reached("models"):
                code = _models_stage(models_args, dry_run)
                if code != 0:
                    logger.error("chain stopped: a data model failed (nothing was promoted to gold_backup)")
                    return code
            if not reached("gold_backup"):
                return 0
            if not skip_models:
                pend = fd.pending_count("silver")
                if pend:
                    logger.error(f"chain stopped: silver has {pend} pending FE file(s); apply them before the models run")
                    return EXIT_FAILED
        pending = fwd_pending_between(src, tgt)
        if pending:
            if not tgt.allow_ddl and not allow_ddl:
                logger.error(f"chain stopped: {len(pending)} FE file(s) must be applied to {tgt.name} ({', '.join(pending[:3])}); "
                             f"run the chain with --allow-ddl in the quiet window")
                return EXIT_FAILED
            logger.info(f"chain: FE (fwd-ddl) files {src.name} -> {tgt.name}")
            if fd.cmd_promote(_fwd_args(tgt.name, from_layer=src.name, to_layer=tgt.name, allow_ddl=allow_ddl or tgt.allow_ddl, dry_run=dry_run)) != 0:
                logger.error(f"chain stopped: fwd-ddl promote {src.name} -> {tgt.name} failed")
                return EXIT_FAILED
        results, code = promote(src.name, tgt.name, tables=tables, dry_run=dry_run, workers=workers, transport=transport,
                                ignore_pending_fwd=dry_run)
        if code != 0:
            logger.error(f"chain stopped at {tgt.name}: {src.name} -> {tgt.name} had failures (later layers are not touched)")
            return code
        if tgt.name == "silver":
            pend = fd.pending_count("silver")
            summary = "none" if not pend else f"{pend} pending - apply them before data modelling"
            logger.info(f"chain: silver is ready for the data models; pending FE files in silver: {summary}")
            if pend:
                logger.error("chain stopped: silver has pending FE files; data modelling must not start")
                return EXIT_FAILED
            if until == "silver":
                logger.info("chain stopped after silver (--until)")
                return 0
        elif until == tgt.name:
            logger.info(f"chain stopped after {tgt.name} (--until)")
            return 0
    return 0


def _models_stage(models_args, dry_run):
    """Runs the data models in silver (K); 0 when there is nothing to run."""
    try:
        import sweep_models
    except ImportError:
        return 0
    return sweep_models.run_models_cmd(**(models_args or {}), dry_run=dry_run)
