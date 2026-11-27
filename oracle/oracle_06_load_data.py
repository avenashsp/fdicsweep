#!/usr/bin/env python3
"""
06_loadData.py - High-Throughput Multi-Worker S3 Streaming Ingestion Engine
Streams data directly from S3 through Unix pipes into PostgreSQL with zero local disk buffering
and zero Python memory deserialization.

Features:
  - Takes GZIP files (*.gz) with priority; falls back to CSV (*.csv) if no gzip exists.
  - Ingestion order strictly from SMALLEST to LARGEST file size.
  - Per-file transactional atomicity: atomic COPY and sweep_ctl.load_log insert.
  - Skips already completed files on re-runs or retries.
  - Multi-threaded worker pool (default: 4 workers, tuned for 2 vCPU Aurora db.t4g.large).
  - Supports --gate-pass to load ONLY tables that passed the pre-flight type gateway.

Generates: reports/06_loadData_report_<timestamp>.xlsx
"""
import os
import re
import sys
import csv
import json
import time
import argparse
import threading
from typing import NamedTuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from sweep_common import (
    log_summary,
    ORACLE_OUTPUTS_DIR,
    ORACLE_REPORTS_DIR,
    ORACLE_LOGS_DIR,
    AWS_REGION,
    DEFAULT_WORKERS,
    ORACLE_LOAD_LOG_TABLE,
    has_db_auth,
    AUTH_HELP,
    check_postgres_version,
    clip_error,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    query_db_dicts,
    run_psql,
    run_psql_script,
    run_psql_script_stream,
    norm_col,
    q,
    LAYER,
    lit,
    format_size_actual,
    DB_ERRORS,
)
from oracle.oracle_06_pipeline import (
    FAST_ROWS, build_fast_script, build_staged_script, copy_from_stdin, csum, ensure_reject_table, fast_baseline_sql, fast_bulk_blockers,
    load_log_insert,
    load_table_meta, parse_output, persist_failed_file_rejects, plan_load, probe_trigger_mode, staged_chunk, staged_epilogue,
    staged_prologue,
)
from sweep_loadpipe import chunked_script
from sweep_s3meta import classify_loaded, head_many, head_object, make_meta, meta_from_row
from oracle.oracle_06_order import plan_levels
from sweep_streaming import read_stream_head, stream_argv, stream_program_literal
from sweep_platform import atomic_write_json
from sweep_targeting import Selector, add_targeting_args, selector_or_exit, validate_or_exit
from sweep_layers import add_layer_arg, ensure_layer, require_bronze, restrict_to_manifest
from sweep_delta import ensure_delta_table, fast_registry_sql
from sweep_fwd_rules import resolve_columns, rules_for
from sweep_loadpipe import history_insert_sql, record_history_failure, tables_with_rows
from sweep_migrate import require_migrated
from sweep_merge import current_run_id
logger = get_logger("oracle_06_load_data", "oracle_06_load_data")
LOCK = threading.Lock()
DEFAULT_CHUNK_ROWS = 2_000_000    # a staged load of a big file runs in chunks of this many records (0 = never chunk)
DEFAULT_CHUNK_MIN_MB = 256         # files at least this big (compressed size) are loaded in chunks
MAX_REJECT_ROWS_IN_REPORT = 10000   # rows shown in the Rejected_Rows sheet; all are in sweep_ctl.load_rejects
FULL_LOAD = False              # --full: promote copies the whole table instead of a captured delta
DEFAULT_MAX_REJECTS = 0       # 0 = valid rows always load; the file fails only on technical errors (--max-rejects N to fail at N rejects)
COL_CACHE = {}
PK_CACHE = {}


def get_table_pk_columns(schema, table):
    """Retrieves primary key column list from PostgreSQL catalog."""
    key = (schema.lower(), table.lower())
    with LOCK:
        if key in PK_CACHE:
            return PK_CACHE[key]

    sql = (f"SELECT a.attname "
           f"FROM pg_index i "
           f"JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
           f"JOIN pg_class c ON c.oid = i.indrelid "
           f"JOIN pg_namespace n ON n.oid = c.relnamespace "
           f"WHERE n.nspname = {lit(schema.lower())} AND c.relname = {lit(table.lower())} "
           f"  AND i.indisprimary "
           f"ORDER BY array_position(i.indkey, a.attnum);")
    # A lookup failure must NOT be treated as "no primary key": that would silently turn an
    # upsert into a blind append and create duplicates. Let the error fail this file instead.
    out = run_psql(sql, tuples_only=True, unaligned=True)
    cols = [norm_col(c) for c in out.splitlines() if c.strip()]
    with LOCK:
        PK_CACHE[key] = cols
    return cols


def is_table_empty(schema, table):
    """Checks whether the target table currently has zero rows. Raises on lookup failure (never guesses)."""
    sql = f"SELECT EXISTS (SELECT 1 FROM {q(schema)}.{q(table)} LIMIT 1);"
    out = run_psql(sql, tuples_only=True, unaligned=True).strip().lower()
    return out in ("f", "false", "0")


ACTIVE_WORKERS_FILE = ORACLE_LOGS_DIR / "active_workers.json"
_ACTIVE_STREAMS = {}


def _flush_active_streams():
    try:
        os.makedirs(ORACLE_LOGS_DIR, exist_ok=True)
        items = []
        now = time.time()
        for w in _ACTIVE_STREAMS.values():
            item = dict(w)
            item["seconds"] = max(1, int(now - item.get("start_time", now)))
            items.append(item)
        atomic_write_json(ACTIVE_WORKERS_FILE, items)
    except OSError as e:
        logger.debug(f"Could not write active worker state: {e}")


def record_stream_start(worker_id, job, size_mb_val):
    with LOCK:
        _ACTIVE_STREAMS[worker_id] = {
            "pid": os.getpid(),
            "worker_id": worker_id,
            "filename": job.get("filename", Path(job.get("uri", "")).name),
            "table": f"{job.get('target_schema', '')}.{job.get('target_table', '')}".strip("."),
            "mb": size_mb_val,
            "start_time": time.time(),
            "uri": job.get("uri", "")
        }
        _flush_active_streams()


def record_stream_end(worker_id):
    with LOCK:
        if worker_id is None:
            _ACTIVE_STREAMS.clear()
        else:
            _ACTIVE_STREAMS.pop(worker_id, None)
        _flush_active_streams()


def get_loaded_rows():
    """{uri: rows loaded} of every SUCCESS row (used to tell an empty file from a truncated table)."""
    try:
        return {r["uri"]: int(r["loaded_rows"] or 0) for r in
                query_db_dicts(f"SELECT uri, loaded_rows FROM {ORACLE_LOAD_LOG_TABLE} WHERE status = 'SUCCESS'")}
    except (RuntimeError, ValueError, KeyError):
        return {}


def get_loaded_log():
    """
    {uri: stored S3 metadata} of every file with a SUCCESS row in sweep_ctl.load_log. Rows loaded before the
    metadata columns existed carry empty metadata (see classify_job).
    """
    try:
        rows = query_db_dicts(f"SELECT uri, s3_last_modified, s3_etag, s3_size FROM {ORACLE_LOAD_LOG_TABLE} WHERE status = 'SUCCESS'")
        return {r["uri"]: make_meta(r.get("s3_last_modified"), r.get("s3_etag"), r.get("s3_size")) for r in rows}
    except DB_ERRORS as e:
        logger.warning(f"Could not read S3 metadata from {ORACLE_LOAD_LOG_TABLE} ({e}); falling back to URIs only. "
                       f"Apply the control DDL (Step 05) to add the s3_* columns.")
    try:
        return {r["uri"]: make_meta("", "", "")
                for r in query_db_dicts(f"SELECT uri FROM {ORACLE_LOAD_LOG_TABLE} WHERE status = 'SUCCESS'")}
    except DB_ERRORS as e:
        logger.warning(f"Could not read loaded URIs from {ORACLE_LOAD_LOG_TABLE} (table might be new): {e}")
        return {}


def classify_job(job, stored, live):
    """('skip' | 'load' | 'reload', reason) - see sweep_s3meta.classify_loaded; `live` = HEAD result, else the file map."""
    return classify_loaded([] if stored is None else [stored], live or meta_from_row(job))


def get_target_columns(schema, table):
    """Retrieves target table column list from PostgreSQL catalog."""
    key = (schema.lower(), table.lower())
    with LOCK:
        if key in COL_CACHE:
            return COL_CACHE[key]

    sql = (f"SELECT column_name FROM information_schema.columns "
           f"WHERE table_schema = {lit(schema.lower())} AND table_name = {lit(table.lower())} "
           f"ORDER BY ordinal_position;")
    out = run_psql(sql)
    cols = [norm_col(c) for c in out.splitlines() if c.strip()]
    with LOCK:
        COL_CACHE[key] = cols
    return cols


def extract_file_header(uri, region=AWS_REGION):
    """Reads the CSV header line of the (decompressed) S3/local file and returns normalised column names."""
    lines, err = read_stream_head(uri, region, max_lines=1)
    line = lines[0] if lines else ""
    if not line.strip():
        raise RuntimeError(f"Cannot read header line from {uri}: {err or 'empty file'}")
    return [norm_col(c) for c in next(csv.reader([line]))]


class LoadOutcome(NamedTuple):
    """Result of one file load. The first four fields keep the old (status, rows, seconds, error) meaning."""
    status: str
    rows: int
    seconds: float
    error: str
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0
    duplicates: int = 0
    read: int = 0
    reject_summary: tuple = ()
    reject_rows: tuple = ()
    trigger_mode: str = ""
    path: str = ""            # how the file was loaded: fast bulk / staged / staged, chunked


META_CACHE = {}


def get_table_meta(schema, table):
    key = (schema.lower(), table.lower())
    with LOCK:
        if key in META_CACHE:
            return META_CACHE[key]
    meta = load_table_meta(schema, table)
    with LOCK:
        META_CACHE[key] = meta
    return meta


def _failed(error, sec=0.0, **extra):
    return LoadOutcome("FAILED", 0, sec, error, **extra)


FWD_RULES = None


def fwd_rules():
    """
    Step 06 loads BRONZE, the raw mirror of the source: FE files are never applied there (they run in silver and move on to gold_backup and
    gold), so a bronze load has no loader rules. The FE rules are applied by `promote` (bronze -> silver). Always empty; the file header is
    still checked against the table, and an unknown column fails the file.
    """
    return []


def plan_file(job, region):
    """
    Reads the header of the file and checks it against the target table and the FE loader rules WITHOUT loading.
    Returns (skip_reason, file_cols, meta, resolution, plan, problem): skip_reason is set for a skip-table rule, problem for anything
    that would make the real load fail (unknown column, missing required column, missing table).
    """
    schema, table = job["target_schema"].lower(), job["target_table"].lower()
    rules = rules_for(fwd_rules(), schema, table)
    if rules.skip:
        return f"table {schema}.{table} was dropped by an FE file (skip-table rule)", [], None, None, None, ""
    file_cols = extract_file_header(job["uri"], region)
    meta = get_table_meta(schema, table)
    if not meta["columns"]:
        return "", file_cols, meta, None, None, f"Target table {schema}.{table} does not exist in PostgreSQL."
    res = resolve_columns(file_cols, meta["columns"], rules)
    if res.problem:
        return "", file_cols, meta, res, None, res.problem
    plan, problem = plan_load(meta, res.table_cols)
    return "", file_cols, meta, res, plan, problem


def load_single_file(job, worker_id, region=AWS_REGION, dry_run=False, max_rejects=DEFAULT_MAX_REJECTS,
                     fast_bulk=False, trigger_mode="replica", chunk_rows=DEFAULT_CHUNK_ROWS, chunk_min_mb=DEFAULT_CHUNK_MIN_MB):
    """
    Loads one S3 file in ONE database transaction (see oracle_06_pipeline for the steps).
    Bad rows are rejected and reported; the file fails (and rolls back completely) only when the number of
    rejected rows reaches `max_rejects` or the row-count check does not add up.
    """
    uri = job["uri"]
    schema = job["target_schema"].lower()
    table = job["target_table"].lower()
    size_bytes = int(job.get("size_bytes") or 0)
    file_format = job.get("file_format", "gzip")

    if dry_run:      # reads the file header and runs the same plan check as the real load, without loading
        try:
            skip, _fc, _m, res, _plan, problem = plan_file(job, region)
        except (RuntimeError, OSError, ValueError) as e:
            return _failed(f"dry-run check: {clip_error(e)}")
        if skip:
            return LoadOutcome("SKIPPED", 0, 0.0, skip)
        if problem:
            return _failed(f"dry-run check: {problem}")
        note = f"; FE rules: {len(res.ignored)} ignored, {sum(1 for t, f in res.col_map.items() if t != f)} renamed, {len(res.defaults)} default" \
            if (res.ignored or res.defaults or any(t != f for t, f in res.col_map.items())) else ""
        return LoadOutcome("DRY_RUN", 0, 0.0, note)

    t0 = time.time()
    size_mb_val = float(job.get("size_mb") or (size_bytes / (1024**2)))
    record_stream_start(worker_id, job, size_mb_val)
    try:
        skip, file_cols, meta, resolution, plan, problem = plan_file(job, region)
        if skip:
            logger.info(f"[Worker {worker_id}] SKIPPED {job.get('filename', uri)}: {skip}")
            return LoadOutcome("SKIPPED", 0, round(time.time() - t0, 2), skip)
        if problem:
            return _failed(problem)
        header_cols = resolution.table_cols
        for ign in resolution.ignored:
            logger.info(f"[Worker {worker_id}] INFO column {ign} of {job.get('filename', uri)} is ignored (FE ignore-column rule)")
        has_rules = bool(resolution.ignored or resolution.defaults or any(t != f for t, f in resolution.col_map.items()))

        # Re-check the object right before loading: the load_log row stores exactly what was loaded, so a stale
        # file map cannot cause a skipped or mis-recorded reload.
        s3_meta = head_object(uri, region)
        stream_cmd = stream_program_literal(uri, region)
        log_sql = load_log_insert(uri, schema, table, size_bytes, file_format, worker_id, s3_meta)
        _c = csum                                  # counters are summed over all chunks (a single pass has one)
        hist = lambda **kw: history_insert_sql("oracle", LAYER or "bronze", "s3", current_run_id(), uri, schema, table, s3_meta=s3_meta, **kw)
        name = job.get("filename", Path(uri).name)

        use_fast = False
        if fast_bulk:
            why = fast_bulk_blockers(meta, header_cols, has_rules)
            use_fast = not why
            if why:
                logger.warning(f"[Worker {worker_id}] --fast-bulk skipped for {schema}.{table} - using the staged load: {'; '.join(why)}")
        chunked = (not use_fast) and chunk_rows > 0 and size_mb_val >= chunk_min_mb
        if use_fast:
            path = "fast bulk"
            logger.info(f"[Worker {worker_id}] Started stream: {name} ({format_size_actual(size_mb_val)}) -> {schema}.{table} (fast bulk - direct COPY into a bare table, no staging)")
            # the rows of THIS file come from the \\copy itself (psql ROW_COUNT -> :n), inside the transaction: the target table is never counted.
            # Registry: always a full copy of this file's rows; ledger: rows_read = rows_inserted = the file's rows, baseline 0 only when the
            # table was empty (see fast_baseline_sql), otherwise unknown
            script = build_fast_script(schema, table, header_cols, stream_cmd, meta, trigger_mode,
                                       fast_registry_sql(schema, table, current_run_id(), uri, header_cols, LAYER or "bronze") + log_sql
                                       + hist(read=FAST_ROWS, inserted=FAST_ROWS, updated="0", unchanged="0", rejected="0", dup="0",
                                              before_sql=fast_baseline_sql(schema, table)))
        else:
            how = f"upsert on {', '.join(plan['arbiter']['cols'])}" if plan["arbiter"] else "append - no key"
            path = "staged, chunked" if chunked else "staged"
            logger.info(f"[Worker {worker_id}] Started stream: {name} ({format_size_actual(size_mb_val)}) -> {schema}.{table} "
                        f"({path} load, {how}; triggers off via {trigger_mode}{f'; chunks of {chunk_rows:,} rows' if chunked else ''})")
            for skipped in plan["skipped_checks"]:
                logger.warning(f"[Worker {worker_id}] CHECK {skipped} references columns that are not in the file; the table still enforces it")
            ensure_delta_table(schema, table)        # applied rows are captured for `promote` (unless the table is empty / --full)
            capture = {"run_id": current_run_id(), "layer": "bronze", "full": FULL_LOAD, "uris": uri, "downstream": True}
            log_sql += "\n" + hist(read=_c("raw"), inserted=_c("inserted"), updated=_c("updated"), unchanged=_c("unchanged"),
                                    rejected=_c("rejected"), dup=_c("dup"))
            if not chunked:
                script = build_staged_script(schema, table, header_cols, stream_cmd, meta, plan, trigger_mode, max_rejects, uri, log_sql,
                                             capture=capture, raw_cols=file_cols, col_map=resolution.col_map, defaults=resolution.defaults)

        # High-performance bulk load options. psql runs in its own process group, with a hard
        # timeout (SWEEP_LOAD_TIMEOUT) that also kills the S3/gzip pipeline it spawned.
        load_options = "-c synchronous_commit=off -c statement_timeout=0 -c lock_timeout=0"
        if chunked:
            # still ONE transaction: the rows are staged chunk by chunk so the temp tables never hold the whole file; the delta is
            # captured under one sequence number and registered once; the counters of all chunks are summed
            res = run_psql_script_stream(
                chunked_script(staged_prologue(schema, table, file_cols, meta, trigger_mode), copy_from_stdin(file_cols),
                               lambda ci: staged_chunk(schema, table, header_cols, meta, plan, max_rejects, ci, capture=capture,
                                                       col_map=resolution.col_map, defaults=resolution.defaults),
                               staged_epilogue(schema, table, header_cols, meta, trigger_mode, uri, log_sql, capture=capture),
                               stream_argv(uri, region), chunk_rows,
                               on_chunk=lambda ci, n: logger.info(f"[Worker {worker_id}] {name}: chunk {ci} applied ({n:,} rows)")),
                options=load_options)
        else:
            res = run_psql_script(script, options=load_options)
        sec = round(time.time() - t0, 2)
        counts, summary, reject_rows = parse_output(res.stdout)
        extra = dict(rejected=counts.get("rejected", 0), duplicates=counts.get("dup", 0), read=counts.get("raw", 0),
                     reject_summary=tuple(summary), reject_rows=tuple(reject_rows), trigger_mode=trigger_mode, path=path)

        if res.returncode != 0:
            err = clip_error(res.stderr or res.stdout or "psql failed")
            if reject_rows:       # the transaction rolled back; keep the rejected rows for review
                try:
                    persist_failed_file_rejects(uri, schema, table, reject_rows)
                except RuntimeError as e:
                    logger.warning(f"[Worker {worker_id}] Could not store the rejected rows of {uri}: {e}")
            record_history_failure("oracle", LAYER or "bronze", "s3", current_run_id(), uri, schema, table, err, counts.get("raw", 0))
            return _failed(err, sec, **extra)

        if use_fast:
            m = re.search(r"COPY\s+(\d+)", res.stdout)
            rows = int(m.group(1)) if m else 0
            counts = {"raw": rows, "inserted": rows}
            extra.update(read=rows)
        inserted, updated = counts.get("inserted", 0), counts.get("updated", 0)
        rows = inserted + updated

        # Update loaded_rows and seconds
        try:
            update_sql = f"UPDATE {ORACLE_LOAD_LOG_TABLE} SET loaded_rows = {rows}, seconds = {sec} WHERE uri = {lit(uri)};"
            run_psql(update_sql)
        except RuntimeError as e:
            # Data and its load_log row are already committed; only the stats update failed.
            logger.warning(f"[Worker {worker_id}] Could not record row count for {uri}: {e}")

        if extra["rejected"]:
            logger.warning(f"[Worker {worker_id}] {name}: {extra['rejected']} row(s) rejected (see sweep_ctl.load_rejects / the report)")
        return LoadOutcome("SUCCESS", rows, sec, "", inserted=inserted, updated=updated,
                           unchanged=counts.get("unchanged", 0), **extra)
    except Exception as e:  # noqa: BLE001 - a worker must report any failure, never crash the pool
        logger.error(f"[Worker {worker_id}] Load failed for {uri}: {e}")
        record_history_failure("oracle", LAYER or "bronze", "s3", current_run_id(), uri, schema, table, clip_error(e))
        return _failed(clip_error(e), round(time.time() - t0, 2))
    finally:
        record_stream_end(worker_id)


def _legacy_selector(tables_filter, exclude_tables, file_uri, filename, dry_run):
    """Selector for callers that still pass the old set / string arguments."""
    ref = lambda t: (None, t.lower()) if "." not in t else tuple(t.lower().split(".", 1))
    return Selector(tables=[ref(t) for t in (tables_filter or ())], exclude=[ref(t) for t in (exclude_tables or ())],
                    files=[f for f in (file_uri, filename) if f], dry_run=bool(dry_run))


def run_stream_load(workers=DEFAULT_WORKERS, tables_filter=None, exclude_tables=None, file_uri=None, filename=None, limit_files=None, max_size_mb=None, min_size_mb=None, dry_run=False, gate_pass=False, reload=False,
                    max_rejects=DEFAULT_MAX_REJECTS, fast_bulk=False, trigger_mode="auto", selector=None, layer=None,
                    chunk_rows=DEFAULT_CHUNK_ROWS, chunk_min_mb=DEFAULT_CHUNK_MIN_MB):
    ts = get_timestamp()
    sel = selector or _legacy_selector(tables_filter, exclude_tables, file_uri, filename, dry_run)
    dry_run = dry_run or sel.dry_run
    global FWD_RULES
    FWD_RULES = None                       # the FE loader rules and the table layouts are read again for every run
    META_CACHE.clear()
    COL_CACHE.clear()
    if not has_db_auth():
        logger.error(AUTH_HELP)
        return False
    map_path = ORACLE_OUTPUTS_DIR / "oracle_02_s3_file_map.csv"
    if not map_path.exists():
        map_path = ORACLE_OUTPUTS_DIR / "s3_file_map.csv"

    if not map_path.exists():
        logger.error(f"File map not found at {map_path}. Run 02_s3Inventory.py first.")
        return False

    with open(map_path, "r", encoding="utf-8") as f:
        all_jobs = list(csv.DictReader(f))

    if not all_jobs:
        logger.error("No jobs found in s3_file_map.csv.")
        return False

    # Apply Gate-Pass filter if requested
    if gate_pass:
        gate_txt = ORACLE_OUTPUTS_DIR / "oracle_03_gate_pass_tables.txt"
        if not gate_txt.exists():
            gate_txt = ORACLE_OUTPUTS_DIR / "gate_pass_tables.txt"
        gate_csv = ORACLE_OUTPUTS_DIR / "gate_results.csv"
        passed_tables = set()

        if gate_txt.exists():
            with open(gate_txt, "r", encoding="utf-8") as f:
                passed_tables = {line.strip().upper() for line in f if line.strip()}
        elif gate_csv.exists():
            with open(gate_csv, "r", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    if r.get("status", "").upper() in ("PASS", "WARN") or int(r.get("blocks", 1) or 0) == 0:
                        passed_tables.add(f"{r['owner']}.{r['table_name']}".upper())
                        passed_tables.add(r['table_name'].upper())
        else:
            # Check latest report in reports/
            gate_reports = sorted(ORACLE_REPORTS_DIR.glob("*type*gate*report*.xlsx"), reverse=True)
            if gate_reports:
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(gate_reports[0], data_only=True)
                    if "Table_Results" in wb.sheetnames:
                        ws = wb["Table_Results"]
                        rows = list(ws.iter_rows(values_only=True))
                        if rows:
                            h = [str(x).lower() if x else "" for x in rows[0]]
                            o_idx = h.index("owner") if "owner" in h else -1
                            t_idx = h.index("table_name") if "table_name" in h else -1
                            s_idx = h.index("status") if "status" in h else -1
                            b_idx = h.index("blocks") if "blocks" in h else -1
                            for r in rows[1:]:
                                is_pass = False
                                if s_idx != -1 and str(r[s_idx]).upper() in ("PASS", "WARN"):
                                    is_pass = True
                                elif b_idx != -1 and int(r[b_idx] or 0) == 0:
                                    is_pass = True
                                if is_pass:
                                    o = str(r[o_idx]).upper() if o_idx != -1 else ""
                                    t = str(r[t_idx]).upper() if t_idx != -1 else ""
                                    passed_tables.add(f"{o}.{t}")
                                    passed_tables.add(t)
                except (OSError, ValueError, KeyError, IndexError) as e:
                    logger.warning(f"Could not read gate report: {e}")

        if not passed_tables:
            logger.error("No passed tables found from type gate! Run 'python3 sweep.py oracle gate' first.")
            return False

        logger.info(f"Gate-Pass Active: Filtering ingestion to only the {len(passed_tables)} table(s) that PASSED the type gate.")
        all_jobs = [j for j in all_jobs if f"{j['owner']}.{j['table_name']}".upper() in passed_tables
                    or f"{j['target_schema']}.{j['target_table']}".upper() in passed_tables
                    or j['table_name'].upper() in passed_tables]

    # Selectors (--tables / --schemas / --exclude-tables / --file / --filename): validated BEFORE anything runs
    names = [(j["owner"], j["table_name"]) for j in all_jobs] + [(j["target_schema"], j["target_table"]) for j in all_jobs]
    validate_or_exit(sel.validate_tables, names, what="the file map", logger=logger)
    validate_or_exit(sel.validate_files, [j["uri"] for j in all_jobs], what="the file map", logger=logger)
    all_jobs = [j for j in all_jobs
                if (sel.matches(j["owner"], j["table_name"]) or sel.matches(j["target_schema"], j["target_table"]))
                and (not sel.files or sel.matches_file(j["uri"]) or sel.matches_file(j.get("filename", "")))]
    file_uri = filename = sel.files[0] if sel.files else None     # a named file is always loaded (like --reload)

    if layer is not None and layer.manifest_path.exists():      # a layer only ever touches the tables of its own manifest
        keep = set(restrict_to_manifest(layer, [(j["target_schema"], j["target_table"]) for j in all_jobs], logger, "the load"))
        all_jobs = [j for j in all_jobs if (j["target_schema"], j["target_table"]) in keep]

    # Filter by size range if specified
    if max_size_mb is not None:
        all_jobs = [j for j in all_jobs if float(j.get("size_mb") or (int(j.get("size_bytes") or 0) / (1024**2))) <= max_size_mb]

    if min_size_mb is not None:
        all_jobs = [j for j in all_jobs if float(j.get("size_mb") or (int(j.get("size_bytes") or 0) / (1024**2))) >= min_size_mb]

    # Small to large inside a level (and overall with --fast-bulk); the level order itself is decided below
    all_jobs.sort(key=lambda x: (int(x.get("size_bytes") or 0), x.get("filename", "")))

    if limit_files:
        all_jobs = all_jobs[:limit_files]

    total_candidates = len(all_jobs)
    changed_count = 0
    if not reload and not file_uri and not filename and not dry_run:
        stored_log = get_loaded_log()
        known = [j["uri"] for j in all_jobs if j["uri"] in stored_log and any(stored_log[j["uri"]].values())]
        live = head_many(known, region=AWS_REGION) if known else {}
        pending_jobs, legacy, would_skip = [], 0, []
        for j in all_jobs:
            action, why = classify_job(j, stored_log.get(j["uri"]), live.get(j["uri"]))
            if action == "skip":
                legacy += "without S3 metadata" in why
                would_skip.append(j)
                continue
            if action == "reload":
                changed_count += 1
                logger.info(f"Reloading {j['filename']}: {why}")
            pending_jobs.append(j)
        if would_skip:     # loaded and unchanged - but only while the target table still holds rows (not truncated by hand)
            loaded_rows = get_loaded_rows()
            nonempty = tables_with_rows({(j["target_schema"], j["target_table"]) for j in would_skip if loaded_rows.get(j["uri"], 1) > 0})
            for j in would_skip:
                if loaded_rows.get(j["uri"], 1) > 0 and (j["target_schema"].lower(), j["target_table"].lower()) not in nonempty:
                    changed_count += 1
                    logger.info(f"Reloading {j['filename']}: target table {j['target_schema']}.{j['target_table']} is empty")
                    pending_jobs.append(j)
        skipped_count = total_candidates - len(pending_jobs)
        if legacy:
            logger.warning(f"{legacy} file(s) were loaded before S3 metadata was recorded and are treated as loaded "
                           f"(run with --reload to load them again and record their metadata).")
    else:
        pending_jobs = all_jobs
        skipped_count = 0

    if fast_bulk:
        # fast bulk loads are not FK-ordered: smallest to largest
        for j in pending_jobs:
            j["load_level"] = 0
        level_plan = [(0, pending_jobs)]
        order_text = "Smallest to Largest File Size (--fast-bulk)"
    else:
        level_plan = plan_levels(pending_jobs) if pending_jobs else []
        order_text = "Parent tables first, level by level"
        pending_jobs = [j for _, files in level_plan for j in files]

    logger.info("=" * 70)
    logger.info(f"SWEEP Ingestion Engine: {len(pending_jobs)} pending file(s) ({skipped_count} already loaded, {changed_count} re-delivered)")
    logger.info(f"Order: {order_text} | Workers: {workers} | Gate-Pass: {gate_pass} | Dry-Run: {dry_run}")
    if len(level_plan) > 1 or (level_plan and not fast_bulk):
        for lv, files in level_plan:
            logger.info(f"  level {lv}: {len({(f['target_schema'], f['target_table']) for f in files}):>5} table(s), {len(files):>5} file(s)")
    logger.info("=" * 70)

    if not pending_jobs:
        logger.info("All planned files have already been loaded into PostgreSQL. Nothing to do.")
        return True

    if not dry_run:
        try:
            check_postgres_version()
        except RuntimeError as e:
            logger.error(str(e))
            return False
        ensure_reject_table()
        if trigger_mode == "auto":
            trigger_mode = probe_trigger_mode()
        logger.info(f"Staged loads of files >= {chunk_min_mb:g} MB run in chunks of {chunk_rows:,} rows" if chunk_rows > 0
                    else "Chunked staging is off (--chunk-rows 0)")
        logger.info(f"Triggers are switched off per load transaction via: {trigger_mode} "
                    f"({'session_replication_role = replica' if trigger_mode == 'replica' else 'ALTER TABLE ... DISABLE TRIGGER (table lock until commit)'})")
        logger.info(("A file fails when " + str(max_rejects) + " rejected rows are reached"
                     if max_rejects else "Rejected rows never fail a file (only technical errors roll it back)")
                    + f"; fast bulk: {fast_bulk}")
    else:
        trigger_mode = trigger_mode if trigger_mode != "auto" else "replica"

    results = []
    reject_summary_rows = []
    reject_rows_all = []
    completed_files = 0
    total_loaded_rows = 0
    total_loaded_bytes = 0
    t_start = time.time()

    for level, level_jobs in level_plan:
        if not level_jobs:
            continue
        if not fast_bulk:
            logger.info(f"--- level {level}: loading {len(level_jobs)} file(s); the next level starts when all of them are done ---")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for idx, job in enumerate(level_jobs):
                worker_id = (idx % workers) + 1
                f = executor.submit(load_single_file, job, worker_id, dry_run=dry_run, max_rejects=max_rejects,
                                    fast_bulk=fast_bulk, trigger_mode=trigger_mode,
                                    chunk_rows=chunk_rows, chunk_min_mb=chunk_min_mb)
                futures[f] = job

            for future in as_completed(futures):
                job = futures[future]
                out = future.result()
                status, rows, sec, msg = out.status, out.rows, out.seconds, out.error
                completed_files += 1

                if status == "SUCCESS":
                    total_loaded_rows += rows
                    total_loaded_bytes += int(job.get("size_bytes") or 0)
                    rate = round(rows / sec) if sec > 0 else 0
                    logger.info(f"[{completed_files}/{len(pending_jobs)}] OK: {job['filename']} -> "
                                f"{job['target_schema']}.{job['target_table']} ({rows:,} rows in {sec}s ~ {rate:,} r/s; "
                                f"inserted {out.inserted:,}, updated {out.updated:,}, rejected {out.rejected:,}, duplicates dropped {out.duplicates:,})")
                elif status == "DRY_RUN":
                    logger.info(f"[{completed_files}/{len(pending_jobs)}] DRY-RUN OK: {job['filename']} -> "
                                f"{job['target_schema']}.{job['target_table']} ({job['size_mb']} MB){msg}")
                elif status == "SKIPPED":
                    logger.warning(f"[{completed_files}/{len(pending_jobs)}] SKIPPED: {job['filename']} -> {msg}")
                else:
                    logger.error(f"[{completed_files}/{len(pending_jobs)}] FAILED: {job['filename']} -> {msg}")

                results.append({
                    "load_level": job.get("load_level", ""),
                    "load_path": out.path,
                "load_order_rank": job.get("load_order_rank", ""),
                    "target_schema": job["target_schema"],
                    "target_table": job["target_table"],
                    "filename": job["filename"],
                    "size_mb": job["size_mb"],
                    "file_format": job.get("file_format", "gzip"),
                    "status": status,
                    "loaded_rows": rows,
                    "rows_read": out.read,
                    "rows_inserted": out.inserted,
                    "rows_updated": out.updated,
                    "rows_unchanged": out.unchanged,
                    "rows_rejected": out.rejected,
                    "duplicates_dropped": out.duplicates,
                    "duration_seconds": sec,
                    "error_message": msg,
                    "uri": job["uri"]
                })
                for g in out.reject_summary:
                    reject_summary_rows.append({"filename": job["filename"], "target_table": f"{job['target_schema']}.{job['target_table']}",
                                                "reason": g.get("reason"), "detail": g.get("detail"), "rows": g.get("n"),
                                                "file_status": status})
                for r in out.reject_rows:
                    reject_rows_all.append({"filename": job["filename"], "target_table": f"{job['target_schema']}.{job['target_table']}",
                                            "record_no": r.get("row_no"), "reason": r.get("reason"), "detail": r.get("detail"),
                                            "row_data": clip_error(json.dumps(r.get("row"), ensure_ascii=False, default=str)),
                                            "file_status": status})

    total_time = round(time.time() - t_start, 2)
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = sum(1 for r in results if r["status"] == "FAILED")

    logger.info("=" * 70)
    logger.info(f"Ingestion finished in {total_time}s. Loaded: {success_count}, Failed: {failed_count}, Rows: {total_loaded_rows:,}")
    logger.info("=" * 70)

    # Generate Excel Report
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_06_load_data_report_{ts}.xlsx"
    summary_sheet = [
        {"Metric": "Ingestion Timestamp", "Value": ts},
        {"Metric": "Gate-Pass Filter Applied", "Value": "YES (Passed Tables Only)" if gate_pass else "NO (All Ingestion Tables)"},
        {"Metric": "Total Candidate Files", "Value": total_candidates},
        {"Metric": "Files Executed This Run", "Value": len(pending_jobs)},
        {"Metric": "Successfully Loaded Files", "Value": success_count},
        {"Metric": "Failed Files", "Value": failed_count},
        {"Metric": "Skipped (Already Completed)", "Value": skipped_count},
        {"Metric": "Total Rows Loaded", "Value": total_loaded_rows},
        {"Metric": "Total Volume Ingested (GB)", "Value": round(total_loaded_bytes / (1024**3), 2)},
        {"Metric": "Total Ingestion Time (s)", "Value": total_time},
        {"Metric": "Overall Throughput", "Value": f"{round(total_loaded_rows / max(total_time, 0.1)):,} rows/sec"}
    ]

    total_rejected = sum(r.get("rows_rejected", 0) for r in results)
    total_dups = sum(r.get("duplicates_dropped", 0) for r in results)
    summary_sheet += [
        {"Metric": "Rows Rejected (bad rows)", "Value": total_rejected},
        {"Metric": "Duplicate Rows Dropped (in-file, last row wins)", "Value": total_dups},
        {"Metric": "Reject Limit Per File", "Value": max_rejects if max_rejects else "none"},
        {"Metric": "Trigger Handling", "Value": trigger_mode},
    ]
    empty_summary = [{"filename": "NONE", "target_table": "NONE", "reason": "No rows were rejected or dropped", "detail": "", "rows": 0, "file_status": ""}]
    empty_rows = [{"filename": "NONE", "target_table": "NONE", "record_no": 0, "reason": "No rows were rejected", "detail": "", "row_data": "", "file_status": ""}]
    sheets = {
        "Summary": summary_sheet,
        "File_Execution_Details": results,
        "Reject_Summary": reject_summary_rows or empty_summary,
        "Rejected_Rows": reject_rows_all[:MAX_REJECT_ROWS_IN_REPORT] or empty_rows,
    }
    generate_excel_report(report_file, sheets)
    logger.info(f"Excel report generated: {report_file}")
    record_stream_end(None)

    total_inserted = sum(r.get("rows_inserted", 0) for r in results)
    total_updated = sum(r.get("rows_updated", 0) for r in results)
    total_unchanged = sum(r.get("rows_unchanged", 0) for r in results)
    log_summary(logger, Path(__file__).name, [
        "Files",
        ("candidate files planned", total_candidates),
        ("skipped (already loaded, unchanged)", skipped_count),
        ("executed this run", len(pending_jobs)),
        ("  - loaded", success_count),
        ("  - failed", failed_count),
        "Rows",
        ("loaded (inserted + updated)", total_loaded_rows),
        ("  - inserted", total_inserted),
        ("  - updated (value changed)", total_updated),
        ("unchanged (identical, not touched)", total_unchanged),
        ("rejected (see Rejected_Rows sheet)", total_rejected),
        ("duplicates dropped", total_dups),
        "Performance",
        ("volume loaded (GB)", round(total_loaded_bytes / (1024**3), 2)),
        ("elapsed (seconds)", total_time),
        ("throughput (rows/sec)", round(total_loaded_rows / max(total_time, 0.1))),
    ])

    return failed_count == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="06_loadData.py - Multi-Worker S3 Streaming Ingestion")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    add_targeting_args(parser, schemas="--owners", file=True)
    add_layer_arg(parser)
    parser.add_argument("--gate-pass", action="store_true", help="Load only tables that passed the pre-flight type gate")
    parser.add_argument("--limit-files", type=int, help="Limit number of files to load (for testing)")
    parser.add_argument("--max-size-mb", type=float, help="Filter files with size <= max_size_mb")
    parser.add_argument("--min-size-mb", type=float, help="Filter files with size >= min_size_mb")
    parser.add_argument("--reload", action="store_true", help="Force reloading/upserting files even if previously loaded")
    parser.add_argument("--full", action="store_true",
                        help="Do not capture the applied delta of this load: the promote hop copies the whole table instead")
    parser.add_argument("--max-rejects", type=int, default=DEFAULT_MAX_REJECTS,
                        help=f"Fail (and roll back) a file when this many rows are rejected (default {DEFAULT_MAX_REJECTS} = never; only technical errors fail a file)")
    parser.add_argument("--fast-bulk", action="store_true",
                        help="Direct COPY (no temp staging) into a BARE table - no key, foreign key, CHECK, index or trigger - empty or not (the part files of one "
                             "table); NOT NULL and identity columns are fine. Registered as a full copy. One bad row fails the file. Otherwise the staged "
                             "load is used and the log names the object that forced it")
    parser.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS,
                        help=f"Staged loads of big files run in chunks of this many records inside the one transaction, so temp storage holds one "
                             f"chunk instead of the whole file (default {DEFAULT_CHUNK_ROWS:,}; 0 = never chunk)")
    parser.add_argument("--chunk-min-mb", type=float, default=DEFAULT_CHUNK_MIN_MB,
                        help=f"Only files at least this big (compressed size, MB) are loaded in chunks (default {DEFAULT_CHUNK_MIN_MB})")
    parser.add_argument("--trigger-mode", choices=["auto", "replica", "disable"], default="auto",
                        help="How triggers are switched off during a load: session_replication_role=replica (no table lock), "
                             "or ALTER TABLE DISABLE TRIGGER (needs table ownership, locks the table). auto = replica if allowed")
    args = parser.parse_args()

    layer = ensure_layer(args, logger)
    require_bronze(layer, logger, "Oracle Step 06")
    require_migrated(logger)
    FULL_LOAD = args.full
    ok = run_stream_load(
        workers=args.workers,
        limit_files=args.limit_files,
        max_size_mb=args.max_size_mb,
        min_size_mb=args.min_size_mb,
        gate_pass=args.gate_pass,
        reload=args.reload,
        max_rejects=args.max_rejects,
        fast_bulk=args.fast_bulk,
        trigger_mode=args.trigger_mode,
        chunk_rows=args.chunk_rows,
        chunk_min_mb=args.chunk_min_mb,
        selector=selector_or_exit(args, logger),
        layer=layer,
    )
    sys.exit(0 if ok else 1)
