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
    ORACLE_OUTPUTS_DIR,
    ORACLE_REPORTS_DIR,
    ORACLE_LOGS_DIR,
    AWS_REGION,
    DEFAULT_WORKERS,
    LOAD_LOG_TABLE,
    has_db_auth,
    AUTH_HELP,
    clip_error,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    run_psql,
    run_psql_script,
    norm_col,
    q,
    lit,
    format_size_actual,
    DB_ERRORS,
)
from oracle.oracle_06_pipeline import (
    build_fast_script, build_staged_script, ensure_reject_table, has_constraints, load_log_insert, load_table_meta,
    parse_output, persist_failed_file_rejects, plan_load, probe_trigger_mode,
)
from sweep_streaming import read_stream_head, stream_program_literal
from sweep_platform import atomic_write_json
logger = get_logger("oracle_06_load_data", "oracle_06_load_data")
LOCK = threading.Lock()
MAX_REJECT_ROWS_IN_REPORT = 10000   # rows shown in the Rejected_Rows sheet; all are in sweep_ctl.load_rejects
DEFAULT_MAX_REJECTS = 10      # the file fails when this many rows are rejected (0 = never)
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


def get_loaded_uris():
    """Queries sweep_ctl.load_log to get all previously successfully loaded S3 URIs."""
    try:
        sql = f"SELECT uri FROM {LOAD_LOG_TABLE} WHERE status = 'SUCCESS';"
        out = run_psql(sql)
        return {line.strip() for line in out.splitlines() if line.strip()}
    except DB_ERRORS as e:
        logger.warning(f"Could not read loaded URIs from {LOAD_LOG_TABLE} (table might be new): {e}")
        return set()


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


def load_single_file(job, worker_id, region=AWS_REGION, dry_run=False, max_rejects=DEFAULT_MAX_REJECTS,
                     fast_bulk=False, trigger_mode="replica"):
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

    if dry_run:
        return LoadOutcome("DRY_RUN", 0, 0.0, "")

    t0 = time.time()
    size_mb_val = float(job.get("size_mb") or (size_bytes / (1024**2)))
    record_stream_start(worker_id, job, size_mb_val)
    try:
        header_cols = extract_file_header(uri, region)
        meta = get_table_meta(schema, table)
        if not meta["columns"]:
            return _failed(f"Target table {schema}.{table} does not exist in PostgreSQL.")
        plan, problem = plan_load(meta, header_cols)
        if problem:
            return _failed(problem)

        stream_cmd = stream_program_literal(uri, region)
        log_sql = load_log_insert(uri, schema, table, size_bytes, file_format, worker_id)
        name = job.get("filename", Path(uri).name)

        use_fast = fast_bulk and not has_constraints(meta) and is_table_empty(schema, table)
        if use_fast:
            logger.info(f"[Worker {worker_id}] Started stream: {name} ({format_size_actual(size_mb_val)}) -> {schema}.{table} (fast bulk - empty table without constraints)")
            script = build_fast_script(schema, table, header_cols, stream_cmd, meta, trigger_mode, log_sql)
        else:
            how = f"upsert on {', '.join(plan['arbiter']['cols'])}" if plan["arbiter"] else "append - no key"
            logger.info(f"[Worker {worker_id}] Started stream: {name} ({format_size_actual(size_mb_val)}) -> {schema}.{table} (staged load, {how}; triggers off via {trigger_mode})")
            for skipped in plan["skipped_checks"]:
                logger.warning(f"[Worker {worker_id}] CHECK {skipped} references columns that are not in the file; the table still enforces it")
            script = build_staged_script(schema, table, header_cols, stream_cmd, meta, plan, trigger_mode, max_rejects, uri, log_sql)

        # High-performance bulk load options. psql runs in its own process group, with a hard
        # timeout (SWEEP_LOAD_TIMEOUT) that also kills the S3/gzip pipeline it spawned.
        res = run_psql_script(
            script,
            options="-c synchronous_commit=off -c statement_timeout=0 -c lock_timeout=0",
        )
        sec = round(time.time() - t0, 2)
        counts, summary, reject_rows = parse_output(res.stdout)
        extra = dict(rejected=counts.get("rejected", 0), duplicates=counts.get("dup", 0), read=counts.get("raw", 0),
                     reject_summary=tuple(summary), reject_rows=tuple(reject_rows), trigger_mode=trigger_mode)

        if res.returncode != 0:
            err = clip_error(res.stderr or res.stdout or "psql failed")
            if reject_rows:       # the transaction rolled back; keep the rejected rows for review
                try:
                    persist_failed_file_rejects(uri, schema, table, reject_rows)
                except RuntimeError as e:
                    logger.warning(f"[Worker {worker_id}] Could not store the rejected rows of {uri}: {e}")
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
            update_sql = f"UPDATE {LOAD_LOG_TABLE} SET loaded_rows = {rows}, seconds = {sec} WHERE uri = {lit(uri)};"
            run_psql(update_sql, capture=False)
        except RuntimeError as e:
            # Data and its load_log row are already committed; only the stats update failed.
            logger.warning(f"[Worker {worker_id}] Could not record row count for {uri}: {e}")

        if extra["rejected"]:
            logger.warning(f"[Worker {worker_id}] {name}: {extra['rejected']} row(s) rejected (see sweep_ctl.load_rejects / the report)")
        return LoadOutcome("SUCCESS", rows, sec, "", inserted=inserted, updated=updated,
                           unchanged=counts.get("unchanged", 0), **extra)
    except Exception as e:  # noqa: BLE001 - a worker must report any failure, never crash the pool
        logger.error(f"[Worker {worker_id}] Load failed for {uri}: {e}")
        return _failed(clip_error(e), round(time.time() - t0, 2))
    finally:
        record_stream_end(worker_id)


def run_stream_load(workers=DEFAULT_WORKERS, tables_filter=None, exclude_tables=None, file_uri=None, filename=None, limit_files=None, max_size_mb=None, min_size_mb=None, dry_run=False, gate_pass=False, reload=False,
                    max_rejects=DEFAULT_MAX_REJECTS, fast_bulk=False, trigger_mode="auto"):
    ts = get_timestamp()
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
            logger.error("No passed tables found from type gate! Run 'python sweep.py gate' first.")
            return False

        logger.info(f"Gate-Pass Active: Filtering ingestion to only the {len(passed_tables)} table(s) that PASSED the type gate.")
        all_jobs = [j for j in all_jobs if f"{j['owner']}.{j['table_name']}".upper() in passed_tables
                    or f"{j['target_schema']}.{j['target_table']}".upper() in passed_tables
                    or j['table_name'].upper() in passed_tables]

    # Filter tables if specified
    if tables_filter:
        all_jobs = [j for j in all_jobs if f"{j['owner']}.{j['table_name']}".upper() in tables_filter
                    or f"{j['target_schema']}.{j['target_table']}".lower() in tables_filter
                    or j['table_name'].upper() in tables_filter]

    # Exclude tables if specified
    if exclude_tables:
        all_jobs = [j for j in all_jobs if f"{j['owner']}.{j['table_name']}".upper() not in exclude_tables
                    and f"{j['target_schema']}.{j['target_table']}".lower() not in exclude_tables
                    and j['table_name'].upper() not in exclude_tables]

    # Filter single file by URI or filename if specified
    if file_uri:
        all_jobs = [j for j in all_jobs if j["uri"].strip().lower() == file_uri.strip().lower()]

    if filename:
        all_jobs = [j for j in all_jobs if j.get("filename", "").strip().lower() == filename.strip().lower()
                    or Path(j["uri"]).name.lower() == filename.strip().lower()]

    # Filter by size range if specified
    if max_size_mb is not None:
        all_jobs = [j for j in all_jobs if float(j.get("size_mb") or (int(j.get("size_bytes") or 0) / (1024**2))) <= max_size_mb]

    if min_size_mb is not None:
        all_jobs = [j for j in all_jobs if float(j.get("size_mb") or (int(j.get("size_bytes") or 0) / (1024**2))) >= min_size_mb]

    # Rule: Load the s3 files in that order (small to large file size)
    all_jobs.sort(key=lambda x: (int(x.get("size_bytes") or 0), x.get("filename", "")))

    if limit_files:
        all_jobs = all_jobs[:limit_files]

    total_candidates = len(all_jobs)
    loaded_uris = get_loaded_uris() if not dry_run else set()
    if not reload and not file_uri and not filename:
        pending_jobs = [j for j in all_jobs if j["uri"] not in loaded_uris]
        skipped_count = total_candidates - len(pending_jobs)
    else:
        pending_jobs = all_jobs
        skipped_count = 0

    logger.info("=" * 70)
    logger.info(f"SWEEP Ingestion Engine: {len(pending_jobs)} pending file(s) ({skipped_count} already loaded)")
    logger.info(f"Order: Smallest to Largest File Size | Workers: {workers} | Gate-Pass: {gate_pass} | Dry-Run: {dry_run}")
    logger.info("=" * 70)

    if not pending_jobs:
        logger.info("All planned files have already been loaded into PostgreSQL. Nothing to do.")
        return True

    if not dry_run:
        ensure_reject_table()
        if trigger_mode == "auto":
            trigger_mode = probe_trigger_mode()
        logger.info(f"Triggers are switched off per load transaction via: {trigger_mode} "
                    f"({'session_replication_role = replica' if trigger_mode == 'replica' else 'ALTER TABLE ... DISABLE TRIGGER (table lock until commit)'})")
        logger.info(f"A file fails when {max_rejects} rejected rows are reached (0 = never); fast bulk: {fast_bulk}")
    else:
        trigger_mode = trigger_mode if trigger_mode != "auto" else "replica"

    results = []
    reject_summary_rows = []
    reject_rows_all = []
    completed_files = 0
    total_loaded_rows = 0
    total_loaded_bytes = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for idx, job in enumerate(pending_jobs):
            worker_id = (idx % workers) + 1
            f = executor.submit(load_single_file, job, worker_id, dry_run=dry_run, max_rejects=max_rejects,
                                fast_bulk=fast_bulk, trigger_mode=trigger_mode)
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
                logger.info(f"[{completed_files}/{len(pending_jobs)}] DRY-RUN: {job['filename']} -> "
                            f"{job['target_schema']}.{job['target_table']} ({job['size_mb']} MB)")
            else:
                logger.error(f"[{completed_files}/{len(pending_jobs)}] FAILED: {job['filename']} -> {msg}")

            results.append({
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

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  candidate files planned    : {total_candidates:>10,}",
        f"  files executed this run    : {len(pending_jobs):>10,}",
        f"  files successfully loaded  : {success_count:>10,}",
        f"  files failed               : {failed_count:>10,}",
        f"  files skipped (completed)  : {skipped_count:>10,}",
        f"  total rows ingested        : {total_loaded_rows:>10,}",
        f"  total volume loaded (GB)   : {round(total_loaded_bytes / (1024**3), 2):>10,}",
        f"  elapsed duration (seconds) : {total_time:>10,}",
        f"  overall throughput (r/s)   : {round(total_loaded_rows / max(total_time, 0.1)):>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)

    return failed_count == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="06_loadData.py - Multi-Worker S3 Streaming Ingestion")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--tables", help="Comma-separated tables to load (e.g. BSS01001.BROKERACCOUNTDETAIL)")
    parser.add_argument("--exclude-tables", help="Comma-separated tables to exclude from load")
    parser.add_argument("--file", help="Specific S3 URI to load")
    parser.add_argument("--filename", help="Specific S3 filename/basename to load")
    parser.add_argument("--gate-pass", action="store_true", help="Load only tables that passed the pre-flight type gate")
    parser.add_argument("--limit-files", type=int, help="Limit number of files to load (for testing)")
    parser.add_argument("--max-size-mb", type=float, help="Filter files with size <= max_size_mb")
    parser.add_argument("--min-size-mb", type=float, help="Filter files with size >= min_size_mb")
    parser.add_argument("--dry-run", action="store_true", help="Dry run: show what would be loaded without database writes")
    parser.add_argument("--reload", action="store_true", help="Force reloading/upserting files even if previously loaded")
    parser.add_argument("--max-rejects", type=int, default=DEFAULT_MAX_REJECTS,
                        help=f"Fail (and roll back) a file when this many rows are rejected (default {DEFAULT_MAX_REJECTS}; 0 = never fail)")
    parser.add_argument("--fast-bulk", action="store_true",
                        help="Direct COPY for an empty table without constraints (fastest first load; one bad row fails the file)")
    parser.add_argument("--trigger-mode", choices=["auto", "replica", "disable"], default="auto",
                        help="How triggers are switched off during a load: session_replication_role=replica (no table lock), "
                             "or ALTER TABLE DISABLE TRIGGER (needs table ownership, locks the table). auto = replica if allowed")
    args = parser.parse_args()

    tables = {t.strip().upper() for t in args.tables.split(",")} if args.tables else None
    exclude_tbls = {t.strip().upper() for t in args.exclude_tables.split(",")} if args.exclude_tables else None
    ok = run_stream_load(
        workers=args.workers,
        tables_filter=tables,
        exclude_tables=exclude_tbls,
        file_uri=args.file,
        filename=args.filename,
        limit_files=args.limit_files,
        max_size_mb=args.max_size_mb,
        min_size_mb=args.min_size_mb,
        dry_run=args.dry_run,
        gate_pass=args.gate_pass,
        reload=args.reload,
        max_rejects=args.max_rejects,
        fast_bulk=args.fast_bulk,
        trigger_mode=args.trigger_mode,
    )
    sys.exit(0 if ok else 1)
