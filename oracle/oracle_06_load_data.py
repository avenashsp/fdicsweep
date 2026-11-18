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
import time
import argparse
import threading
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
from sweep_streaming import read_stream_head, stream_program_literal
from sweep_platform import atomic_write_json
logger = get_logger("oracle_06_load_data", "oracle_06_load_data")
LOCK = threading.Lock()
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


def load_single_file(job, worker_id, region=AWS_REGION, dry_run=False):
    """
    Loads one S3 file inside a dedicated database transaction.
    Returns: (status, rows_loaded, duration_sec, error_message)
    """
    uri = job["uri"]
    schema = job["target_schema"].lower()
    table = job["target_table"].lower()
    size_bytes = int(job.get("size_bytes") or 0)
    file_format = job.get("file_format", "gzip")

    if dry_run:
        return "DRY_RUN", 0, 0.0, ""

    t0 = time.time()
    size_mb_val = float(job.get("size_mb") or (size_bytes / (1024**2)))
    record_stream_start(worker_id, job, size_mb_val)
    try:
        header_cols = extract_file_header(uri, region)
        target_cols = get_target_columns(schema, table)

        if not target_cols:
            return "FAILED", 0, 0.0, f"Target table {schema}.{table} does not exist in PostgreSQL."

        # Verify header columns
        missing_cols = [c for c in header_cols if c not in target_cols]
        if missing_cols:
            return "FAILED", 0, 0.0, f"Columns in S3 file not found in table: {', '.join(missing_cols[:5])}"

        # Determine whether to bulk copy or upsert
        pk_cols = get_table_pk_columns(schema, table)
        table_empty = is_table_empty(schema, table)
        can_upsert = (not table_empty) and bool(pk_cols)

        col_list_str = ", ".join(q(c) for c in header_cols)
        stream_cmd = stream_program_literal(uri, region)

        if not can_upsert:
            if not table_empty and not pk_cols:
                logger.info(f"[Worker {worker_id}] Started stream: {job.get('filename', Path(uri).name)} ({format_size_actual(size_mb_val)}) -> {schema}.{table} (append load - no PK constraint)")
            else:
                logger.info(f"[Worker {worker_id}] Started stream: {job.get('filename', Path(uri).name)} ({format_size_actual(size_mb_val)}) -> {schema}.{table} (bulk load - empty table)")

            sql_script = (
                f"BEGIN;\n"
                f"\\copy {q(schema)}.{q(table)} ({col_list_str}) FROM PROGRAM '{stream_cmd}' "
                f"WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n"
                f"INSERT INTO {LOAD_LOG_TABLE} (uri, target_schema, target_table, size_bytes, file_format, worker_id, status) "
                f"VALUES ({lit(uri)}, {lit(schema)}, {lit(table)}, {size_bytes}, {lit(file_format)}, {worker_id}, 'SUCCESS') "
                f"ON CONFLICT (uri) DO UPDATE SET loaded_at = now(), status = 'SUCCESS', size_bytes = EXCLUDED.size_bytes;\n"
                f"COMMIT;\n"
            )
        else:
            pk_set = set(pk_cols)
            update_cols = [c for c in header_cols if c not in pk_set]
            pk_conflict_str = ", ".join(q(c) for c in pk_cols)

            if update_cols:
                set_clause = ", ".join(f"{q(c)} = EXCLUDED.{q(c)}" for c in update_cols)
                on_conflict_clause = f"ON CONFLICT ({pk_conflict_str}) DO UPDATE SET {set_clause}"
            else:
                on_conflict_clause = f"ON CONFLICT ({pk_conflict_str}) DO NOTHING"

            logger.info(f"[Worker {worker_id}] Started stream: {job.get('filename', Path(uri).name)} ({format_size_actual(size_mb_val)}) -> {schema}.{table} (upsert load on PK: {', '.join(pk_cols)})")

            sql_script = (
                f"BEGIN;\n"
                f"CREATE TEMP TABLE _stg_load (LIKE {q(schema)}.{q(table)} INCLUDING DEFAULTS) ON COMMIT DROP;\n"
                f"\\copy _stg_load ({col_list_str}) FROM PROGRAM '{stream_cmd}' "
                f"WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n"
                f"INSERT INTO {q(schema)}.{q(table)} ({col_list_str}) "
                f"SELECT DISTINCT ON ({pk_conflict_str}) {col_list_str} FROM _stg_load "
                f"ORDER BY {pk_conflict_str} "
                f"{on_conflict_clause};\n"
                f"INSERT INTO {LOAD_LOG_TABLE} (uri, target_schema, target_table, size_bytes, file_format, worker_id, status) "
                f"VALUES ({lit(uri)}, {lit(schema)}, {lit(table)}, {size_bytes}, {lit(file_format)}, {worker_id}, 'SUCCESS') "
                f"ON CONFLICT (uri) DO UPDATE SET loaded_at = now(), status = 'SUCCESS', size_bytes = EXCLUDED.size_bytes;\n"
                f"COMMIT;\n"
            )

        # High-performance bulk load options. psql runs in its own process group, with a hard
        # timeout (SWEEP_LOAD_TIMEOUT) that also kills the S3/gzip pipeline it spawned.
        res = run_psql_script(
            sql_script,
            options="-c synchronous_commit=off -c statement_timeout=0 -c lock_timeout=0",
        )
        sec = round(time.time() - t0, 2)

        if res.returncode != 0:
            err = (res.stderr or res.stdout).strip().splitlines()[-1][:300] if (res.stderr or res.stdout) else "psql failed"
            return "FAILED", 0, sec, err

        # Extract loaded rows from COPY output
        m = re.search(r"(?:COPY|INSERT\s+\d+)\s+(\d+)", res.stdout)
        rows = int(m.group(1)) if m else 0

        # Update loaded_rows and seconds
        try:
            update_sql = f"UPDATE {LOAD_LOG_TABLE} SET loaded_rows = {rows}, seconds = {sec} WHERE uri = {lit(uri)};"
            run_psql(update_sql, capture=False)
        except RuntimeError as e:
            # Data and its load_log row are already committed; only the stats update failed.
            logger.warning(f"[Worker {worker_id}] Could not record row count for {uri}: {e}")

        return "SUCCESS", rows, sec, ""
    except Exception as e:  # noqa: BLE001 - a worker must report any failure, never crash the pool
        logger.error(f"[Worker {worker_id}] Load failed for {uri}: {e}")
        sec = round(time.time() - t0, 2)
        return "FAILED", 0, sec, str(e)[:300]
    finally:
        record_stream_end(worker_id)


def run_stream_load(workers=DEFAULT_WORKERS, tables_filter=None, exclude_tables=None, file_uri=None, filename=None, limit_files=None, max_size_mb=None, min_size_mb=None, dry_run=False, gate_pass=False, reload=False):
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

    results = []
    completed_files = 0
    total_loaded_rows = 0
    total_loaded_bytes = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for idx, job in enumerate(pending_jobs):
            worker_id = (idx % workers) + 1
            f = executor.submit(load_single_file, job, worker_id, dry_run=dry_run)
            futures[f] = job

        for future in as_completed(futures):
            job = futures[future]
            status, rows, sec, msg = future.result()
            completed_files += 1

            if status == "SUCCESS":
                total_loaded_rows += rows
                total_loaded_bytes += int(job.get("size_bytes") or 0)
                rate = round(rows / sec) if sec > 0 else 0
                logger.info(f"[{completed_files}/{len(pending_jobs)}] OK: {job['filename']} -> "
                            f"{job['target_schema']}.{job['target_table']} ({rows:,} rows in {sec}s ~ {rate:,} r/s)")
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
                "duration_seconds": sec,
                "error_message": msg,
                "uri": job["uri"]
            })

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

    sheets = {
        "Summary": summary_sheet,
        "File_Execution_Details": results
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
        reload=args.reload
    )
    sys.exit(0 if ok else 1)
