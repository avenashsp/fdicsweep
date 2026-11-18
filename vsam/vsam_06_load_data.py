#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_06_load_data.py - Step 06: Schema-Segregated S3 Streaming & Upsert
=============================================================================
Streams VSAM flat files from S3 directly into PostgreSQL.

KEY FEATURES:
  1. Primary Key Pre-Requisite & Dynamic Upsert:
     Runs AFTER Step 06 (load-obj), so all target tables already have primary
     key constraints active in PostgreSQL. When inserting rows, dynamic UPSERT
     logic (ON CONFLICT (pk) DO UPDATE SET ...) guarantees newer file versions
     or files with duplicate keys update existing records seamlessly.
  2. Latest File Version Filter:
     With --latest-only (or --latest-version-only), automatically identifies
     timestamps in filenames (e.g. MMDDYYYY, YYYYMMDD, YYYYMMDDHHMMSS) and
     ingests only the newest version per target table.
     Without --latest-only, loads all versions chronologically (oldest to newest)
     so the latest version always takes precedence through upsert.
  3. Dynamic Schema Segregation:
     When a source file maps to multiple schemas, filters rows dynamically
     matching the inferred schema discriminator column to route records to their
     dedicated schema without hardcoding.

Generates:
  - outputs/vsam/vsam_06_load_log_<run_id>.csv
  - reports/vsam/<db>_vsam_06_load_data_report_<ts>.xlsx
=============================================================================
"""
import sys
import os
import re
import csv
import json
import time
import argparse
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from sweep_common import (
    VSAM_GEN_DDL_DIR,
    VSAM_LOGS_DIR,
    VSAM_REPORTS_DIR,
    DEFAULT_WORKERS,
    CTL_SCHEMA,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    run_psql,
    q,
    lit,
    norm_col,
)
from sweep_platform import atomic_write_json
from collections import defaultdict
from vsam.vsam_common import (
    _opener,
    AUDIT_COL_NAMES,
    build_table_ddl,
    check_table_exists,
    delim_for,
    filter_latest_versions,
    get_all_active_schemas,
    get_pk_validation_failure_reason,
    load_csv_safe,
    match_schema_by_last3,
    OUT,
    read_json,
    record_passes_pk_validation,
)

logger = get_logger("vsam_06_load_data", "vsam_06_load_data")
LOCK = threading.Lock()
ACTIVE_WORKERS_FILE = VSAM_LOGS_DIR / "active_workers.json"
_ACTIVE_STREAMS = {}


def _flush_active_streams():
    try:
        os.makedirs(VSAM_LOGS_DIR, exist_ok=True)
        items = []
        now = time.time()
        for w in _ACTIVE_STREAMS.values():
            item = dict(w)
            item["seconds"] = max(1, int(now - item.get("start_time", now)))
            items.append(item)
        atomic_write_json(ACTIVE_WORKERS_FILE, items)
    except OSError as e:
        logger.debug(f"Could not write active worker state: {e}")


def record_stream_start(worker_id, schema, table, full_path, filename, size_mb=0):
    with LOCK:
        _ACTIVE_STREAMS[worker_id] = {
            "pid": os.getpid(),
            "worker_id": worker_id,
            "filename": filename,
            "table": f"{schema}.{table}",
            "mb": float(size_mb or 0.0),
            "rows": 0,
            "start_time": time.time(),
            "uri": full_path
        }
        _flush_active_streams()


def record_stream_progress(worker_id, rows_added):
    with LOCK:
        if worker_id in _ACTIVE_STREAMS:
            _ACTIVE_STREAMS[worker_id]["rows"] = int(_ACTIVE_STREAMS[worker_id].get("rows", 0) + rows_added)
            _flush_active_streams()


def record_stream_end(worker_id):
    with LOCK:
        if worker_id is None:
            _ACTIVE_STREAMS.clear()
        else:
            _ACTIVE_STREAMS.pop(worker_id, None)
        _flush_active_streams()


# -----------------------------------------------------------------------------
# Truncate-once-per-run bookkeeping
# -----------------------------------------------------------------------------
# Several source files (e.g. different companies routed to the shared 'bcs' schema) can map to the
# SAME target table. A table is therefore "claimed" by the first file that touches it in this run;
# only that first claim may TRUNCATE (and only with --force-truncate). Later files always append /
# upsert, so they can never wipe rows loaded earlier in the same run.
FORCE_TRUNCATE = False
_CLAIM_LOCK = threading.Lock()
_CLAIMED_TABLES = set()


def claim_table(dest_schema, table, strategy):
    """
    Called once per (file, destination table). Truncates the table at most once per run and only
    when the operator opted in. Raises RuntimeError when truncation is needed but not permitted,
    or when the TRUNCATE itself fails (a failed truncate must never be ignored: it would leave old
    rows in place and silently duplicate data).
    Returns True when this call truncated the table.
    """
    key = (dest_schema.lower(), table.lower())
    with _CLAIM_LOCK:
        if key in _CLAIMED_TABLES:
            return False
        if strategy["action"] == "TRUNCATE_AND_LOAD":
            if not FORCE_TRUNCATE:
                raise RuntimeError(
                    f"{dest_schema}.{table} already contains rows and has no primary key. Refusing to TRUNCATE it "
                    f"without --force-truncate (re-run with that flag to truncate once and reload)."
                )
            run_psql(f"TRUNCATE TABLE {q(dest_schema)}.{q(table)};")
            _CLAIMED_TABLES.add(key)
            return True
        _CLAIMED_TABLES.add(key)
        return False


def write_load_log(full_path, dest_schema, table, size_bytes, rows, seconds, worker_id, status="SUCCESS", message=None):
    """
    Records a per-destination-schema load in sweep_ctl.load_log (keyed `<uri>#<schema>`, which Step 07
    strips). Errors propagate: a lost log row would cause the file to be reloaded.
    """
    msg_cols = ", message" if message else ""
    msg_val = f", {lit(message)}" if message else ""
    msg_upd = f", message={lit(message)}" if message else ""
    run_psql(
        f"INSERT INTO {CTL_SCHEMA}.load_log "
        f"(uri, target_schema, target_table, size_bytes, loaded_rows, seconds, worker_id, status{msg_cols}, loaded_at) "
        f"VALUES ({lit(f'{full_path}#{dest_schema}')}, {lit(dest_schema)}, {lit(table)}, {size_bytes}, "
        f"{int(rows)}, {seconds}, {worker_id}, {lit(status)}{msg_val}, now()) "
        f"ON CONFLICT (uri) DO UPDATE SET status={lit(status)}, loaded_rows={int(rows)}, "
        f"seconds={seconds}, loaded_at=now(){msg_upd};"
    )


def load_segregated(task, worker_id, run_id, tg_prof, cb_meta, mode="default"):
    """
    Loads a single source file into PostgreSQL:
    - If NO schema_col is identified for the file: the entire file is loaded ONLY into the 'bcs' schema.
      None of the mapped schemas receive data.
    - If schema_col is identified: records are segregated to mapped destination schemas based on last 3 digits
      of schema_col (unmatched records -> bcs).
    - Table PK constraint check:
      * If table contains PK constraint: checks every record and loads ONLY records that pass PK validation
        (non-null, non-blank, non-sentinel PK values; all-null/sentinel rows filtered). Upserts via ON CONFLICT DO UPDATE.
      * If table contains NO PK constraint: loads ALL records (does not remove/ignore NULL, blank, empty whitespace,
        or sentinel values). If table has existing data, truncates table in that mapped schema then loads fresh.
    - Truncation / upsert is performed ONLY on the target destination schemas for that file.
    """
    target_schemas = list(task.get("target_schemas") or [task["target_schema"]])
    table = task["target_table"]
    full_path = task["full_path"]
    filename = task.get("s3_filename") or Path(full_path).name
    co = task.get("source_company", "")
    src = task.get("source_filename", task.get("source_file", ""))
    source_key = f"{co}|{src}"
    size_mb = float(task.get("size_mb", 0) or 0)
    size_bytes = int(size_mb * 1024 * 1024)
    has_header = task.get("has_header", "NO").upper() in ("YES", "UNVERIFIED")

    # Step 03 profile
    tg = tg_prof.get(source_key, {})
    schema_col = tg.get("schema_col")
    has_schema_col = bool(schema_col and str(schema_col).strip().upper() not in ("", "NONE", "NO_SCHEMA_COL", "NO_KORG"))

    raw_mapped = [s.strip().lower() for s in (task.get("target_schemas") or [task.get("target_schema", "")]) if s and s.strip()]
    if not has_schema_col or raw_mapped == ["bcs"]:
        mapped_schemas_set = {"bcs"}
    else:
        mapped_schemas_set = set(raw_mapped) - {"bcs"}

    if not has_schema_col:
        # User requirement: If no schema_col identified, the file data is fully loaded ONLY to bcs schema.
        # That file should not be loaded to any of the mapped schemas.
        target_schemas = ["bcs"]
        logger.info(f"[Worker {worker_id}] File {filename} has NO schema_col identified -> FULLY ROUTING TO 'bcs' ONLY.")
    else:
        # Ensure all mapped schemas are included (e.g. bss01104 AND bss01112 for FDIC)
        prof_schemas = list(tg.get("schema_disc_map", {}).keys()) or tg.get("target_schemas", [])
        for ps in prof_schemas:
            if ps and ps not in target_schemas:
                target_schemas.append(ps)
        # Also ensure active schemas are known for destination tracking
        for act_s in get_all_active_schemas():
            if act_s not in target_schemas:
                target_schemas.append(act_s)

    t0 = time.time()
    record_stream_start(worker_id, ",".join(target_schemas), table, full_path, filename, size_mb)

    try:
        cb_name_raw = task.get("copybook_map", "")
        cb_key = cb_name_raw.upper().split(".")[0] if (cb_name_raw and cb_name_raw.upper() != "NO_COPYBOOK") else ""
        cb_display = cb_name_raw if (cb_name_raw and cb_name_raw.upper() != "NO_COPYBOOK") else "NO_COPYBOOK"
        columns_meta = cb_meta.get(cb_key, []) if cb_key else []

        if not columns_meta:
            # When copybook is missing: retrieve columns inferred in Step 03
            columns_meta = tg.get("columns", [])

        if not columns_meta:
            with _opener(full_path) as f:
                first_line = f.readline()
                if isinstance(first_line, bytes):
                    first_line = first_line.decode("utf-8-sig", errors="replace")
                d_probe = delim_for(full_path)
                header_tokens = [norm_col(t.strip()) for t in first_line.strip().split(d_probe) if t.strip()]
                columns_meta = [{"col_name": h, "sql_type": "text", "is_filler": "NO"} for h in header_tokens]

        if not columns_meta:
            return {
                "schema": ",".join(target_schemas), "table": table, "uri": full_path, "filename": filename,
                "copybook_display": cb_display, "korg_display": "NO_KORG",
                "status": "FAILED", "seconds": 0.0, "worker_id": worker_id, "rows_loaded": 0,
                "error": f"No column metadata or header found for '{cb_display}'"
            }

        cols = [c["col_name"] for c in columns_meta if c.get("is_filler") != "YES"]
        schema_col = tg.get("schema_col")
        sc_display = schema_col if (schema_col and schema_col.upper() != "NO_KORG") else "NO_KORG"
        d = delim_for(full_path)
        type_overrides = tg.get("type_overrides", {})
        type_map = {c["col_name"]: type_overrides.get(c["col_name"], c.get("sql_type", "text")) for c in columns_meta if c.get("is_filler") != "YES"}

        # Dynamic strategy per destination schema (inspects PostgreSQL live)
        from vsam.vsam_common import check_table_load_strategy
        schema_strategies = {}

        def get_schema_strategy(dest_s):
            if dest_s not in schema_strategies:
                # Ensure destination table exists (auto-provision on the fly if needed)
                if not check_table_exists(dest_s, table):
                    try:
                        pre_sql, _ = build_table_ddl(dest_s, table, columns_meta, [])
                        run_psql(f"CREATE SCHEMA IF NOT EXISTS {q(dest_s)};\n{pre_sql}")
                        logger.info(f"[Worker {worker_id}] Provisioned table {dest_s}.{table} on the fly.")
                    except RuntimeError as pe:
                        # The load below will fail with the real database error if the table is truly missing.
                        logger.warning(f"Could not provision {dest_s}.{table} on the fly: {pe}")

                strat = check_table_load_strategy(
                    dest_s, table,
                    tg_prof_pk=[norm_col(c) for c in tg.get("pk_cols", []) if c] if tg.get("pk_valid") else [],
                    mode=mode
                )
                schema_strategies[dest_s] = strat
                if claim_table(dest_s, table, strat):
                    logger.info(f"[Worker {worker_id}] Table {dest_s}.{table} had existing data and NO PK constraints. Truncated once (--force-truncate) before load.")
                elif strat["action"] == "TRUNCATE_AND_LOAD":
                    logger.info(f"[Worker {worker_id}] Table {dest_s}.{table} already loaded earlier in this run (no PK). Appending.")
                elif strat["action"] == "UPSERT":
                    logger.info(f"[Worker {worker_id}] Table {dest_s}.{table} has existing data and active PK ({', '.join(strat['pk_cols'])}). Upsert enabled.")
                elif strat["action"] == "INSERT_FRESH":
                    logger.info(f"[Worker {worker_id}] Table {dest_s}.{table} is fresh. Applied PK ({', '.join(strat['pk_cols'])}). Validated record insert enabled.")
                else:
                    logger.info(f"[Worker {worker_id}] Table {dest_s}.{table} is fresh (empty). Direct load enabled (mode={mode}).")
            return schema_strategies[dest_s]

        # Cast value helper
        def cast_val(col_name, val):
            t = type_map.get(col_name, "text")
            if val is None or str(val).strip() == "":
                return "NULL"
            v = str(val).strip().replace("'", "''")
            if t.startswith(("varchar", "text", "char")):
                return f"'{v}'"
            if t == "date":
                if re.match(r"^\d{4}-\d{2}-\d{2}$", v) and not v.startswith("9999"):
                    return f"'{v}'::date"
                return "NULL"
            if t in ("integer", "bigint", "smallint"):
                if re.match(r"^-?\d+$", v):
                    return f"'{v}'::{t}"
                return "NULL"
            if t.startswith("numeric"):
                if re.match(r"^-?(\d+(\.\d*)?|\.\d+)$", v):
                    return f"'{v}'::{t}"
                return "NULL"
            return f"'{v}'::{t}"

        qcols = ", ".join(q(c) for c in cols)
        BATCH = 10000
        dest_batches = defaultdict(list)
        dest_loaded = defaultdict(int)

        def flush_batch(dest_s):
            batch = dest_batches[dest_s]
            if not batch:
                return
            tgt = f"{q(dest_s)}.{q(table)}"
            strat = get_schema_strategy(dest_s)
            active_pks = strat.get("pk_cols", [])

            if active_pks:
                dedup = {}
                for r in batch:
                    key_vals = tuple(r.get(kc) for kc in active_pks)
                    dedup[key_vals] = r
                to_insert = list(dedup.values())
                upd_cols = [c for c in cols if c not in active_pks and c not in AUDIT_COL_NAMES]
                if upd_cols:
                    upd = ", ".join(f"{q(c)} = EXCLUDED.{q(c)}" for c in upd_cols)
                    upd += ', "modifiedby" = CURRENT_USER, "modifieddate" = CURRENT_TIMESTAMP'
                    conflict_clause = f' ON CONFLICT ({", ".join(q(k) for k in active_pks)}) DO UPDATE SET {upd}'
                else:
                    conflict_clause = f' ON CONFLICT ({", ".join(q(k) for k in active_pks)}) DO NOTHING'
            else:
                to_insert = batch
                conflict_clause = ""

            vals = ",\n".join(
                "(" + ", ".join(cast_val(c, row.get(c)) for c in cols) + ")"
                for row in to_insert
            )
            sql = f"INSERT INTO {tgt} ({qcols}) VALUES\n{vals}{conflict_clause};\n"
            run_psql(sql)
            dest_loaded[dest_s] += len(to_insert)
            record_stream_progress(worker_id, len(to_insert))
            batch.clear()

        # Stream file line by line
        skipped_records = []
        total_rows_read = 0

        def route_row(row_idx, row):
            """Routes one parsed record to its destination schema batch (or the skipped list)."""
            if not has_schema_col or not schema_col or schema_col not in row or schema_col == "NO_KORG":
                dest_s = "bcs"
            else:
                dest_s = match_schema_by_last3(row.get(schema_col), target_schemas)

            strat = get_schema_strategy(dest_s)
            if strat["has_pk"] and not record_passes_pk_validation(row, strat["pk_cols"]):
                # Table has a PK constraint: load only records that pass PK validation
                skipped_records.append({
                    "s3_filename": filename,
                    "table": table,
                    "row_num": row_idx,
                    "dest_schema_attempted": dest_s,
                    "skipped_reason": get_pk_validation_failure_reason(row, strat["pk_cols"]),
                    "pk_cols": ", ".join(strat["pk_cols"]),
                    "pk_values": json.dumps({k: row.get(k) for k in strat["pk_cols"]}),
                    "record_sample": json.dumps(row),
                    "full_path": full_path
                })
                return
            # Table has NO PK constraint -> load ALL records (NULL / blank / sentinel values are kept)
            dest_batches[dest_s].append(row)
            if len(dest_batches[dest_s]) >= BATCH:
                flush_batch(dest_s)

        with _opener(full_path) as f:
            if not has_header:
                for row_idx, raw_row in enumerate(csv.reader(f, delimiter=d), start=1):
                    total_rows_read += 1
                    route_row(row_idx, dict(zip(cols, raw_row)))
            else:
                for row_idx, raw_row in enumerate(csv.DictReader(f, delimiter=d), start=2):
                    total_rows_read += 1
                    route_row(row_idx, {norm_col(k): v for k, v in raw_row.items()})

        for dest_s in list(dest_batches.keys()):
            flush_batch(dest_s)

        total_loaded = sum(dest_loaded.values())
        skipped_count = len(skipped_records)
        dur = round(time.time() - t0, 2)

        schema_breakdown_str = ", ".join(f"{s} = {cnt}" for s, cnt in sorted(dest_loaded.items())) if dest_loaded else "None (0 loaded)"
        skipped_str = f"skipped = {skipped_count}"

        has_pk_flag = any(st.get("has_pk") for st in schema_strategies.values())
        all_active_pks = []
        for st in schema_strategies.values():
            for pk in st.get("pk_cols", []):
                if pk not in all_active_pks:
                    all_active_pks.append(pk)

        if skipped_records:
            samples = [f"Row {sr['row_num']}: {sr['skipped_reason']}" for sr in skipped_records[:5]]
            if len(skipped_records) > 5:
                samples.append(f"... (+{len(skipped_records) - 5} more)")
            skipped_summary = f"{len(skipped_records)} skipped [{'; '.join(samples)}]"
        else:
            skipped_summary = "None (0 skipped)" if has_pk_flag else "None (table has no PK constraint; 100% rows loaded)"

        # Log load for each schema (errors propagate -> the file is reported FAILED, never silently unlogged)
        for dest_s, cnt in dest_loaded.items():
            write_load_log(full_path, dest_s, table, size_bytes, cnt, dur, worker_id)

        bcs_loaded = dest_loaded.get("bcs", 0)
        if mapped_schemas_set == {"bcs"}:
            mapped_loaded = bcs_loaded
            unmapped_loaded = sum(cnt for s, cnt in dest_loaded.items() if s.lower() != "bcs")
        else:
            mapped_loaded = sum(cnt for s, cnt in dest_loaded.items() if s.lower() in mapped_schemas_set and s.lower() != "bcs")
            unmapped_loaded = sum(cnt for s, cnt in dest_loaded.items() if s.lower() not in mapped_schemas_set and s.lower() != "bcs")

        if total_rows_read > 0:
            pct_val = round((mapped_loaded / total_rows_read) * 100.0, 2)
            if pct_val == int(pct_val):
                load_pct_str = f"{int(pct_val)}%"
            else:
                load_pct_str = f"{pct_val}%"
        else:
            load_pct_str = "0%"

        logger.info(f"[Worker {worker_id}] Loaded {filename} -> {dict(dest_loaded)} ({total_loaded} rows loaded, {skipped_str}, {dur}s, load%={load_pct_str})")
        return {
            "s3_filename": filename,
            "filename": filename,
            "table": table,
            "copybook_display": cb_display,
            "korg_display": sc_display,
            "total_records": total_rows_read,
            "records_loaded": total_loaded,
            "skipped_count": skipped_count,
            "records_loaded_mapped": mapped_loaded,
            "records_loaded_unmapped": unmapped_loaded,
            "records_loaded_bcs": bcs_loaded,
            "load_pct": load_pct_str,
            "schema_breakdown": schema_breakdown_str,
            "skipped": skipped_str,
            "has_pk": "YES" if has_pk_flag else "NO",
            "pk_cols": ", ".join(all_active_pks) if all_active_pks else "NONE",
            "skipped_reasons_summary": skipped_summary,
            "skipped_records": skipped_records,
            "schema": ",".join(dest_loaded.keys()) or ",".join(target_schemas),
            "uri": full_path,
            "full_path": full_path,
            "status": "SUCCESS",
            "seconds": dur,
            "worker_id": worker_id,
            "rows_loaded": total_loaded,
            "schema_counts": dict(dest_loaded),
            "breakdown": dict(dest_loaded),
            "error": ""
        }

    except Exception as e:  # noqa: BLE001 - worker boundary: report any failure for this file, never kill the pool
        dur = round(time.time() - t0, 2)
        err = str(e)[:300]
        logger.error(f"[Worker {worker_id}] Load failed for {filename}: {e}")
        schema_str = ",".join(target_schemas)
        try:
            fail_sql = (
                f"INSERT INTO {CTL_SCHEMA}.load_log "
                f"(uri, target_schema, target_table, size_bytes, loaded_rows, seconds, worker_id, status, message, loaded_at) "
                f"VALUES ({lit(full_path)}, {lit(schema_str)}, {lit(table)}, {size_bytes}, 0, {dur}, {worker_id}, 'FAILED', {lit(err)}, now()) "
                f"ON CONFLICT (uri) DO UPDATE SET status='FAILED', message={lit(err)}, loaded_at=now();"
            )
            run_psql(fail_sql)
        except RuntimeError as log_err:
            logger.error(f"[Worker {worker_id}] Could not record FAILED status for {filename}: {log_err}")
        return {
            "s3_filename": filename,
            "filename": filename,
            "table": table,
            "copybook_display": cb_display if "cb_display" in locals() else "NO_COPYBOOK",
            "korg_display": sc_display if "sc_display" in locals() else "NO_KORG",
            "total_records": 0,
            "records_loaded": 0,
            "skipped_count": 0,
            "records_loaded_mapped": 0,
            "records_loaded_unmapped": 0,
            "records_loaded_bcs": 0,
            "load_pct": "0%",
            "schema_breakdown": "None (0 loaded)",
            "skipped": "skipped = 0",
            "has_pk": "UNKNOWN",
            "pk_cols": "NONE",
            "skipped_reasons_summary": f"FAILED: {err}",
            "skipped_records": [],
            "schema": schema_str,
            "uri": full_path,
            "full_path": full_path,
            "status": "FAILED",
            "seconds": dur,
            "worker_id": worker_id,
            "rows_loaded": 0,
            "schema_counts": {},
            "breakdown": {},
            "error": err
        }
    finally:
        record_stream_end(worker_id)


def build_load_summary(results):
    """Flattens per-file load results into the LoadSummary rows and the skipped-record rows."""
    load_summary_rows = []
    all_skipped_rows = []

    for r in results:
        if r.get("status") == "FAILED":
            reason = r.get("error") or r.get("skipped_reasons_summary") or "Load execution failed"
        else:
            reason = r.get("skipped_reasons_summary") or "None (0 skipped)"

        load_summary_rows.append({
            "s3 filename": r.get("s3_filename") or r.get("filename", ""),
            "tablename": r.get("table", ""),
            "copybook": r.get("copybook_display", "NO_COPYBOOK"),
            "korg column": r.get("korg_display", "NO_KORG"),
            "total recs": r.get("total_records", 0),
            "loaded recs": r.get("records_loaded", r.get("rows_loaded", 0)),
            "skipped recs": r.get("skipped_count", 0),
            "records loaded in mapped schema": r.get("records_loaded_mapped", 0),
            "records loaded in unmapped schema": r.get("records_loaded_unmapped", 0),
            "records loaded in bcs schema": r.get("records_loaded_bcs", 0),
            "load %": r.get("load_pct", "0%"),
            "has PK?": r.get("has_pk", "NO"),
            "PK columns": r.get("pk_cols", "NONE"),
            "skipped/failed reason": reason,
            "duration": round(float(r.get("seconds", 0) or 0), 2),
            "status": r.get("status", "SUCCESS"),
            "full s3 uri": r.get("full_path") or r.get("uri", "")
        })

        for sk in r.get("skipped_records", []):
            all_skipped_rows.append({
                "S3 file name": sk.get("s3_filename", ""),
                "table": sk.get("table", ""),
                "file line number": sk.get("row_num", 0),
                "destination schema attempted": sk.get("dest_schema_attempted", ""),
                "skipped reason": sk.get("skipped_reason", ""),
                "PK Columns": sk.get("pk_cols", ""),
                "PK Values": sk.get("pk_values", ""),
                "record sample": sk.get("record_sample", ""),
                "Full S3 URI": sk.get("full_path", "")
            })

    return load_summary_rows, all_skipped_rows


def build_schema_breakdown(results):
    """Pivot: one row per file/table, one column per schema, loaded-record counts at the intersections."""
    # SchemaBreakdown sheet: list the s3 filenames and tablenames as rows and list the schema names as columns and put the loaded record counts on the intersection (file/table - schema).
    active_schemas = get_all_active_schemas()
    discovered_schemas = set()
    for r in results:
        discovered_schemas.update(r.get("schema_counts", {}).keys())

    all_known_schemas = set(active_schemas).union(discovered_schemas).union({"bcs"})
    schema_cols = sorted([s for s in all_known_schemas if s != "bcs"])
    if "bcs" in all_known_schemas:
        schema_cols.append("bcs")

    schema_breakdown_rows = []
    for r in results:
        s3_fn = r.get("s3_filename") or r.get("filename", "")
        tbl = r.get("table", "")
        counts = r.get("schema_counts", {})
        row = {
            "s3 filename": s3_fn,
            "tablename": tbl
        }
        row_tot = 0
        for s in schema_cols:
            cnt = counts.get(s, 0)
            row[s] = cnt
            row_tot += cnt
        row["Total"] = row_tot
        schema_breakdown_rows.append(row)

    if schema_breakdown_rows:
        total_row = {
            "s3 filename": "TOTAL",
            "tablename": "ALL TABLES"
        }
        grand_total = 0
        for s in schema_cols:
            col_sum = sum(r.get(s, 0) for r in schema_breakdown_rows)
            total_row[s] = col_sum
            grand_total += col_sum
        total_row["Total"] = grand_total
        schema_breakdown_rows.append(total_row)
    else:
        dummy_row = {"s3 filename": "NONE", "tablename": "NONE"}
        for s in (schema_cols or ["bcs"]):
            dummy_row[s] = 0
        dummy_row["Total"] = 0
        schema_breakdown_rows.append(dummy_row)

    return schema_breakdown_rows


def main():
    parser = argparse.ArgumentParser(description="Step 06: Stream VSAM data from S3 to PostgreSQL with dynamic upsert and schema segregation.")
    parser.add_argument("--mode", choices=["default", "data"], default="default", help="Ingestion mode: default (apply PK to fresh tables, validate records, upsert) or data (no PK on fresh tables, direct load)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--company", help="Comma-separated company filter")
    parser.add_argument("--tables", help="Comma-separated tables to load")
    parser.add_argument("--limit-files", type=int, help="Limit number of files loaded")
    parser.add_argument("--latest-only", "--latest-version-only", action="store_true", help="Load only the latest file version per table (based on timestamp in filename)")
    parser.add_argument("--force-truncate", action="store_true",
                        help="Allow TRUNCATE of existing tables that have rows but no primary key (once per table per run). "
                             "Without it such tables make the file fail instead of being wiped.")
    parser.add_argument("--dry-run", action="store_true", help="List the files and tables that would be loaded, then exit without writing")
    parser.add_argument("--bg", action="store_true", help="Run in background daemon mode")
    args = parser.parse_args()

    global FORCE_TRUNCATE
    FORCE_TRUNCATE = args.force_truncate

    run_id = get_timestamp()
    logger.info("=" * 75)
    logger.info(f"SWEEP VSAM MIGRATION - STEP 06: S3 STREAMING INGESTION (Run: {run_id})")
    logger.info(f"Workers: {args.workers}")
    logger.info("Strategy: Live Database Inspection (Direct load for fresh tables; Dynamic Upsert if PK exists; Truncate & load if no PK)")
    if args.latest_only:
        logger.info("Version Filter: LATEST FILE VERSION ONLY (--latest-only active)")
    else:
        logger.info("Version Filter: LATEST FILE VERSION (Preserving all mapped destination schemas)")
    logger.info("=" * 75)

    # Load Step 02 load matrix
    lm_file = OUT / "vsam_02_load_matrix.csv" if (OUT / "vsam_02_load_matrix.csv").exists() else OUT / "load_matrix.csv"
    lm = load_csv_safe(lm_file)

    if args.company:
        cos = [c.strip().upper() for c in args.company.split(",")]
        lm = lm[lm["source_company"].isin(cos)]
    if args.tables:
        tbls = [t.strip().lower() for t in args.tables.split(",")]
        lm = lm[lm["target_table"].isin(tbls)]

    if lm.empty:
        logger.warning("Load matrix is empty. Nothing to load.")
        return True

    # MAJOR CHANGE: Only consider the single latest version for each file
    orig_count = len(lm)
    src_col = "source_filename" if "source_filename" in lm.columns else "source_file"
    lm = filter_latest_versions(lm, ["source_company", src_col])

    # Sort files based on file size (small to large)
    lm["_size_mb"] = pd.to_numeric(lm["size_mb"], errors="coerce").fillna(0.0)
    lm = lm.sort_values(by="_size_mb", ascending=True).reset_index(drop=True)
    lm = lm.drop(columns=["_size_mb"])
    logger.info(f"Filtered to latest file versions and sorted small to large: {len(lm)} tasks (was {orig_count})")

    if args.limit_files:
        lm = lm.head(args.limit_files)

    # Load copybook metadata from Step 01
    cb_csv = OUT / "vsam_01_copybook_extracts.csv"
    cb_meta = {}
    if cb_csv.exists():
        cb_df = pd.read_csv(cb_csv, dtype=str).fillna("")
        for cb_name, grp in cb_df.groupby("copybook_file"):
            cb_key = cb_name.upper().split(".")[0]
            cb_meta[cb_key] = grp.to_dict(orient="records")

    # Load type-gate profile from Step 03
    tg_file = OUT / "vsam_03_type_gate.json"
    tg_prof = read_json(tg_file, default={})

    tasks = []
    skipped = 0

    # Group by file so each unique file is streamed once and segregated to target schemas + bcs
    for (co, src, fp, tbl), grp in lm.groupby(["source_company", src_col, "full_path", "target_table"], sort=False):
        target_schemas = list(grp["target_schema"].dropna().unique())
        rec = grp.iloc[0].to_dict()
        rec["target_schemas"] = target_schemas
        tasks.append(rec)

    logger.info(f"Total tasks: {len(lm)} | Already loaded (skipping): {skipped} | Pending: {len(tasks)}")

    if not tasks:
        logger.info("All files already loaded successfully. Nothing to do.")
        return True

    if args.dry_run:
        for t in tasks:
            logger.info(f"[DRY-RUN] {t.get('s3_filename') or Path(t['full_path']).name} -> "
                        f"{','.join(t.get('target_schemas') or [])}.{t['target_table']} ({t.get('size_mb', 0)} MB)")
        logger.info(f"[DRY-RUN] {len(tasks)} file(s) would be loaded. No database changes made.")
        return True

    # In Default Mode: Apply PK constraints to available tables before streaming
    if args.mode == "default":
        pk_file = VSAM_GEN_DDL_DIR / "all_pk.sql"
        if pk_file.exists():
            logger.info(f"[Default Mode] Applying PK constraints to available tables before loading data: {pk_file}")
            try:
                run_psql(pk_file, is_file=True)
                logger.info("[+] PK constraints applied to available tables.")
            except RuntimeError as pe:
                logger.warning(f"Could not pre-apply PK constraints (tables keep their current constraints): {pe}")

    results = []
    max_workers = min(args.workers, max(1, len(tasks)))
    logger.info(f"Starting ingestion with {max_workers} worker threads...")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(load_segregated, task, i % max_workers + 1, run_id, tg_prof, cb_meta, args.mode): task
            for i, task in enumerate(tasks)
        }
        for fut in as_completed(future_map):
            task = future_map[fut]
            try:
                res = fut.result()
                results.append(res)
                st = res.get("status", "UNKNOWN")
                rows = res.get("rows_loaded", 0)
                sec = res.get("seconds", 0)
                logger.info(f"[{st}] {res.get('schema')}.{res.get('table')} <- {res.get('filename')} ({rows} rows, {sec}s)")
            except Exception as e:  # noqa: BLE001 - worker boundary: record the failure for this item and continue
                logger.error(f"[ERROR] Task failed for {task.get('s3_filename')}: {e}")
                results.append({
                    "schema": task.get("target_schema"), "table": task.get("target_table"),
                    "uri": task.get("full_path"), "filename": task.get("s3_filename"),
                    "s3_filename": task.get("s3_filename"),
                    "copybook_display": task.get("copybook_map") or "NO_COPYBOOK",
                    "korg_display": task.get("korg_map") or "NO_KORG",
                    "total_records": 0, "records_loaded": 0, "skipped_count": 0,
                    "records_loaded_mapped": 0, "records_loaded_unmapped": 0, "records_loaded_bcs": 0,
                    "load_pct": "0%", "status": "FAILED", "seconds": 0, "worker_id": 0, "rows_loaded": 0,
                    "error": str(e)[:300], "schema_counts": {}
                })

    record_stream_end(None)

    # Save outputs and report
    ts = get_timestamp()
    log_csv = OUT / f"vsam_06_load_log_{run_id}.csv"

    load_summary_rows, all_skipped_rows = build_load_summary(results)
    schema_breakdown_rows = build_schema_breakdown(results)

    pd.DataFrame(load_summary_rows).to_csv(log_csv, index=False)
    logger.info(f"Load log saved: {log_csv}")

    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_06_load_data_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = sum(1 for r in results if r["status"] == "FAILED")
    total_loaded_rows = sum(r.get("records_loaded", r.get("rows_loaded", 0)) for r in results)
    total_skipped_rows = sum(r.get("skipped_count", 0) for r in results)

    run_overview_rows = [{
        "run_id": run_id,
        "total_tasks": len(tasks),
        "success": success_count,
        "failed": failed_count,
        "skipped_already_done": skipped,
        "total_rows_loaded": total_loaded_rows,
        "total_rows_skipped_pk_validation": total_skipped_rows,
        "latest_only_mode": "YES" if args.latest_only else "NO",
        "workers": max_workers
    }]

    if not all_skipped_rows:
        all_skipped_rows = [{
            "S3 file name": "NONE",
            "table": "NONE",
            "file line number": 0,
            "destination schema attempted": "NONE",
            "skipped reason": "No records were skipped during this load.",
            "PK Columns": "NONE",
            "PK Values": "NONE",
            "record sample": "NONE",
            "Full S3 URI": "NONE"
        }]

    generate_excel_report(report_file, {
        "LoadSummary": load_summary_rows,
        "SchemaBreakdown": schema_breakdown_rows,
        "RunOverview": run_overview_rows
    })
    logger.info(f"Excel Report: {report_file}")

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  tasks executed             : {len(results):>10,}",
        f"  successful loads           : {success_count:>10,}",
        f"  failed loads               : {failed_count:>10,}",
        f"  previously completed       : {skipped:>10,}",
        f"  total rows loaded/upserted : {total_loaded_rows:>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)

    return failed_count == 0


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
