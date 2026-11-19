#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_06_load_data.py - Step 06: Schema-Segregated S3 Streaming & Upsert
=============================================================================
Streams VSAM flat files from S3 into PostgreSQL with the SAME workflow as the Oracle track: temp staging tables,
bulk \\copy, set-based validation in SQL, one transaction per file (see vsam/vsam_06_pipeline.py).

  1. Keys: only a primary key from the copybook KEY group (every key column reliably mapped) or from
     korg_overrides is applied - never a guessed one. A key that cannot be applied is an ERROR. A table without
     a known key is loaded keyless and REPLACED by each new snapshot.
  2. Latest file version per (company, source file) only; VSAM files are full snapshots.
  3. Schema routing: the korg column's last 3 digits select the active schema, anything else goes to `bcs`.
  4. Bad rows are rejected (BAD_VALUE, NOT_NULL, PK_BLANK, PK_SENTINEL, ALL_NULL) and listed in
     sweep_ctl.load_rejects and the Reject_Summary / Rejected_Rows sheets; valid rows always load unless
     --max-rejects N is given (then a file with N rejects fails and rolls back).
  5. A file is loaded again only when its S3 LastModified, ETag or size changed (or with --reload).
  6. Keyed tables are upserted (only rows that differ are updated); rows missing from a newer snapshot are
     never deleted.

Generates:
  - outputs/vsam/vsam_06_load_log_<run_id>.csv
  - reports/vsam/<db>_vsam_06_load_data_report_<ts>.xlsx
=============================================================================
"""
import argparse
import json
import os
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from sweep_common import (
    CTL_SCHEMA,
    DEFAULT_WORKERS,
    VSAM_LOGS_DIR,
    VSAM_REPORTS_DIR,
    clip_error,
    generate_excel_report,
    get_db_name,
    get_logger,
    get_timestamp,
    lit,
    norm_col,
    q,
    query_db_dicts,
    query_db_rows,
    run_psql,
    run_psql_script,
)
from sweep_loadpipe import REJECT_TABLE, ensure_reject_table, load_table_meta, parse_output, probe_trigger_mode
from sweep_platform import atomic_write_json
from sweep_s3meta import classify_loaded, head_many, make_meta, meta_from_row
from sweep_streaming import stream_program_literal
from vsam.common_colmap import effective_columns_meta, header_to_table_map
from vsam.common_config import PG_ENCODING
from vsam.common_files import first_row, has_trailing_delim, pg_delim, pg_quote
from vsam.common_mapping import get_active_schemas_map, get_all_active_schemas, get_korg_override_pkey
from vsam.vsam_06_pipeline import build_unit_script, parse_counts
from vsam.vsam_common import (
    OUT,
    build_table_ddl,
    check_table_exists,
    delim_for,
    filter_latest_versions,
    load_csv_safe,
    read_json,
)

logger = get_logger("vsam_06_load_data", "vsam_06_load_data")
LOCK = threading.Lock()
ACTIVE_WORKERS_FILE = VSAM_LOGS_DIR / "active_workers.json"
_ACTIVE_STREAMS = {}
DEFAULT_MAX_REJECTS = 0          # 0 = valid rows always load; the file fails only on technical errors
PSQL_LOAD_OPTIONS = "-c synchronous_commit=off -c statement_timeout=0 -c lock_timeout=0"

# Tables without a primary key are REPLACED by the newest snapshot. In a PARTIAL run (--company, --tables,
# --limit-files) other source files of a shared table could be missing, so replacing a table that already holds
# rows needs --force-truncate.
FORCE_TRUNCATE = False
PARTIAL_RUN = False


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
            "pid": os.getpid(), "worker_id": worker_id, "filename": filename, "table": f"{schema}.{table}",
            "mb": float(size_mb or 0.0), "rows": 0, "start_time": time.time(), "uri": full_path,
        }
        _flush_active_streams()


def record_stream_end(worker_id):
    with LOCK:
        if worker_id is None:
            _ACTIVE_STREAMS.clear()
        else:
            _ACTIVE_STREAMS.pop(worker_id, None)
        _flush_active_streams()


# -----------------------------------------------------------------------------
# load_log helpers
# -----------------------------------------------------------------------------
def load_log_sql(full_path, dest_schema, table, size_bytes, rows, seconds, worker_id, status="SUCCESS", message=None):
    """One load_log row (keyed `<uri>#<schema>`, which Step 07 strips)."""
    msg_cols = ", message" if message else ""
    msg_val = f", {lit(message)}" if message else ""
    msg_upd = f", message={lit(message)}" if message else ""
    return (
        f"INSERT INTO {CTL_SCHEMA}.load_log "
        f"(uri, target_schema, target_table, size_bytes, loaded_rows, seconds, worker_id, status{msg_cols}, loaded_at) "
        f"VALUES ({lit(f'{full_path}#{dest_schema}')}, {lit(dest_schema)}, {lit(table)}, {size_bytes}, "
        f"{int(rows)}, {seconds}, {worker_id}, {lit(status)}{msg_val}, now()) "
        f"ON CONFLICT (uri) DO UPDATE SET status={lit(status)}, loaded_rows={int(rows)}, "
        f"seconds={seconds}, loaded_at=now(){msg_upd};"
    )


def write_load_log(full_path, dest_schema, table, size_bytes, rows, seconds, worker_id, status="SUCCESS", message=None):
    """Records a per-destination-schema load in sweep_ctl.load_log. Errors propagate (a lost row means a reload)."""
    run_psql(load_log_sql(full_path, dest_schema, table, size_bytes, rows, seconds, worker_id, status, message))


def get_loaded_log():
    """{file uri: [S3 metadata stored with its SUCCESS rows]} - one entry per destination schema of the file."""
    try:
        rows = query_db_dicts(f"SELECT uri, s3_last_modified, s3_etag, s3_size FROM {CTL_SCHEMA}.load_log WHERE status = 'SUCCESS'")
    except RuntimeError as e:
        logger.warning(f"Could not read S3 metadata from load_log ({e}); falling back to URIs only. "
                       f"Apply the control DDL (Step 05) to add the s3_* columns.")
        try:
            rows = query_db_dicts(f"SELECT uri FROM {CTL_SCHEMA}.load_log WHERE status = 'SUCCESS'")
        except RuntimeError as e2:
            logger.warning(f"Could not read load_log (table might be new): {e2}")
            return {}
    out = defaultdict(list)
    for r in rows:
        out[r["uri"].rsplit("#", 1)[0]].append(make_meta(r.get("s3_last_modified"), r.get("s3_etag"), r.get("s3_size")))
    return out


def newer_snapshot_loaded(table, source_key, version):
    """True when load_log already holds a snapshot of this source that is newer than `version`."""
    if not version:
        return False
    try:
        rows = query_db_rows(
            f"SELECT message FROM {CTL_SCHEMA}.load_log WHERE target_table = {lit(table)} "
            f"AND message LIKE {lit(f'source={source_key};snapshot=%')}")
    except RuntimeError:
        return False
    loaded = [r.split("snapshot=", 1)[1].strip() for r in rows if "snapshot=" in r]
    return any(v > str(version) for v in loaded)


# -----------------------------------------------------------------------------
# Per-file preparation (columns, key, routing)
# -----------------------------------------------------------------------------
def expected_key(task, tg):
    """Primary key of the source: copybook KEY group (Step 02) or korg_overrides - never guessed."""
    raw = str(task.get("key_columns", "") or "")
    key = [norm_col(c.strip()) for c in re.split(r"[,;+]", raw) if c.strip()]
    if not key:
        key = [norm_col(c) for c in (tg.get("extracted_pk_cols") or []) if c]
    if not key:
        key = [norm_col(c) for c in get_korg_override_pkey(
            task.get("source_company", ""), task.get("source_filename", task.get("source_file", "")),
            task.get("s3_filename", "")) if c]
    return key


def prepare_file(task, tg_prof, cb_meta, index, worker_id, snapshot_msg=""):
    """Everything the SQL pipeline needs to know about one file. Raises RuntimeError when it cannot be loaded."""
    full_path = task["full_path"]
    filename = task.get("s3_filename") or Path(full_path).name
    co = task.get("source_company", "")
    src = task.get("source_filename", task.get("source_file", ""))
    source_key = f"{co}|{src}"
    tg = tg_prof.get(source_key, {})
    size_mb = float(task.get("size_mb", 0) or 0)
    has_header = str(task.get("has_header", "NO")).upper() in ("YES", "UNVERIFIED")
    schema_col = tg.get("schema_col")
    has_schema_col = bool(schema_col and str(schema_col).strip().upper() not in ("", "NONE", "NO_SCHEMA_COL", "NO_KORG"))

    cb_name_raw = task.get("copybook_map", "")
    cb_key = cb_name_raw.upper().split(".")[0] if (cb_name_raw and cb_name_raw.upper() != "NO_COPYBOOK") else ""
    cb_display = cb_name_raw if (cb_name_raw and cb_name_raw.upper() != "NO_COPYBOOK") else "NO_COPYBOOK"
    columns_meta = effective_columns_meta(co, src, cb_meta.get(cb_key, []) if cb_key else [])
    h2t = header_to_table_map(columns_meta)
    if not columns_meta:
        columns_meta = tg.get("columns", [])
    if not columns_meta:
        header = first_row(full_path)
        columns_meta = [{"col_name": norm_col(h.strip()), "sql_type": "text", "is_filler": "NO"} for h in header if h.strip()]
    if not columns_meta:
        raise RuntimeError(f"No column metadata or header found for '{cb_display}'")

    cols = [c["col_name"] for c in columns_meta if c.get("is_filler") != "YES"]
    route_name = h2t.get(schema_col, schema_col) if has_schema_col else None

    # physical columns of the file -> raw temp-table columns named after the table columns
    row0 = first_row(full_path)
    n_phys = len(row0) + (1 if has_trailing_delim(full_path) else 0)
    if has_header:
        names = [(h2t.get(norm_col(t.strip()), norm_col(t.strip())) if t.strip() else "_skip") for t in row0]
    else:
        names = cols[:len(row0)]
    names += ["_skip"] * (n_phys - len(names))
    seen, raw_cols = set(), []
    for pos in range(len(names) - 1, -1, -1):          # a column named twice: the LAST one wins, like the old loader
        n = names[pos]
        if n.startswith("_skip") or n in seen:
            raw_cols.append(f"_skip{pos}")
        else:
            seen.add(n)
            raw_cols.append(n)
    raw_cols.reverse()

    tg_key = expected_key(task, tg)
    return {
        "i": index, "uri": full_path, "name": filename, "table": task["target_table"], "task": task,
        "source_key": source_key, "co": co, "src": src, "tg": tg, "cb_display": cb_display,
        "columns_meta": columns_meta, "stream_cmd": None, "raw_cols": raw_cols, "header": has_header,
        "route_col": route_name if (route_name and route_name in raw_cols) else None,
        "sc_display": route_name if has_schema_col else "NO_KORG",
        "delim_sql": pg_delim(delim_for(full_path)), "quote_sql": pg_quote(full_path),
        "size_bytes": int(size_mb * 1024 * 1024), "size_mb": size_mb, "worker_id": worker_id,
        "snapshot_msg": snapshot_msg or f"source={source_key};snapshot={task.get('version_date', '')}",
        "expected_key": tg_key, "pk_error": tg.get("pk_error", ""), "s3_meta": None, "has_schema_col": has_schema_col,
    }


# -----------------------------------------------------------------------------
# Tables: destinations, provisioning, key
# -----------------------------------------------------------------------------
def destinations(specs):
    """Schemas a table can receive rows in: every active schema + bcs when a file is routed by korg, else bcs only."""
    if any(s["route_col"] for s in specs):
        return list(dict.fromkeys(list(get_active_schemas_map().values()) + ["bcs"]))
    return ["bcs"]


def live_pk(schema, table):
    """Primary key columns of schema.table; raises on a lookup failure (never guesses 'no key')."""
    rows = query_db_rows(
        f"SELECT a.attname FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey) "
        f"WHERE n.nspname = {lit(schema.lower())} AND c.relname = {lit(table.lower())} AND i.indisprimary "
        f"ORDER BY array_position(i.indkey, a.attnum)")
    return [r.strip() for r in rows if r.strip()]


def table_has_rows(schema, table):
    return bool(query_db_rows(f"SELECT 1 FROM {q(schema)}.{q(table)} LIMIT 1"))


def resolve_table(table, specs):
    """
    Creates missing destination tables, applies the expected primary key and returns
    {dests, cols, arbiter ([] = keyless), has_data}. A key that cannot be applied is an error.
    """
    dests = destinations(specs)
    for d in dests:
        if not check_table_exists(d, table):
            pre_sql, _ = build_table_ddl(d, table, specs[0]["columns_meta"], [])
            run_psql(f"CREATE SCHEMA IF NOT EXISTS {q(d)};\n{pre_sql}")
            logger.info(f"Provisioned table {d}.{table} on the fly.")
    expected = next((s["expected_key"] for s in specs if s["expected_key"]), [])
    pk = []
    for d in dests:
        d_pk = live_pk(d, table)
        if expected and not d_pk:
            try:
                run_psql(f"ALTER TABLE {q(d)}.{q(table)} ADD CONSTRAINT {q('pk_' + table)} PRIMARY KEY ({', '.join(q(k) for k in expected)});")
            except RuntimeError as e:
                raise RuntimeError(clip_error(f"Primary key ({', '.join(expected)}) from the copybook cannot be applied to {d}.{table}: {e}"))
            logger.info(f"Applied primary key ({', '.join(expected)}) to {d}.{table}")
            d_pk = live_pk(d, table)
        pk = pk or d_pk
    cols = load_table_meta(dests[0], table)["columns"]
    has_data = any(table_has_rows(d, table) for d in dests) if not pk else True
    return {"dests": dests, "cols": cols, "arbiter": pk, "has_data": has_data}


def table_is_keyed(table, tasks, tg_prof):
    """Cheap check used for planning: does this table have (or will it get) a primary key?"""
    for t in tasks:
        tg = tg_prof.get(f"{t.get('source_company', '')}|{t.get('source_filename', t.get('source_file', ''))}", {})
        if expected_key(t, tg):
            return True
    try:
        return bool(query_db_rows(
            f"SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid WHERE c.relname = {lit(table.lower())} "
            f"AND i.indisprimary LIMIT 1"))
    except RuntimeError:
        return False


# -----------------------------------------------------------------------------
# Running one unit (a keyed file, or all files of a keyless table)
# -----------------------------------------------------------------------------
def _reject_totals(rows, summary, fi):
    by_reason = defaultdict(int)
    detail = {}
    for g in summary:
        if g.get("f") == fi:
            by_reason[g["reason"]] += int(g["n"])
            detail.setdefault(g["reason"], g.get("detail") or "")
    return dict(by_reason), detail


def _file_result(spec, status, seconds, error, info, counts, summary, rows, worker_id, mapped_set):
    fi = spec["i"]
    c = counts.get(fi, {"raw": 0, "rejected": 0, "dup": 0, "by_dest": {}})
    by_dest = c["by_dest"]
    loaded_by = {d: v["inserted"] + v["updated"] + v["unchanged"] for d, v in by_dest.items() if (v["inserted"] + v["updated"] + v["unchanged"]) > 0}
    inserted = sum(v["inserted"] for v in by_dest.values())
    updated = sum(v["updated"] for v in by_dest.values())
    unchanged = sum(v["unchanged"] for v in by_dest.values())
    total_loaded = sum(loaded_by.values()) if status == "SUCCESS" else 0
    if status != "SUCCESS":
        loaded_by, inserted, updated, unchanged = {}, 0, 0, 0
    reasons, details = _reject_totals(rows, summary, fi)
    reasons_dups = reasons.pop("DUP_IN_FILE", 0)
    reasons_txt = ", ".join(f"{k} = {v}" for k, v in sorted(reasons.items()))
    bcs_loaded = loaded_by.get("bcs", 0)
    if mapped_set == {"bcs"}:
        mapped_by = {"bcs": bcs_loaded}
        unmapped_by = {s: n for s, n in loaded_by.items() if s.lower() != "bcs"}
    else:
        mapped_by = {s: n for s, n in loaded_by.items() if s.lower() in mapped_set and s.lower() != "bcs"}
        unmapped_by = {s: n for s, n in loaded_by.items() if s.lower() not in mapped_set and s.lower() != "bcs"}
    mapped_loaded = sum(mapped_by.values())
    pct = round(mapped_loaded / c["raw"] * 100.0, 2) if c["raw"] else 0
    load_pct = f"{int(pct)}%" if pct == int(pct) else f"{pct}%"
    keyed = bool(info and info["arbiter"])
    if status != "SUCCESS":
        summary_txt = f"FAILED: {error}"
    else:
        parts = []
        if c["rejected"]:
            parts.append(f"{c['rejected']} rejected [{reasons_txt}]")
        if reasons_dups:
            parts.append(f"{reasons_dups} duplicate key rows dropped (last row wins)")
        summary_txt = "; ".join(parts) or ("None (0 rejected)" if keyed else "None (table has no PK constraint; 100% rows loaded)")
    line_offset = 1 if spec["header"] else 0
    skipped = []
    for r in rows:
        if r.get("f") == fi:
            skipped.append({
                "s3_filename": spec["name"], "table": spec["table"], "row_num": int(r["row_no"]) + line_offset,
                "dest_schema_attempted": r.get("dest") or "", "skipped_reason": r["reason"], "detail": r.get("detail") or "",
                "row": r.get("row"), "full_path": spec["uri"],
            })
    return {
        "s3_filename": spec["name"], "filename": spec["name"], "table": spec["table"],
        "copybook_display": spec["cb_display"], "korg_display": spec["sc_display"],
        "total_records": c["raw"], "records_loaded": total_loaded, "skipped_count": c["rejected"],
        "records_loaded_mapped": mapped_loaded, "records_loaded_unmapped": sum(unmapped_by.values()),
        "records_loaded_mapped_detail": format_schema_counts(mapped_by),
        "records_loaded_unmapped_detail": format_schema_counts(unmapped_by),
        "records_loaded_bcs": bcs_loaded, "load_pct": load_pct,
        "rows_inserted": inserted, "rows_updated": updated, "rows_unchanged": unchanged,
        "rows_rejected": c["rejected"], "duplicates_dropped": c["dup"], "reject_reasons": reasons_txt,
        "has_pk": "YES" if keyed else "NO", "pk_cols": ", ".join(info["arbiter"]) if keyed else "NONE",
        "skipped_reasons_summary": summary_txt, "skipped_records": skipped, "reject_summary_rows": [
            {"filename": spec["name"], "target_table": spec["table"], "reason": k, "detail": details.get(k, ""),
             "rows": v, "file_status": status} for k, v in sorted(reasons.items())] + (
            [{"filename": spec["name"], "target_table": spec["table"], "reason": "DUP_IN_FILE", "detail": details.get("DUP_IN_FILE", ""),
              "rows": reasons_dups, "file_status": status}] if reasons_dups else []),
        "schema": ",".join(loaded_by.keys()) or ",".join(info["dests"] if info else ["bcs"]),
        "uri": spec["uri"], "full_path": spec["uri"], "status": status, "seconds": seconds, "worker_id": worker_id,
        "rows_loaded": total_loaded, "schema_counts": dict(loaded_by), "breakdown": dict(loaded_by), "error": error,
    }


def _mapped_set(spec):
    task = spec["task"]
    raw_mapped = [s.strip().lower() for s in (task.get("target_schemas") or [task.get("target_schema", "")]) if s and s.strip()]
    if not spec["has_schema_col"] or raw_mapped == ["bcs"]:
        return {"bcs"}
    return set(raw_mapped) - {"bcs"}


def _failed_results(specs, error, seconds, worker_id, info=None):
    return [_file_result(s, "FAILED", seconds, error, info, {}, [], [], worker_id, _mapped_set(s)) for s in specs]


def persist_failed_rejects(specs, table, rows):
    """A failed unit rolled back; keep the echoed reject sample of its files for review."""
    by_file = defaultdict(list)
    for r in rows:
        by_file[r.get("f")].append(r)
    for s in specs:
        sample = by_file.get(s["i"])
        if not sample:
            continue
        values = ",\n".join(
            f"({lit(s['uri'])}, {lit(r.get('dest') or 'bcs')}, {lit(table)}, {int(r['row_no'])}, {lit(r['reason'])}, "
            f"{lit(r.get('detail'))}, {lit(json.dumps(r.get('row')))}::jsonb, 'FILE_FAILED')" for r in sample)
        run_psql(f"DELETE FROM {REJECT_TABLE} WHERE uri = {lit(s['uri'])} AND target_table = {lit(table)};\n"
                 f"INSERT INTO {REJECT_TABLE} (uri, target_schema, target_table, row_no, reason, detail, row_data, file_status) "
                 f"VALUES {values};")


def run_unit(table, specs, worker_id, run, tg_prof):
    """Loads one unit in ONE transaction. Returns one result dict per file."""
    t0 = time.time()
    names = ", ".join(s["name"] for s in specs)
    record_stream_start(worker_id, "bcs", table, specs[0]["uri"], names if len(specs) == 1 else f"{len(specs)} files", sum(s["size_mb"] for s in specs))
    info = None
    try:
        bad = [s for s in specs if s["pk_error"]]
        if bad:
            raise RuntimeError(bad[0]["pk_error"])
        info = resolve_table(table, specs)
        if not info["arbiter"] and info["has_data"] and PARTIAL_RUN and not FORCE_TRUNCATE:
            raise RuntimeError(
                f"{table} already contains rows and has no primary key; this partial run (--company/--tables/--limit-files) "
                f"cannot replace it safely because other source files of the table may be missing. "
                f"Run without filters, or pass --force-truncate.")
        for s in specs:
            s["present"] = [c for c in s["raw_cols"] if not c.startswith("_skip") and c in info["cols"]]
            if not s["present"]:
                raise RuntimeError(f"No column of {s['name']} matches the columns of {table}")
            missing_key = [k for k in info["arbiter"] if k not in s["present"]]
            if missing_key:
                raise RuntimeError(f"Primary key column(s) {', '.join(missing_key)} of {table} are not in {s['name']}")
            s["stream_cmd"] = stream_program_literal(s["uri"])
            s["s3_meta"] = s["s3_meta"] or run["heads"].get(s["uri"])
        route = [(k, v) for k, v in get_active_schemas_map().items()]
        unit = {"table": table, "files": specs, "dests": info["dests"], "cols": info["cols"], "arbiter": info["arbiter"],
                "route": route, "max_rejects": run["max_rejects"], "replica": run["replica"], "encoding": PG_ENCODING}
        logger.info(f"[Worker {worker_id}] Started: {names} -> {table} ({'upsert on ' + ', '.join(info['arbiter']) if info['arbiter'] else 'snapshot replace, no key'})")
        res = run_psql_script(build_unit_script(unit), options=PSQL_LOAD_OPTIONS)
        sec = round(time.time() - t0, 2)
        counts_raw, summary, rows = parse_output(res.stdout)
        counts = parse_counts(counts_raw) if counts_raw else {}
        if res.returncode != 0:
            err = clip_error(res.stderr or res.stdout or "psql failed")
            try:
                persist_failed_rejects(specs, table, rows)
            except RuntimeError as e:
                logger.warning(f"Could not store the rejected rows of the failed load: {e}")
            raise RuntimeError(err)
        # per-schema seconds (the in-transaction rows were written with 0)
        try:
            for s in specs:
                run_psql(f"UPDATE {CTL_SCHEMA}.load_log SET seconds = {round(sec / len(specs), 2)} "
                         f"WHERE uri = {lit(s['uri'])} OR starts_with(uri, {lit(s['uri'] + '#')});")
        except RuntimeError as e:
            logger.warning(f"Could not record load seconds: {e}")
        results = [_file_result(s, "SUCCESS", round(sec / len(specs), 2), "", info, counts, summary, rows, worker_id, _mapped_set(s)) for s in specs]
        for r in results:
            if r["rows_rejected"]:
                logger.warning(f"[Worker {worker_id}] {r['filename']}: {r['rows_rejected']} row(s) rejected [{r['reject_reasons']}] "
                               f"(see sweep_ctl.load_rejects / the report)")
        return results
    except Exception as e:  # noqa: BLE001 - worker boundary: report any failure for this unit, never kill the pool
        sec = round(time.time() - t0, 2)
        err = clip_error(e)
        if len(specs) > 1:
            err = clip_error(f"{err} (all {len(specs)} files of {table} were rolled back together; the table keeps its old data)")
        logger.error(f"[Worker {worker_id}] Load failed for {names}: {e}")
        for s in specs:
            try:
                run_psql(
                    f"INSERT INTO {CTL_SCHEMA}.load_log (uri, target_schema, target_table, size_bytes, loaded_rows, seconds, worker_id, "
                    f"status, message, loaded_at) VALUES ({lit(s['uri'])}, {lit(','.join(info['dests']) if info else 'bcs')}, {lit(table)}, "
                    f"{s['size_bytes']}, 0, {sec}, {worker_id}, 'FAILED', {lit(err)}, now()) "
                    f"ON CONFLICT (uri) DO UPDATE SET status='FAILED', message={lit(err)}, loaded_at=now();")
            except RuntimeError as log_err:
                logger.error(f"[Worker {worker_id}] Could not record FAILED status for {s['name']}: {log_err}")
        return _failed_results(specs, err, sec, worker_id, info)
    finally:
        record_stream_end(worker_id)


def format_schema_counts(counts):
    """{'bss01112': 34, 'bss01104': 20} -> 'bss01104 = 20, bss01112 = 34' (empty string when nothing was loaded)."""
    return ", ".join(f"{s} = {n}" for s, n in sorted(counts.items()) if n)


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

def build_load_summary(results):
    """Flattens per-file load results into the LoadSummary rows."""
    rows = []
    for r in results:
        if r.get("status") == "FAILED":
            reason = r.get("error") or r.get("skipped_reasons_summary") or "Load execution failed"
        else:
            reason = r.get("skipped_reasons_summary") or "None (0 rejected)"
        rows.append({
            "s3 filename": r.get("s3_filename") or r.get("filename", ""),
            "tablename": r.get("table", ""),
            "copybook": r.get("copybook_display", "NO_COPYBOOK"),
            "korg column": r.get("korg_display", "NO_KORG"),
            "total recs": r.get("total_records", 0),
            "loaded recs": r.get("records_loaded", r.get("rows_loaded", 0)),
            "inserted": r.get("rows_inserted", 0),
            "updated": r.get("rows_updated", 0),
            "unchanged": r.get("rows_unchanged", 0),
            "skipped recs": r.get("skipped_count", 0),
            "rejected by reason": r.get("reject_reasons", ""),
            "duplicates dropped": r.get("duplicates_dropped", 0),
            "records loaded in mapped schema": r.get("records_loaded_mapped_detail", ""),
            "records loaded in unmapped schema": r.get("records_loaded_unmapped_detail", ""),
            "records loaded in bcs schema": r.get("records_loaded_bcs", 0),
            "load %": r.get("load_pct", "0%"),
            "has PK?": r.get("has_pk", "NO"),
            "PK columns": r.get("pk_cols", "NONE"),
            "skipped/failed reason": reason,
            "duration": round(float(r.get("seconds", 0) or 0), 2),
            "status": r.get("status", "SUCCESS"),
            "full s3 uri": r.get("full_path") or r.get("uri", ""),
        })
    return rows


def build_reject_sheets(results):
    """Reject_Summary (per file and reason) and Rejected_Rows (the rows, capped like the stored copy)."""
    summary, rows = [], []
    for r in results:
        summary.extend(r.get("reject_summary_rows", []))
        for sk in r.get("skipped_records", []):
            rows.append({
                "filename": sk["s3_filename"], "target_table": sk["table"], "dest_schema": sk["dest_schema_attempted"],
                "file_line_no": sk["row_num"], "reason": sk["skipped_reason"], "detail": sk["detail"],
                "row_data": clip_error(json.dumps(sk.get("row"), ensure_ascii=False, default=str)),
                "file_status": r.get("status", ""),
            })
    if not summary:
        summary = [{"filename": "NONE", "target_table": "NONE", "reason": "No rows were rejected or dropped", "detail": "", "rows": 0, "file_status": ""}]
    if not rows:
        rows = [{"filename": "NONE", "target_table": "NONE", "dest_schema": "", "file_line_no": 0, "reason": "No rows were rejected",
                 "detail": "", "row_data": "", "file_status": ""}]
    return summary, rows[:10000]


def classify_tasks(tasks, tg_prof, reload, heads_live):
    """
    Splits tasks into those to load and those to skip. A file is skipped only when load_log holds SUCCESS rows
    whose S3 LastModified / ETag / size equal what S3 reports now. A table WITHOUT a key is replaced as a whole, so
    when any of its files changed ALL of its files are loaded together; keyed tables load only the changed files.
    """
    stored = {} if reload else get_loaded_log()
    by_table = defaultdict(list)
    for t in tasks:
        by_table[t["target_table"]].append(t)
    pending, skipped = [], []
    for table, group in by_table.items():
        verdict = {}
        for t in group:
            current = heads_live.get(t["full_path"]) or meta_from_row({"s3_last_modified": t.get("s3_last_modified"),
                                                                       "s3_etag": t.get("s3_etag"), "s3_size": t.get("s3_size"),
                                                                       "size_bytes": t.get("size_bytes")})
            verdict[t["full_path"]] = ("reload", "--reload given") if reload else classify_loaded(stored.get(t["full_path"], []), current)
            if verdict[t["full_path"]][0] == "reload" and not reload:
                logger.info(f"Reloading {t.get('s3_filename') or t['full_path']}: {verdict[t['full_path']][1]}")
        keyed = table_is_keyed(table, group, tg_prof)
        any_load = any(v[0] != "skip" for v in verdict.values())
        for t in group:
            if keyed:
                (pending if verdict[t["full_path"]][0] != "skip" else skipped).append(t)
            else:
                (pending if any_load else skipped).append(t)
    return pending, skipped


def main():
    parser = argparse.ArgumentParser(description="Step 06: Stream VSAM data from S3 to PostgreSQL with staged validation, upsert and schema segregation.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--company", help="Comma-separated company filter")
    parser.add_argument("--tables", help="Comma-separated tables to load")
    parser.add_argument("--limit-files", type=int, help="Limit number of files loaded")
    parser.add_argument("--force-truncate", action="store_true",
                        help="Tables without a primary key are replaced by the new snapshot automatically in a full run. "
                             "In a PARTIAL run (--company/--tables/--limit-files) replacing a table that already has rows "
                             "needs this flag, because other source files of the table may be missing.")
    parser.add_argument("--reload", action="store_true", help="Load files again even when their S3 LastModified/ETag/size are unchanged")
    parser.add_argument("--max-rejects", type=int, default=DEFAULT_MAX_REJECTS,
                        help=f"Fail (and roll back) a file when this many rows are rejected (default {DEFAULT_MAX_REJECTS} = never; only technical errors fail a file)")
    parser.add_argument("--dry-run", action="store_true", help="List the files and tables that would be loaded, then exit without writing")
    parser.add_argument("--bg", action="store_true", help="Run in background daemon mode")
    args = parser.parse_args()

    global FORCE_TRUNCATE, PARTIAL_RUN
    FORCE_TRUNCATE = args.force_truncate
    PARTIAL_RUN = bool(args.company or args.tables or args.limit_files)

    run_id = get_timestamp()
    logger.info("=" * 75)
    logger.info(f"SWEEP VSAM MIGRATION - STEP 06: S3 STREAMING INGESTION (Run: {run_id})")
    logger.info(f"Workers: {args.workers}")
    logger.info("Strategy: staged load, one transaction per file (keyed tables: upsert of changed rows; "
                "tables without a key: all their files replace the table together)")
    logger.info("Version Filter: LATEST FILE VERSION per (company, source file); VSAM files are full snapshots")
    logger.info(("A file fails when " + str(args.max_rejects) + " rows are rejected") if args.max_rejects
                else "Rejected rows never fail a file (only technical errors roll it back)")
    logger.info("=" * 75)

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

    orig_count = len(lm)
    src_col = "source_filename" if "source_filename" in lm.columns else "source_file"
    lm = filter_latest_versions(lm, ["source_company", src_col])
    lm["_size_mb"] = pd.to_numeric(lm["size_mb"], errors="coerce").fillna(0.0)
    lm = lm.sort_values(by="_size_mb", ascending=True).reset_index(drop=True).drop(columns=["_size_mb"])
    logger.info(f"Filtered to latest file versions and sorted small to large: {len(lm)} tasks (was {orig_count})")
    if args.limit_files:
        lm = lm.head(args.limit_files)

    cb_csv = OUT / "vsam_01_copybook_extracts.csv"
    cb_meta = {}
    if cb_csv.exists():
        cb_df = pd.read_csv(cb_csv, dtype=str).fillna("")
        for cb_name, grp in cb_df.groupby("copybook_file"):
            cb_meta[cb_name.upper().split(".")[0]] = grp.to_dict(orient="records")
    tg_prof = read_json(OUT / "vsam_03_type_gate.json", default={})

    tasks = []
    for (co, src, fp, tbl), grp in lm.groupby(["source_company", src_col, "full_path", "target_table"], sort=False):
        rec = grp.iloc[0].to_dict()
        rec["target_schemas"] = list(grp["target_schema"].dropna().unique())
        tasks.append(rec)
    total_tasks = len(tasks)

    if args.dry_run:
        for t in tasks:
            logger.info(f"[DRY-RUN] {t.get('s3_filename') or Path(t['full_path']).name} -> "
                        f"{','.join(t.get('target_schemas') or [])}.{t['target_table']} ({t.get('size_mb', 0)} MB)")
        logger.info(f"[DRY-RUN] {len(tasks)} file(s) would be considered. No database changes made.")
        return True

    ensure_reject_table()
    if not args.reload:
        known = get_loaded_log()
        live = head_many([t["full_path"] for t in tasks if t["full_path"] in known]) if known else {}
    else:
        live = {}
    tasks, skipped_tasks = classify_tasks(tasks, tg_prof, args.reload, live)
    skipped = len(skipped_tasks)
    logger.info(f"Total tasks: {total_tasks} | Unchanged since the last load (skipping): {skipped} | Pending: {len(tasks)}")
    if not tasks:
        logger.info("All files already loaded successfully. Nothing to do.")
        return True

    # A full snapshot must never overwrite a NEWER snapshot of the same source that is already loaded.
    runnable, stale = [], []
    for t in tasks:
        sk = f"{t.get('source_company', '')}|{t.get('source_filename', t.get('source_file', ''))}"
        if newer_snapshot_loaded(t["target_table"], sk, t.get("version_date", "")):
            logger.warning(f"[SKIPPED] {t.get('s3_filename')}: a newer snapshot of {sk} is already loaded; not loading an older file")
            stale.append(t)
        else:
            runnable.append(t)
    tasks = runnable

    # HEAD right before loading: the load_log rows store exactly what was loaded
    heads = head_many([t["full_path"] for t in tasks])
    run = {"max_rejects": args.max_rejects, "heads": heads, "replica": probe_trigger_mode() == "replica"}

    results = [{"s3_filename": t.get("s3_filename"), "filename": t.get("s3_filename"), "table": t["target_table"], "uri": t["full_path"],
                "full_path": t["full_path"], "status": "SKIPPED", "seconds": 0, "rows_loaded": 0, "records_loaded": 0,
                "error": "older snapshot than the one already loaded"} for t in stale]

    by_table = defaultdict(list)
    for t in tasks:
        by_table[t["target_table"]].append(t)
    units = []        # (table, [task...])
    for table, group in by_table.items():
        if table_is_keyed(table, group, tg_prof):
            units.extend((table, [t]) for t in group)
        else:
            units.append((table, group))        # all files of a table without a key replace it together

    max_workers = min(args.workers, max(1, len(units)))
    logger.info(f"Starting ingestion with {max_workers} worker threads ({len(units)} unit(s) of work)...")

    def prepare_unit(table, group, worker_id):
        specs, failed = [], []
        for n, t in enumerate(group, 1):
            try:
                specs.append(prepare_file(t, tg_prof, cb_meta, n, worker_id))
            except Exception as e:  # noqa: BLE001 - a file that cannot be prepared fails (and with it a keyless table's replace)
                failed.append((t, clip_error(e)))
        return specs, failed

    def work(unit, worker_id):
        table, group = unit
        specs, failed = prepare_unit(table, group, worker_id)
        if failed:
            out = []
            for t, err in failed:
                logger.error(f"[Worker {worker_id}] Cannot prepare {t.get('s3_filename')}: {err}")
                out.append({"s3_filename": t.get("s3_filename"), "filename": t.get("s3_filename"), "table": table,
                            "copybook_display": t.get("copybook_map") or "NO_COPYBOOK", "korg_display": "NO_KORG",
                            "uri": t["full_path"], "full_path": t["full_path"], "status": "FAILED", "seconds": 0,
                            "rows_loaded": 0, "records_loaded": 0, "total_records": 0, "skipped_count": 0, "load_pct": "0%",
                            "error": err, "schema_counts": {}})
            if len(group) > 1 and specs:      # keyless table: one broken file blocks the whole replace
                for s in specs:
                    out.append({"s3_filename": s["name"], "filename": s["name"], "table": table, "copybook_display": s["cb_display"],
                                "korg_display": s["sc_display"], "uri": s["uri"], "full_path": s["uri"], "status": "FAILED",
                                "seconds": 0, "rows_loaded": 0, "records_loaded": 0, "total_records": 0, "skipped_count": 0,
                                "load_pct": "0%", "schema_counts": {},
                                "error": f"not loaded: another file of {table} could not be prepared; the table keeps its old data"})
            return out
        return run_unit(table, specs, worker_id, run, tg_prof)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(work, u, i % max_workers + 1): u for i, u in enumerate(units)}
        for fut in as_completed(future_map):
            table, group = future_map[fut]
            try:
                out = fut.result()
            except Exception as e:  # noqa: BLE001 - last line of defence
                logger.error(f"[ERROR] Unit for {table} failed: {e}")
                out = [{"s3_filename": t.get("s3_filename"), "filename": t.get("s3_filename"), "table": table, "uri": t["full_path"],
                        "full_path": t["full_path"], "status": "FAILED", "seconds": 0, "rows_loaded": 0, "records_loaded": 0,
                        "error": clip_error(e), "schema_counts": {}} for t in group]
            results.extend(out)
            for res in out:
                logger.info(f"[{res.get('status')}] {res.get('schema', '')}.{res.get('table')} <- {res.get('filename')} "
                            f"({res.get('rows_loaded', 0):,} rows, {res.get('seconds', 0)}s)")
    record_stream_end(None)

    ts = get_timestamp()
    log_csv = OUT / f"vsam_06_load_log_{run_id}.csv"
    load_summary_rows = build_load_summary(results)
    schema_breakdown_rows = build_schema_breakdown(results)
    reject_summary, rejected_rows = build_reject_sheets(results)
    pd.DataFrame(load_summary_rows).to_csv(log_csv, index=False)
    logger.info(f"Load log saved: {log_csv}")

    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_06_load_data_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = sum(1 for r in results if r["status"] == "FAILED")
    skipped_stale = sum(1 for r in results if r["status"] == "SKIPPED")
    total_loaded_rows = sum(r.get("records_loaded", r.get("rows_loaded", 0)) for r in results)
    total_rejected = sum(r.get("rows_rejected", 0) for r in results)
    total_dups = sum(r.get("duplicates_dropped", 0) for r in results)
    run_overview_rows = [{
        "run_id": run_id, "total_tasks": len(tasks), "success": success_count, "failed": failed_count,
        "skipped_unchanged": skipped, "skipped_older_snapshot": skipped_stale, "total_rows_loaded": total_loaded_rows,
        "total_rows_rejected": total_rejected, "duplicate_rows_dropped": total_dups,
        "reject_limit_per_file": args.max_rejects if args.max_rejects else "none", "workers": max_workers,
    }]
    generate_excel_report(report_file, {
        "LoadSummary": load_summary_rows,
        "SchemaBreakdown": schema_breakdown_rows,
        "Reject_Summary": reject_summary,
        "Rejected_Rows": rejected_rows,
        "RunOverview": run_overview_rows,
    })
    logger.info(f"Excel Report: {report_file}")
    logger.info("=" * 65)
    logger.info(f" EXECUTION SUMMARY: {Path(__file__).name}")
    logger.info("=" * 65)
    for label, value in (("files executed", len(results)), ("files loaded", success_count), ("files failed", failed_count),
                         ("files skipped (unchanged)", skipped), ("rows loaded", total_loaded_rows),
                         ("rows rejected", total_rejected), ("duplicate rows dropped", total_dups)):
        logger.info(f"  {label:<28}: {value:>10,}")
    logger.info("=" * 65)
    return failed_count == 0


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
