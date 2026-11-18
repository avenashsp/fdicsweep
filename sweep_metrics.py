#!/usr/bin/env python3
"""
sweep_metrics.py - Throughput and progress metrics for the live monitor (log files + pg_stat views).
"""
import os
import re
import json
import time
import subprocess
from datetime import datetime
from sweep_common import (
    BASE_DIR,
    LOAD_LOG_TABLE,
    LOGS_DIR,
    ORACLE_LOGS_DIR,
    ORACLE_OUTPUTS_DIR,
    OUTPUTS_DIR,
    query_db_rows,
    VSAM_LOGS_DIR,
    VSAM_OUTPUTS_DIR,
)
from sweep_format import format_runtime_display
from sweep_jobs import list_jobs


def get_track_metrics(track_name, jobs=None):
    """
    Computes planned vs loaded metrics for a specific track ('oracle' or 'vsam').
    Accurately deduplicates multi-schema VSAM load records to prevent volume/file inflation.
    """
    if jobs is None:
        jobs = list_jobs()

    t_lower = track_name.lower()
    is_vsam = "vsam" in t_lower

    # 1. Active daemon discovery
    track_jobs = [j for j in jobs if (t_lower in j.get("name", "").lower() or (is_vsam and "vsam" in j.get("name", "").lower()))]
    active_job = None
    if track_jobs:
        running_jobs = [j for j in track_jobs if j.get("alive")]
        if running_jobs:
            active_job = max(running_jobs, key=lambda j: j.get("started_at", ""))
        else:
            active_job = max(track_jobs, key=lambda j: j.get("started_at", ""))

    # Also check unmanaged process tree on Unix if active_job is not alive
    if (not active_job or not active_job.get("alive")) and os.name != "nt":
        try:
            res = subprocess.run(["ps", "-eo", "pid,ppid,etimes,args"], capture_output=True, text=True)
            if res.returncode == 0:
                for line in res.stdout.splitlines()[1:]:
                    parts = line.strip().split(None, 3)
                    if len(parts) >= 4:
                        p_pid, p_etimes, p_args = parts[0], parts[2], parts[3]
                        if is_vsam and ("vsam_07_load_data" in p_args or "vsam_06_load_data" in p_args) and "sweep_bg.py" not in p_args and "monitor" not in p_args:
                            try:
                                sec_val = int(p_etimes)
                            except ValueError:
                                sec_val = 0
                            active_job = {
                                "name": "vsam_07_load_data" if "vsam_07" in p_args else "vsam_06_load_data",
                                "pid": int(p_pid),
                                "alive": True,
                                "status": "RUNNING",
                                "runtime": format_runtime_display(sec_val),
                                "started_at": datetime.now().isoformat()
                            }
                            break
                        elif not is_vsam and ("oracle_06_load_data" in p_args or "oracle_05_load_data" in p_args) and "sweep_bg.py" not in p_args and "monitor" not in p_args:
                            try:
                                sec_val = int(p_etimes)
                            except ValueError:
                                sec_val = 0
                            active_job = {
                                "name": "oracle_06_load_data",
                                "pid": int(p_pid),
                                "alive": True,
                                "status": "RUNNING",
                                "runtime": format_runtime_display(sec_val),
                                "started_at": datetime.now().isoformat()
                            }
                            break
        except Exception:
            pass

    daemon_name = active_job.get("name", "None") if active_job else "None"
    daemon_pid = str(active_job.get("pid", "-")) if active_job else "-"
    daemon_status = active_job.get("status", "IDLE") if active_job else "IDLE"
    daemon_runtime = format_runtime_display(active_job.get("runtime", "-")) if active_job else "0s"
    is_alive = bool(active_job and active_job.get("alive"))

    # 2. Planned metrics from CSV
    total_files_planned = 0
    total_bytes_planned = 0
    search_dirs = [VSAM_OUTPUTS_DIR, OUTPUTS_DIR] if is_vsam else [ORACLE_OUTPUTS_DIR, OUTPUTS_DIR]
    candidates = (
        ["vsam_02_load_matrix.csv", "vsam_02_inventory.csv", "vsam_01_load_matrix.csv", "vsam_01_inventory.csv", "load_matrix.csv", "inventory.csv"]
        if is_vsam else
        ["oracle_02_s3_file_map.csv", "oracle_02_inventory.csv", "s3_file_map.csv", "s3_inventory.csv"]
    )

    src_file = None
    for d in search_dirs:
        for fname in candidates:
            cand = d / fname
            if cand.exists() and cand.stat().st_size > 0:
                src_file = cand
                break
        if src_file:
            break

    if src_file:
        import csv
        seen_files = set()
        try:
            with open(src_file, "r", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    fkey = r.get("full_path") or r.get("s3_filename") or r.get("uri") or r.get("source_filename") or str(total_files_planned)
                    if fkey in seen_files:
                        continue
                    seen_files.add(fkey)
                    total_files_planned += 1

                    sb = r.get("size_bytes")
                    smb = r.get("size_mb")
                    if sb and sb.strip():
                        try:
                            total_bytes_planned += int(float(sb.strip()))
                        except ValueError:
                            pass
                    elif smb and smb.strip():
                        try:
                            total_bytes_planned += int(float(smb.strip()) * 1024 * 1024)
                        except ValueError:
                            pass
        except Exception:
            pass

    total_gb_planned = round(total_bytes_planned / (1024**3), 2)

    # 3. Query loaded stats from sweep_ctl.load_log
    files_loaded = 0
    rows_loaded = 0
    gb_loaded = 0.0
    speed = 0

    track_filter = (
        "(target_table ILIKE 'stg_%' OR uri ILIKE '%/vsam/%' OR uri ILIKE '%#%')"
        if is_vsam else
        "NOT (target_table ILIKE 'stg_%' OR uri ILIKE '%/vsam/%' OR uri ILIKE '%#%')"
    )

    try:
        sql_log = (
            f"SELECT COUNT(*), COALESCE(SUM(loaded_rows), 0), "
            f"COALESCE(ROUND(SUM(file_bytes)/1073741824.0, 2), 0) "
            f"FROM ("
            f"    SELECT split_part(uri, '#', 1) AS s3_file, "
            f"           MAX(size_bytes) AS file_bytes, "
            f"           SUM(loaded_rows) AS loaded_rows "
            f"    FROM {LOAD_LOG_TABLE} "
            f"    WHERE status = 'SUCCESS' AND {track_filter} "
            f"    GROUP BY split_part(uri, '#', 1)"
            f") sub;"
        )
        rows_log = query_db_rows(sql_log)
        if rows_log and "|" in rows_log[0]:
            parts = rows_log[0].split("|")
            files_loaded = int(parts[0].strip() or 0)
            rows_loaded = int(parts[1].strip() or 0)
            gb_loaded = float(parts[2].strip() or 0.0)
    except Exception:
        pass

    # 4. Instant Speed
    try:
        sql_spd = (
            f"SELECT COALESCE(ROUND(AVG(CASE WHEN seconds > 0 THEN loaded_rows / seconds ELSE rows_per_sec END)), 0) "
            f"FROM ("
            f"    SELECT loaded_rows, seconds, COALESCE(rows_per_sec, 0) AS rows_per_sec "
            f"    FROM {LOAD_LOG_TABLE} "
            f"    WHERE status = 'SUCCESS' AND (loaded_rows > 0 OR seconds > 0) AND {track_filter} "
            f"    ORDER BY loaded_at DESC LIMIT 10"
            f") spd_sub;"
        )
        spd_rows = query_db_rows(sql_spd)
        if spd_rows and spd_rows[0].strip():
            speed = int(float(spd_rows[0].strip()))
    except Exception:
        pass

    pct = round((files_loaded / total_files_planned) * 100, 1) if total_files_planned > 0 else 0.0

    return {
        "track": "VSAM" if is_vsam else "ORACLE",
        "daemon": daemon_name,
        "pid": daemon_pid,
        "status": daemon_status,
        "runtime": daemon_runtime,
        "is_alive": is_alive,
        "files_planned": total_files_planned,
        "bytes_planned": total_bytes_planned,
        "gb_planned": total_gb_planned,
        "files_loaded": files_loaded,
        "rows_loaded": rows_loaded,
        "gb_loaded": gb_loaded,
        "pct": pct,
        "speed": speed
    }


def get_active_workers():
    """
    Discovers live in-flight worker streams across both Oracle and VSAM tracks
    using multi-tier inspection (heartbeat JSON, OS process inspection, PostgreSQL active sessions).
    """
    active_workers = []
    seen_keys = set()

    # Tier 1: Heartbeat files
    hb_candidates = [
        ("VSAM", VSAM_LOGS_DIR / "active_workers.json"),
        ("ORACLE", ORACLE_LOGS_DIR / "active_workers.json"),
        ("ORACLE", LOGS_DIR / "active_workers.json"),
        ("COMMON", BASE_DIR / "active_workers.json"),
    ]

    now = time.time()
    for default_track, hbf in hb_candidates:
        if hbf.exists():
            try:
                mtime = hbf.stat().st_mtime
                if now - mtime > 300:
                    continue
                with open(hbf, "r", encoding="utf-8") as fp:
                    hb_data = json.load(fp)
                    if isinstance(hb_data, list):
                        for w in hb_data:
                            w_pid = str(w.get("pid", "-"))
                            w_tbl = w.get("filename") or w.get("table") or "streaming chunk"
                            track = "VSAM" if ("stg_" in str(w_tbl).lower() or "/vsam/" in str(w.get("uri", "")).lower() or default_track == "VSAM") else "ORACLE"
                            key = (track, w_pid, w_tbl)
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)
                            start_t = float(w.get("start_time") or now)
                            w_sec = max(1, int(now - start_t)) if start_t <= now else int(w.get("seconds") or 1)
                            active_workers.append({
                                "track": track,
                                "pid": w_pid,
                                "table": w_tbl,
                                "rows": int(w.get("rows") or 0),
                                "mb": float(w.get("mb") or 0.0),
                                "seconds": w_sec
                            })
            except Exception:
                pass

    # Tier 2: Check OS process tree (Linux / Unix native inspection)
    if os.name != "nt":
        try:
            ps_cmd = ["ps", "-eo", "pid,ppid,etimes,args"]
            res = subprocess.run(ps_cmd, capture_output=True, text=True)
            if res.returncode == 0:
                for line in res.stdout.splitlines()[1:]:
                    parts = line.strip().split(None, 3)
                    if len(parts) >= 4:
                        p_pid, p_etimes, p_args = parts[0], parts[2], parts[3]
                        if "sweep_stream.sh" in p_args:
                            m_s3 = re.search(r's3://[^\s\'"]+/([^\s\'"/]+\.(?:csv(?:\.gz)?|tsv(?:\.gz)?|gz))', p_args, re.IGNORECASE)
                            disp = m_s3.group(1) if m_s3 else "sweep_stream worker"
                            try:
                                sec_val = int(p_etimes)
                            except ValueError:
                                sec_val = 1
                            key = ("ORACLE", p_pid, disp)
                            if key not in seen_keys:
                                seen_keys.add(key)
                                active_workers.append({
                                    "track": "ORACLE",
                                    "pid": p_pid,
                                    "table": disp,
                                    "rows": 0,
                                    "mb": 0.0,
                                    "seconds": sec_val
                                })
                        elif ("vsam_07_load_data" in p_args or "vsam_06_load_data" in p_args) and "python" in p_args and "sweep_bg.py" not in p_args:
                            key = ("VSAM", p_pid, "vsam_load_engine")
                            if key not in seen_keys and not any(w["track"] == "VSAM" for w in active_workers):
                                seen_keys.add(key)
                                try:
                                    sec_val = int(p_etimes)
                                except ValueError:
                                    sec_val = 1
                                active_workers.append({
                                    "track": "VSAM",
                                    "pid": p_pid,
                                    "table": "vsam ingestion worker",
                                    "rows": 0,
                                    "mb": 0.0,
                                    "seconds": sec_val
                                })
        except Exception:
            pass

    # Tier 3: Query PostgreSQL Database Catalog (pg_stat_activity & pg_stat_progress_copy)
    try:
        sql_workers = (
            "SELECT "
            "    a.pid, "
            "    COALESCE(to_regclass(p.relid::text)::text, '') AS target_table, "
            "    COALESCE(p.tuples_processed, 0) AS rows_streamed, "
            "    COALESCE(ROUND(p.bytes_processed / 1048576.0, 1), 0) AS mb_streamed, "
            "    EXTRACT(EPOCH FROM (clock_timestamp() - a.query_start))::int AS seconds_active, "
            "    substr(regexp_replace(COALESCE(a.query, ''), E'[\\r\\n]+', ' ', 'g'), 1, 300) AS clean_query "
            "FROM pg_stat_activity a "
            "LEFT JOIN pg_stat_progress_copy p ON a.pid = p.pid "
            "WHERE (a.query ~* '^\\s*(copy|\\\\copy)\\b' OR a.query ILIKE '%COPY %' "
            "       OR a.query ~* '^\\s*INSERT\\s+INTO\\s+' OR a.query ILIKE '%INSERT INTO %' "
            "       OR p.pid IS NOT NULL) "
            "  AND a.pid <> pg_backend_pid() "
            "  AND a.state = 'active' "
            "ORDER BY a.pid;"
        )
        worker_rows = []
        try:
            worker_rows = query_db_rows(sql_workers)
        except Exception:
            sql_fallback = (
                "SELECT "
                "    a.pid, "
                "    '' AS target_table, "
                "    0 AS rows_streamed, "
                "    0.0 AS mb_streamed, "
                "    EXTRACT(EPOCH FROM (clock_timestamp() - a.query_start))::int AS seconds_active, "
                "    substr(regexp_replace(COALESCE(a.query, ''), E'[\\r\\n]+', ' ', 'g'), 1, 300) AS clean_query "
                "FROM pg_stat_activity a "
                "WHERE (a.query ~* '^\\s*(copy|\\\\copy)\\b' OR a.query ILIKE '%COPY %' "
                "       OR a.query ~* '^\\s*INSERT\\s+INTO\\s+' OR a.query ILIKE '%INSERT INTO %') "
                "  AND a.pid <> pg_backend_pid() "
                "  AND a.state = 'active' "
                "ORDER BY a.pid;"
            )
            try:
                worker_rows = query_db_rows(sql_fallback)
            except Exception:
                worker_rows = []

        for wr in worker_rows:
            wp = wr.split("|")
            if len(wp) >= 5:
                w_pid = wp[0].strip()
                w_tbl = wp[1].strip()
                try:
                    w_rows = int(wp[2].strip() or 0)
                except ValueError:
                    w_rows = 0
                try:
                    w_mb = float(wp[3].strip() or 0.0)
                except ValueError:
                    w_mb = 0.0
                try:
                    w_sec = int(wp[4].strip() or 0)
                except ValueError:
                    w_sec = 0
                w_query = wp[5].strip() if len(wp) > 5 else ""

                chunk_match = re.search(r's3://[^\s\'"]+/([^\s\'"/]+\.(?:csv(?:\.gz)?|tsv(?:\.gz)?|gz))', w_query, re.IGNORECASE)
                if chunk_match:
                    w_display = chunk_match.group(1)
                elif w_tbl and w_tbl.lower() != "null":
                    w_display = w_tbl
                else:
                    m = re.search(r'(?:\\copy|copy|insert\s+into)\s+([^\s(]+)', w_query, re.IGNORECASE)
                    w_display = m.group(1).replace('"', '').strip() if m else "streaming chunk"

                track = "VSAM" if ("stg_" in w_display.lower() or "stg_" in w_query.lower()) else "ORACLE"
                key = (track, w_pid, w_display)
                if key not in seen_keys:
                    seen_keys.add(key)
                    active_workers.append({
                        "track": track,
                        "pid": w_pid,
                        "table": w_display,
                        "rows": w_rows,
                        "mb": w_mb,
                        "seconds": w_sec
                    })
    except Exception:
        pass

    return active_workers


def get_top_completed(limit=5, track_filter=None):
    """
    Returns the top recently completed table loads with track indicator.
    """
    completed = []
    where_extra = ""
    if track_filter == "vsam":
        where_extra = " AND (target_table ILIKE 'stg_%' OR uri ILIKE '%/vsam/%' OR uri ILIKE '%#%')"
    elif track_filter == "oracle":
        where_extra = " AND NOT (target_table ILIKE 'stg_%' OR uri ILIKE '%/vsam/%' OR uri ILIKE '%#%')"

    try:
        sql = (
            f"SELECT "
            f"    uri, "
            f"    COALESCE(target_schema, ''), "
            f"    COALESCE(target_table, ''), "
            f"    COALESCE(loaded_rows, 0) AS rows, "
            f"    COALESCE(seconds, 0) AS duration, "
            f"    COALESCE(ROUND(CASE WHEN seconds > 0 THEN loaded_rows / seconds ELSE 0 END), 0) AS speed, "
            f"    CASE WHEN target_table ILIKE 'stg_%' OR uri ILIKE '%/vsam/%' OR uri ILIKE '%#%' THEN 'VSAM' ELSE 'ORACLE' END AS track "
            f"FROM {LOAD_LOG_TABLE} "
            f"WHERE status = 'SUCCESS'{where_extra} "
            f"ORDER BY loaded_at DESC "
            f"LIMIT {limit};"
        )
        for r in query_db_rows(sql):
            parts = r.split("|")
            if len(parts) >= 6:
                full_uri = parts[0].strip()
                t_schema = parts[1].strip()
                t_table = parts[2].strip()
                r_count = int(parts[3].strip() or 0)
                sec_val = float(parts[4].strip() or 0.0)
                spd_val = int(float(parts[5].strip() or 0.0))
                t_track = parts[6].strip() if len(parts) > 6 else ("VSAM" if "stg_" in t_table else "ORACLE")

                if t_schema and t_table:
                    disp_name = f"{t_schema}.{t_table}".lower()
                elif t_table:
                    disp_name = t_table.lower()
                else:
                    disp_name = os.path.basename(full_uri.split("#")[0])

                completed.append({
                    "track": t_track,
                    "filename": disp_name,
                    "rows": r_count,
                    "time": sec_val,
                    "speed": spd_val
                })
    except Exception:
        pass
    return completed
