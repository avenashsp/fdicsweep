#!/usr/bin/env python3
"""
sweep_bg.py - Background Process Management and Live Operational Monitoring
Allows any migration task to run as a detached background daemon, with complete
operational visibility (status, live tail logs, kill/stop, and live throughput monitoring).
"""
import os
import sys
import re
import json
import time
import signal
import subprocess
from pathlib import Path
from datetime import datetime
from sweep_common import (
    BASE_DIR, LOGS_DIR, ORACLE_LOGS_DIR, VSAM_LOGS_DIR, get_timestamp, get_db_name, run_psql, query_db_rows,
    LOAD_LOG_TABLE, OUTPUTS_DIR, ORACLE_OUTPUTS_DIR, VSAM_OUTPUTS_DIR
)

JOBS_METADATA_FILE = BASE_DIR / ".sweep_bg_jobs.json"


def _load_jobs_registry():
    if JOBS_METADATA_FILE.exists():
        try:
            with open(JOBS_METADATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_jobs_registry(registry):
    try:
        with open(JOBS_METADATA_FILE, "w", encoding="utf-8") as f:
            json.dump(registry, f, indent=2)
    except Exception:
        pass


def is_pid_alive(pid):
    """Checks if a process ID is currently running."""
    if pid <= 0:
        return False
    if os.name == "nt":
        # Windows process check
        res = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
        return str(pid) in res.stdout
    else:
        # Unix process check
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False


def _detect_job_outcome(log_path, started_iso):
    """Inspects log file content and modification time to infer final status and runtime."""
    status = "FINISHED"
    ended_time = None
    p = Path(log_path) if log_path else None

    if p and p.exists() and p.stat().st_size > 0:
        ended_time = datetime.fromtimestamp(p.stat().st_mtime)
        try:
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-100:]
                tail_text = "".join(lines).lower()
                if "traceback" in tail_text or "[error]" in tail_text or "failed" in tail_text or "exception" in tail_text:
                    status = "ERRORED"
                elif "completed successfully" in tail_text or "report generated" in tail_text or "migration audit completed" in tail_text or "done" in tail_text:
                    status = "FINISHED"
                else:
                    status = "FINISHED"
        except Exception:
            pass

    if not ended_time:
        try:
            ended_time = datetime.fromisoformat(started_iso)
        except Exception:
            ended_time = datetime.now()

    return status, ended_time


def _runner_main(job_name, cmd_args_json, log_file_str):
    """Internal supervisor executed inside the detached background daemon process."""
    cmd_args = json.loads(cmd_args_json)
    log_path = Path(log_file_str)
    pid = os.getpid()
    st = datetime.now()

    # Register start
    registry = _load_jobs_registry()
    if job_name in registry:
        registry[job_name]["pid"] = pid
        registry[job_name]["status"] = "RUNNING"
        registry[job_name]["started_at"] = st.isoformat()
        registry[job_name]["log_file"] = str(log_path)
        registry[job_name].pop("ended_at", None)
        registry[job_name].pop("runtime", None)
        registry[job_name].pop("exit_code", None)
        _save_jobs_registry(registry)

    # Execute target migration command with log output redirection
    code = 1
    ended = datetime.now()
    delta_str = "0:00:00"

    # Pass daemon environment to child process so get_logger suppresses duplicate StreamHandler
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["SWEEP_DAEMON"] = "1"
    env["SWEEP_LOG_FILE"] = str(log_path)
    env["SWEEP_JOB_NAME"] = str(job_name)

    try:
        with open(log_path, "a", encoding="utf-8") as f_out:
            f_out.write(f"=== [START JOB: {job_name}] PID: {pid} at {st.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f_out.flush()
            res = subprocess.run(cmd_args, stdout=f_out, stderr=subprocess.STDOUT, env=env)
            code = res.returncode
            ended = datetime.now()
            delta_str = str(ended - st).split(".")[0]
            status = "FINISHED" if code == 0 else "ERRORED"
            f_out.write(f"\n=== [END JOB: {job_name}] Status: {status} (Code: {code}) Duration: {delta_str} at {ended.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f_out.flush()
    except Exception as e:
        status = "ERRORED"
        code = 2
        ended = datetime.now()
        delta_str = str(ended - st).split(".")[0]
        try:
            with open(log_path, "a", encoding="utf-8") as f_out:
                f_out.write(f"\n[FATAL] Job supervisor exception: {e}\n")
                f_out.flush()
        except Exception:
            pass

    # Save final outcome
    registry = _load_jobs_registry()
    if job_name in registry:
        curr = registry[job_name].get("status")
        if curr != "STOPPED":
            registry[job_name]["status"] = status
        registry[job_name]["ended_at"] = ended.isoformat()
        registry[job_name]["runtime"] = delta_str
        registry[job_name]["exit_code"] = code
        _save_jobs_registry(registry)

    sys.exit(code)


def start_background_job(job_name, cmd_args, custom_log_name=None, track=None):
    """
    Spawns a command as a fully detached background process supervised by _runner.
    Tracks PID, start time, command, and log file in registry with track segregation.
    """
    ts = get_timestamp()
    db = get_db_name()
    db_prefix = f"{db}_" if db else ""
    if custom_log_name:
        log_name = custom_log_name if custom_log_name.lower().startswith(db_prefix) else f"{db_prefix}{custom_log_name}"
    else:
        if db_prefix and job_name.lower().startswith(db_prefix):
            log_name = f"{job_name}_{ts}.log"
        else:
            log_name = f"{db_prefix}{job_name}_{ts}.log"

    determined_track = track
    if not determined_track:
        if job_name.startswith("oracle_") or "oracle" in job_name.lower():
            determined_track = "oracle"
        elif job_name.startswith("vsam_") or "vsam" in job_name.lower():
            determined_track = "vsam"
        else:
            cmd_str = " ".join(str(a).lower() for a in cmd_args)
            if "oracle" in cmd_str:
                determined_track = "oracle"
            elif "vsam" in cmd_str:
                determined_track = "vsam"

    if determined_track == "oracle":
        target_log_dir = ORACLE_LOGS_DIR
    elif determined_track == "vsam":
        target_log_dir = VSAM_LOGS_DIR
    else:
        target_log_dir = ORACLE_LOGS_DIR

    target_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = target_log_dir / log_name

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["SWEEP_DAEMON"] = "1"
    env["SWEEP_LOG_FILE"] = str(log_path)
    env["SWEEP_JOB_NAME"] = str(job_name)

    # Wrap target command with internal runner to monitor exit status and freeze runtime
    runner_cmd = [
        sys.executable, str(Path(__file__).resolve()),
        "_runner", job_name, json.dumps(cmd_args), str(log_path)
    ]

    if os.name == "nt":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
        proc = subprocess.Popen(
            runner_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            cwd=str(Path(__file__).resolve().parent),
            env=env,
            creationflags=flags
        )
    else:
        proc = subprocess.Popen(
            runner_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            cwd=str(Path(__file__).resolve().parent),
            env=env,
            start_new_session=True
        )

    pid = proc.pid
    registry = _load_jobs_registry()
    registry[job_name] = {
        "pid": pid,
        "name": job_name,
        "command": " ".join(cmd_args),
        "status": "RUNNING",
        "started_at": datetime.now().isoformat(),
        "log_file": str(log_path)
    }
    _save_jobs_registry(registry)

    # Maintain latest log pointer
    latest_log = BASE_DIR / "sweep_latest.log"
    try:
        if os.name != "nt":
            if latest_log.is_symlink() or latest_log.exists():
                latest_log.unlink()
            latest_log.symlink_to(log_path)
    except Exception:
        pass

    print(f"\n[+] Background job '{job_name}' started successfully.")
    print(f"    PID      : {pid}")
    print(f"    Log File : {log_path}")
    print("    To view  : python sweep.py status")
    print("    To tail  : python sweep.py logs -f")
    print(f"    To stop  : python sweep.py stop --name {job_name}\n")
    return pid


def list_jobs():
    """Lists all registered background jobs with frozen runtimes and explicit statuses."""
    registry = _load_jobs_registry()
    results = []
    dirty = False

    for name, info in list(registry.items()):
        pid = info.get("pid", 0)
        alive = is_pid_alive(pid)
        started = info.get("started_at", datetime.now().isoformat())

        try:
            st = datetime.fromisoformat(started)
        except Exception:
            st = datetime.now()

        if alive:
            status = "RUNNING"
            delta = datetime.now() - st
            runtime = str(delta).split(".")[0]
        else:
            status = info.get("status")
            if status in ("RUNNING", "FINISHED/STOPPED", None):
                # Process died or finished without runner update; auto-detect
                detected_status, ended_dt = _detect_job_outcome(info.get("log_file"), started)
                status = detected_status
                info["status"] = status
                info["ended_at"] = ended_dt.isoformat()
                info["runtime"] = str(ended_dt - st).split(".")[0]
                dirty = True

            # Use frozen runtime
            runtime = info.get("runtime")
            if not runtime:
                ended_iso = info.get("ended_at")
                if ended_iso:
                    try:
                        et = datetime.fromisoformat(ended_iso)
                        runtime = str(et - st).split(".")[0]
                    except Exception:
                        runtime = "N/A"
                else:
                    runtime = "N/A"

        results.append({
            "name": name,
            "pid": pid,
            "alive": alive,
            "status": status,
            "runtime": runtime,
            "started_at": started,
            "ended_at": info.get("ended_at", ""),
            "log_file": info.get("log_file", "")
        })

    if dirty:
        _save_jobs_registry(registry)

    return results


def parse_duration_to_seconds(val):
    """Parses timedelta, numeric seconds, or duration strings (HH:MM:SS, DD days, HH:MM:SS) to integer seconds."""
    if val is None or val == "" or val == "N/A" or val == "-":
        return None
    from datetime import timedelta
    if isinstance(val, timedelta):
        return int(val.total_seconds())
    if isinstance(val, (int, float)):
        return int(val)

    s = str(val).strip()
    days = 0
    if "day" in s:
        m = re.match(r"(\d+)\s*days?,\s*(.*)", s, re.I)
        if m:
            days = int(m.group(1))
            s = m.group(2).strip()

    parts = s.split(":")
    if len(parts) == 3:
        try:
            h, m, sec = int(parts[0]), int(parts[1]), int(float(parts[2]))
            return days * 86400 + h * 3600 + m * 60 + sec
        except ValueError:
            return None
    elif len(parts) == 2:
        try:
            m, sec = int(parts[0]), int(float(parts[1]))
            return days * 86400 + m * 60 + sec
        except ValueError:
            return None
    elif len(parts) == 1:
        try:
            return days * 86400 + int(float(parts[0]))
        except ValueError:
            return None
    return None


def format_runtime_display(val):
    """
    Formats duration according to runtime display rules:
      >= 1 day   (e.g., 27:20:13) -> 1d 3h
      >= 1 hour  (e.g., 03:39:39) -> 3h 39m
      >= 1 min   (e.g., 00:04:15) -> 4m 15s
      < 1 min    (e.g., 00:00:12) -> 12s
    """
    sec = parse_duration_to_seconds(val)
    if sec is None:
        return str(val) if val else "0s"
    if sec < 0:
        sec = 0

    if sec >= 86400:
        d = sec // 86400
        h = (sec % 86400) // 3600
        return f"{d}d {h}h"
    elif sec >= 3600:
        h = sec // 3600
        m = (sec % 3600) // 60
        return f"{h}h {m}m"
    elif sec >= 60:
        m = sec // 60
        s = sec % 60
        return f"{m}m {s}s"
    else:
        return f"{sec}s"


def print_status():
    """Prints a formatted CLI table showing background job status matching user layout."""
    jobs = list_jobs()
    print("=" * 80)
    print(f"{'DATE':<9}{'PID':<11}{'JOB NAME':<25}{'RUNTIME':<15}{'STATUS'}")
    print("-" * 80)
    if not jobs:
        print("No background jobs found in registry.")
        print("=" * 80)
        return

    # Sort jobs chronologically by started_at
    jobs = sorted(jobs, key=lambda x: x.get("started_at", ""))
    for j in jobs:
        started = j.get("started_at")
        date_str = "- "
        if started:
            try:
                st_dt = datetime.fromisoformat(started)
                date_str = st_dt.strftime("%b-%d")
            except Exception:
                try:
                    from dateutil import parser as dt_parser
                    st_dt = dt_parser.parse(str(started))
                    date_str = st_dt.strftime("%b-%d")
                except Exception:
                    date_str = str(started)[:6]

        pid_str = str(j.get("pid", ""))
        name_str = j.get("name", "")
        runtime_str = format_runtime_display(j.get("runtime"))
        st = str(j.get("status", "")).upper()
        if st == "RUNNING":
            status_display = "\033[93mRUNNING\033[0m"  # yellow
        elif st == "STOPPED":
            status_display = "\033[94mSTOPPED\033[0m"  # blue
        elif st == "ERRORED":
            status_display = "\033[91mERRORED\033[0m"  # red
        elif st == "FINISHED":
            status_display = "\033[92mFINISHED\033[0m"  # green
        else:
            status_display = j.get("status", "")

        print(f"{date_str:<9}{pid_str:<11}{name_str:<25}{runtime_str:<15}{status_display}")
    print("=" * 80)


def tail_logs(lines=40, follow=False, log_file=None, track=None):
    """
    Displays the tail of the latest active log file and streams new lines.
    Prioritizes actively RUNNING background jobs over idle or empty log files.
    """
    target_log = None
    if log_file:
        target_log = Path(log_file)
        if not target_log.is_absolute() and not target_log.exists():
            for d in [ORACLE_LOGS_DIR, VSAM_LOGS_DIR]:
                candidate = d / log_file
                if candidate.exists():
                    target_log = candidate
                    break
    else:
        search_dirs = [ORACLE_LOGS_DIR, VSAM_LOGS_DIR]
        if track == "oracle":
            search_dirs = [ORACLE_LOGS_DIR]
        elif track == "vsam":
            search_dirs = [VSAM_LOGS_DIR]
        else:
            search_dirs = [ORACLE_LOGS_DIR, VSAM_LOGS_DIR]

        # 1. First priority: currently RUNNING background job from registry
        registry = _load_jobs_registry()
        for j_name, info in registry.items():
            if track and track not in j_name.lower():
                continue
            pid = info.get("pid", 0)
            if is_pid_alive(pid) and info.get("log_file"):
                p = Path(info["log_file"])
                if p.exists():
                    target_log = p
                    break

        # 2. Second priority: most recent non-empty job log from registry
        if not target_log and registry:
            sorted_jobs = sorted(
                registry.values(),
                key=lambda x: x.get("started_at", ""),
                reverse=True
            )
            for j in sorted_jobs:
                if track and track not in str(j.get("log_file", "")).lower() and track not in j.get("name", "").lower():
                    continue
                p = Path(j.get("log_file", ""))
                if p.exists() and p.stat().st_size > 0:
                    target_log = p
                    break

        # 3. Third priority: newest non-empty step log file in search dirs
        if not target_log:
            candidate_logs = []
            for s_dir in search_dirs:
                if s_dir.exists():
                    candidate_logs.extend([
                        f for f in s_dir.rglob("*.log")
                        if f.is_file() and f.stat().st_size > 0 and not f.name.startswith("sweep_latest") and f.name != "latest.log"
                    ])
            if candidate_logs:
                target_log = max(candidate_logs, key=lambda f: f.stat().st_mtime)

    if not target_log or not target_log.exists():
        loc_str = f"{track} logs directory" if track else f"{LOGS_DIR}"
        print(f"No active or non-empty log files found in {loc_str}")
        return

    print(f"\n--- Tailing: {target_log.name} (last {lines} lines) ---")
    sys.stdout.flush()

    try:
        with open(target_log, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
            for l in all_lines[-lines:]:
                sys.stdout.write(l)
            sys.stdout.flush()

            if follow:
                print(f"\n--- Following live log: {target_log.name} (Ctrl+C to exit) ---\n")
                sys.stdout.flush()
                while True:
                    line = f.readline()
                    if line:
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    else:
                        time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nLog tail stopped.")


def stop_jobs(job_name=None, kill_pg_backends=False):
    """Stops one or all running background migration processes and freezes runtime."""
    registry = _load_jobs_registry()
    stopped = 0
    now_iso = datetime.now().isoformat()

    for name, info in list(registry.items()):
        if job_name and name.lower() != job_name.lower():
            continue
        pid = info.get("pid", 0)
        started = info.get("started_at")
        try:
            st = datetime.fromisoformat(started)
            delta = str(datetime.now() - st).split(".")[0]
        except Exception:
            delta = "N/A"

        if is_pid_alive(pid):
            print(f"Stopping job '{name}' (PID {pid})...")
            try:
                if os.name == "nt":
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True)
                else:
                    os.kill(pid, signal.SIGTERM)
                    time.sleep(1)
                    if is_pid_alive(pid):
                        os.kill(pid, signal.SIGKILL)
                stopped += 1
                print(f"Job '{name}' stopped.")
            except Exception as e:
                print(f"Failed to kill PID {pid}: {e}")
        else:
            print(f"Job '{name}' (PID {pid}) is already terminated.")

        info["status"] = "STOPPED"
        info["ended_at"] = now_iso
        info["runtime"] = delta
        registry[name] = info

    _save_jobs_registry(registry)

    if kill_pg_backends:
        print("\nTerminating in-flight PostgreSQL COPY sessions...")
        try:
            sql = ("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                   "WHERE query ILIKE 'copy %' AND pid <> pg_backend_pid();")
            run_psql(sql)
            print("All active PostgreSQL COPY sessions terminated.")
        except Exception as e:
            print(f"Error terminating database backends: {e}")

    return stopped


def format_time_duration(seconds):
    """
    Converts seconds to human-readable duration:
      < 60s         -> Xs        (e.g., 47s)
      60s - 3599s   -> XmYs      (e.g., 635s -> 10m35s)
      >= 3600s      -> XhrYm     (e.g., 3625s -> 1hr0m, no seconds when showing hours)
    """
    if seconds is None:
        return "0s"
    try:
        total_sec = int(round(float(seconds)))
    except Exception:
        return str(seconds)
    if total_sec < 0:
        return "0s"
    if total_sec < 60:
        return f"{total_sec}s"
    elif total_sec < 3600:
        m = total_sec // 60
        s = total_sec % 60
        return f"{m}m{s}s"
    else:
        h = total_sec // 3600
        m = (total_sec % 3600) // 60
        return f"{h}hr{m}m"


def format_approx_number(num, decimals=2):
    """
    Converts full numbers to approximate count with magnitude suffixes:
      K for Thousand, M for Million, B for Billion, T for Trillion.
      Examples: 100235 -> 100K, 44022345 -> 44.02M
    """
    if num is None:
        return "0"
    try:
        n = float(num)
    except Exception:
        return str(num)
    if n < 0:
        return f"-{format_approx_number(-n, decimals)}"
    if n < 1000:
        return f"{int(n)}"
    units = [
        (1_000_000_000_000, "T"),
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    ]
    for threshold, suffix in units:
        if n >= threshold:
            val = n / threshold
            if val >= 100:
                return f"{int(round(val))}{suffix}"
            else:
                s = f"{val:.{decimals}f}".rstrip("0").rstrip(".")
                return f"{s}{suffix}"
    return f"{int(n)}"


def format_word_number(num):
    """
    Converts large counts to word format for high-level dashboard summaries:
      >= 1 Million -> 'X.X Million', 'X.X Billion', 'X.X Trillion'
      < 1 Million  -> format_approx_number (e.g. 100K)
    """
    if num is None:
        return "0"
    try:
        n = float(num)
    except Exception:
        return str(num)
    if n < 0:
        return f"-{format_word_number(-n)}"
    if n < 1000:
        return f"{int(n)}"
    units = [
        (1_000_000_000_000, "Trillion"),
        (1_000_000_000, "Billion"),
        (1_000_000, "Million"),
    ]
    for threshold, word in units:
        if n >= threshold:
            val = n / threshold
            if val >= 100:
                s = f"{int(round(val))}"
            else:
                s = f"{val:.1f}".rstrip("0").rstrip(".")
            return f"{s} {word}"
    return format_approx_number(n)


def format_size_actual(mb_val):
    """
    Converts size in MB to actual size names:
      < 1 MB         -> X KB
      1 MB - 1023 MB -> X MB  (e.g., 999.0MB, 78.3MB)
      1024 MB - 1 TB -> X GB  (e.g., 1500MB -> 1.5GB, 3365.7MB -> 3.3GB)
      >= 1 TB        -> X TB  (e.g., 1500GB -> 1.5TB)
    """
    if mb_val is None:
        return "0MB"
    try:
        mb = float(mb_val)
    except Exception:
        return str(mb_val)
    if mb < 0:
        return f"-{format_size_actual(-mb)}"
    if mb >= 1024 * 1024:
        tb = mb / (1024.0 * 1024.0)
        s = f"{tb:.1f}".rstrip("0").rstrip(".")
        return f"{s}TB"
    elif mb >= 1024:
        gb = mb / 1024.0
        s = f"{gb:.1f}".rstrip("0").rstrip(".")
        return f"{s}GB"
    elif mb >= 1:
        s = f"{mb:.1f}".rstrip("0").rstrip(".")
        return f"{s}MB"
    elif mb > 0:
        kb = mb * 1024.0
        return f"{kb:.0f}KB"
    return "0MB"


# -----------------------------------------------------------------------------
# Terminal Color Palette for Monitoring Dashboard
# Strict 3-color scheme:
#   - All Topics: Yellow
#   - All Column Headers: Green
#   - All Rows Items: White
#   - No other colors
# -----------------------------------------------------------------------------
C_RESET  = "\033[0m"
C_YELLOW = "\033[1;33m"  # All Topics and Topic Borders
C_GREEN  = "\033[1;32m"  # All Column Headers & Overview Labels
C_WHITE  = "\033[97m"    # All Row Items & Data Values


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


def render_dashboard(use_color=True, track="all"):
    """
    Renders the live operational dataload monitoring dashboard.
    When track='all', clearly displays both ORACLE and VSAM track progress side-by-side,
    along with combined migration totals, active worker streams, and completed tables.
    When track='vsam' or track='oracle', renders the focused single-track dashboard.
    """
    if os.name == "nt":
        try:
            os.system("")
        except Exception:
            pass

    if os.environ.get("NO_COLOR"):
        use_color = False

    track = (track or "all").lower()
    jobs = list_jobs()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    W = 80
    lines = []

    ora = get_track_metrics("oracle", jobs)
    vsam = get_track_metrics("vsam", jobs)
    active_workers = get_active_workers()

    # Main Header
    b_main = "=" * W
    if track == "vsam":
        t_main = "FDICSWEEP LIVE DATALOAD DASHBOARD (VSAM TRACK)"
    elif track == "oracle":
        t_main = "FDICSWEEP LIVE DATALOAD DASHBOARD (ORACLE TRACK)"
    else:
        t_main = "FDICSWEEP LIVE DATALOAD DASHBOARD"

    t_pad = " " * ((W - len(t_main)) // 2)

    if use_color:
        lines.append(f"{C_YELLOW}{b_main}{C_RESET}")
        lines.append(f"{C_YELLOW}{t_pad}{t_main}{C_RESET}")
        lines.append(f"{C_YELLOW}{b_main}{C_RESET}")
    else:
        lines.append(b_main)
        lines.append(f"{t_pad}{t_main}")
        lines.append(b_main)

    def format_2col_row(l_lbl, l_val, r_lbl, r_val):
        l_plain = f"{l_lbl:<14}: {l_val}" if l_lbl else ""
        r_plain = f"{r_lbl:<14} : {r_val}" if r_lbl else ""
        pad = " " * max(2, 48 - len(l_plain))
        if use_color:
            l_col = f"{C_GREEN}{l_lbl:<14}: {C_RESET}{C_WHITE}{l_val}{C_RESET}" if l_lbl else ""
            r_col = f"{C_GREEN}{r_lbl:<14} : {C_RESET}{C_WHITE}{r_val}{C_RESET}" if r_lbl else ""
            return f"{l_col}{pad}{r_col}".rstrip()
        else:
            return f"{l_plain}{pad}{r_plain}".rstrip()

    if track in ("vsam", "oracle"):
        # Single-Track Focused Dashboard (matches reference layout)
        m = vsam if track == "vsam" else ora
        track_workers = [w for w in active_workers if w["track"].lower() == track]
        if m["is_alive"] and track_workers:
            status_str = f"RUNNING ({len(track_workers)} workers)"
        else:
            status_str = m["status"]

        inflight_rows = sum(w.get("rows", 0) for w in track_workers)

        left_items = [
            ("Active Daemon", m["daemon"]),
            ("Progress", f"{m['files_loaded']:,}/{m['files_planned']:,} files ({m['pct']:.1f}%)"),
            ("Volume", f"{m['gb_loaded']:.2f}/{m['gb_planned']:.2f} GB"),
            ("Average Speed", f"~{format_approx_number(m['speed'], decimals=1)} rows/sec" if m['speed'] > 0 else "0 rows/sec"),
            ("Current Time", now_str),
        ]
        right_items = [
            ("Status", status_str),
            ("Runtime", m["runtime"]),
            ("Committed Rows", format_word_number(m["rows_loaded"])),
            ("In-flight Rows", format_word_number(inflight_rows)),
            ("PID", m["pid"]),
        ]
        for (l_lbl, l_val), (r_lbl, r_val) in zip(left_items, right_items):
            lines.append(format_2col_row(l_lbl, l_val, r_lbl, r_val))

    else:
        # Dual-Track Dashboard showing BOTH Oracle and VSAM clearly
        # Section 1: Oracle Track
        s_ora = "-- ORACLE TRACK " + "-" * (W - 16)
        lines.append(f"{C_YELLOW}{s_ora}{C_RESET}" if use_color else s_ora)
        ora_workers = [w for w in active_workers if w["track"] == "ORACLE"]
        ora_status_str = f"RUNNING ({len(ora_workers)} workers)" if ora["is_alive"] and ora_workers else ora["status"]
        ora_left = [
            ("Active Daemon", ora["daemon"]),
            ("Progress", f"{ora['files_loaded']:,}/{ora['files_planned']:,} files ({ora['pct']:.1f}%)"),
            ("Volume", f"{ora['gb_loaded']:.2f}/{ora['gb_planned']:.2f} GB"),
            ("Average Speed", f"~{format_approx_number(ora['speed'], decimals=1)} rows/sec" if ora['speed'] > 0 else "0 rows/sec"),
        ]
        ora_right = [
            ("Status", ora_status_str),
            ("Runtime", ora["runtime"]),
            ("Committed Rows", format_word_number(ora["rows_loaded"])),
            ("PID", ora["pid"]),
        ]
        for (l_lbl, l_val), (r_lbl, r_val) in zip(ora_left, ora_right):
            lines.append(format_2col_row(l_lbl, l_val, r_lbl, r_val))

        # Section 2: VSAM Track
        s_vsam = "-- VSAM TRACK " + "-" * (W - 14)
        lines.append(f"{C_YELLOW}{s_vsam}{C_RESET}" if use_color else s_vsam)
        vsam_workers = [w for w in active_workers if w["track"] == "VSAM"]
        vsam_status_str = f"RUNNING ({len(vsam_workers)} workers)" if vsam["is_alive"] and vsam_workers else vsam["status"]
        vsam_left = [
            ("Active Daemon", vsam["daemon"]),
            ("Progress", f"{vsam['files_loaded']:,}/{vsam['files_planned']:,} files ({vsam['pct']:.1f}%)"),
            ("Volume", f"{vsam['gb_loaded']:.2f}/{vsam['gb_planned']:.2f} GB"),
            ("Average Speed", f"~{format_approx_number(vsam['speed'], decimals=1)} rows/sec" if vsam['speed'] > 0 else "0 rows/sec"),
        ]
        vsam_right = [
            ("Status", vsam_status_str),
            ("Runtime", vsam["runtime"]),
            ("Committed Rows", format_word_number(vsam["rows_loaded"])),
            ("PID", vsam["pid"]),
        ]
        for (l_lbl, l_val), (r_lbl, r_val) in zip(vsam_left, vsam_right):
            lines.append(format_2col_row(l_lbl, l_val, r_lbl, r_val))

        # Section 3: Migration Overview / Summary
        s_tot = "-- COMBINED MIGRATION SUMMARY " + "-" * (W - 30)
        lines.append(f"{C_YELLOW}{s_tot}{C_RESET}" if use_color else s_tot)
        tot_f_plan = ora["files_planned"] + vsam["files_planned"]
        tot_f_load = ora["files_loaded"] + vsam["files_loaded"]
        tot_pct = round((tot_f_load / tot_f_plan) * 100, 1) if tot_f_plan > 0 else 0.0
        tot_gb_plan = ora["gb_planned"] + vsam["gb_planned"]
        tot_gb_load = ora["gb_loaded"] + vsam["gb_loaded"]
        tot_rows = ora["rows_loaded"] + vsam["rows_loaded"]

        tot_left = [
            ("Total Progress", f"{tot_f_load:,}/{tot_f_plan:,} files ({tot_pct:.1f}%)"),
            ("Total Volume", f"{tot_gb_load:.2f}/{tot_gb_plan:.2f} GB"),
        ]
        tot_right = [
            ("Committed Rows", format_word_number(tot_rows)),
            ("Current Time", now_str),
        ]
        for (l_lbl, l_val), (r_lbl, r_val) in zip(tot_left, tot_right):
            lines.append(format_2col_row(l_lbl, l_val, r_lbl, r_val))

    # Currently Running Streams
    b_mid = "-" * W
    t_run = "CURRENTLY RUNNING STREAMS"
    t_run_pad = " " * ((W - len(t_run)) // 2)

    if use_color:
        lines.append(f"{C_YELLOW}{b_mid}{C_RESET}")
        lines.append(f"{C_YELLOW}{t_run_pad}{t_run}{C_RESET}")
        lines.append(f"{C_YELLOW}{b_mid}{C_RESET}")
    else:
        lines.append(b_mid)
        lines.append(f"{t_run_pad}{t_run}")
        lines.append(b_mid)

    filtered_workers = [w for w in active_workers if (track == "all" or w["track"].lower() == track)]

    if track == "all":
        h_run = f"{'TRACK':<8}{'PID':<8}{'FILENAME':<42}{'SIZE':<12}{'STREAMING'}"
    else:
        h_run = f"{'PID':<8}{'FILENAME':<48}{'SIZE':<12}{'STREAMING'}"

    lines.append(f"{C_GREEN}{h_run}{C_RESET}" if use_color else h_run)

    if filtered_workers:
        for w in filtered_workers:
            w_pid = str(w["pid"])
            w_tbl = w["table"]
            w_size = format_size_actual(w["mb"])
            w_stream = format_time_duration(w["seconds"])
            if track == "all":
                row_str = f"{w['track']:<8}{w_pid:<8}{w_tbl[:40]:<42}{w_size:<12}{w_stream}"
            else:
                row_str = f"{w_pid:<8}{w_tbl[:46]:<48}{w_size:<12}{w_stream}"
            lines.append(f"{C_WHITE}{row_str}{C_RESET}" if use_color else row_str)
    else:
        msg = "  (No active worker streams)"
        lines.append(f"{C_WHITE}{msg}{C_RESET}" if use_color else msg)

    # Top 5 Completed Streams
    t_comp = "TOP 5 COMPLETED STREAMS"
    t_comp_pad = " " * ((W - len(t_comp)) // 2)
    if use_color:
        lines.append(f"{C_YELLOW}{b_mid}{C_RESET}")
        lines.append(f"{C_YELLOW}{t_comp_pad}{t_comp}{C_RESET}")
        lines.append(f"{C_YELLOW}{b_mid}{C_RESET}")
    else:
        lines.append(b_mid)
        lines.append(f"{t_comp_pad}{t_comp}")
        lines.append(b_mid)

    top_completed = get_top_completed(limit=5, track_filter=(None if track == "all" else track))

    if track == "all":
        h_comp = f"{'TRACK':<8}{'TABLENAME':<46}{'ROWS':<10}{'TIME':<6}{'SPEED'}"
    else:
        h_comp = f"{'TABLENAME':<52}{'ROWS':<12}{'TIME':<6}{'SPEED'}"

    lines.append(f"{C_GREEN}{h_comp}{C_RESET}" if use_color else h_comp)

    if top_completed:
        for tc in top_completed:
            fname = tc["filename"]
            rows_str = format_approx_number(tc["rows"])
            time_str = format_time_duration(tc["time"])
            speed_str = f"{format_approx_number(tc['speed'], decimals=1)} r/s"
            t_tag = f"[{tc['track']}]"
            if track == "all":
                row_str = f"{t_tag:<8}{fname[:45]:<46}{rows_str:<10}{time_str:<6}{speed_str}"
            else:
                row_str = f"{fname[:51]:<52}{rows_str:<12}{time_str:<6}{speed_str}"
            lines.append(f"{C_WHITE}{row_str}{C_RESET}" if use_color else row_str)
    else:
        msg = "  (No completed tables yet)"
        lines.append(f"{C_WHITE}{msg}{C_RESET}" if use_color else msg)

    # Footer
    b_foot = "=" * W
    hint_text = "(Press Ctrl+C to exit dashboard - background ingestion will continue running)"
    if use_color:
        lines.append(f"{C_YELLOW}{b_foot}{C_RESET}")
        lines.append(f"{C_WHITE}{hint_text}{C_RESET}")
    else:
        lines.append(b_foot)
        lines.append(hint_text)

    return "\n".join(lines)


def monitor_live(interval=3, watch=False, use_color=True, track="all"):
    """
    Operational dashboard for active data ingestion.
    By default (watch=False), prints the dashboard once without auto refresh.
    If watch=True, continuously refreshes at specified interval.
    """
    if not watch:
        # Non-auto-refresh mode: render snapshot once and exit immediately
        print(render_dashboard(use_color=use_color, track=track))
        sys.stdout.flush()
        return

    # Continuous watch mode
    try:
        while True:
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.write(render_dashboard(use_color=use_color, track=track) + "\n")
            sys.stdout.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")


if __name__ == "__main__":
    if len(sys.argv) >= 5 and sys.argv[1] == "_runner":
        _runner_main(sys.argv[2], sys.argv[3], sys.argv[4])
