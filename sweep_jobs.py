#!/usr/bin/env python3
"""
sweep_jobs.py - Background job registry, detached supervision, status, log tailing and stop.
"""
import contextlib
import os
import sys
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime
from sweep_common import (
    PSQL_APP_NAME,
    BASE_DIR,
    get_db_name,
    get_timestamp,
    LOGS_DIR,
    ORACLE_LOGS_DIR,
    run_psql,
    VSAM_LOGS_DIR,
    PARSE_ERRORS,
)
from sweep_format import format_runtime_display
from sweep_platform import (
    atomic_write_json, detached_popen_kwargs, file_lock, make_symlink, pid_alive, pid_matches, read_json_or,
    terminate_tree,
)


JOBS_METADATA_FILE = BASE_DIR / ".sweep_bg_jobs.json"
RUNNER_SCRIPT = BASE_DIR / "sweep_bg.py"
# Substring that must appear in a PID's command line before we are willing to signal it.
RUNNER_MARKER = "sweep_bg.py"


def _load_jobs_registry():
    """Reads the job registry ({} when missing or corrupt)."""
    return read_json_or(JOBS_METADATA_FILE, {})


def _save_jobs_registry(registry):
    """Atomically replaces the job registry file."""
    atomic_write_json(JOBS_METADATA_FILE, registry)


@contextlib.contextmanager
def registry_transaction():
    """
    Locked read-modify-write of the job registry shared by the CLI, the monitor and every
    background supervisor process:  `with registry_transaction() as reg: reg[name]["x"] = 1`
    """
    with file_lock(JOBS_METADATA_FILE):
        registry = _load_jobs_registry()
        yield registry
        _save_jobs_registry(registry)


is_pid_alive = pid_alive


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
        except OSError:
            pass

    if not ended_time:
        try:
            ended_time = datetime.fromisoformat(started_iso)
        except PARSE_ERRORS:
            ended_time = datetime.now()

    return status, ended_time


def _runner_main(job_name, cmd_args_json, log_file_str):
    """Internal supervisor executed inside the detached background daemon process."""
    cmd_args = json.loads(cmd_args_json)
    log_path = Path(log_file_str)
    pid = os.getpid()
    st = datetime.now()

    # Register start
    with registry_transaction() as registry:
        if job_name in registry:
            registry[job_name]["pid"] = pid
            registry[job_name]["status"] = "RUNNING"
            registry[job_name]["started_at"] = st.isoformat()
            registry[job_name]["log_file"] = str(log_path)
            registry[job_name].pop("ended_at", None)
            registry[job_name].pop("runtime", None)
            registry[job_name].pop("exit_code", None)

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
    except (OSError, ValueError) as e:
        status = "ERRORED"
        code = 2
        ended = datetime.now()
        delta_str = str(ended - st).split(".")[0]
        try:
            with open(log_path, "a", encoding="utf-8") as f_out:
                f_out.write(f"\n[FATAL] Job supervisor exception: {e}\n")
                f_out.flush()
        except OSError:
            pass  # log file itself is unwritable; the exit code (2) still records the failure

    # Save final outcome
    with registry_transaction() as registry:
        if job_name in registry:
            curr = registry[job_name].get("status")
            if curr != "STOPPED":
                registry[job_name]["status"] = status
            registry[job_name]["ended_at"] = ended.isoformat()
            registry[job_name]["runtime"] = delta_str
            registry[job_name]["exit_code"] = code

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
        sys.executable, str(RUNNER_SCRIPT),
        "_runner", job_name, json.dumps(cmd_args), str(log_path)
    ]

    # The supervisor leads its own session/process group, so `stop` can terminate it together with
    # every child (psql, aws, gzip) it spawned.
    proc = subprocess.Popen(
        runner_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        cwd=str(BASE_DIR),
        env=env,
        **detached_popen_kwargs()
    )

    pid = proc.pid
    with registry_transaction() as registry:
        registry[job_name] = {
            "pid": pid,
            "name": job_name,
            "command": " ".join(cmd_args),
            "status": "RUNNING",
            "started_at": datetime.now().isoformat(),
            "log_file": str(log_path)
        }

    # Maintain latest log pointer
    make_symlink(BASE_DIR / "sweep_latest.log", log_path)

    print(f"\n[+] Background job '{job_name}' started successfully.")
    print(f"    PID      : {pid}")
    print(f"    Log File : {log_path}")
    print("    To view  : python3 sweep.py status")
    print("    To tail  : python3 sweep.py logs -f")
    print(f"    To stop  : python3 sweep.py stop --name {job_name}\n")
    return pid


def list_jobs():
    """Lists all registered background jobs with frozen runtimes and explicit statuses."""
    registry = _load_jobs_registry()
    results = []
    dirty = False

    for name, info in list(registry.items()):
        pid = info.get("pid", 0)
        # A registered PID only counts as 'alive' if it is still our supervisor (guards against PID re-use)
        alive = pid_matches(pid, RUNNER_MARKER, unknown_ok=True)
        started = info.get("started_at", datetime.now().isoformat())

        try:
            st = datetime.fromisoformat(started)
        except PARSE_ERRORS:
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
                    except PARSE_ERRORS:
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
        # Merge only the fields we derived into the live registry (it may have changed meanwhile).
        with registry_transaction() as live:
            for name, info in registry.items():
                if name in live and live[name].get("status") in ("RUNNING", "FINISHED/STOPPED", None):
                    live[name].update({k: info[k] for k in ("status", "ended_at", "runtime") if k in info})

    return results


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
            except PARSE_ERRORS:
                try:
                    from dateutil import parser as dt_parser
                    st_dt = dt_parser.parse(str(started))
                    date_str = st_dt.strftime("%b-%d")
                except (ImportError, ValueError, OverflowError, TypeError):
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
    """
    Stops one or all running background migration jobs and freezes their runtime.

    * Only PIDs whose command line still identifies the SWEEP supervisor are signalled, so a stale
      registry entry whose PID was re-used by an unrelated process is never killed.
    * The whole process group is terminated (supervisor + psql/aws/gzip children).
    * `kill_pg_backends` ends only database sessions opened by SWEEP (application_name 'sweep'),
      never other users' or applications' sessions.
    """
    stopped = 0
    now_iso = datetime.now().isoformat()

    with registry_transaction() as registry:
        for name, info in list(registry.items()):
            if job_name and name.lower() != job_name.lower():
                continue
            pid = info.get("pid", 0)
            try:
                st = datetime.fromisoformat(info.get("started_at"))
                delta = str(datetime.now() - st).split(".")[0]
            except (TypeError, ValueError):
                delta = "N/A"

            if pid_matches(pid, RUNNER_MARKER):
                print(f"Stopping job '{name}' (PID {pid}) and its child processes...")
                if terminate_tree(pid):
                    stopped += 1
                    print(f"Job '{name}' stopped.")
                else:
                    print(f"Failed to stop PID {pid}; it is still running.")
                    continue
            elif pid_alive(pid):
                print(f"Job '{name}': PID {pid} is running but is not a SWEEP supervisor "
                      f"(stale registry entry / re-used PID). Not signalling it.")
            else:
                print(f"Job '{name}' (PID {pid}) is already terminated.")

            if info.get("status") in (None, "RUNNING", "STOPPED"):
                info["status"] = "STOPPED"
                info["ended_at"] = now_iso
                info["runtime"] = delta

    if kill_pg_backends:
        print("\nTerminating in-flight PostgreSQL sessions opened by SWEEP...")
        try:
            sql = ("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                   f"WHERE application_name = '{PSQL_APP_NAME}' AND pid <> pg_backend_pid();")
            out = run_psql(sql, tuples_only=True, unaligned=True)
            print(f"Terminated {out.strip() or 0} SWEEP database session(s).")
        except RuntimeError as e:
            print(f"Error terminating database backends: {e}")

    return stopped
