#!/usr/bin/env python3
"""
sweep_platform.py - All OS-specific behaviour for SWEEP in one place.

Everything that differs between POSIX and Windows (process detachment, process-tree
termination, PID liveness, file locking, ANSI enablement, sharing-flag file reads) lives
here so the rest of the code base stays platform-neutral.
"""
import contextlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

IS_WINDOWS = os.name == "nt"


# -----------------------------------------------------------------------------
# Process helpers
# -----------------------------------------------------------------------------
def detached_popen_kwargs():
    """Popen keyword arguments that fully detach a child from the parent's session/console."""
    if IS_WINDOWS:
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
        return {"creationflags": flags}
    return {"start_new_session": True}


def group_popen_kwargs():
    """Popen keyword arguments that place the child in its own process group (POSIX only)."""
    return {} if IS_WINDOWS else {"start_new_session": True}


def pid_alive(pid):
    """True if a process with this PID is currently running."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    if IS_WINDOWS:
        res = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
        return str(pid) in res.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def pid_cmdline(pid):
    """Best-effort command line of a PID ('' when it cannot be determined)."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return ""
    if IS_WINDOWS:
        try:
            res = subprocess.run(
                ["wmic", "process", "where", f"ProcessId={pid}", "get", "CommandLine", "/value"],
                capture_output=True, text=True, timeout=10)
            return res.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return ""
    proc_file = Path(f"/proc/{pid}/cmdline")
    try:
        if proc_file.exists():
            return proc_file.read_bytes().replace(b"\x00", b" ").decode("utf-8", "replace").strip()
    except OSError:
        pass
    try:
        res = subprocess.run(["ps", "-p", str(pid), "-o", "args="], capture_output=True, text=True, timeout=10)
        return res.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def pid_matches(pid, marker, unknown_ok=False):
    """
    True when PID is alive AND its command line contains `marker`.
    Guards against signalling (or trusting) an unrelated process that re-used a stale PID.
    `unknown_ok` decides the answer when the command line cannot be read at all.
    """
    if not pid_alive(pid):
        return False
    cmd = pid_cmdline(pid)
    if not cmd:
        return unknown_ok
    return marker in cmd


def terminate_tree(pid, grace_seconds=3.0):
    """
    Terminates a process and all of its descendants.
    POSIX: SIGTERM the whole process group, wait, then SIGKILL stragglers.
    Windows: taskkill /T /F.
    Returns True if the process is gone afterwards.
    """
    pid = int(pid)
    if IS_WINDOWS:
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True)
        return not pid_alive(pid)

    try:
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        return True
    # Only signal the group when the target leads it (we start jobs with start_new_session=True);
    # otherwise fall back to the single PID so we never signal our own group.
    use_group = pgid == pid and pgid != os.getpgrp()

    def _send(sig):
        try:
            if use_group:
                os.killpg(pgid, sig)
            else:
                os.kill(pid, sig)
        except ProcessLookupError:
            pass

    _send(signal.SIGTERM)
    deadline = time.time() + grace_seconds
    while time.time() < deadline and pid_alive(pid):
        time.sleep(0.1)
    if pid_alive(pid):
        _send(signal.SIGKILL)
        time.sleep(0.2)
    return not pid_alive(pid)


def kill_process_group(proc):
    """Hard-kills a subprocess.Popen object (and its group on POSIX). Never raises."""
    try:
        if IS_WINDOWS:
            proc.kill()
        else:
            os.killpg(proc.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        try:
            proc.kill()
        except OSError:
            pass


def enable_ansi():
    """Enables ANSI escape processing on Windows consoles (no-op elsewhere)."""
    if IS_WINDOWS:
        os.system("")


def make_symlink(link, target):
    """Creates/refreshes a symlink on POSIX. Silently skipped on Windows. Never raises."""
    if IS_WINDOWS:
        return
    try:
        link = Path(link)
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(target)
    except OSError:
        pass


# -----------------------------------------------------------------------------
# File helpers
# -----------------------------------------------------------------------------
def read_file_bytes_shared(path):
    """
    Reads all bytes of a file. On Windows uses full sharing flags so files that are open in
    Excel can still be read.
    """
    p = str(Path(path).resolve())
    if sys.platform == "win32":
        import ctypes
        import msvcrt

        GENERIC_READ = 0x80000000
        SHARE_ALL = 0x1 | 0x2 | 0x4
        OPEN_EXISTING = 3
        FILE_ATTRIBUTE_NORMAL = 0x80
        kernel32 = ctypes.windll.kernel32
        h = kernel32.CreateFileW(p, GENERIC_READ, SHARE_ALL, None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None)
        if h not in (-1, 0xFFFFFFFFFFFFFFFF, 0):
            try:
                fd = msvcrt.open_osfhandle(h, 0)
                with open(fd, "rb", closefd=True) as fh:
                    return fh.read()
            except OSError:
                kernel32.CloseHandle(h)
    with open(p, "rb") as fh:
        return fh.read()


@contextlib.contextmanager
def file_lock(path, timeout=30.0):
    """
    Cross-process advisory lock on `<path>.lock`.
    Raises TimeoutError if the lock cannot be acquired within `timeout` seconds.
    """
    lock_path = Path(str(path) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+")
    deadline = time.time() + timeout
    try:
        while True:
            try:
                if IS_WINDOWS:
                    import msvcrt
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.time() >= deadline:
                    raise TimeoutError(f"Could not acquire lock {lock_path} within {timeout}s")
                time.sleep(0.05)
        yield
    finally:
        try:
            if IS_WINDOWS:
                import msvcrt
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        fh.close()


def atomic_write_text(path, text, encoding="utf-8"):
    """Writes text to `path` atomically (temp file in the same directory + os.replace)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding=encoding) as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def atomic_write_json(path, obj, indent=2):
    atomic_write_text(path, json.dumps(obj, indent=indent))


def read_json_or(path, default):
    """Reads JSON from `path`; returns `default` if missing or unreadable/corrupt."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default
