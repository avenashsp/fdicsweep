#!/usr/bin/env python3
"""
sweep_streaming.py - Safe helpers around sweep_stream.sh (S3/gzip -> stdout).

Never builds shell command lines from untrusted text (S3 keys may contain spaces, quotes,
semicolons, `$()` ...). Python callers pass an argument list; the one place a command *string*
is unavoidable (psql `\\copy ... FROM PROGRAM '<cmd>'`) goes through shlex.quote.
"""
import shlex
import subprocess
import tempfile
import threading

from sweep_common import STREAM_SCRIPT, AWS_REGION
from sweep_platform import group_popen_kwargs, kill_process_group

DEFAULT_STREAM_TIMEOUT = 300  # seconds for header / sample peeks


def validate_uri(uri):
    """Rejects empty URIs and URIs carrying control characters (newlines would break psql \\copy)."""
    s = str(uri)
    if not s.strip():
        raise ValueError("Empty source URI")
    if any(ord(c) < 32 or ord(c) == 127 for c in s):
        raise ValueError(f"Source URI contains control characters: {s!r}")
    return s


def stream_argv(uri, region=AWS_REGION):
    """Argument list that streams `uri` (decompressed) to stdout."""
    return ["bash", str(STREAM_SCRIPT), validate_uri(uri), str(region)]


def stream_program_literal(uri, region=AWS_REGION):
    """
    Shell command string for psql `\\copy ... FROM PROGRAM '<this>'`, already quoted for the shell
    (shlex.quote) and with single quotes doubled for psql's own quoting. The text still contains
    `sweep_stream.sh`, which the dashboard uses to recognise worker streams.
    """
    cmd = shlex.join(stream_argv(uri, region))
    return cmd.replace("'", "''")


def read_stream_head(uri, region=AWS_REGION, max_lines=1, timeout=DEFAULT_STREAM_TIMEOUT, encoding="utf-8"):
    """
    Reads up to `max_lines` text lines from the stream.
    Returns (lines, error_message). `error_message` is '' on success and is populated from the
    stream's stderr / exit code when no data could be read (S3 auth error, missing key, bad gzip ...).
    """
    lines = []
    with tempfile.TemporaryFile() as errf:
        try:
            proc = subprocess.Popen(stream_argv(uri, region), stdout=subprocess.PIPE, stderr=errf,
                                    **group_popen_kwargs())
        except (OSError, ValueError) as e:
            return [], str(e)

        timed_out = threading.Event()

        def _expire():
            timed_out.set()
            kill_process_group(proc)

        timer = threading.Timer(timeout, _expire)
        timer.start()
        try:
            while len(lines) < max_lines:
                raw = proc.stdout.readline()
                if not raw:
                    break
                lines.append(raw.decode(encoding, errors="replace").rstrip("\r\n"))
        finally:
            timer.cancel()
            stopped_early = len(lines) >= max_lines
            if proc.poll() is None:
                kill_process_group(proc)
            try:
                proc.stdout.close()
            except OSError:
                pass
            rc = proc.wait()

        errf.seek(0)
        stderr_txt = errf.read().decode("utf-8", errors="replace").strip()

    if timed_out.is_set():
        return lines, f"Timed out after {timeout}s reading {uri}"
    if not lines:
        if rc != 0 or stderr_txt:
            return [], stderr_txt or f"Stream exited with code {rc}"
        return [], "Empty stream output"
    if not stopped_early and rc not in (0, None) and stderr_txt:
        # Short file fully read but the pipeline reported a failure (e.g. truncated gzip)
        return lines, stderr_txt
    return lines, ""
