#!/usr/bin/env python3
"""
sweep_bg.py - Background process management and live monitoring (facade + supervisor entry point).

The implementation lives in focused modules:
  sweep_jobs.py       job registry, detached supervision, status, logs, stop
  sweep_format.py     duration / number / size formatting
  sweep_metrics.py    throughput and progress metrics
  sweep_dashboard.py  live terminal dashboard

This module re-exports their public names (so `import sweep_bg` keeps working) and is also the
script the detached supervisor runs:  python sweep_bg.py _runner <job> <cmd-json> <log-file>
"""
import sys

from sweep_dashboard import monitor_live, render_dashboard
from sweep_format import (
    format_approx_number, format_runtime_display, format_size_actual, format_time_duration,
    format_word_number, parse_duration_to_seconds,
)
from sweep_jobs import (
    JOBS_METADATA_FILE, _runner_main, is_pid_alive, list_jobs, print_status, registry_transaction,
    start_background_job, stop_jobs, tail_logs,
)
from sweep_metrics import get_active_workers, get_top_completed, get_track_metrics

__all__ = [
    "JOBS_METADATA_FILE", "_runner_main", "format_approx_number", "format_runtime_display", "format_size_actual",
    "format_time_duration", "format_word_number", "get_active_workers", "get_top_completed", "get_track_metrics",
    "is_pid_alive", "list_jobs", "monitor_live", "parse_duration_to_seconds", "print_status", "registry_transaction",
    "render_dashboard", "start_background_job", "stop_jobs", "tail_logs",
]

if __name__ == "__main__":
    if len(sys.argv) >= 5 and sys.argv[1] == "_runner":
        _runner_main(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        sys.stderr.write("sweep_bg.py is the background supervisor; use `python3 sweep.py status|monitor|logs|stop`.\n")
        sys.exit(2)
