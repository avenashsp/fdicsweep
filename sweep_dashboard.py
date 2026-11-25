#!/usr/bin/env python3
"""
sweep_dashboard.py - Terminal rendering of the live ingestion dashboard.
"""
import os
import sys
import time
from datetime import datetime
from sweep_platform import enable_ansi
from sweep_format import format_approx_number, format_size_actual, format_time_duration, format_word_number
from sweep_jobs import list_jobs
from sweep_metrics import get_active_workers, get_top_completed, get_track_metrics


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


def layer_info_lines(layer):
    """Header lines of `monitor --layer L`: layer + database, the last promote into the layer (compact) and the pending FE files."""
    from sweep_common import CTL_SCHEMA, clip_error, query_db_dicts
    from sweep_fwd_ddl import FwdError, list_files, pending_files
    from sweep_layers import LayerError, use_layer
    out = [f"LAYER: {layer.name}    DATABASE: {layer.database}"]
    try:
        with use_layer(layer):
            last = query_db_dicts(
                f"SELECT run_id, source_layer, count(*) AS tables, COALESCE(sum(inserted), 0) AS ins, COALESCE(sum(updated), 0) AS upd, "
                f"count(*) FILTER (WHERE status = 'FAILED') AS failed, max(finished_at)::timestamp(0) AS at FROM {CTL_SCHEMA}.promote_log "
                f"WHERE target_layer = {layer.name!r} AND run_id = (SELECT run_id FROM {CTL_SCHEMA}.promote_log WHERE target_layer = {layer.name!r} "
                f"ORDER BY id DESC LIMIT 1) GROUP BY run_id, source_layer", dbname=layer.database)
            if last:
                r = last[0]
                out.append(f"last promote: {r['source_layer']}>{layer.name} {r['at']}  {r['tables']} table(s)  +{r['ins']} inserted  ~{r['upd']} updated  "
                           f"{r['failed']} failed  (run {r['run_id']})")
            else:
                out.append("last promote: none")
            if layer.upstream is None:
                out.append("pending FE files: n/a (FE files are applied in silver, never in bronze)")
                return out
            try:
                pend = pending_files(layer, list_files())
                out.append("pending FE files: " + (f"{len(pend)} ({', '.join(f.name for f in pend[:5])}{' ...' if len(pend) > 5 else ''})" if pend else "0"))
            except FwdError as e:
                out.append(f"pending FE files: unknown ({e})")
    except (RuntimeError, LayerError) as e:
        out.append(f"layer database not readable: {clip_error(e)}")
    return out


def render_dashboard(use_color=True, track="all", layer=None):
    """
    Renders the live operational dataload monitoring dashboard.
    When track='all', clearly displays both ORACLE and VSAM track progress side-by-side,
    along with combined migration totals, active worker streams, and completed tables.
    When track='vsam' or track='oracle', renders the focused single-track dashboard.
    """
    enable_ansi()

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

    if layer is not None:
        for ln in layer_info_lines(layer):
            lines.append(f"{C_WHITE}{ln}{C_RESET}" if use_color else ln)
        lines.append(("=" * W))

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


def monitor_live(interval=3, watch=False, use_color=True, track="all", layer=None):
    """
    Operational dashboard for active data ingestion.
    By default (watch=False), prints the dashboard once without auto refresh.
    If watch=True, continuously refreshes at specified interval.
    """
    if not watch:
        # Non-auto-refresh mode: render snapshot once and exit immediately
        print(render_dashboard(use_color=use_color, track=track, layer=layer))
        sys.stdout.flush()
        return

    # Continuous watch mode
    try:
        while True:
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.write(render_dashboard(use_color=use_color, track=track, layer=layer) + "\n")
            sys.stdout.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
