#!/usr/bin/env python3
"""
sweep_format.py - Duration, number and size formatting helpers shared by status/monitor output.
"""
import re
from sweep_common import PARSE_ERRORS


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
    except PARSE_ERRORS:
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
    except PARSE_ERRORS:
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
    except PARSE_ERRORS:
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
    except PARSE_ERRORS:
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
