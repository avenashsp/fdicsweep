#!/usr/bin/env python3
"""
sweep_s3meta.py - S3 object metadata (LastModified, ETag, size) used to detect new or re-delivered files.

A file is only "already loaded" when sweep_ctl.load_log holds a SUCCESS row for its URI AND the stored
s3_last_modified, s3_etag and s3_size all equal what S3 reports now. Same name + same path with different
metadata means the file was re-delivered and is loaded again.

Values are kept as plain text in a canonical form so they compare exactly:
  last modified  -> UTC ISO 8601, whole seconds        e.g. 2026-11-19T10:11:12Z
  etag           -> the ETag without its quotes
  size           -> integer bytes
"""
import json
import re
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from sweep_common import AWS_ERRORS, AWS_REGION, get_logger

logger = get_logger("sweep_s3meta", "sweep_s3meta")
HEAD_THREADS = int(os.getenv("SWEEP_HEAD_THREADS", "16"))


def canon_time(value):
    """datetime / ISO string / epoch -> 'YYYY-MM-DDTHH:MM:SSZ' (UTC). '' when unknown."""
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace(" ", "T", 1) if " " in str(value).strip()[:11] else str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        elif re.search(r"\d:\d{2}(\.\d+)?[+-]\d{2}$", text):
            text += ":00"                       # '+00' -> '+00:00' (fromisoformat on Python 3.9)
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return str(value).strip()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canon_etag(value):
    return str(value or "").strip().strip('"')


def canon_size(value):
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return ""


def make_meta(last_modified, etag, size):
    return {"last_modified": canon_time(last_modified), "etag": canon_etag(etag), "size": canon_size(size)}


def meta_from_row(row, prefix="s3_"):
    """Reads canonical metadata from a dict with s3_last_modified / s3_etag / s3_size (or size_bytes)."""
    size = row.get(prefix + "size")
    if size in (None, ""):
        size = row.get("size_bytes")
    return make_meta(row.get(prefix + "last_modified"), row.get(prefix + "etag"), size)


def has_meta(meta):
    return bool(meta and meta.get("last_modified") and meta.get("etag") and meta.get("size"))


def meta_equal(a, b):
    """True only when LastModified, ETag and size are all present on both sides and equal."""
    return has_meta(a) and has_meta(b) and a["last_modified"] == b["last_modified"] \
        and a["etag"] == b["etag"] and a["size"] == b["size"]


def describe_difference(stored, current):
    diffs = []
    for key, label in (("last_modified", "LastModified"), ("etag", "ETag"), ("size", "size")):
        if stored.get(key) != current.get(key):
            diffs.append(f"{label} {stored.get(key) or '-'} -> {current.get(key) or '-'}")
    return ", ".join(diffs)


def classify_loaded(stored_list, current):
    """
    ('load' | 'skip' | 'reload', reason) for a file given the S3 metadata stored with its SUCCESS load_log row(s)
    (`stored_list`, empty = never loaded) and the metadata S3 reports now (`current`).
    A file is already loaded only when every stored row equals LastModified + ETag + size.
    """
    if not stored_list:
        return "load", "not loaded yet"
    if any(not any(m.values()) for m in stored_list):
        return "skip", "loaded by an earlier version without S3 metadata (use --reload to re-verify)"
    if not has_meta(current):
        return "skip", "S3 metadata unavailable; cannot compare, treated as loaded"
    changed = [m for m in stored_list if not meta_equal(m, current)]
    if not changed:
        return "skip", "unchanged since the last load"
    return "reload", "S3 object changed: " + describe_difference(changed[0], current)


def split_s3_uri(uri):
    rest = uri[5:]
    bucket, _, key = rest.partition("/")
    return bucket, key


def head_object(uri, region=AWS_REGION):
    """
    Current metadata of one object (local paths are supported for tests and dry runs).
    Raises RuntimeError when the object cannot be inspected.
    """
    uri = str(uri)
    if not uri.startswith("s3://"):
        try:
            st = Path(uri).stat()
        except OSError as e:
            raise RuntimeError(f"Cannot inspect {uri}: {e}")
        return make_meta(st.st_mtime, f"local-{st.st_size}-{int(st.st_mtime)}", st.st_size)

    bucket, key = split_s3_uri(uri)
    try:
        import boto3
        resp = boto3.client("s3", region_name=region).head_object(Bucket=bucket, Key=key)
        return make_meta(resp.get("LastModified"), resp.get("ETag"), resp.get("ContentLength"))
    except AWS_ERRORS as e:
        logger.debug(f"boto3 head_object failed for {uri}: {e}; trying the AWS CLI")
    try:
        res = subprocess.run(["aws", "s3api", "head-object", "--bucket", bucket, "--key", key,
                              "--region", region, "--output", "json"], capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.SubprocessError) as e:
        raise RuntimeError(f"Cannot inspect {uri}: {e}")
    if res.returncode != 0:
        raise RuntimeError(f"Cannot inspect {uri}: {(res.stderr or res.stdout).strip()[:300]}")
    data = json.loads(res.stdout)
    return make_meta(data.get("LastModified"), data.get("ETag"), data.get("ContentLength"))


def head_many(uris, region=AWS_REGION, threads=HEAD_THREADS):
    """{uri: meta or None}. Failures give None (the caller decides what to do) and are logged once."""
    uris = list(dict.fromkeys(uris))
    out = {}

    def one(u):
        try:
            return u, head_object(u, region)
        except RuntimeError as e:
            logger.warning(str(e))
            return u, None

    with ThreadPoolExecutor(max_workers=max(1, min(threads, len(uris) or 1))) as ex:
        for u, m in ex.map(one, uris):
            out[u] = m
    return out


def meta_sql_values(meta):
    """(last_modified, etag, size) as SQL literals for load_log (NULL when unknown)."""
    from sweep_common import lit
    if not meta:
        return "NULL", "NULL", "NULL"
    return (lit(meta["last_modified"]) if meta["last_modified"] else "NULL",
            lit(meta["etag"]) if meta["etag"] else "NULL",
            meta["size"] or "NULL")
