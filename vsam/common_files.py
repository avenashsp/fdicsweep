#!/usr/bin/env python3
"""
vsam/common_files.py - File/S3 access, delimiter and header detection, CSV reading, JSON helpers, version selection.
"""
import csv
import io
import json
import os
import re
import subprocess
import time
import types
from datetime import date
from pathlib import Path
import pandas as pd
from sweep_platform import group_popen_kwargs, kill_process_group
from sweep_streaming import stream_argv
from sweep_common import AWS_REGION, FILE_READ_ERRORS, lit, norm_col
from vsam.common_config import COPYBOOK_DIR, DELIM, IDENT, OUT, logger
from vsam.common_copybook import dedupe
from vsam.common_records import filter_blank_rows


def extract_version_key(filename: str) -> str:
    """
    Extracts a sortable canonical version string from a filename.
    Handles:
      - 14-digit timestamps: YYYYMMDDHHMMSS -> 20260909120000
      - 8-digit dates:
          - MMDDYYYY (e.g. 09092026 where year in 1980..2045) -> 20260909
          - YYYYMMDD (e.g. 20260909) -> 20260909
      - 6-digit dates: YYMMDD / MMDDYY
    Returns sortable string e.g. '20260909', or filename as fallback.
    """
    if not filename:
        return ""
    m14 = re.search(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", filename)
    if m14:
        return "".join(m14.groups())

    m8 = re.search(r"(\d{8})", filename)
    if m8:
        raw = m8.group(1)
        valid_years = {str(y) for y in range(1980, 2045)}
        if raw[:4] in valid_years:
            return raw
        if raw[4:8] in valid_years:
            mm, dd, yyyy = raw[:2], raw[2:4], raw[4:8]
            return f"{yyyy}{mm}{dd}"
        return raw

    m6 = re.search(r"(\d{6})", filename)
    if m6:
        return m6.group(1)

    return ""


def filter_latest_versions(df: pd.DataFrame, group_cols=None) -> pd.DataFrame:
    """
    Filters a DataFrame of files (e.g. load_matrix or inventory) to keep ONLY
    the latest file version for each logical file.
    CRITICAL: Preserves ALL target schemas mapped to that latest file!
    """
    if df.empty:
        return df

    src_fn_col = "s3_filename" if "s3_filename" in df.columns else ("filename" if "filename" in df.columns else "full_path")
    if src_fn_col not in df.columns:
        return df

    if group_cols is None:
        if "source_company" in df.columns and "source_filename" in df.columns:
            group_cols = ["source_company", "source_filename"]
        elif "company_token" in df.columns and "source_filename" in df.columns:
            group_cols = ["company_token", "source_filename"]
        elif "target_schema" in df.columns and "target_table" in df.columns:
            group_cols = ["target_schema", "target_table"]
        else:
            return df

    # Logical file group: company + source_filename (exclude target_schema/broker so multi-schema rows aren't dropped)
    logical_group = [c for c in group_cols if c not in ("target_schema", "broker", "broker_name")]
    if not logical_group:
        logical_group = group_cols

    df_copy = df.copy()
    df_copy["_sort_ver"] = df_copy[src_fn_col].apply(extract_version_key)
    
    # Identify latest file identifier for each logical file
    idx_latest = df_copy.groupby(logical_group)["_sort_ver"].idxmax()
    latest_files = set(df_copy.loc[idx_latest, src_fn_col].dropna().unique())
    
    # Keep ALL rows belonging to these latest files (preserving all mapped schemas)
    res = df[df[src_fn_col].isin(latest_files)].copy()
    if "target_schema" in group_cols:
        res = res.drop_duplicates(subset=group_cols)
    return res


def make_columns_unique(cols):
    """
    Ensures every column name in cols is unique and cleanly normalized.
    Takes exact column names from the file header.
    If duplicate column names exist, appends an incrementing numeric suffix (e.g. col, col_1, col_2).
    """
    seen = {}
    out = []
    for c in cols:
        norm_c = norm_col(str(c).strip())
        if not norm_c:
            norm_c = "unnamed"
        if norm_c not in seen:
            seen[norm_c] = 0
            out.append(norm_c)
        else:
            seen[norm_c] += 1
            out.append(f"{norm_c}_{seen[norm_c]}")
    return out


def find_copybook_files(cb, src):
    if not cb or not COPYBOOK_DIR.exists():
        return []
    hits = [p for p in COPYBOOK_DIR.iterdir() if p.is_file() and p.name.upper().startswith(cb)]
    if len(hits) > 1:
        narrowed = [p for p in hits if src in p.name.upper()] or hits
        if len(narrowed) > 1 and src[-1:].isdigit():
            narrowed = [p for p in narrowed if f"TYPE{src[-1]}" in p.name.upper()] or narrowed
        hits = narrowed
    return sorted(hits)


# -----------------------------------------------------------------------------
# S3 Streaming & CSV Reading
# -----------------------------------------------------------------------------
class S3Text(io.TextIOWrapper):
    """Text stream of the decompressed S3/local object, produced by sweep_stream.sh (no shell, own process group)."""
    def __init__(self, uri):
        self._proc = subprocess.Popen(
            stream_argv(uri, AWS_REGION),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            **group_popen_kwargs()
        )
        super().__init__(self._proc.stdout, encoding="utf-8-sig", errors="replace", newline="")

    def close(self):
        p = getattr(self, "_proc", None)
        if p is not None and p.poll() is None:
            kill_process_group(p)
        super().close()
        if p is not None:
            p.wait()


class S3File:
    """Mock file object for S3 objects: name, exists(), stat().st_size / st_mtime."""
    def __init__(self, uri, size=0, mtime=0.0):
        self.uri, self.size, self.mtime = str(uri), int(float(size or 0)), float(mtime or 0)
        self.name = self.uri.rsplit("/", 1)[-1]

    def __str__(self):
        return self.uri

    def exists(self):
        return True

    def stat(self):
        return types.SimpleNamespace(st_size=self.size, st_mtime=self.mtime, st_mtime_ns=int(self.mtime * 1e9))


_S3_META = {}


def as_file(p):
    """
    Returns an S3File mock object if p is an S3 URI, or a pathlib.Path if p is a local file.
    Caches S3 size and mtime metadata from outputs/load_matrix.csv, inventory.csv, or s3 listings.
    """
    if isinstance(p, S3File):
        return p
    sp = str(p)
    if sp.startswith("s3://"):
        if not _S3_META:
            for cand in (OUT / "vsam_02_load_matrix.csv", OUT / "vsam_01_load_matrix.csv", OUT / "load_matrix.csv", OUT / "vsam_02_inventory.csv", OUT / "vsam_01_inventory.csv", OUT / "inventory.csv", OUT / "s3" / "vsam_s3_listing.csv"):
                if cand.exists():
                    try:
                        with open(cand, newline="", encoding="utf-8") as f:
                            for r in csv.DictReader(f):
                                uri = r.get("full_path") or r.get("s3_uri")
                                if uri and uri.startswith("s3://"):
                                    size = r.get("size")
                                    if size is None and "size_mb" in r:
                                        try:
                                            size = int(float(r["size_mb"]) * 1024 * 1024)
                                        except (ValueError, TypeError):
                                            size = 0
                                    mtime = r.get("mtime", 0.0)
                                    _S3_META[uri] = (int(size or 0), float(mtime or 0.0))
                    except (OSError, ValueError, csv.Error) as exc:
                        logger.debug(f"Skipping unreadable size map {cand}: {exc}")
        size, mtime = _S3_META.get(sp, (0, 0.0))
        return S3File(sp, size=size, mtime=mtime)
    return Path(p)


_PROG_T0 = {}


def _atomic_json(name, obj):
    OUT.mkdir(parents=True, exist_ok=True)
    tmp = OUT / (name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, default=str), encoding="utf-8")
    os.replace(tmp, OUT / name)


def set_progress(stage, done, total, current=""):
    t0 = _PROG_T0.setdefault(stage, time.time())
    el = time.time() - t0
    eta = int(el / done * (total - done)) if done else None
    try:
        _atomic_json("progress.json", {
            "stage": stage, "done": done, "total": total, "current": current,
            "started": t0, "updated": time.time(), "eta_seconds": eta
        })
    except OSError:
        pass


def _opener(path):
    import gzip
    if str(path).startswith("s3://"):
        return S3Text(str(path))
    if str(path).lower().endswith(".gz"):
        return gzip.open(path, "rt", newline="", encoding="utf-8-sig", errors="replace")
    return open(path, newline="", encoding="utf-8-sig", errors="replace")


def head_lines(path, n=6):
    out = []
    with _opener(path) as f:
        for _ in range(n):
            ln = f.readline()
            if not ln:
                break
            out.append(ln.rstrip("\r\n"))
    return out


_DELIM_CACHE = {}


def detect_delim(path):
    best, best_key = ",", (0, 0)
    lines = [x for x in head_lines(path, 6) if x.strip()]
    for d in (",", "|", "\t", ";", "~", "^"):
        counts = [len(next(csv.reader([ln], delimiter=d), [])) for ln in lines]
        if not counts or counts[0] < 2:
            continue
        frac = sum(1 for x in counts if x == counts[0]) / len(counts)
        if frac >= 0.8 and (frac, counts[0]) > best_key:
            best, best_key = d, (frac, counts[0])
    return best


def delim_for(path):
    if DELIM != "auto":
        return DELIM
    p = str(path)
    if p.startswith("s3://") and os.getenv("VSAM_S3_ASSUME_DEFAULTS") == "1":
        return ","
    if p not in _DELIM_CACHE:
        _DELIM_CACHE[p] = detect_delim(p)
    return _DELIM_CACHE[p]


def pg_delim(d):
    return "E'\\t'" if d == "\t" else lit(d)


QUOTE_MODE = os.getenv("VSAM_QUOTE", "auto")


_QUOTE_CACHE = {}


def file_quoted(path):
    if QUOTE_MODE != "auto":
        return QUOTE_MODE == "quoted"
    p = str(path)
    if p.startswith("s3://") and os.getenv("VSAM_S3_ASSUME_DEFAULTS") == "1":
        return True
    if p not in _QUOTE_CACHE:
        lines = [x for x in head_lines(p, 3) if x.strip()]
        parts = lines[0].split(delim_for(p)) if lines else []
        _QUOTE_CACHE[p] = bool(parts) and sum(1 for x in parts if x.startswith('"')) >= max(1, len(parts) // 2)
    return _QUOTE_CACHE[p]


def pg_quote(path):
    return "" if (delim_for(path) == "," or file_quoted(path)) else ", QUOTE E'\\x01'"


def has_trailing_delim(path):
    if str(path).startswith("s3://") and os.getenv("VSAM_S3_ASSUME_DEFAULTS") == "1":
        return False
    d = delim_for(path)
    lines = [x for x in head_lines(path, 6) if x.strip()]
    return bool(lines) and all(x.rstrip().endswith(d) for x in lines)


def first_row(path):
    d = delim_for(path)
    with _opener(path) as f:
        row = next(csv.reader(f, delimiter=d, quoting=csv.QUOTE_MINIMAL if (d == "," or file_quoted(path)) else csv.QUOTE_NONE), [])
    if row and row[-1] == "" and has_trailing_delim(path):
        row = row[:-1]
    return row


HEADER_MODE = {}


def resolve_columns(path, fields, folder):
    first = first_row(path)
    if not first:
        return None, False, "EMPTY_FILE"
    names = dedupe([norm_col(x) for x in first])
    cb_all = dedupe([norm_col(f["name"]) for f in fields])
    cb = dedupe([norm_col(f["name"]) for f in fields if not f.get("filler")])
    mode = HEADER_MODE.get(folder.upper(), "auto")
    ident = all(IDENT.match(x.strip()) for x in first)
    match = len(set(names) & set(cb))
    if mode == "yes":
        hdr = True
    elif mode == "no":
        hdr = False
    elif cb:
        hdr = match >= max(1, len(names) // 2) or (match == 0 and ident)
    else:
        hdr = ident
    if hdr:
        return names, True, None
    if not cb:
        return None, False, "NO_COPYBOOK_NO_HEADER"
    if len(cb) == len(first):
        return cb, False, None
    if len(cb_all) == len(first):
        return cb_all, False, None
    return None, False, f"COLUMN_COUNT_MISMATCH(copybook={len(cb)},with_filler={len(cb_all)},file={len(first)})"


def read_csv(path, cols, hdr, usecols=None, chunksize=None, nrows=None):
    import pandas as pd
    d = delim_for(path)
    extra = {} if (d == "," or file_quoted(path)) else {"quoting": csv.QUOTE_NONE}
    if str(path).startswith("s3://"):
        st = S3Text(str(path))
        try:
            rd = pd.read_csv(st, sep=d, dtype=str, keep_default_na=False, header=0 if hdr else None, names=cols, usecols=usecols,
                             index_col=False, chunksize=chunksize, nrows=nrows, **extra)
        except BaseException:
            st.close()
            raise
        if chunksize is None:
            st.close()
            return filter_blank_rows(rd)

        def _chunk_gen():
            try:
                for chunk in rd:
                    yield filter_blank_rows(chunk)
            finally:
                st.close()
        return _chunk_gen()
    df = pd.read_csv(path, sep=d, dtype=str, keep_default_na=False,
                     header=0 if hdr else None, names=cols, usecols=usecols, index_col=False,
                     encoding="utf-8", encoding_errors="replace",
                     chunksize=chunksize, nrows=nrows, **extra)
    if chunksize is None:
        return filter_blank_rows(df)
    return (filter_blank_rows(c) for c in df)


def load_date_of(path, vdate):
    if vdate:
        return vdate if isinstance(vdate, date) else date.fromisoformat(str(vdate))
    if str(path).startswith("s3://"):
        return date.today()
    return date.fromtimestamp(Path(path).stat().st_mtime)


def load_csv_safe(path, default_cols=None):
    """Safely reads a CSV file, returning an empty DataFrame with default columns if empty or missing."""
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        import pandas as pd
        return pd.DataFrame(columns=default_cols or [])
    try:
        import pandas as pd
        return pd.read_csv(p, dtype=str).fillna("")
    except FILE_READ_ERRORS:
        import pandas as pd
        return pd.DataFrame(columns=default_cols or [])


def read_json(name, default=None):
    """Read a JSON file from OUT dir.

    ``name`` can be:
      - a bare filename str  (e.g. ``"vsam_03_type_gate.json"``) → resolved under OUT
      - a full ``Path`` object → used as-is; also falls back to OUT/<basename>

    If the resolved file does not exist and ``default`` is provided, return ``default``.
    If ``default`` is None and the file does not exist, raise FileNotFoundError.
    """
    # Accept full Path objects as well as bare filenames
    if isinstance(name, Path) and name.is_absolute():
        target = name
        # Also try OUT/<basename> if the absolute path doesn't exist
        if not target.exists():
            target = OUT / name.name
    else:
        target = OUT / name

    if not target.exists():
        # Try common versioned prefixes
        for prefix in ["vsam_02_", "vsam_03_", "vsam_04_"]:
            cand = OUT / f"{prefix}{Path(str(name)).name}"
            if cand.exists():
                target = cand
                break

    if not target.exists():
        if default is not None:
            return default
        raise FileNotFoundError(f"read_json: file not found: {target}")

    return json.loads(target.read_text(encoding="utf-8"))


def write_json(name, obj):
    (OUT / name).write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
