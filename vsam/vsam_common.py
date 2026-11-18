#!/usr/bin/env python3
"""
=============================================================================
vsam_common.py - Shared Configuration & Helpers for VSAM Migration Pipeline
Integrated with SWEEP core: S3 streaming, copybook parsing, and native audit columns.
=============================================================================
"""
import csv
import functools
import io
import json
import os
import re
import signal
import subprocess
import time
import types
from datetime import date, datetime
from pathlib import Path
import sys
import pandas as pd
import numpy as np

# Ensure parent directory is on sys.path
_ROOT = Path(__file__).resolve().parent.parent
_TRACK = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_TRACK) not in sys.path:
    sys.path.insert(0, str(_TRACK))

from sweep_common import (
    BASE_DIR, VSAM_INPUTS_DIR, VSAM_COPYBOOK_DIR, VSAM_OUTPUTS_DIR,
    AWS_REGION, STREAM_SCRIPT, CTL_SCHEMA, META_SCHEMA,
    get_logger, query_db_rows, run_psql, q, lit, norm_col
)

logger = get_logger("vsam_common", "vsam_common")


def read_file_bytes_safe(path) -> bytes:
    """
    Reads all bytes from a file with full Windows file sharing flags (FILE_SHARE_READ | WRITE | DELETE).
    This allows reading Excel or CSV files even if they are currently open in Microsoft Excel.
    """
    p = str(Path(path).resolve())
    if sys.platform == "win32":
        import ctypes
        import msvcrt

        GENERIC_READ = 0x80000000
        FILE_SHARE_READ = 0x00000001
        FILE_SHARE_WRITE = 0x00000002
        FILE_SHARE_DELETE = 0x00000004
        OPEN_EXISTING = 3
        FILE_ATTRIBUTE_NORMAL = 0x00000080

        kernel32 = ctypes.windll.kernel32
        h = kernel32.CreateFileW(
            p, GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None
        )
        if h != -1 and h != 0xFFFFFFFFFFFFFFFF:
            try:
                fd = msvcrt.open_osfhandle(h, 0)
                with open(fd, "rb", closefd=True) as f:
                    return f.read()
            except Exception:
                kernel32.CloseHandle(h)

    with open(p, "rb") as f:
        return f.read()


def read_excel_safe(path, sheet_name=0, **kwargs):
    """
    Reads an Excel sheet into a pandas DataFrame safely, even if locked by Microsoft Excel.
    """
    raw_bytes = read_file_bytes_safe(path)
    return pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet_name, **kwargs)

BASE = Path(os.getenv("VSAM_BASE", str(BASE_DIR)))
COPYBOOK_DIR = Path(os.getenv("VSAM_COPYBOOK_DIR", str(VSAM_COPYBOOK_DIR)))
MASTER_MAPPING_FILE = Path(os.getenv("VSAM_MASTER_MAPPING", str(VSAM_INPUTS_DIR / "vsam_master_mapping.xlsx")))
MASTER_OVERRIDES_FILE = Path(os.getenv("VSAM_MASTER_OVERRIDES", str(VSAM_INPUTS_DIR / "vsam_master_overrides.xlsx")))
MASTER_REF = MASTER_MAPPING_FILE
OUT = VSAM_OUTPUTS_DIR
VSAM_GEN_DDL_DIR = OUT / "gen_ddl"
VSAM_PRE_LOAD_DIR = VSAM_GEN_DDL_DIR
VSAM_POST_LOAD_DIR = VSAM_GEN_DDL_DIR
CTL = CTL_SCHEMA
META = META_SCHEMA
DELIM = os.getenv("VSAM_DELIM", "auto")
PG_ENCODING = os.getenv("VSAM_ENCODING", "UTF8")
MAX_ROWS = int(os.getenv("VSAM_ANALYZE_MAX_ROWS", "0")) or None
COMMON_SCHEMA_CASE = os.getenv("VSAM_COMMON_SCHEMA_CASE", "lower")
VERSION_MODE = os.getenv("VSAM_VERSION_MODE", "upsert")


def get_vsam_folders(s3_uri=None):
    """
    Dynamically discovers folders / sub-prefixes to scan for VSAM files.
    1. If VSAM_FOLDERS env var is set, uses comma-separated list.
    2. If S3 URI provided, discovers common sub-prefixes under that URI via S3 API.
    3. If local data directory has subdirectories, lists all non-hidden directories.
    4. Defaults to [''] (scans the root prefix directly).
    """
    env_f = os.getenv("VSAM_FOLDERS")
    if env_f:
        return [f.strip() for f in env_f.split(",") if f.strip()]

    folders = []
    if s3_uri and s3_uri.startswith("s3://"):
        try:
            import boto3
            bucket_name = s3_uri[5:].split("/")[0]
            prefix = s3_uri[5:].split("/", 1)[1] if "/" in s3_uri[5:] else ""
            prefix = prefix.rstrip("/") + "/" if prefix else ""
            s3 = boto3.client("s3", region_name=AWS_REGION)
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    fld = cp["Prefix"][len(prefix):].strip("/")
                    if fld:
                        folders.append(fld)
        except Exception:
            pass

    if not folders and BASE.exists():
        folders = [d.name for d in BASE.iterdir() if d.is_dir() and not d.name.startswith((".", "__")) and d.name not in ("logs", "outputs", "reports", "inputs")]

    return folders if folders else [""]

# Dynamically initialize FOLDERS
FOLDERS = get_vsam_folders()


def common_schema_name(name: str) -> str:
    """Normalizes common target schema name based on VSAM_COMMON_SCHEMA_CASE ('lower' or 'upper')."""
    if not name:
        return name
    return name.lower() if COMMON_SCHEMA_CASE == "lower" else name.upper()

FN = re.compile(r"^FDICSWP_([A-Z0-9]+)_(.+?)_FLAT(?:_(\d{8}))?\.csv(?:\.gz)?$", re.I)
IDENT = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]*$")
USAGE = re.compile(r"\b(COMPUTATIONAL-[1-5]|COMPUTATIONAL|COMP-[1-5]|COMP|BINARY|PACKED-DECIMAL)\b", re.I)


def is_key_group_or_field(name: str) -> bool:
    """
    Determines whether a COBOL copybook AST group or elementary field is a primary/composite key.
    Handles:
      - Standalone: KEY, KEM
      - Suffixes: _KEY, -KEY, _KEM, -KEM (e.g. M405_KEY, BLK-FUND-WORK-KEY, M905-KEM, MXS-KEY)
      - Prefixes: KEY_, KEY-, KEM_, KEM- (e.g. KEY-FIELDS, KEY_FIELDS, KEM-FIELDS)
      - Infixes: _KEY_, -KEY-, _KEM_, -KEM- (e.g. ACCT-KEY-PART)
      - Numeric indexed: KEY1, KEY01, KEM1, etc.
    """
    if not name:
        return False
    if re.search(r"(?:^|[-_])(?:KEY\d*|KEYS|KEY-FIELDS|KEY_FIELDS|KEM\d*|PKEY\d*)(?:[-_]|\b|$)", name, re.I):
        return True
    tokens = re.split(r"[-_\s]+", name.strip().upper())
    for t in tokens:
        if t in ("KEY", "KEYS", "PKEY", "PRIMARYKEY", "KEM"):
            return True
        if re.match(r"^(KEY|KEM)\d+$", t):
            return True
    return False


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


def norm_usage(u):
    """COMPUTATIONAL-3 / PACKED-DECIMAL -> COMP-3, COMPUTATIONAL / BINARY -> COMP."""
    u = (u or "").upper().replace("COMPUTATIONAL", "COMP")
    return {"BINARY": "COMP", "PACKED-DECIMAL": "COMP-3"}.get(u, u)


def dedupe(names):
    seen, out = {}, []
    for n in names:
        if n in seen:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            out.append(n)
    return out


def parse_filename(name):
    """FDICSWP_<COMPANY>_<SOURCE>_FLAT[_MMDDYYYY].csv[.gz] -> (company_token, source, date|None)"""
    m = FN.match(name)
    if not m:
        return None
    co, src, d = m.groups()
    try:
        dt = datetime.strptime(d, "%m%d%Y").date() if d else None
    except ValueError:
        dt = None
    return co.upper(), src.upper(), dt


# -----------------------------------------------------------------------------
# References & Override CSVs
# -----------------------------------------------------------------------------
def _side_csv(name):
    """Reads rows from optional CSV in inputs/vsam, scripts dir, or outputs."""
    rows = []
    candidates = [
        VSAM_INPUTS_DIR / name,
        Path(__file__).parent / name,
        OUT / name,
        BASE_DIR / name
    ]
    for f in candidates:
        if f.exists():
            lines = [ln for ln in f.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
            rows += list(csv.DictReader(lines))
            break
    return rows


def ref_tables():
    """
    Loads schema and korg mappings.
    Primary source: vsam_master_mapping.xlsx
      - sheet 'schema_mapping' (schemaname, sourcecompany, brokername, status) [active only]
      - sheet 'copybook_mapping' (filename, copybook, postgres_tablename)
    Overrides: vsam_master_overrides.xlsx
      - sheet 'korg_overrides' (filename, tablename, pkey)
    """
    from collections import defaultdict
    schemas = defaultdict(list)
    korg = {}

    if MASTER_MAPPING_FILE.exists():
        try:
            df_sch = read_excel_safe(MASTER_MAPPING_FILE, sheet_name="schema_mapping", dtype=str).fillna("")
            for _, r in df_sch.iterrows():
                status = str(r.get("status", "")).strip().lower()
                if status == "active":
                    co = str(r.get("sourcecompany", "")).strip().upper()
                    sch = str(r.get("schemaname", "")).strip().lower()
                    brk = str(r.get("brokername", "")).strip()
                    item = {"source_company": co, "schema": sch, "broker": brk, "status": "active", "vsam_s3_file": "Yes"}
                    schemas[co].append(item)
                    # Suffix variation handling (e.g. singular/plural token variations)
                    if co.endswith("S"):
                        schemas[co[:-1]].append(item)
                    else:
                        schemas[co + "S"].append(item)
        except Exception:
            pass

    if MASTER_MAPPING_FILE.exists():
        try:
            df_cb = read_excel_safe(MASTER_MAPPING_FILE, sheet_name="copybook_mapping", dtype=str).fillna("")
            for _, r in df_cb.iterrows():
                fn = str(r.get("filename", "")).strip().upper()
                tbl = str(r.get("postgres_tablename", "")).strip()
                korg_val = str(r.get("korg", "")).strip()
                if not korg_val or korg_val.lower() in ("nan", "none", "null", "no_korg", ""):
                    korg_val = None
                else:
                    korg_val = norm_col(korg_val)
                if fn and tbl:
                    korg[fn] = (tbl, korg_val)
        except Exception:
            pass

    # Apply korg_overrides from vsam_master_overrides.xlsx
    if MASTER_OVERRIDES_FILE.exists():
        try:
            df_ko = read_excel_safe(MASTER_OVERRIDES_FILE, sheet_name="korg_overrides", dtype=str).fillna("")
            for _, r in df_ko.iterrows():
                fn = str(r.get("filename", "")).strip().upper()
                tbl = str(r.get("tablename", "")).strip()
                pkey = str(r.get("pkey", "") or r.get("key_column", "")).strip() or None
                if fn:
                    existing_tbl = korg.get(fn, (None, None))[0]
                    korg[fn] = (tbl or existing_tbl or fn, pkey)
        except Exception:
            pass

    for r in _side_csv("korg_extras.csv"):
        korg[r["source_file"].strip().upper()] = (r["postgres_table"].strip(), (r.get("key_column") or "").strip() or None)

    return dict(schemas), korg


def get_korg_override_pkey(source_company: str = "", source_filename: str = "", s3_filename: str = "") -> list:
    """
    Looks up primary key override in the 'korg_overrides' sheet of vsam_master_overrides.xlsx.
    Matches filename case-insensitively against:
      - exact s3_filename (with or without extension)
      - source_filename (e.g. USA013)
      - compound key: COMPANY_SOURCE or COMPANY|SOURCE
    Returns list of normalized column names, or [] if no entry or blank.
    """
    if not MASTER_OVERRIDES_FILE.exists():
        return []
    try:
        df_ko = pd.read_excel(MASTER_OVERRIDES_FILE, sheet_name="korg_overrides", dtype=str).fillna("")
    except Exception:
        return []

    cands = set()
    for s in (source_filename, s3_filename):
        if s:
            cands.add(s.upper())
            base = re.sub(r"\.csv(?:\.gz)?$", "", s, flags=re.I).upper()
            cands.add(base)
    if source_company and source_filename:
        cands.add(f"{source_company.upper()}_{source_filename.upper()}")
        cands.add(f"{source_company.upper()}|{source_filename.upper()}")

    for _, r in df_ko.iterrows():
        fn = str(r.get("filename", "")).strip().upper()
        if not fn:
            continue
        fn_base = re.sub(r"\.csv(?:\.gz)?$", "", fn, flags=re.I)
        if fn in cands or fn_base in cands:
            pkey_raw = str(r.get("pkey", "") or r.get("key_column", "")).strip()
            if pkey_raw:
                return [norm_col(c.strip()) for c in re.split(r"[,;+]", pkey_raw) if c.strip()]
    return []


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


def check_schema_col_is_pk(schema_col, pk_cols) -> str:
    """
    Determines whether the identified schema column is a primary key:
      - 'SOLE_PK': If schema_col is the sole single primary key column.
      - 'COMPOSITE_PK_PART': If schema_col is part of a composite primary key.
      - 'NOT_IN_PK': If schema_col exists but is not part of the primary key.
      - 'NO_SCHEMA_COL': If no schema column was detected.
    Handles exact matching, case insensitivity, and normalized variations (e.g. 'org' vs 'm405_korg').
    """
    if not schema_col or str(schema_col).strip().upper() in ("NONE", "N/A", ""):
        return "NO_SCHEMA_COL"
    if not pk_cols:
        return "NOT_IN_PK"

    sc_norm = norm_col(str(schema_col).strip())
    sc_clean = sc_norm.replace("_", "")

    matched = False
    for pk in pk_cols:
        pk_norm = norm_col(str(pk).strip())
        pk_clean = pk_norm.replace("_", "")
        if sc_norm == pk_norm or sc_clean == pk_clean:
            matched = True
            break
        if len(sc_norm) >= 3 and len(pk_norm) >= 3:
            if sc_norm.endswith("_" + pk_norm) or pk_norm.endswith("_" + sc_norm):
                matched = True
                break
            if sc_norm.endswith(pk_norm) or pk_norm.endswith(sc_norm):
                matched = True
                break

    if not matched:
        return "NOT_IN_PK"

    if len(pk_cols) == 1:
        return "SOLE_PK"
    else:
        return f"COMPOSITE_PK_PART ({len(pk_cols)} cols)"


def get_pipeline_mode(arg_mode: str = None) -> str:
    """
    Returns active pipeline mode:
      - 'data': Raw data priority, NO PK constraints, TRUNCATE & LOAD, all rows loaded (incl. NULL/sentinel).
      - 'quality': Strict data quality, active PK constraints, DYNAMIC UPSERT, purges NULL/sentinel rows.
    Resolution: CLI argument -> VSAM_MODE env var -> default 'quality'.
    """
    mode = (arg_mode or os.getenv("VSAM_MODE", "quality")).strip().lower()
    if mode in ("data", "data-priority", "raw", "truncate", "no-pk"):
        return "data"
    return "quality"


def is_sentinel_or_empty_val(v) -> bool:
    """Checks if a single value is None, empty, whitespace, or sentinel placeholder (0s or 9s)."""
    if v is None:
        return True
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return True
    # All 9s sentinel (e.g. '9', '9999', '999999', '9999-99-99', '9999_99_99')
    if re.fullmatch(r"9+", s) or re.fullmatch(r"9{4}[-_]9{2}[-_]9{2}", s):
        return True
    # All 0s sentinel (e.g. '0', '00', '0000', '0000-00-00', '0000_00_00')
    if re.fullmatch(r"0+", s) or re.fullmatch(r"0{4}[-_]0{2}[-_]0{2}", s):
        return True
    return False


def is_invalid_or_sentinel_record(vals) -> bool:
    """
    Returns True if every value in vals is either empty, whitespace, NULL,
    or a known sentinel placeholder value (0s, 9s).
    """
    return all(is_sentinel_or_empty_val(v) for v in vals)


def is_blank_record(vals) -> bool:
    """Alias for is_invalid_or_sentinel_record to filter empty and sentinel rows."""
    return is_invalid_or_sentinel_record(vals)


def is_pk_val_invalid(v) -> bool:
    """Checks if a PK column value is NULL, empty, whitespace, or sentinel placeholder."""
    if v is None:
        return True
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return True
    # All 9s sentinel (e.g. '9', '9999', '999999', '9999-99-99', '9999_99_99')
    if re.fullmatch(r"9+", s) or re.fullmatch(r"9{4}[-_]9{2}[-_]9{2}", s):
        return True
    return False


def record_passes_pk_validation(row: dict, pk_cols: list) -> bool:
    """
    Checks if a record passes PK validation:
    1. Entire record cannot be all-NULL, blank, or sentinel placeholder values.
    2. Every PK column must have a valid value (not NULL, blank, empty whitespace, or all-9s sentinel).
    """
    if not pk_cols:
        return True
    vals = list(row.values()) if isinstance(row, dict) else list(row)
    if is_invalid_or_sentinel_record(vals):
        return False
    for pk in pk_cols:
        val = row.get(pk) if isinstance(row, dict) else None
        if is_pk_val_invalid(val):
            return False
    return True


def get_pk_validation_failure_reason(row: dict, pk_cols: list) -> str:
    """
    Returns a detailed description of why a record failed PK validation, or '' if valid.
    Identifies:
    - Entire record is NULL/empty/whitespace.
    - Entire record consists of sentinel placeholder values (all 9s or all 0s).
    - Specific PK columns that are NULL, empty, whitespace, or contain sentinel values.
    """
    if not pk_cols:
        return ""
    vals = list(row.values()) if isinstance(row, dict) else list(row)

    # 1. Check if literally all values are empty / blank / None
    all_empty = all(v is None or not str(v).strip() or str(v).strip().lower() in ("nan", "none", "null") for v in vals)
    if all_empty:
        return "All columns in record are empty / blank"

    # 2. Check if all values are 9s sentinels
    all_9s = all(v is not None and (re.fullmatch(r"9+", str(v).strip()) or re.fullmatch(r"9{4}[-_]9{2}[-_]9{2}", str(v).strip())) for v in vals)
    if all_9s:
        return "All columns in record contain all-9s sentinel values (e.g. 9999, 9999-99-99)"

    if is_invalid_or_sentinel_record(vals):
        return "Entire record consists of invalid sentinel or blank placeholder values"

    # 3. Check specific PK columns
    reasons = []
    for pk in pk_cols:
        val = row.get(pk) if isinstance(row, dict) else None
        if val is None:
            reasons.append(f"PK column '{pk}' is NULL")
        else:
            s = str(val).strip()
            if not s:
                reasons.append(f"PK column '{pk}' is empty / whitespace")
            elif s.lower() in ("nan", "none", "null"):
                reasons.append(f"PK column '{pk}' has literal '{val}'")
            elif re.fullmatch(r"9+", s) or re.fullmatch(r"9{4}[-_]9{2}[-_]9{2}", s):
                reasons.append(f"PK column '{pk}' contains sentinel value '{val}'")
            elif re.fullmatch(r"0+", s) and len(s) > 4:
                reasons.append(f"PK column '{pk}' contains zero sentinel '{val}'")

    if reasons:
        return "; ".join(reasons)
    return ""


def filter_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops rows where ALL column values are NULL, empty (''), whitespace ('   '),
    or sentinel placeholder values (all 0s or all 9s).
    Resets index and returns cleaned DataFrame.
    """
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()

    mask = pd.Series(False, index=df.index)
    num_cols = df.shape[1]
    for i in range(num_cols):
        s = df.iloc[:, i]
        valid = s.notna()
        s_str = s.astype(str).str.strip()
        # Has meaningful data that is not empty, not null literal, and not all 9s or all 0s
        has_content = (
            valid &
            (s_str != "") &
            (~s_str.str.lower().isin(["nan", "none", "null"])) &
            (~s_str.str.fullmatch(r"9+|9{4}[-_]9{2}[-_]9{2}")) &
            (~s_str.str.fullmatch(r"0+|0{4}[-_]0{2}[-_]0{2}"))
        )
        mask |= has_content

    return df[mask].reset_index(drop=True)


def filter_blank_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Alias for filter_invalid_rows."""
    return filter_invalid_rows(df)


def find_unique_key_columns_sequential(df: pd.DataFrame, max_cols: int = 8) -> list:
    """
    Positional sequential search for PK columns when copybook is NOT present.
    Evaluates:
      1. col_1 alone -> SINGLE
      2. col_1 + col_2 -> COMPOSITE(2)
      3. col_1 + col_2 + col_3 -> COMPOSITE(3)
      ... up to min(max_cols, len(columns)).
    Does NOT pick columns randomly. If not unique within the first 8 columns, returns [].
    """
    if df is None or df.empty or len(df) <= 1:
        return []
    clean_df = filter_invalid_rows(df)
    if clean_df.empty or len(clean_df) <= 1:
        return []
    total_rows = len(clean_df)
    cols = list(clean_df.columns)
    limit = min(max_cols, len(cols))

    current_combo = []
    for i in range(limit):
        current_combo.append(cols[i])
        sub = clean_df[current_combo]
        has_null = sub.isna().any(axis=1) | (sub.astype(str).apply(lambda s: s.str.strip().isin(["", "none", "nan", "null"]))).any(axis=1)
        if has_null.any():
            continue
        if len(sub.drop_duplicates()) == total_rows:
            return current_combo
    return []


def col_ends_with_schema_suffix(c: str) -> bool:
    """
    Checks if column name ends with any of the 5 allowed suffixes:
      (org, korg, bank, nbr, no)
    EXCLUDING column names ending with (recorg, recbank).
    """
    cn = norm_col(c).lower()
    for excl in ("recorg", "recbank"):
        if cn == excl or cn.endswith("_" + excl) or cn.endswith(excl):
            return False
    for sfx in ("korg", "org", "bank", "nbr", "no"):
        if cn == sfx or cn.endswith("_" + sfx) or cn.endswith(sfx):
            return True
    return False


def is_col_in_pk(c: str, pk_cols: list) -> bool:
    """Checks if column is an exact or normalized part of pk_cols."""
    if not pk_cols:
        return False
    cn = norm_col(c)
    cn_clean = cn.replace("_", "")
    for pk in pk_cols:
        pkn = norm_col(pk)
        pkn_clean = pkn.replace("_", "")
        if cn == pkn or cn_clean == pkn_clean:
            return True
        if cn.endswith("_" + pkn) or pkn.endswith("_" + cn):
            return True
        if cn.endswith(pkn) or pkn.endswith(cn):
            return True
    return False


def identify_schema_col(columns: list, pk_cols: list) -> tuple:
    """
    Identifies the schema discriminator column:
      Condition 1: Column name must end with one of: (org, korg, bank, nbr, no).
      Condition 2: Column must be a PK column or part of composite PK.
    - If a column meets BOTH condition 1 and condition 2:
        return (column_name, f"YES ({check_schema_col_is_pk(c, pk_cols)})")
    - If no column meets both, check positional order for condition 1:
        return (column_name, 'NO')
    - If no column meets condition 1 at all:
        return (None, 'NO_SCHEMA_COL')
    """
    # Pass 1: Column meets BOTH condition 1 and condition 2
    for c in columns:
        if col_ends_with_schema_suffix(c) and is_col_in_pk(c, pk_cols):
            detail = check_schema_col_is_pk(c, pk_cols)
            return c, f"YES ({detail})"
    # Pass 2: Column meets condition 1 but not condition 2
    for c in columns:
        if col_ends_with_schema_suffix(c):
            return c, "NO"
    return None, "NO_SCHEMA_COL"


AUDIT_COLS = [
    '  "createddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "createdby" character varying(40) DEFAULT CURRENT_USER',
    '  "modifieddate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP',
    '  "modifiedby" character varying(40) DEFAULT CURRENT_USER',
    '  "createddate_m" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP',
    '  "createdby_m" character varying(40) NOT NULL DEFAULT CURRENT_USER',
]

AUDIT_COL_NAMES = {
    "createddate", "createdby", "modifieddate", "modifiedby",
    "createddate_m", "createdby_m"
}


def build_table_ddl(schema: str, table: str, columns: list, key_cols: list):
    """
    Returns (pre_load_sql, post_load_sql) tuple.
    pre_load: CREATE TABLE without PK constraint.
    post_load: ALTER TABLE ADD CONSTRAINT pk.
    Deduplicates column names (case-insensitive) — first occurrence wins.
    Skips data columns whose names clash with reserved audit column names.
    """
    col_lines = []
    seen_cols = set()
    for c in columns:
        if c.get("is_filler") == "YES":
            continue
        col_name_raw = norm_col(c.get("col_name", ""))
        if not col_name_raw:
            continue
        if col_name_raw in seen_cols:
            logger.warning(
                "Duplicate column '%s' in %s.%s — keeping first occurrence, skipping duplicate.",
                col_name_raw, schema, table
            )
            continue
        if col_name_raw in AUDIT_COL_NAMES:
            logger.warning(
                "Data column '%s' in %s.%s clashes with audit column name — skipping data column.",
                col_name_raw, schema, table
            )
            continue
        seen_cols.add(col_name_raw)
        col_type = c.get("sql_type") or "text"
        col_lines.append(f"  {q(col_name_raw)} {col_type}")

    col_lines.extend(AUDIT_COLS)

    pre_sql = (
        f"CREATE TABLE IF NOT EXISTS {q(schema)}.{q(table)} (\n"
        + ",\n".join(col_lines)
        + "\n);"
    )

    post_sql = ""
    if key_cols:
        pk_name = q(f"pk_{table}")
        pk_def = ", ".join(q(k) for k in key_cols)
        post_sql = (
            f"ALTER TABLE {q(schema)}.{q(table)}\n"
            f"  ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_def});"
        )

    return pre_sql, post_sql


def check_table_exists(schema: str, table: str) -> bool:
    """Checks if a table exists in PostgreSQL live catalog."""
    sql = f"""
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = '{schema.lower()}' AND table_name = '{table.lower()}';
    """
    try:
        rows = query_db_rows(sql)
        return len(rows) > 0
    except Exception as e:
        logger.debug(f"Could not check existence of table {schema}.{table}: {e}")
        return False


@functools.lru_cache(maxsize=1)
def get_active_schemas_map() -> dict:
    """
    Returns mapping of trailing 3-digits -> active schema name.
    Source: vsam_master_mapping.xlsx sheet 'schema_mapping' where status == 'active'.
    e.g. {'104': 'bss01104', '112': 'bss01112', '110': 'bss01110', '105': 'bss01105', ...}
    Inactive schemas (e.g. '106' -> 'bss01106', '107' -> 'bss01107', '120' -> 'bss01120') are excluded.
    """
    active_map = {}
    if MASTER_MAPPING_FILE.exists():
        try:
            df_sch = read_excel_safe(MASTER_MAPPING_FILE, sheet_name="schema_mapping", dtype=str).fillna("")
            for _, r in df_sch.iterrows():
                status = str(r.get("status", "")).strip().lower()
                if status == "active":
                    sch = str(r.get("schemaname", "")).strip().lower()
                    if sch and sch != "bcs":
                        last3 = get_last_3_digits(sch)
                        if last3:
                            active_map[last3] = sch
        except Exception as e:
            logger.debug(f"Could not read active schemas from master mapping: {e}")
    if not active_map:
        active_map = {
            "001": "bss01001",
            "104": "bss01104",
            "105": "bss01105",
            "108": "bss01108",
            "110": "bss01110",
            "112": "bss01112",
            "115": "bss01115",
            "116": "bss01116",
            "099": "bss00099",
        }
    return active_map


def get_all_active_schemas() -> list:
    """Returns unique list of all active schema names including 'bcs'."""
    m = get_active_schemas_map()
    res = list(dict.fromkeys(m.values()))
    if "bcs" not in res:
        res.append("bcs")
    return res


def get_last_3_digits(val) -> str:
    """Extracts trailing 3 digits from a string or number, zero-padded."""
    if val is None:
        return ""
    s_val = str(val).strip().split(".")[0]
    m = re.findall(r"\d+", s_val)
    if not m:
        return ""
    d_str = m[-1]
    return d_str[-3:] if len(d_str) >= 3 else d_str.zfill(3)


def match_schema_by_last3(val, target_schemas: list = None) -> str:
    """
    Matches the schema discriminator column value's trailing 3 digits against active schemas.
    1. If the trailing 3 digits match an active schema (either in target_schemas or any active schema
       in the master mapping such as bss01110), routes to that active schema.
       e.g. '0104' -> '104' -> 'bss01104', '0112' -> '112' -> 'bss01112', '0110' -> '110' -> 'bss01110'.
    2. Records that are inactive schemas (e.g. '0106' -> 'bss01106', '0107' -> 'bss01107', '0120' -> 'bss01120')
       or other invalid schema numbers (e.g. '0100', '9999', blank) are moved to 'bcs'.
    """
    last3 = get_last_3_digits(val)
    if not last3:
        return "bcs"

    active_map = get_active_schemas_map()

    # If target_schemas is provided and matches last3, verify it's active
    if target_schemas:
        for s in target_schemas:
            s_clean = str(s).strip().lower()
            if s_clean != "bcs" and (s_clean.endswith(last3) or get_last_3_digits(s_clean) == last3):
                # Ensure the target schema itself is active
                if last3 in active_map and active_map[last3] == s_clean:
                    return s_clean
                elif s_clean in active_map.values():
                    return s_clean

    # Cross-company active schema routing:
    # If the file contains records for another active schema (e.g. 110 for SWP's bss01110 in an FDIC file),
    # route to that active schema.
    if last3 in active_map:
        return active_map[last3]

    # Inactive schemas (e.g. 106, 107, 120) or other invalid numbers -> bcs
    return "bcs"


def get_table_pk_columns_live(schema: str, table: str) -> list:
    """
    Queries PostgreSQL live catalog to check if schema.table has an active PRIMARY KEY constraint.
    Returns list of primary key column names (in ordinal order), or [] if no PK exists.
    """
    sql = f"""
    SELECT a.attname
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
    WHERE n.nspname = '{schema.lower()}'
      AND c.relname = '{table.lower()}'
      AND i.indisprimary
    ORDER BY array_position(i.indkey, a.attnum);
    """
    try:
        rows = query_db_rows(sql)
        return [r.strip() for r in rows if r.strip()]
    except Exception as e:
        logger.debug(f"Could not check PK for {schema}.{table}: {e}")
        return []


def table_has_existing_data(schema: str, table: str) -> bool:
    """
    Checks if schema.table currently has at least 1 row in PostgreSQL.
    """
    sql = f"SELECT 1 FROM {q(schema)}.{q(table)} LIMIT 1;"
    try:
        rows = query_db_rows(sql)
        return len(rows) > 0
    except Exception as e:
        logger.debug(f"Could not check row existence for {schema}.{table}: {e}")
        return False


def load_master_copybook_info() -> dict:
    """
    Reads the 'copybook_mapping' sheet from vsam_master_mapping.xlsx.
    Returns a dictionary mapping:
      filename (upper) -> {
         'filename': fn,
         'copybook': cb or None,
         'table': tbl or None,
         'korg': korg or None,
         'copybook_display': cb if cb else 'NO_COPYBOOK',
         'korg_display': korg if korg else 'NO_KORG'
      }
      and also maps postgres_tablename (lower) -> same dict.
    If copybook or korg is blank/empty/nan/null in the mapping sheet,
    their values are None and displays are 'NO_COPYBOOK' / 'NO_KORG'.
    """
    info_map = {}
    if MASTER_MAPPING_FILE.exists():
        try:
            df = read_excel_safe(MASTER_MAPPING_FILE, sheet_name="copybook_mapping", dtype=str).fillna("")
            for _, r in df.iterrows():
                fn = str(r.get("filename", "")).strip().upper()
                cb = str(r.get("copybook", "")).strip().upper()
                tbl = str(r.get("postgres_tablename", "")).strip().lower()
                korg = str(r.get("korg", "")).strip()

                if not cb or cb.lower() in ("nan", "none", "null", "no_copybook"):
                    cb = None
                if not korg or korg.lower() in ("nan", "none", "null", "no_korg"):
                    korg = None
                else:
                    korg = norm_col(korg)

                item = {
                    "filename": fn,
                    "copybook": cb,
                    "table": tbl or (f"stg_{norm_col(fn).lower()}_v" if fn else None),
                    "korg": korg,
                    "copybook_display": cb if cb else "NO_COPYBOOK",
                    "korg_display": korg if korg else "NO_KORG"
                }
                if fn:
                    info_map[fn] = item
                if tbl:
                    info_map[tbl] = item
        except Exception as e:
            logger.debug(f"Could not load copybook_mapping: {e}")
    return info_map


def load_copybook_korg_mapping() -> dict:
    """
    Reads the 'korg' column from copybook_mapping sheet in vsam_master_mapping.xlsx.
    Returns a dictionary mapping:
      filename (upper) -> korg (norm_col)
      copybook (upper) -> korg (norm_col)
      postgres_tablename (lower) -> korg (norm_col)
    """
    korg_map = {}
    if MASTER_MAPPING_FILE.exists():
        try:
            df = read_excel_safe(MASTER_MAPPING_FILE, sheet_name="copybook_mapping", dtype=str).fillna("")
            for _, r in df.iterrows():
                korg_val = str(r.get("korg", "")).strip()
                if not korg_val or korg_val.lower() in ("nan", "none", "null", "no_korg", ""):
                    continue
                k_norm = norm_col(korg_val)
                fn = str(r.get("filename", "")).strip().upper()
                cb = str(r.get("copybook", "")).strip().upper()
                tbl = str(r.get("postgres_tablename", "")).strip().lower()
                if fn:
                    korg_map[fn] = k_norm
                if cb:
                    korg_map[cb] = k_norm
                if tbl:
                    korg_map[tbl] = k_norm
        except Exception as e:
            logger.debug(f"Could not load korg from copybook_mapping: {e}")
    return korg_map


def infer_columns_from_df(df: pd.DataFrame) -> list:
    """
    Infers column names and PostgreSQL datatypes directly from a DataFrame's header and data.
    Used for AGILETIC source company or when copybook is missing.
    """
    inferred_cols = []
    if df is None or df.empty:
        return inferred_cols

    for col_name in df.columns:
        c_norm = norm_col(col_name)
        val = df[col_name]
        col_series = val.iloc[:, 0] if isinstance(val, pd.DataFrame) else val
        s_clean = col_series.dropna().astype(str).str.strip()
        s_non_empty = s_clean[s_clean != ""]
        if s_non_empty.empty:
            sql_type = "text"
        elif (s_non_empty.str.fullmatch(r"^\d{4}-\d{2}-\d{2}$").sum() / len(s_non_empty)) >= 0.8:
            sql_type = "date"
        elif (s_non_empty.str.fullmatch(r"^\d{4}_\d{2}_\d{2}$").sum() / len(s_non_empty)) >= 0.8:
            sql_type = "varchar(10)"
        elif (s_non_empty.str.fullmatch(r"^-?\d+$").sum() / len(s_non_empty)) == 1.0:
            max_len = int(s_non_empty.str.len().max())
            sql_type = "integer" if max_len <= 9 else "bigint"
        elif (s_non_empty.str.fullmatch(r"^-?\d+(\.\d+)?$").sum() / len(s_non_empty)) >= 0.9:
            sql_type = "numeric"
        else:
            max_len = int(s_non_empty.str.len().max())
            sql_type = f"varchar({max(max_len + 4, 10)})" if max_len <= 255 else "text"

        inferred_cols.append({
            "col_name": c_norm,
            "sql_type": sql_type,
            "is_filler": "NO"
        })
    return inferred_cols


def check_table_load_strategy(schema: str, table: str, tg_prof_pk: list = None, mode: str = "default") -> dict:
    """
    Inspects PostgreSQL live for schema.table to determine load strategy:
    Mode: 'default'
      - If table is fresh (no existing data):
        * If table has PK defined, apply PK constraint to table in PostgreSQL!
        * Set action = 'INSERT_FRESH', has_pk = True, pk_cols = live_pk_cols
      - If table has existing data and YES PK constraint -> action = 'UPSERT', has_pk = True
      - If table has existing data and NO PK constraint -> action = 'TRUNCATE_AND_LOAD', has_pk = False

    Mode: 'data'
      - If table is fresh (no existing data):
        * Do NOT apply PK constraints. Load data only.
        * Set action = 'DIRECT_INSERT', has_pk = False, pk_cols = []
      - If table has existing data and YES PK constraint -> action = 'UPSERT', has_pk = True
      - If table has existing data and NO PK constraint -> action = 'TRUNCATE_AND_LOAD', has_pk = False
    """
    has_data = table_has_existing_data(schema, table)
    live_pk_cols = get_table_pk_columns_live(schema, table)
    has_pk = bool(live_pk_cols)
    candidate_pks = [norm_col(c) for c in (tg_prof_pk or []) if c]
    mode_clean = (mode or "default").lower().strip()

    if not has_data:
        # Table is fresh (0 rows)
        if mode_clean != "data" and candidate_pks and not has_pk:
            # Default mode: apply PK constraint to the fresh table before data load
            try:
                pk_name = q(f"pk_{table}")
                pk_def = ", ".join(q(k) for k in candidate_pks)
                alter_sql = f"ALTER TABLE {q(schema)}.{q(table)} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_def});"
                run_psql(alter_sql)
                logger.info(f"Applied PK constraint on fresh table {schema}.{table} ({', '.join(candidate_pks)})")
                live_pk_cols = get_table_pk_columns_live(schema, table)
                has_pk = bool(live_pk_cols)
            except Exception as e:
                logger.debug(f"Could not apply PK constraint to {schema}.{table}: {e}")
                has_pk = False

        if has_pk:
            action = "INSERT_FRESH"
        else:
            action = "DIRECT_INSERT"
    elif has_pk:
        # Table has existing data and has PK constraint in DB
        action = "UPSERT"
    else:
        # Table has existing data and NO PK constraint in DB
        action = "TRUNCATE_AND_LOAD"

    return {
        "has_data": has_data,
        "has_pk": has_pk,
        "pk_cols": live_pk_cols if has_pk else [],
        "action": action
    }


def find_unique_key_columns(df: pd.DataFrame, max_cols: int = 10, base_cols: list = None) -> list:
    """
    Automatically parses a DataFrame of records to discover a column or composite
    combination of columns (up to max_cols, default 10) that has 100% unique values
    and no null or empty values.
    If base_cols is provided (e.g. from copybook), tests extending base_cols first.
    Completely ignores records where all column values are NULL, empty, or whitespace.
    """
    if df is None or df.empty:
        return []
    df = filter_blank_rows(df)
    if df.empty or len(df) <= 1:
        return []
    total_rows = len(df)

    valid_cols = []
    cardinality = {}
    for c in df.columns:
        s = df[c].astype(str).str.strip()
        has_empty = (s == "") | (s.str.lower() == "nan") | (s.str.lower() == "none") | df[c].isna()
        if has_empty.any():
            continue
        n_uniq = s.nunique()
        if n_uniq <= 1:
            continue
        valid_cols.append(c)
        cardinality[c] = n_uniq

    if not valid_cols:
        return []

    # Priority 1: If base_cols is provided, try extending base_cols first!
    import itertools
    if base_cols:
        clean_base = [c for c in base_cols if c in valid_cols]
        if clean_base:
            if len(df.drop_duplicates(subset=clean_base)) == total_rows:
                return [norm_col(c) for c in clean_base]

            candidates = [c for c in valid_cols if c not in clean_base]
            def cand_priority(c):
                score = cardinality[c] / total_rows
                c_l = c.lower()
                if any(kw in c_l for kw in ("id", "key", "num", "no", "code", "nbr", "acct", "rec", "seq", "dt", "date")):
                    score += 0.5
                return score

            sorted_cands = sorted(candidates, key=cand_priority, reverse=True)
            max_add = max(0, max_cols - len(clean_base))
            pool_cands = sorted_cands[:min(len(sorted_cands), 25)]

            for add_k in range(1, max_add + 1):
                combos = list(itertools.combinations(pool_cands, add_k))
                if len(combos) > 20000:
                    combos = combos[:20000]
                for extra in combos:
                    test_cols = clean_base + list(extra)
                    if len(df.drop_duplicates(subset=test_cols)) == total_rows:
                        return [norm_col(c) for c in test_cols]

    col_hashes = {c: pd.util.hash_pandas_object(df[c].astype(str).str.strip()).values for c in valid_cols}

    def col_priority(c):
        score = cardinality[c] / total_rows
        c_l = c.lower()
        if any(kw in c_l for kw in ("id", "key", "num", "no", "code", "nbr", "acct", "rec", "seq")):
            score += 0.5
        return score

    sorted_cols = sorted(valid_cols, key=col_priority, reverse=True)

    # Level 1: Single column check
    for c in sorted_cols:
        if cardinality[c] == total_rows:
            return [norm_col(c)]

    # Level 2 to min(max_cols, 4): Fast exact hash combinations
    pool = sorted_cols[:min(len(sorted_cols), 25)]
    beam = []
    cutoff_k = min(max_cols, 4)

    for k in range(2, cutoff_k + 1):
        if len(pool) < k:
            break
        combos = list(itertools.combinations(pool, k))
        if len(combos) > 20000:
            combos = combos[:20000]
        for combo in combos:
            h_combined = col_hashes[combo[0]]
            for next_c in combo[1:]:
                h_combined = h_combined ^ (col_hashes[next_c] * 1000003)
            u_cnt = len(np.unique(h_combined))
            if u_cnt == total_rows:
                if len(df.drop_duplicates(subset=list(combo))) == total_rows:
                    return [norm_col(c) for c in combo]
            if k == cutoff_k:
                beam.append((u_cnt, combo, h_combined))

    # Level 5 to max_cols: Beam search expansion up to 10 columns
    if max_cols >= 5 and beam:
        beam.sort(key=lambda x: x[0], reverse=True)
        current_beam = beam[:30]
        for k in range(5, max_cols + 1):
            next_beam = []
            for u_cnt, combo, h_prev in current_beam:
                combo_set = set(combo)
                for next_c in pool:
                    if next_c in combo_set:
                        continue
                    new_h = h_prev ^ (col_hashes[next_c] * 1000003)
                    new_u = len(np.unique(new_h))
                    new_combo = combo + (next_c,)
                    if new_u == total_rows:
                        if len(df.drop_duplicates(subset=list(new_combo))) == total_rows:
                            return [norm_col(c) for c in new_combo]
                    next_beam.append((new_u, new_combo, new_h))
            if not next_beam:
                break
            next_beam.sort(key=lambda x: x[0], reverse=True)
            current_beam = next_beam[:30]

    return []


def resolve_file_key(source_company: str, source_filename: str, s3_filename: str = "",
                     cb_key_cols: list = None, df_sample: pd.DataFrame = None, max_cols: int = 10) -> tuple:
    """
    Determines primary key using the strict 3-tier hierarchy:
      1. Copybook key columns (if present)
      2. korg_overrides sheet in vsam_master_overrides.xlsx
      3. Auto-discovered unique column(s) from data (single or composite up to max_cols)
    Returns: (key_cols_list, source_tag)
    """
    # 1. Copybook keys
    if cb_key_cols and any(cb_key_cols):
        clean_keys = [norm_col(c.strip()) for c in cb_key_cols if c and c.strip()]
        if clean_keys:
            return clean_keys, "COPYBOOK"

    # 2. korg_overrides in vsam_master_overrides.xlsx
    override_pkeys = get_korg_override_pkey(source_company, source_filename, s3_filename)
    if override_pkeys:
        return override_pkeys, "OVERRIDE_EXCEL"

    # 3. Auto-discovery from data
    if df_sample is not None and not df_sample.empty:
        discovered = find_unique_key_columns(df_sample, max_cols=max_cols)
        if discovered:
            tag = "AUTO_DISCOVERED(1)" if len(discovered) == 1 else f"AUTO_DISCOVERED({len(discovered)})"
            return discovered, tag

    return [], "NONE"


def copybook_overrides():
    """Reads copybook_overrides from vsam_master_overrides.xlsx (fallback copybook_overrides.csv)."""
    if MASTER_OVERRIDES_FILE.exists():
        try:
            import pandas as pd
            df = pd.read_excel(MASTER_OVERRIDES_FILE, sheet_name="copybook_overrides", dtype=str).fillna("")
            return {str(r.filename).strip().upper(): str(r.copybook).strip() for r in df.itertuples() if str(getattr(r, "filename", "")).strip()}
        except Exception:
            pass
    return {r["source_file"].strip().upper(): r["copybook_file"].strip() for r in _side_csv("copybook_overrides.csv")}


def match_source(company, src, korg):
    """Dynamically matches a source file to its target table in korg mapping."""
    key = src.upper()
    if key in korg:
        return (key,) + korg[key]
    compound_key = f"{company}_{src}".upper()
    if compound_key in korg:
        return (compound_key,) + korg[compound_key]
    return None


def copybook_ref():
    """Reads copybook_mapping from vsam_master_mapping.xlsx (fallback Master_reference.xlsx)."""
    if MASTER_MAPPING_FILE.exists():
        try:
            import pandas as pd
            df = pd.read_excel(MASTER_MAPPING_FILE, sheet_name="copybook_mapping", dtype=str).fillna("")
            res = {}
            for r in df.itertuples():
                fn = str(getattr(r, "filename", "")).strip().upper()
                cb = str(getattr(r, "copybook", "")).strip().upper()
                if cb.lower() in ("nan", "none", "null", "no_copybook", ""):
                    cb = ""
                if fn:
                    res[fn] = cb
            return res
        except Exception:
            pass
    if MASTER_REF.exists():
        try:
            import pandas as pd
            df = pd.read_excel(MASTER_REF, sheet_name="Copybook reference", dtype=str).fillna("")
            res = {}
            for r in df.itertuples():
                sf = str(getattr(r, "source_file", "")).strip().upper()
                cb = str(getattr(r, "copybook", "")).strip().upper()
                if cb.lower() in ("nan", "none", "null", "no_copybook", ""):
                    cb = ""
                if sf:
                    res[sf] = cb
            return res
        except Exception:
            return {}
    return {}


def load_type_overrides():
    """Reads type_overrides from vsam_master_overrides.xlsx (fallback type_overrides.csv)."""
    ov = {}
    if MASTER_OVERRIDES_FILE.exists():
        try:
            import pandas as pd
            df = pd.read_excel(MASTER_OVERRIDES_FILE, sheet_name="type_overrides", dtype=str).fillna("")
            for _, r in df.iterrows():
                co = str(r.get("sourcecompany", "")).strip().upper()
                fn = str(r.get("filename", "")).strip().upper()
                col = norm_col(str(r.get("column", "")))
                dt = str(r.get("datatype", "")).strip()
                if co and fn and col and dt:
                    ov[(co, fn, col)] = dt
        except Exception:
            pass
    if not ov:
        for r in _side_csv("type_overrides.csv"):
            ov[(r["company"].upper(), r["source_file"].upper(), norm_col(r["column"]))] = r["sql_type"].strip()
    return ov


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
# Copybook Parser
# -----------------------------------------------------------------------------
def sql_type(pic, usage):
    u = norm_usage(usage)
    if u == "COMP-1":
        return "real"
    if u == "COMP-2":
        return "double precision"
    p = re.sub(r"([A-Za-z9])\((\d+)\)", lambda m: m.group(1) * int(m.group(2)), (pic or "").upper())
    if p and set(p) <= set("S9V"):
        ip, _, dp = p.replace("S", "").partition("V")
        tot = len(ip) + len(dp)
        if not dp:
            return "integer" if tot <= 9 else "bigint" if tot <= 18 else f"numeric({tot},0)"
        return f"numeric({tot},{len(dp)})"
    if p and set(p) <= set("XA9") and ("X" in p or "A" in p):
        return f"varchar({len(p)})"
    return "text"


def field_lengths(pic, usage):
    u = norm_usage(usage)
    if u == "COMP-1":
        return 4, 4
    if u == "COMP-2":
        return 8, 8
    p = re.sub(r"([A-Za-z9])\((\d+)\)", lambda m: m.group(1) * int(m.group(2)), (pic or "").upper())
    if "X" in p or "A" in p:
        return len(p), len(p)
    digits = p.count("9")
    if u in ("COMP", "COMP-4", "COMP-5"):
        return digits, 2 if digits <= 4 else 4 if digits <= 9 else 8
    if u == "COMP-3":
        return digits, digits // 2 + 1
    return digits, digits


def parse_copybook(path):
    lines = []
    for ln in Path(path).read_text(errors="ignore").splitlines():
        ln = re.sub(r"^\d{5,8}\s*", "", ln)
        if ln[:1] == "*" or ln.strip().startswith(("*", "/")) or (len(ln) > 0 and ln[0] in "*/"):
            continue
        lines.append(ln.strip())
    stmts = [s.strip() for s in re.split(r"\.(?=\s|$)", " ".join(lines)) if s.strip()]
    root = {"level": 0, "children": []}
    stack = [root]
    for s in stmts:
        m = re.match(r"(\d{1,2})\s+([A-Za-z0-9_\-]+)\s*(.*)$", s, re.S)
        if not m:
            continue
        lvl, name, rest = int(m.group(1)), m.group(2), m.group(3)
        if lvl in (66, 88):
            continue
        pic = re.search(r"\bPIC(?:TURE)?\s+(?:IS\s+)?(\S+)", rest, re.I)
        occ = re.search(r"\bOCCURS\s+(\d+)", rest, re.I)
        node = {
            "level": lvl, "name": name,
            "redef": bool(re.search(r"\bREDEFINES\b", rest, re.I)),
            "pic": pic.group(1) if pic else None,
            "usage": (norm_usage(USAGE.search(rest).group(1)) if USAGE.search(rest) else None),
            "occurs": int(occ.group(1)) if occ else None, "children": []
        }
        while len(stack) > 1 and stack[-1]["level"] >= lvl:
            stack.pop()
        stack[-1]["children"].append(node)
        stack.append(node)

    def flat(nodes, sfx=""):
        for n in nodes:
            if n["redef"]:
                continue
            for i in range(1, (n["occurs"] or 1) + 1):
                s2 = sfx + (f"_{i}" if n["occurs"] else "")
                if n["children"]:
                    yield from flat(n["children"], s2)
                elif n["pic"] or n["usage"] in ("COMP-1", "COMP-2"):
                    ld, ls = field_lengths(n["pic"], n["usage"])
                    yield {
                        "name": n["name"] + s2, "pic": n["pic"], "usage": n["usage"],
                        "filler": n["name"].upper() == "FILLER", "len_display": ld, "len_storage": ls,
                        "sql_type": sql_type(n["pic"], n["usage"])
                    }
    return list(flat(root["children"]))


def copybook_types(fields):
    names = dedupe([norm_col(f["name"]) for f in fields])
    return dict(zip(names, [f["sql_type"] for f in fields]))


# -----------------------------------------------------------------------------
# S3 Streaming & CSV Reading
# -----------------------------------------------------------------------------
class S3Text(io.TextIOWrapper):
    """Text stream of decompressed S3 object via sweep_stream.sh."""
    def __init__(self, uri):
        sh_cmd = "bash" if os.name != "nt" else "bash"
        self._proc = subprocess.Popen(
            [sh_cmd, str(STREAM_SCRIPT), uri, AWS_REGION],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=(os.name != "nt")
        )
        super().__init__(self._proc.stdout, encoding="utf-8-sig", errors="replace", newline="")

    def close(self):
        p = getattr(self, "_proc", None)
        if p is not None and p.poll() is None:
            try:
                if os.name != "nt":
                    os.killpg(p.pid, signal.SIGKILL)
                else:
                    p.kill()
            except OSError:
                pass
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
                    except Exception:
                        pass
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
    except Exception:
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
        except Exception:
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


# -----------------------------------------------------------------------------
# DDL Generation with Native Application Audit Columns
# -----------------------------------------------------------------------------
def table_ddl(schema, table, p):
    """
    CREATE TABLE IF NOT EXISTS for target table.
    Natively embeds the FDICSweep application audit columns:
      createdby_m   varchar(40) NOT NULL DEFAULT CURRENT_USER
      createddate_m timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
      modifiedby    varchar(40) DEFAULT CURRENT_USER
      modifieddate  timestamp DEFAULT CURRENT_TIMESTAMP
    """
    key = p.get("key", [])
    lines = [f'  {q(c["name"])} {c["sql_type"]}' + (" NOT NULL" if c["name"] in key else "") for c in p["columns"]]

    # FDICSweep Standard Application Audit Columns (All 6 native audit columns)
    lines.extend([
        "  \"createddate\" timestamp without time zone DEFAULT CURRENT_TIMESTAMP",
        "  \"createdby\" character varying(40) DEFAULT CURRENT_USER",
        "  \"modifieddate\" timestamp without time zone DEFAULT CURRENT_TIMESTAMP",
        "  \"modifiedby\" character varying(40) DEFAULT CURRENT_USER",
        "  \"createddate_m\" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "  \"createdby_m\" character varying(40) NOT NULL DEFAULT CURRENT_USER"
    ])

    if key:
        lines.append(f"  CONSTRAINT {q('pk_' + table)} PRIMARY KEY (" + ", ".join(q(k) for k in key) + ")")

    return f"CREATE TABLE IF NOT EXISTS {q(schema)}.{q(table)} (\n" + ",\n".join(lines) + "\n);"


def load_csv_safe(path, default_cols=None):
    """Safely reads a CSV file, returning an empty DataFrame with default columns if empty or missing."""
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        import pandas as pd
        return pd.DataFrame(columns=default_cols or [])
    try:
        import pandas as pd
        return pd.read_csv(p, dtype=str).fillna("")
    except Exception:
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


@functools.lru_cache(maxsize=1)
def load_policy():
    """Reads loadpolicy_overrides from vsam_master_overrides.xlsx (fallback load_policy.csv)."""
    if MASTER_OVERRIDES_FILE.exists():
        try:
            import pandas as pd
            df = pd.read_excel(MASTER_OVERRIDES_FILE, sheet_name="loadpolicy_overrides", dtype=str).fillna("")
            return {
                (str(r.sourcecompany).strip().upper(), str(r.filename).strip().upper()): str(r.loadpolicy).strip().lower()
                for r in df.itertuples() if str(getattr(r, "loadpolicy", "")).strip()
            }
        except Exception:
            pass
    return {(r["company"].strip().upper(), r["source_file"].strip().upper()): r["policy"].strip().lower()
            for r in _side_csv("load_policy.csv") if r.get("policy")}


def version_mode_for(p):
    pol = load_policy().get(((p.get("company") or "").upper(), (p.get("source_file") or "").upper()))
    return pol if pol in ("snapshot", "upsert") else VERSION_MODE


# -----------------------------------------------------------------------------
# Target Profiles Merger for Multi-Company Shared Tables
# -----------------------------------------------------------------------------
def _num_info(t):
    t = t.lower()
    if t in ("smallint", "integer", "bigint"):
        return {"smallint": (5, 0), "integer": (10, 0), "bigint": (19, 0)}[t]
    m = re.fullmatch(r"numeric\((\d+),(\d+)\)", t)
    return (int(m.group(1)), int(m.group(2))) if m else ("any", 0) if t == "numeric" else None


def merge_type(a, b):
    if a == b:
        return a
    ta, tb = a.lower(), b.lower()
    va, vb = re.fullmatch(r"varchar\((\d+)\)", ta), re.fullmatch(r"varchar\((\d+)\)", tb)
    if va and vb:
        return f"varchar({max(int(va.group(1)), int(vb.group(1)))})"
    na, nb = _num_info(ta), _num_info(tb)
    if na and nb:
        if na[0] == "any" or nb[0] == "any":
            return "numeric"
        scale = max(na[1], nb[1])
        ints = max(na[0] - na[1], nb[0] - nb[1])
        if na[1] == 0 and nb[1] == 0:
            return "bigint" if max(na[0], nb[0]) > 10 else "integer"
        return f"numeric({ints + scale},{scale})"
    if {ta, tb} <= {"date", "timestamp"}:
        return "timestamp"
    return "text"


def merge_profiles(plist):
    import copy
    base = copy.deepcopy(plist[0])
    byname = {c["name"]: c for c in base["columns"]}
    for p in plist[1:]:
        for c in p["columns"]:
            if c["name"] in byname:
                byname[c["name"]]["sql_type"] = merge_type(byname[c["name"]]["sql_type"], c["sql_type"])
            else:
                nc = copy.deepcopy(c)
                base["columns"].append(nc)
                byname[nc["name"]] = nc
    keys = [list(p.get("key") or []) for p in plist]
    common = [k for k in keys[0] if all(k in kk for kk in keys)]
    if all(kk == keys[0] for kk in keys):
        base["key"] = keys[0]
    else:
        base["key"] = common
        base["key_source"] = "SHARED_COMMON_COLUMNS" if common else "NONE"
    base["shared_by"] = sorted({f"{p.get('company')}|{p.get('source_file')}" for p in plist})
    base["issues"] = list(base.get("issues", [])) + [
        f"SHARED_TABLE_MULTI_COMPANY:{len(base['shared_by'])} sources, key {base['key']} verified per company only"
    ]
    return base


def target_profiles(lm, prof):
    out = {}
    for (s, t), g in lm.groupby(["target_schema", "target_table"]):
        ps = []
        for co, src in dict.fromkeys(zip(g["source_company"], g["source_file"])):
            p = prof.get(f"{co}|{src}")
            if p and p.get("status") == "OK":
                ps.append(p)
        if ps:
            out[(s, t)] = ps[0] if len(ps) == 1 else merge_profiles(ps)
    return out
