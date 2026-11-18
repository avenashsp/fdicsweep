#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_03_type_gate.py - Step 03: Type Gate, PK Verification & Schema Column Detection
=============================================================================
Reads the latest file version for each table from S3/local data and performs:
  1. PK Uniqueness Verification: confirms key columns (single or composite) are
     truly unique across all rows in the actual data.
  2. Schema Column Detection: for tables mapped to multiple schemas, finds the
     single column where the data values match the schema number (last 3 digits).
  3. Datatype Validation: checks that actual data matches the SQL types from
     Step 01 copybook extraction.
  4. Multi-Version Propagation: applies the analysis from the latest version
     across all historical versions of each table, generating a file-level
     report for every file in S3.

Generates:
  - outputs/vsam/vsam_03_type_gate.json        (full profile per source)
  - outputs/vsam/vsam_03_type_gate_status.csv  (file-level pass/fail)
  - reports/vsam/<db>_vsam_03_type_gate_report_<ts>.xlsx
=============================================================================
"""
import sys
import os
import re
import json
import time
import argparse
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
_TRACK = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_TRACK) not in sys.path:
    sys.path.insert(0, str(_TRACK))

from sweep_common import (
    VSAM_REPORTS_DIR, AWS_REGION,
    get_logger, get_timestamp, get_db_name, generate_excel_report
)
from vsam_common import (
    OUT, norm_col, write_json, delim_for,
    _opener, load_csv_safe, filter_latest_versions,
    get_korg_override_pkey, extract_version_key,
    filter_invalid_rows,
    find_unique_key_columns_sequential,
    check_schema_col_is_pk, load_copybook_korg_mapping, infer_columns_from_df,
    get_last_3_digits
)

logger = get_logger("vsam_03_type_gate", "vsam_03_type_gate")

MAX_ROWS = int(os.getenv("VSAM_ANALYZE_MAX_ROWS", "0")) or None


def make_columns_unique(cols):
    """
    Ensures every column name in cols is unique.
    If duplicate column names exist in file headers or copybooks,
    appends an incrementing numeric suffix: col, col_1, col_2.
    """
    seen = {}
    out = []
    for c in cols:
        norm_c = norm_col(str(c).strip())
        if norm_c not in seen:
            seen[norm_c] = 0
            out.append(norm_c)
        else:
            seen[norm_c] += 1
            out.append(f"{norm_c}_{seen[norm_c]}")
    return out


def read_file_data(path, has_hdr=True, columns_meta=None, nrows=MAX_ROWS, mode="quality"):
    """
    Safely reads data from an S3 URI or local file into a pandas DataFrame.
    Every file has headers: column names are taken directly from the first line (header=0).
    In 'quality' mode, filters out invalid and sentinel rows (0s, 9s).
    In 'data' mode, preserves 100% of rows for raw truncate-and-load ingestion.
    """
    p_str = str(path)
    delim = delim_for(p_str)
    df = None

    if p_str.startswith("s3://"):
        try:
            import boto3
            bucket = p_str[5:].split("/")[0]
            key = p_str[5:].split("/", 1)[1] if "/" in p_str[5:] else ""
            s3 = boto3.client("s3", region_name=AWS_REGION)
            resp = s3.get_object(Bucket=bucket, Key=key)
            body = resp["Body"]

            if p_str.lower().endswith((".gz", ".gzip")):
                import gzip
                with gzip.GzipFile(fileobj=body, mode="rb") as gz:
                    df = pd.read_csv(
                        gz, sep=delim, nrows=nrows, dtype=str,
                        header=0, keep_default_na=False,
                        on_bad_lines="skip"
                    )
            else:
                df = pd.read_csv(
                    body, sep=delim, nrows=nrows, dtype=str,
                    header=0, keep_default_na=False,
                    on_bad_lines="skip"
                )
        except Exception as e:
            logger.warning(f"Boto3 read failed for {p_str} ({e}); falling back to standard opener...")
            try:
                with _opener(p_str) as f:
                    df = pd.read_csv(
                        f, sep=delim, nrows=nrows, dtype=str,
                        header=0, keep_default_na=False,
                        on_bad_lines="skip"
                    )
            except Exception as e2:
                logger.error(f"Failed to read S3 file {p_str}: {e2}")
                raise
    else:
        lp = Path(p_str)
        if not lp.exists() or lp.stat().st_size == 0:
            return pd.DataFrame()
        try:
            if p_str.lower().endswith((".gz", ".gzip")):
                import gzip
                with gzip.open(lp, "rt", newline="", encoding="utf-8-sig", errors="replace") as f:
                    df = pd.read_csv(
                        f, sep=delim, nrows=nrows, dtype=str,
                        header=0, keep_default_na=False,
                        on_bad_lines="skip"
                    )
            else:
                with open(lp, "r", newline="", encoding="utf-8-sig", errors="replace") as f:
                    df = pd.read_csv(
                        f, sep=delim, nrows=nrows, dtype=str,
                        header=0, keep_default_na=False,
                        on_bad_lines="skip"
                    )
        except Exception as e:
            logger.error(f"Failed to read local file {lp}: {e}")
            raise

    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()

    # Exact column names taken from the first line of the file!
    df.columns = make_columns_unique(df.columns)
    df = filter_invalid_rows(df)
    return df


def resolve_df_key_cols(df_columns, key_cols):
    """
    Maps key_cols to actual column names present in df_columns,
    handling exact match, prefix/suffix alignment, and underscore differences.
    """
    col_map = {}
    df_col_set = set(df_columns)
    for k in key_cols:
        k_norm = norm_col(k)
        if k_norm in df_col_set:
            col_map[k] = k_norm
            continue
        found = False
        k_clean = k_norm.replace("_", "")
        for df_c in df_columns:
            if df_c == k_norm or df_c.replace("_", "") == k_clean:
                col_map[k] = df_c
                found = True
                break
            if len(df_c) >= 3 and len(k_norm) >= 3:
                if df_c.endswith("_" + k_norm) or k_norm.endswith("_" + df_c):
                    col_map[k] = df_c
                    found = True
                    break
        if not found:
            col_map[k] = k_norm
    return col_map


def verify_pk_uniqueness(df, resolved_key_cols):
    """
    Verifies that resolved_key_cols combination is unique across all rows in df,
    with zero null or blank values.
    Returns: (is_unique: bool, null_count: int, dup_count: int, checked_rows: int, message: str, detail: str)
    """
    valid_cols = [c for c in resolved_key_cols if c in df.columns]
    if not valid_cols or len(valid_cols) != len(resolved_key_cols):
        missing = [c for c in resolved_key_cols if c not in df.columns]
        return False, 0, 0, len(df), f"KEY_COLS_MISSING_IN_DATA:{','.join(missing)}", f"Columns missing: {missing}"

    if df.empty:
        return False, 0, 0, 0, "EMPTY_DATASET", "Dataset is empty"

    sub = df[valid_cols]
    null_mask = sub.isnull().any(axis=1) | (sub.astype(str).apply(lambda s: s.str.strip().isin(["", "none", "nan", "null"]))).any(axis=1)
    null_count = int(null_mask.sum())
    if null_count > 0:
        first_null_row = int(null_mask.idxmax()) + 2
        return False, null_count, 0, len(df), f"PK_HAS_NULLS:{null_count}_rows", f"NULL/blank key on row {first_null_row}"

    dup_mask = sub.duplicated(keep=False)
    dup_count = int(sub.duplicated().sum())
    if dup_count > 0:
        dup_sub = sub[dup_mask]
        first_dup_vals = tuple(dup_sub.iloc[0])
        offending_rows = [i + 2 for i, row in sub.iterrows() if tuple(row) == first_dup_vals]
        key_repr = dict(zip(valid_cols, first_dup_vals))
        detail = f"Uniqueness compromised on rows {offending_rows[:4]} with duplicate key: {key_repr}"
        return False, 0, dup_count, len(df), f"PK_NOT_UNIQUE:{dup_count}_duplicates", detail

    return True, 0, 0, len(df), "PK_VALID_UNIQUE", ""


def build_schema_candidates(schema_name):
    """
    Builds set of possible representations for a schema name:
      - schema name itself in lower/upper ('bss01105', 'BSS01105')
      - embedded digit sequences ('01105', '1105')
      - trailing 3 digits ('105', '001', '099')
      - 4-digit zero-padded ('0105', '0001')
      - stripped leading zeroes ('105', '1', '99')
    """
    cand = {schema_name.lower(), schema_name.upper()}
    nums = re.findall(r"\d+", schema_name)
    for n in nums:
        cand.add(n)
        cand.add(n.lstrip("0") or "0")
        if len(n) >= 3:
            cand.add(n[-3:])
            cand.add(n[-3:].lstrip("0") or "0")
            cand.add(n[-3:].zfill(4))
        cand.add(n.zfill(4))
    return cand


def detect_schema_col(df, target_schemas):
    """
    Dynamically finds the column whose values match/discriminate the target schemas.
    Runs for ALL files (single-schema as well as multi-schema mappings).
    Evaluates against schema candidates (e.g. trailing digits, stripped zeroes, full name).
    Returns (col_name, {target_schema: discriminator_val}) or (None, {}).
    """
    if not target_schemas or df is None or df.empty:
        return None, {}

    schema_cands = {s: build_schema_candidates(s) for s in target_schemas}
    all_cands = set().union(*schema_cands.values())

    best_col = None
    best_map = {}
    best_score = -1.0

    col_keywords = ("org", "korg", "schema", "comp", "company", "co")

    for col in df.columns:
        val = df[col]
        col_series = val.iloc[:, 0] if isinstance(val, pd.DataFrame) else val
        s_vals = col_series.dropna().astype(str).str.strip()
        if s_vals.empty:
            continue

        unique_vals = set(s_vals.unique())

        val_map = {}
        for s in target_schemas:
            cands = schema_cands[s]
            direct = unique_vals.intersection(cands)
            if direct:
                val_map[s] = next(iter(direct))
            else:
                for u in unique_vals:
                    if len(u) >= 3 and u[-3:] in cands:
                        val_map[s] = u[-3:]
                        break
                    elif (u.lstrip("0") or "0") in cands:
                        val_map[s] = (u.lstrip("0") or "0")
                        break

        if not val_map:
            continue

        if len(target_schemas) > 1 and len(set(val_map.values())) < len(val_map):
            continue

        matches = s_vals.apply(lambda v: v in all_cands or (len(v) >= 3 and v[-3:] in all_cands) or (v.lstrip("0") or "0") in all_cands)
        coverage = matches.sum() / len(s_vals)

        score = coverage * (len(val_map) / len(target_schemas))
        c_lower = str(col).lower()
        if any(kw in c_lower for kw in col_keywords):
            score += 0.5

        if len(val_map) >= min(1, len(target_schemas)) and score > best_score and coverage > 0.3:
            best_col = col
            best_map = val_map
            best_score = score

    return best_col, best_map


def detect_type_overrides(df, columns_meta):
    """
    Profiles actual column values using regexes (NO column name hardcoding) to:
      1. Detect date formats:
         - Values matching YYYY-MM-DD -> DATE datatype
         - Values matching YYYY_MM_DD -> VARCHAR(10) datatype
      2. Dynamic Length Auto-Widening:
         - If actual max string length > declared varchar/char length, auto-widen.
    Returns: dict of {col_name: overridden_sql_type}
    """
    overrides = {}
    if df is None or df.empty or not columns_meta:
        return overrides

    for c in columns_meta:
        col_name = c["col_name"]
        orig_type = c.get("sql_type", "text")
        if col_name not in df.columns:
            continue

        val = df[col_name]
        col_series = val.iloc[:, 0] if isinstance(val, pd.DataFrame) else val
        s_clean = col_series.dropna().astype(str).str.strip()
        s_non_empty = s_clean[s_clean != ""]
        if s_non_empty.empty:
            continue

        total_cnt = len(s_non_empty)

        # 1. Regex date detection (NO hardcoded column names)
        # Check standard ISO hyphen date YYYY-MM-DD
        match_hyp = int(s_non_empty.str.fullmatch(r"^\d{4}-\d{2}-\d{2}$").sum())
        # Check underscore date YYYY_MM_DD (e.g. 2026_09_21, sentinels 9999_99_99)
        match_und = int(s_non_empty.str.fullmatch(r"^\d{4}_\d{2}_\d{2}$").sum())

        if match_hyp / total_cnt >= 0.8:
            overrides[col_name] = "date"
        elif match_und / total_cnt >= 0.8:
            overrides[col_name] = "varchar(10)"
        else:
            # 2. Dynamic Length Auto-Widening for Varchar / Char
            curr_type = orig_type.lower()
            m = re.search(r"\((\d+)\)", curr_type)
            if m:
                decl_len = int(m.group(1))
                actual_max = int(s_non_empty.str.len().max())
                if actual_max > decl_len:
                    widened_len = max(decl_len, actual_max + 4)
                    overrides[col_name] = f"varchar({widened_len})"

    return overrides


def validate_types(df, columns_meta, type_overrides=None):
    """
    Check actual data against expected SQL types, using type_overrides if present.
    Returns list of {col, type, bad_count, sample} dicts.
    """
    issues = []
    if df is None or df.empty or not columns_meta:
        return issues

    type_overrides = type_overrides or {}

    for c in columns_meta:
        name = c["col_name"]
        t = type_overrides.get(name, c["sql_type"])
        if name not in df.columns:
            continue
        val = df[name]
        col_series = val.iloc[:, 0] if isinstance(val, pd.DataFrame) else val
        s = col_series.dropna().astype(str).str.strip()
        s = s[s != ""]
        if s.empty:
            continue
        bad_mask = None
        if t in ("integer", "bigint"):
            bad_mask = ~s.str.fullmatch(r"-?\d+")
        elif t.startswith("numeric"):
            bad_mask = ~s.str.fullmatch(r"-?(\d+(\.\d*)?|\.\d+)")
        elif t.startswith("varchar") or t.startswith("char"):
            vlen = re.search(r"\((\d+)\)", t)
            if vlen:
                bad_mask = s.str.len() > int(vlen.group(1))
        elif t == "date":
            bad_mask = ~s.str.fullmatch(r"\d{4}-\d{2}-\d{2}")
        elif t.startswith("timestamp"):
            bad_mask = ~s.str.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?")

        if bad_mask is not None and bad_mask.any():
            bad_count = int(bad_mask.sum())
            sample = s[bad_mask].head(3).tolist()
            issues.append({
                "col_name": name,
                "sql_type": t,
                "bad_count": bad_count,
                "sample_bad_values": str(sample)
            })
    return issues


def profile_source(source_key, rows, key_cols, target_schemas, columns_meta, mode="quality", korg_map=None, target_table=None, cb_key=None):
    """
    Profile a single source file group (possibly multiple versions).
    Reads ONLY the latest file version for evaluation, then returns the result
    to be applied across all file versions in the group.
    """
    co, src = source_key.split("|", 1) if "|" in source_key else ("", source_key)
    is_agiletic = "AGILETIC" in str(co).upper()

    # Sort files to find the latest version
    sorted_rows = sorted(
        rows,
        key=lambda r: extract_version_key(r.get("s3_filename", "") or r.get("full_path", "")),
        reverse=True
    )
    latest_row = sorted_rows[0]
    first_fn = latest_row.get("s3_filename", "") or Path(latest_row["full_path"]).name

    # Priority 2: If key_cols is empty, check korg_overrides in vsam_master_overrides.xlsx
    if not key_cols:
        override_pkeys = get_korg_override_pkey(co, src, first_fn)
        if override_pkeys:
            key_cols = override_pkeys
            logger.info(f"  [OVERRIDE] Using primary key from korg_overrides for {source_key}: {', '.join(key_cols)}")

    result = {
        "source_key": source_key,
        "mode": mode,
        "files": len(rows),
        "latest_filename": first_fn,
        "status": "OK",
        "issues": [],
        "pk_valid": False,
        "pk_cols": list(key_cols),
        "pk_type": "NONE",
        "schema_col": None,
        "schema_col_is_pk": "NO_SCHEMA_COL",
        "schema_disc_map": {},
        "type_issues": [],
        "type_overrides": {},
        "rows_total": 0
    }

    if key_cols:
        result["pk_type"] = "SINGLE" if len(key_cols) == 1 else f"COMPOSITE({len(key_cols)})"

    # Read data from latest version (every file has headers on line 1)
    # If the latest version has no data (or only blank rows), try previous version(s)
    df = pd.DataFrame()
    for cand_row in sorted_rows:
        cand_path = cand_row["full_path"]
        try:
            cand_df = read_file_data(cand_path, columns_meta=columns_meta, nrows=MAX_ROWS, mode=mode)
            if not cand_df.empty:
                df = cand_df
                latest_row = cand_row
                first_fn = cand_row.get("s3_filename", "") or Path(cand_row["full_path"]).name
                result["latest_filename"] = first_fn
                break
        except Exception as e:
            logger.warning(f"Could not read {cand_path} ({e}); trying next version if available...")

    total_rows = len(df)
    result["rows_total"] = total_rows

    if df.empty:
        result["issues"].append("EMPTY_NO_DATA")
        result["status"] = "EMPTY_NO_DATA"
        result["pk_valid"] = False
        result["pk_cols"] = []
        result["pk_type"] = "NONE"
        result["schema_col"] = None
        result["schema_col_is_pk"] = "NO_SCHEMA_COL"
        return result

    if is_agiletic:
        # AGILETIC: Directly create column names and datatype by inferring from S3 CSV file and data
        inferred_cols = infer_columns_from_df(df)
        result["columns"] = inferred_cols
        result["type_overrides"] = {c["col_name"]: c["sql_type"] for c in inferred_cols}
        result["issues"].append(f"AGILETIC_INFERRED_COLUMNS:{len(inferred_cols)}_columns")
        logger.info(f"  [AGILETIC] Inferred {len(inferred_cols)} columns directly from CSV data for {source_key}")

        # Only get key columns from mapped copybook. If no copybook, auto detect key columns in positional order.
        has_copybook = bool(columns_meta)
        if has_copybook and key_cols:
            result["extracted_pk_cols"] = list(key_cols)
            col_map = resolve_df_key_cols(df.columns, key_cols)
            actual_key_cols = [col_map[k] for k in key_cols if k in col_map]
            if len(actual_key_cols) == len(key_cols):
                is_unique, null_cnt, dup_cnt, checked_rows, msg, detail = verify_pk_uniqueness(df, actual_key_cols)
                if is_unique and null_cnt == 0:
                    result["pk_valid"] = True
                    result["pk_cols"] = list(key_cols)
                    result["pk_type"] = "SINGLE" if len(key_cols) == 1 else f"COMPOSITE({len(key_cols)})"
                    result["issues"].append(msg)
                else:
                    result["pk_valid"] = False
                    result["pk_cols"] = []
                    result["pk_type"] = "NO_PK"
                    comp_msg = f"PK_COMPROMISED: {detail}" if detail else msg
                    result["issues"].append(comp_msg)
            else:
                missing = [k for k in key_cols if k not in col_map]
                result["pk_valid"] = False
                result["pk_cols"] = []
                result["pk_type"] = "NO_PK"
                result["issues"].append(f"KEY_COLS_MISSING_IN_DATA:{','.join(missing)}")
        else:
            discovered = find_unique_key_columns_sequential(df, max_cols=8)
            result["extracted_pk_cols"] = list(discovered) if discovered else []
            if discovered:
                result["pk_valid"] = True
                result["pk_cols"] = list(discovered)
                result["pk_type"] = "SINGLE" if len(discovered) == 1 else f"COMPOSITE({len(discovered)})"
                result["issues"].append(f"KEY_AUTO_DISCOVERED({len(discovered)}): {', '.join(discovered)}")
                logger.info(f"  [AGILETIC-AUTO-PK] Found sequential unique key for {source_key}: {', '.join(discovered)}")
            else:
                result["pk_valid"] = False
                result["pk_cols"] = []
                result["pk_type"] = "NO_PK"
                result["issues"].append("NO_UNIQUE_DATA_UPTO_8_COLS")
    else:
        # Other source companies: follow existing process
        has_copybook = bool(columns_meta)
        if has_copybook:
            result["extracted_pk_cols"] = list(key_cols) if key_cols else []
            if key_cols:
                col_map = resolve_df_key_cols(df.columns, key_cols)
                actual_key_cols = [col_map[k] for k in key_cols if k in col_map]
                if len(actual_key_cols) == len(key_cols):
                    is_unique, null_cnt, dup_cnt, checked_rows, msg, detail = verify_pk_uniqueness(df, actual_key_cols)
                    if is_unique and null_cnt == 0:
                        result["pk_valid"] = True
                        result["pk_cols"] = list(key_cols)
                        result["pk_type"] = "SINGLE" if len(key_cols) == 1 else f"COMPOSITE({len(key_cols)})"
                        result["issues"].append(msg)
                    else:
                        result["pk_valid"] = False
                        result["pk_cols"] = []
                        result["pk_type"] = "NO_PK"
                        comp_msg = f"PK_COMPROMISED: {detail}" if detail else msg
                        result["issues"].append(comp_msg)
                        logger.warning(f"  [PK COMPROMISED] Copybook PK {key_cols} failed on {source_key}: {comp_msg}. Table will load WITHOUT PK.")
                else:
                    missing = [k for k in key_cols if k not in col_map]
                    result["pk_valid"] = False
                    result["pk_cols"] = []
                    result["pk_type"] = "NO_PK"
                    result["issues"].append(f"KEY_COLS_MISSING_IN_DATA:{','.join(missing)}")
            else:
                result["pk_valid"] = False
                result["pk_cols"] = []
                result["pk_type"] = "NO_PK"
                result["issues"].append("NO_PK_IN_COPYBOOK")
        else:
            # Copybook NOT present -> Sequential positional auto-discovery up to 8 columns
            discovered = find_unique_key_columns_sequential(df, max_cols=8)
            result["extracted_pk_cols"] = list(discovered) if discovered else []
            if discovered:
                result["pk_valid"] = True
                result["pk_cols"] = list(discovered)
                result["pk_type"] = "SINGLE" if len(discovered) == 1 else f"COMPOSITE({len(discovered)})"
                result["issues"].append(f"KEY_AUTO_DISCOVERED({len(discovered)}): {', '.join(discovered)}")
                logger.info(f"  [AUTO-DISCOVERY] Found sequential unique key for {source_key}: {', '.join(discovered)}")
            else:
                result["pk_valid"] = False
                result["pk_cols"] = []
                result["pk_type"] = "NO_PK"
                result["issues"].append("NO_UNIQUE_DATA_UPTO_8_COLS")

        # Dynamic Datatype Profiling (Regex dates + Dynamic Varchar Auto-Widening)
        if columns_meta:
            type_overrides = detect_type_overrides(df, columns_meta)
            result["type_overrides"] = type_overrides
            if type_overrides:
                retyped_desc = [f"{c}->{t}" for c, t in type_overrides.items()]
                result["issues"].append(f"DYNAMIC_TYPES:{len(type_overrides)}_columns({', '.join(retyped_desc[:5])})")
                logger.info(f"  [DYNAMIC-TYPES] {source_key}: {len(type_overrides)} column type adjustment(s) applied ({', '.join(retyped_desc[:5])})")

            type_issues = validate_types(df, columns_meta, type_overrides)
            result["type_issues"].extend(type_issues)
            if type_issues:
                result["issues"].append(f"TYPE_MISMATCHES:{len(type_issues)}_columns")

            effective_cols = []
            for c in columns_meta:
                c_eff = dict(c)
                cn = c_eff.get("col_name", "")
                if cn in type_overrides:
                    c_eff["sql_type"] = type_overrides[cn]
                effective_cols.append(c_eff)
            result["columns"] = effective_cols
        else:
            # Copybook is missing: infer column metadata and types directly from file headers & data
            inferred_cols = infer_columns_from_df(df)
            result["columns"] = inferred_cols
            result["type_overrides"] = {c["col_name"]: c["sql_type"] for c in inferred_cols}
            result["issues"].append(f"HEADER_INFERRED_COLUMNS:{len(inferred_cols)}_columns")
            logger.info(f"  [MISSING-COPYBOOK] Inferred {len(inferred_cols)} columns from file headers for {source_key}")

    # Schema Column Extraction: extracted from "korg" column in copybook_mapping sheet
    korg_cand = None
    if korg_map:
        korg_cand = (
            korg_map.get(src.upper())
            or korg_map.get(Path(first_fn).stem.upper())
            or (korg_map.get(cb_key.upper()) if cb_key else None)
            or (korg_map.get(str(target_table).lower()) if target_table else None)
            or korg_map.get(src.lower())
            or korg_map.get(Path(first_fn).stem.lower())
        )
    if not korg_cand:
        for c in df.columns:
            c_norm = norm_col(c)
            if c_norm in ("b_korg", "korg") or c_norm.endswith("_korg"):
                korg_cand = c
                break

    schema_col_found = None
    is_pk_status = "NO_SCHEMA_COL"

    if korg_cand:
        k_norm = norm_col(korg_cand)
        for c in df.columns:
            if norm_col(c) == k_norm:
                schema_col_found = c
                break
        if schema_col_found:
            cand_pks = result.get("extracted_pk_cols") or result.get("pk_cols") or []
            pk_check = check_schema_col_is_pk(schema_col_found, cand_pks)
            is_pk_status = f"YES ({pk_check})" if pk_check != "NO" else "NO"
        else:
            schema_col_found = None
            is_pk_status = "NO_SCHEMA_COL"

    has_copybook = bool(columns_meta and cb_key and cb_key != "NO_COPYBOOK")

    # RULE 1: If there is no copybook and no korg mapping, directly load it in the bcs schema.
    if not has_copybook and not schema_col_found:
        result["target_schemas"] = ["bcs"]
        result["schema_col"] = None
        result["schema_col_is_pk"] = "NO_SCHEMA_COL"
        result["schema_disc_map"] = {}
        result["issues"].append("DIRECT_BCS_LOAD:NO_COPYBOOK_NO_KORG")
        logger.info(f"  [DIRECT-BCS] No copybook and no korg for {source_key}: mapped directly to 'bcs' schema.")

    # RULE 2: If there is no copybook but korg column is present, segregate records and insert based on korg values.
    elif not has_copybook and schema_col_found:
        result["target_schemas"] = list(target_schemas) if target_schemas else ["bcs"]
        result["schema_col"] = schema_col_found
        result["schema_col_is_pk"] = is_pk_status
        schema_disc_map = {}
        for s in result["target_schemas"]:
            schema_disc_map[s] = get_last_3_digits(s)
        result["schema_disc_map"] = schema_disc_map
        result["issues"].append(f"SEGREGATED_BY_KORG:{schema_col_found}")
        logger.info(f"  [SEGREGATED-KORG] No copybook for {source_key}, but korg column '{schema_col_found}' present: segregating records by korg.")

    else:
        # Copybook is present
        if schema_col_found:
            result["target_schemas"] = list(target_schemas) if target_schemas else ["bcs"]
            result["schema_col"] = schema_col_found
            result["schema_col_is_pk"] = is_pk_status
            schema_disc_map = {}
            for s in result["target_schemas"]:
                schema_disc_map[s] = get_last_3_digits(s)
            result["schema_disc_map"] = schema_disc_map
        else:
            # Copybook present but NO korg: load directly to bcs (or single mapped schema)
            result["target_schemas"] = ["bcs"] if len(target_schemas) > 1 else list(target_schemas)
            result["schema_col"] = None
            result["schema_col_is_pk"] = "NO_SCHEMA_COL"
            result["schema_disc_map"] = {}
            result["issues"].append("DIRECT_BCS_LOAD:NO_KORG")

    return result


def main():
    parser = argparse.ArgumentParser(description="Step 03: Type Gate, PK verification and schema column detection.")
    parser.add_argument("--workers", type=int, default=int(os.getenv("VSAM_WORKERS", str(min(4, os.cpu_count() or 1)))))
    parser.add_argument("--company", help="Comma-separated company tokens to filter")
    parser.add_argument("--latest-only", "--latest-version-only", action="store_true", help="Process only latest file version per table")
    parser.add_argument("--bg", action="store_true", help="Run in background")
    args = parser.parse_args()

    logger.info("=" * 75)
    logger.info("SWEEP VSAM MIGRATION - STEP 03: TYPE GATE & PK VERIFICATION")
    logger.info("=" * 75)

    # Load inventory from Step 02
    lm_file = OUT / "vsam_02_load_matrix.csv" if (OUT / "vsam_02_load_matrix.csv").exists() else OUT / "load_matrix.csv"
    lm = load_csv_safe(lm_file)

    if args.company:
        cos = [c.strip().upper() for c in args.company.split(",")]
        lm = lm[lm["source_company"].isin(cos)]

    if args.latest_only and not lm.empty:
        orig_count = len(lm)
        lm = filter_latest_versions(lm, ["target_schema", "target_table"])
        logger.info(f"Filtered to latest file versions: {len(lm)} tasks (was {orig_count})")

    # Load copybook field metadata from Step 01
    cb_csv = OUT / "vsam_01_copybook_extracts.csv"
    cb_meta = {}  # copybook_name -> [field dict]
    if cb_csv.exists():
        cb_df = pd.read_csv(cb_csv, dtype=str).fillna("")
        for cb_name, grp in cb_df.groupby("copybook_file"):
            cb_key = cb_name.upper().split(".")[0]
            cb_meta[cb_key] = grp.to_dict(orient="records")

    # Load korg column mapping from copybook_mapping sheet in vsam_master_mapping.xlsx
    korg_map = load_copybook_korg_mapping()

    src_col = "source_filename" if "source_filename" in lm.columns else "source_file"
    all_results = {}
    type_gate_summary_rows = []
    gate_rows = []
    pk_rows = []
    schema_col_rows = []
    type_issue_rows = []

    total = len(lm.groupby(["source_company", src_col]))
    done = 0

    for (co, src), grp in lm.groupby(["source_company", src_col]):
        unique_files = grp.drop_duplicates("full_path")
        rows_meta = unique_files.to_dict(orient="records")

        # Key columns from load_matrix
        key_cols_raw = grp["key_columns"].iloc[0] if "key_columns" in grp.columns else ""
        key_cols = [norm_col(c.strip()) for c in re.split(r"[,;+]", key_cols_raw) if c.strip()] if key_cols_raw else []

        # Target table
        target_table = grp["target_table"].iloc[0] if "target_table" in grp.columns else ""

        # Copybook field metadata
        cb_name_raw = grp["copybook_map"].iloc[0] if "copybook_map" in grp.columns else ""
        cb_key = cb_name_raw.upper().split(".")[0] if cb_name_raw else ""
        columns_meta = cb_meta.get(cb_key, [])

        target_schemas = grp["target_schema"].dropna().unique().tolist()
        source_key_std = f"{co}|{src}"
        source_key_display = f"{co} | {src}"

        logger.info(f"[{done+1}/{total}] Profiling {source_key_display} ({len(rows_meta)} file version(s), pk={key_cols or 'none'}, schemas={target_schemas})")
        t0 = time.time()
        try:
            result = profile_source(
                source_key_std, rows_meta, key_cols, target_schemas, columns_meta,
                korg_map=korg_map, target_table=target_table, cb_key=cb_key
            )
        except Exception as e:
            logger.exception(f"Error profiling {source_key_std}: {e}")
            result = {
                "source_key": source_key_std, "status": "BLOCKED", "issues": [str(e)[:200]],
                "pk_valid": False, "pk_cols": key_cols, "extracted_pk_cols": key_cols, "pk_type": "ERROR",
                "schema_col": None, "schema_disc_map": {}, "type_issues": [], "rows_total": 0, "files": len(rows_meta)
            }
        elapsed = round(time.time() - t0, 2)
        all_results[source_key_std] = result
        all_results[source_key_display] = result
        done += 1

        if result["pk_valid"] and result["pk_cols"]:
            new_pk_str = ", ".join(result["pk_cols"])
            if new_pk_str != key_cols_raw:
                lm.loc[(lm["source_company"] == co) & (lm[src_col] == src), "key_columns"] = new_pk_str
        else:
            # Table has no valid PK constraint: clear key_columns so downstream knows it has NO PK
            lm.loc[(lm["source_company"] == co) & (lm[src_col] == src), "key_columns"] = ""

        status = result["status"]
        logger.info(f"  -> [{status}] pk_valid={result['pk_valid']} rows={result['rows_total']} schema_col={result['schema_col']} ({result.get('schema_col_is_pk')}) map={result['schema_disc_map']} ({elapsed}s)")

        # APPLY IDENTIFIED RESULTS TO ALL VERSIONS OF THIS FILE
        sorted_files = sorted(
            rows_meta,
            key=lambda r: extract_version_key(r.get("s3_filename", "") or r.get("full_path", "")),
            reverse=True
        )
        latest_file_path = sorted_files[0]["full_path"] if sorted_files else ""

        for file_rec in sorted_files:
            is_latest = "YES" if file_rec["full_path"] == latest_file_path else "NO"
            s3_fn = file_rec.get("s3_filename", "") or Path(file_rec["full_path"]).name
            vdate = file_rec.get("version_date", "")

            # Exact requested columns
            extracted_pk = result.get("extracted_pk_cols") or result.get("pk_cols") or []
            has_pk_val = "YES" if extracted_pk else "NO"
            pk_cols_str = ", ".join(extracted_pk) if extracted_pk else "NONE"
            is_pk_valid_str = "VALID" if result.get("pk_valid") else "NOT_VALID"
            sc = result.get("schema_col")
            has_schema_col_str = "YES" if (sc and str(sc).strip().upper() not in ("", "NONE", "NO_SCHEMA_COL", "NO_KORG")) else "NO"
            schema_col_name = sc if has_schema_col_str == "YES" else "NO_KORG"
            copybook_display = cb_name_raw if (cb_name_raw and cb_name_raw.upper() != "NO_COPYBOOK") else "NO_COPYBOOK"
            effective_schemas = result.get("target_schemas", target_schemas)

            # 1. Primary Sheet with the exact 11 requested columns
            type_gate_summary_rows.append({
                "S3 file name": s3_fn,
                "source key": source_key_display,
                "schema": ", ".join(effective_schemas),
                "copybook": copybook_display,
                "table": target_table,
                "has PK?": has_pk_val,
                "PK Columns": pk_cols_str,
                "is PK valid": is_pk_valid_str,
                "has schema_col?": has_schema_col_str,
                "schema_col": schema_col_name,
                "Full S3 URI": file_rec["full_path"]
            })

            gate_rows.append({
                "source_key": source_key_display,
                "company": co,
                "source_file": src,
                "s3_filename": s3_fn,
                "version_date": vdate,
                "is_latest_version": is_latest,
                "rows_checked": result["rows_total"],
                "status": result["status"],
                "pk_type": result["pk_type"],
                "pk_cols": ", ".join(result["pk_cols"]),
                "pk_valid": "YES" if result["pk_valid"] else "NO",
                "copybook": copybook_display,
                "schema_col": schema_col_name,
                "schema_col_is_pk": result.get("schema_col_is_pk", "NO_SCHEMA_COL"),
                "schema_map": json.dumps(result.get("schema_disc_map", {})),
                "type_issues_count": len(result["type_issues"]),
                "full_path": file_rec["full_path"],
                "issues": " | ".join(result["issues"])
            })

            pk_rows.append({
                "source_key": source_key_display,
                "s3_filename": s3_fn,
                "version_date": vdate,
                "is_latest": is_latest,
                "pk_cols": pk_cols_str,
                "pk_type": result["pk_type"],
                "pk_valid": "YES" if result["pk_valid"] else "NO",
                "rows_checked": result["rows_total"],
                "issues": " | ".join(i for i in result["issues"] if "PK" in i.upper() or "KEY" in i.upper()) or "OK"
            })

        schema_col_rows.append({
            "source_key": source_key_display,
            "company": co,
            "source_file": src,
            "target_schemas": ", ".join(effective_schemas),
            "copybook": copybook_display,
            "schema_col_detected": schema_col_name,
            "schema_col_is_pk": result.get("schema_col_is_pk", "NO_SCHEMA_COL"),
            "pk_type": result["pk_type"],
            "pk_cols": ", ".join(result["pk_cols"]),
            "discriminator_mapping": json.dumps(result.get("schema_disc_map", {})),
            "schemas_count": len(effective_schemas)
        })

        for ti in result["type_issues"]:
            type_issue_rows.append({"source_key": source_key_display, **ti})

    # Save outputs
    OUT.mkdir(parents=True, exist_ok=True)
    write_json("vsam_03_type_gate.json", all_results)
    write_json("type_gate.json", all_results)
    lm.to_csv(OUT / "vsam_02_load_matrix.csv", index=False)
    lm.to_csv(OUT / "load_matrix.csv", index=False)

    status_df = pd.DataFrame(type_gate_summary_rows)
    status_df.to_csv(OUT / "vsam_03_type_gate_status.csv", index=False)

    ts = get_timestamp()
    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_03_type_gate_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generate_excel_report(report_file, {
        "TypeGateSummary": type_gate_summary_rows,
        "GateSummary": gate_rows,
        "PKVerification": pk_rows,
        "SchemaColumnDetect": schema_col_rows,
        "TypeIssues": type_issue_rows if type_issue_rows else [{"info": "No type issues found"}]
    })

    passed = sum(1 for r in gate_rows if r["pk_valid"] == "YES")
    failed = sum(1 for r in gate_rows if r["pk_valid"] == "NO")
    with_schema_col = sum(1 for r in gate_rows if r["schema_col"] and r["schema_col"] != "NONE")
    schema_col_pk = sum(1 for r in gate_rows if "PK" in r.get("schema_col_is_pk", "") and "NOT" not in r.get("schema_col_is_pk", ""))

    logger.info(f"Type Gate Complete: {len(gate_rows)} total file version(s) evaluated across {total} source table(s).")
    logger.info(f"Output CSV   : {OUT / 'vsam_03_type_gate_status.csv'}")
    logger.info(f"Excel Report : {report_file}")

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  source tables profiled     : {total:>10,}",
        f"  total S3 files analyzed    : {len(gate_rows):>10,}",
        f"  PK verified (unique) files : {passed:>10,}",
        f"  PK failed files            : {failed:>10,}",
        f"  schema col detected files  : {with_schema_col:>10,}",
        f"  schema col in PK files     : {schema_col_pk:>10,}",
        f"  type issue columns         : {len(type_issue_rows):>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)


if __name__ == "__main__":
    main()
