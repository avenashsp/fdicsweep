#!/usr/bin/env python3
"""
vsam/common_records.py - Record validation, sentinel handling, key discovery and schema-column detection.
"""
import re
import pandas as pd
import numpy as np
from sweep_common import norm_col
from vsam.common_mapping import get_korg_override_pkey


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
