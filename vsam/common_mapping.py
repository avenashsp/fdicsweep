#!/usr/bin/env python3
"""
vsam/common_mapping.py - Readers for the master mapping / override workbooks and schema-routing helpers.
"""
import csv
import functools
import re
from pathlib import Path
import pandas as pd
from sweep_common import BASE_DIR, FILE_READ_ERRORS, norm_col, VSAM_INPUTS_DIR
from vsam.common_config import MASTER_MAPPING_FILE, MASTER_OVERRIDES_FILE, MASTER_REF, OUT, VERSION_MODE, logger, read_excel_safe


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
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read schema_mapping from {MASTER_MAPPING_FILE}: {exc}")

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
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read copybook_mapping from {MASTER_MAPPING_FILE}: {exc}")

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
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read korg_overrides from {MASTER_OVERRIDES_FILE}: {exc}")

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
    except FILE_READ_ERRORS as exc:
        logger.warning(f"Could not read korg_overrides sheet: {exc}")
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
        except FILE_READ_ERRORS as e:
            logger.warning(f"Could not read active schemas from master mapping: {e}")
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
        except FILE_READ_ERRORS as e:
            logger.warning(f"Could not load copybook_mapping: {e}")
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
        except FILE_READ_ERRORS as e:
            logger.warning(f"Could not load korg from copybook_mapping: {e}")
    return korg_map


def copybook_overrides():
    """Reads copybook_overrides from vsam_master_overrides.xlsx (fallback copybook_overrides.csv)."""
    if MASTER_OVERRIDES_FILE.exists():
        try:
            import pandas as pd
            df = pd.read_excel(MASTER_OVERRIDES_FILE, sheet_name="copybook_overrides", dtype=str).fillna("")
            return {str(r.filename).strip().upper(): str(r.copybook).strip() for r in df.itertuples() if str(getattr(r, "filename", "")).strip()}
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read copybook_overrides: {exc}")
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
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read copybook_mapping: {exc}")
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
        except FILE_READ_ERRORS:
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
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read type_overrides: {exc}")
    if not ov:
        for r in _side_csv("type_overrides.csv"):
            ov[(r["company"].upper(), r["source_file"].upper(), norm_col(r["column"]))] = r["sql_type"].strip()
    return ov


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
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read loadpolicy_overrides: {exc}")
    return {(r["company"].strip().upper(), r["source_file"].strip().upper()): r["policy"].strip().lower()
            for r in _side_csv("load_policy.csv") if r.get("policy")}


def version_mode_for(p):
    pol = load_policy().get(((p.get("company") or "").upper(), (p.get("source_file") or "").upper()))
    return pol if pol in ("snapshot", "upsert") else VERSION_MODE
