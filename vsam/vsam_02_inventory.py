#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_02_inventory.py - Step 02: S3 Discovery & VSAM Source-to-Target Mapping
=============================================================================
Maps all S3 VSAM files with tables/schema/copybook using the mapping excels
(vsam_master_mapping.xlsx) and the copybook extracts from Step 01.

Generates:
  - outputs/vsam/vsam_02_inventory.csv
  - outputs/vsam/vsam_02_load_matrix.csv
  - reports/vsam/<db>_vsam_02_inventory_report_<ts>.xlsx
=============================================================================
"""
import sys
import re
import csv
import gzip
import argparse
from pathlib import Path
from collections import defaultdict
import pandas as pd

from sweep_common import (
    VSAM_REPORTS_DIR,
    VSAM_S3_BUCKET,
    AWS_REGION,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    AWS_ERRORS,
    FILE_READ_ERRORS,
    norm_col,
)
from vsam.vsam_common import (
    BASE,
    common_schema_name,
    COPYBOOK_DIR,
    copybook_overrides,
    copybook_ref,
    delim_for,
    extract_version_key,
    filter_invalid_rows,
    filter_latest_versions,
    find_copybook_files,
    find_unique_key_columns_sequential,
    first_row,
    get_korg_override_pkey,
    get_vsam_folders,
    load_master_copybook_info,
    make_columns_unique,
    match_source,
    OUT,
    parse_filename,
    ref_tables,
    S3File,
)

logger = get_logger("vsam_02_inventory", "vsam_02_inventory")


def discover_s3_files(s3_uri, folders):
    """Lists all S3 objects under target prefix handling pagination."""
    if not s3_uri or not s3_uri.startswith("s3://"):
        return []
    bucket_name = s3_uri[5:].split("/")[0]
    prefix = s3_uri[5:].split("/", 1)[1] if "/" in s3_uri[5:] else ""
    s3_files = []
    try:
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        paginator = s3.get_paginator("list_objects_v2")
        logger.info(f"Listing S3 objects from bucket '{bucket_name}' prefix '{prefix}' (region: {AWS_REGION})...")
        for fld in folders:
            fld_prefix = f"{prefix.rstrip('/')}/{fld}/".lstrip("/")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=fld_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.lower().endswith((".csv", ".csv.gz")):
                        uri = f"s3://{bucket_name}/{key}"
                        size = obj.get("Size", 0)
                        mtime = obj.get("LastModified").timestamp() if obj.get("LastModified") else 0
                        s3_files.append((fld, S3File(uri, size=size, mtime=mtime)))
        logger.info(f"Discovered {len(s3_files)} S3 candidate file(s) across {len(folders)} folder(s).")
    except AWS_ERRORS as e:
        logger.warning(f"S3 listing failed ({e}); checking local data directories.")
    return s3_files


def fast_read_first_row(path):
    """
    Fast extraction of the first line from a local file or S3 URI.
    Uses Boto3 byte-range / streaming for S3 to avoid full file download.
    Detects delimiter inline from the first line.
    """
    p_str = str(path)
    line = ""

    # Case 1: S3 object
    if p_str.startswith("s3://"):
        try:
            import boto3
            bucket = p_str[5:].split("/")[0]
            key = p_str[5:].split("/", 1)[1] if "/" in p_str[5:] else ""
            s3 = boto3.client("s3", region_name=AWS_REGION)

            is_gz = p_str.lower().endswith((".gz", ".gzip"))
            if not is_gz:
                # Fetch only first 8KB for uncompressed CSV
                resp = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-8192")
                chunk = resp["Body"].read().decode("utf-8-sig", errors="replace")
                lines = chunk.splitlines()
                if lines:
                    line = lines[0]
            else:
                # Gzip stream: read first decompressed line then terminate HTTP stream immediately
                resp = s3.get_object(Bucket=bucket, Key=key)
                body = resp["Body"]
                try:
                    with gzip.GzipFile(fileobj=body, mode="rb") as gz:
                        raw = gz.readline()
                        line = raw.decode("utf-8-sig", errors="replace").rstrip("\r\n")
                finally:
                    body.close()
        except AWS_ERRORS as e:
            logger.warning(f"Fast S3 read failed for {p_str} ({e}); falling back to standard reader.")
            try:
                return first_row(p_str)
            except (OSError, ValueError, KeyError, UnicodeError, csv.Error):
                return []
    # Case 2: Local file
    else:
        lp = Path(p_str)
        if not lp.exists() or lp.stat().st_size == 0:
            return []
        try:
            if p_str.lower().endswith((".gz", ".gzip")):
                with gzip.open(lp, "rt", newline="", encoding="utf-8-sig", errors="replace") as f:
                    line = f.readline().rstrip("\r\n")
            else:
                with open(lp, "r", newline="", encoding="utf-8-sig", errors="replace") as f:
                    line = f.readline().rstrip("\r\n")
        except FILE_READ_ERRORS as e:
            logger.warning(f"Failed reading local file {lp}: {e}")
            return []

    if not line:
        return []

    # Detect delimiter from line
    counts = {d: line.count(d) for d in (",", "|", "\t", ";")}
    best_delim = max(counts, key=counts.get) if any(counts.values()) else ","
    try:
        row = next(csv.reader([line], delimiter=best_delim))
        if row and row[-1] == "" and line.rstrip().endswith(best_delim):
            row = row[:-1]
        return row
    except (csv.Error, ValueError, IndexError):
        return [c.strip() for c in line.split(best_delim)]


def inspect_file_header(p, cb_cols):
    """
    Extracts the 1st line from a file (local or S3) and compares tokens
    against copybook column names.
    Returns: (status: str, file_tokens: list, matched_cols: list)
    where status is:
      - 'YES': copybook matched header
      - 'NO': copybook present but header did not match
      - 'UNVERIFIED': no copybook available to compare against
    """
    first_line_tokens = fast_read_first_row(str(p))
    file_tokens = [norm_col(x.strip().strip('"').strip("'")) for x in first_line_tokens if x and x.strip()]

    if not cb_cols:
        # User requirement: if there is no copybook for a file, do not mark as 'NO', mark as 'UNVERIFIED'
        return "UNVERIFIED", file_tokens, []

    if not file_tokens:
        return "NO", [], []

    matched_cols = [tok for tok in file_tokens if token_matches_copybook(tok, cb_cols)]
    matched_count = len(matched_cols)

    if matched_count == len(file_tokens) and matched_count > 0:
        return "YES", file_tokens, matched_cols
    elif len(file_tokens) >= 2 and matched_count >= 2 and (matched_count / len(file_tokens)) >= 0.3:
        return "YES", file_tokens, matched_cols
    elif len(file_tokens) == 1 and matched_count == 1:
        return "YES", file_tokens, matched_cols
    else:
        return "NO", file_tokens, matched_cols


def load_copybook_key_map():
    """Load copybook PK field names from Step 01 output."""
    cb_csv = OUT / "vsam_01_copybook_extracts.csv"
    key_map = {}  # copybook_name_upper -> [col_name, ...]
    if cb_csv.exists():
        df = pd.read_csv(cb_csv, dtype=str).fillna("")
        for _, row in df[df["is_key"] == "YES"].iterrows():
            cb = row["copybook_file"].upper().split(".")[0]  # strip .txt
            key_map.setdefault(cb, [])
            if row["col_name"] and row["col_name"] not in key_map[cb] and row["is_filler"] != "YES":
                key_map[cb].append(row["col_name"])
    return key_map


def load_copybook_columns_map():
    """Load all non-filler column names per copybook from Step 01 output."""
    cb_csv = OUT / "vsam_01_copybook_extracts.csv"
    cols_map = {}  # copybook_name_upper -> set of normalized column names
    if cb_csv.exists():
        try:
            df = pd.read_csv(cb_csv, dtype=str).fillna("")
            for _, row in df.iterrows():
                if row.get("is_filler") == "YES":
                    continue
                cb = row["copybook_file"].upper().split(".")[0]
                cols_map.setdefault(cb, set())
                cname = norm_col(row.get("col_name", ""))
                fname = norm_col(row.get("field_name", ""))
                if cname:
                    cols_map[cb].add(cname)
                if fname:
                    cols_map[cb].add(fname)
        except FILE_READ_ERRORS as e:
            logger.warning(f"Error reading {cb_csv}: {e}")
    return cols_map


def token_matches_copybook(token, cb_set):
    """
    Check if a normalized 1st-line token matches any copybook column name.
    Supports exact match, underscore normalization, and prefix/suffix alignment.
    """
    if not token or not cb_set:
        return False
    if token in cb_set:
        return True
    tok_clean = token.replace("_", "")
    for cbc in cb_set:
        if token == cbc:
            return True
        if tok_clean == cbc.replace("_", ""):
            return True
        if len(token) >= 3 and len(cbc) >= 3:
            if token.endswith("_" + cbc) or cbc.endswith("_" + token):
                return True
    return False


def main():
    parser = argparse.ArgumentParser(description="Step 02: Discover and map VSAM files to PostgreSQL tables.")
    parser.add_argument("--s3", default=VSAM_S3_BUCKET, help="Target S3 URI prefix")
    parser.add_argument("--company", help="Comma-separated company tokens to filter")
    parser.add_argument("--latest-only", "--latest-version-only", action="store_true", help="Keep only latest file version per table")
    parser.add_argument("--bg", action="store_true", help="Run in background")
    args = parser.parse_args()

    filter_companies = [c.strip().upper() for c in args.company.split(",")] if args.company else None

    logger.info("=" * 75)
    logger.info("SWEEP VSAM MIGRATION - STEP 02: S3 DISCOVERY & INVENTORY MAPPING")
    logger.info("=" * 75)

    schemas_ref, korg = ref_tables()
    master_info = load_master_copybook_info()
    cbref = copybook_ref()
    cbo = copybook_overrides()
    cb_key_map = load_copybook_key_map()
    cb_cols_map = load_copybook_columns_map()

    inv = []
    loads = []
    latest_headers_records = []

    # 1. Discover folders dynamically
    folders = get_vsam_folders(args.s3)
    s3_discovered = discover_s3_files(args.s3, folders)
    s3_by_folder = defaultdict(list)
    for fld, s3_file in s3_discovered:
        s3_by_folder[fld].append(s3_file)

    # 2. Iterate across discovered folders
    for folder in folders:
        fp = BASE / folder if folder else BASE
        local_files = [x for x in fp.iterdir() if x.is_file() and x.name.lower().endswith((".csv", ".csv.gz"))] if fp.exists() and fp.is_dir() else []
        folder_s3_files = s3_by_folder.get(folder, [])
        all_files = sorted(local_files + folder_s3_files, key=lambda x: x.name)

        if not all_files:
            continue

        logger.info(f"Folder '{folder or '.'}': {len(all_files)} total file(s) ({len(local_files)} local, {len(folder_s3_files)} S3)")

        # Group parseable files by logical table source: (company_token, source_name)
        groups = defaultdict(list)
        for p in all_files:
            parsed = parse_filename(p.name)
            if parsed:
                g_tok, g_src, _ = parsed
                groups[(g_tok.upper(), g_src.upper())].append(p)

        logger.info(f"Analyzing {len(groups)} logical source table(s)...")

        # Header detection cache:
        # Check latest file version first; if latest has NO headers, check previous version.
        header_cache = {}
        header_tokens_cache = {}
        keys_cache = {}

        for (g_tok, g_src), g_files in groups.items():
            if filter_companies and g_tok not in filter_companies:
                continue

            m = match_source(g_tok, g_src, korg)
            m_info = master_info.get(g_src.upper()) or master_info.get(g_src.lower(), {})
            key = (m[0] if m else g_src).upper()
            name = m_info.get("copybook") or cbref.get(key, "") or cbref.get(g_src, "")
            if not name and g_src in cbref.values():
                name = g_src
            files_found = find_copybook_files(name, key) if name else []
            cb_key = (name or "").upper().split(".")[0] if name else ""
            cb_cols = cb_cols_map.get(cb_key) or cb_cols_map.get(g_src, set())

            if not cb_cols and files_found:
                try:
                    from vsam.vsam_01_extract_copybooks import parse_copybook_with_keys
                    target_cb_file = next((f for f in files_found if f.exists()), None)
                    if target_cb_file:
                        fields = parse_copybook_with_keys(target_cb_file)
                        cb_cols = set()
                        for f in fields:
                            if not f.get("is_filler"):
                                cn = norm_col(f.get("col_name", ""))
                                fn = norm_col(f.get("field_name", ""))
                                if cn:
                                    cb_cols.add(cn)
                                if fn:
                                    cb_cols.add(fn)
                        if cb_key:
                            cb_cols_map[cb_key] = cb_cols
                        cb_cols_map[g_src] = cb_cols
                except FILE_READ_ERRORS as e:
                    logger.debug(f"Could not parse copybook {files_found}: {e}")
                    cb_cols = set()

            # Sort versions: latest/newest first
            sorted_v = sorted(g_files, key=lambda f: extract_version_key(f.name), reverse=True)
            latest_file = sorted_v[0]

            # 1. Inspect latest version first
            status, file_tokens, matched_cols = inspect_file_header(latest_file, cb_cols)

            # Every file has headers: take column names from the first line of the file
            final_status = "YES"
            if status == "YES":
                logger.info(f"[{g_tok}/{g_src}] Header confirmed (YES) in latest version '{latest_file.name}' ({len(g_files)} version(s) mapped)")
            elif status == "UNVERIFIED":
                logger.info(f"[{g_tok}/{g_src}] Header confirmed from file (YES - UNVERIFIED copybook) in latest version '{latest_file.name}' ({len(g_files)} version(s) mapped)")
            else:
                logger.info(f"[{g_tok}/{g_src}] Header confirmed from 1st line of latest version '{latest_file.name}' ({len(g_files)} version(s) mapped)")

            header_cache[(g_tok, g_src)] = final_status
            header_tokens_cache[(g_tok, g_src)] = file_tokens

            cb_disp_name = (cb_key or name) if cb_cols else "NO_COPYBOOK"
            latest_headers_records.append({
                "company_token": g_tok,
                "source_filename": g_src,
                "latest_filename": latest_file.name,
                "has_header": final_status,
                "copybook_name": cb_disp_name,
                "copybook_status": "FOUND" if cb_cols else "NO_COPYBOOK",
                "column_count": len(file_tokens),
                "header_columns": ", ".join(file_tokens),
                "matched_copybook_columns": ", ".join(matched_cols),
                "unmatched_columns": ", ".join([c for c in file_tokens if c not in matched_cols]),
                "full_path": str(latest_file)
            })

        # MAJOR CHANGE: Only consider the single latest version for each file
        target_files = []
        for (g_tok, g_src), g_files in groups.items():
            sorted_v = sorted(g_files, key=lambda f: extract_version_key(f.name), reverse=True)
            target_files.append(sorted_v[0])
        parsed_names = {f.name for (g_tok, g_src), g_files in groups.items() for f in g_files}
        for f in all_files:
            if f.name not in parsed_names:
                target_files.append(f)

        for p in target_files:
            issues = []
            parsed = parse_filename(p.name)
            if not parsed:
                inv.append({
                    "folder": folder, "s3_filename": p.name, "full_path": str(p),
                    "company_token": "", "source_company": "", "source_filename": "",
                    "version_date": "", "postgres_table": "", "key_columns": "",
                    "target_schemas": "", "broker_name": "",
                    "has_header": "NO", "copybook_map": "NO_COPYBOOK", "korg_map": "NO_KORG",
                    "copybook_status": "NO", "table_name": "", "file_size_mb": 0,
                    "mapping_status": "REVIEW", "issues": "UNPARSEABLE_FILENAME"
                })
                continue

            tok, src, vdate = parsed
            # Match company dynamically against schema_mapping
            company = tok.upper()
            if company not in schemas_ref:
                # Try finding a matching company case-insensitively
                for c_cand in schemas_ref:
                    if c_cand.upper() == company or c_cand.upper().startswith(company) or company.startswith(c_cand.upper()):
                        company = c_cand
                        break

            if filter_companies and company and company.upper() not in filter_companies:
                continue

            schemas = schemas_ref.get(company, []) if company else []

            # Master mapping info lookup (copybook, table, korg)
            m_info = master_info.get(src.upper()) or master_info.get(src.lower(), {})
            m = match_source(company or tok, src, korg)

            cb_mapped_sheet = m_info.get("copybook") or cbref.get(src.upper(), "") or ""
            if cb_mapped_sheet.upper() in ("NAN", "NONE", "NULL", "NO_COPYBOOK", ""):
                cb_mapped_sheet = ""

            korg_mapped_sheet = m_info.get("korg") or (norm_col(m[2]) if m and m[2] else "")
            if korg_mapped_sheet.upper() in ("NAN", "NONE", "NULL", "NO_KORG", ""):
                korg_mapped_sheet = ""

            tbl_mapped = m_info.get("table") or (m[1] if m else "") or f"stg_{norm_col(src).lower()}_v"

            # Check copybook files on disk
            cb_file = ""
            files_found = []
            key = (m[0] if m else src).upper()
            name = cb_mapped_sheet or cbref.get(key, "") or cbref.get(src, "")
            if not name and src in cbref.values():
                name = src
            if name:
                files_found = find_copybook_files(name, key)

            if key in cbo and cbo[key].upper() in ("NONE", "HEADER"):
                files_found, name = [], ""
            elif key in cbo:
                cp = Path(cbo[key]) if Path(cbo[key]).is_absolute() else COPYBOOK_DIR / cbo[key]
                files_found, name = ([cp], cbo[key]) if cp.exists() else ([], "")

            cb_found = bool(name and files_found and any(f.exists() for f in files_found))
            if cb_found:
                cb_status_yn = "YES"
                cb_file = str(files_found[0])
                cb_map_display = name
            else:
                cb_status_yn = "NO"
                cb_file = ""
                cb_map_display = "NO_COPYBOOK"

            # Detect korg in file header tokens
            tokens_for_file = header_tokens_cache.get((tok.upper(), src.upper()), [])
            if not tokens_for_file and isinstance(p, Path) and p.exists():
                tokens_for_file = [norm_col(x.strip().strip('"').strip("'")) for x in fast_read_first_row(str(p)) if x and x.strip()]

            korg_in_file = None
            if korg_mapped_sheet and korg_mapped_sheet in tokens_for_file:
                korg_in_file = korg_mapped_sheet
            else:
                for t in tokens_for_file:
                    if t in ("b_korg", "korg") or t.endswith("_korg"):
                        korg_in_file = t
                        break

            has_korg_col = bool(korg_mapped_sheet or korg_in_file)
            korg_display = korg_mapped_sheet if korg_mapped_sheet else "NO_KORG"

            # Every file has headers: take column names from the first line of the file
            has_header_str = "YES"

            # Determine key columns using hierarchy:
            # 1. Copybook keys
            # 2. korg_overrides sheet in vsam_master_overrides.xlsx
            # 3. Auto-discovery from data (single or composite up to 8 columns)
            cb_key_name = name.upper().split(".")[0] if name else ""
            if (tok.upper(), src.upper()) in keys_cache:
                final_keys = keys_cache[(tok.upper(), src.upper())]
            else:
                cb_keys = cb_key_map.get(cb_key_name, [])
                override_keys = get_korg_override_pkey(company, src, p.name)
                korg_key_raw = (m[2] or "") if m else ""
                korg_keys = [norm_col(c.strip()) for c in re.split(r"[,;+]", korg_key_raw) if c.strip()] if korg_key_raw else []

                if cb_keys:
                    final_keys = cb_keys
                elif override_keys:
                    final_keys = override_keys
                elif korg_keys:
                    final_keys = korg_keys
                else:
                    final_keys = []
                    # Fallback: Auto-discover ONLY if copybook is not present, using sequential positional search!
                    if isinstance(p, Path) and p.exists() and p.stat().st_size > 0:
                        try:
                            d = delim_for(str(p))
                            sample_df = pd.read_csv(p, sep=d, nrows=5000, header=0, dtype=str)
                            sample_df.columns = make_columns_unique(sample_df.columns)
                            sample_df = filter_invalid_rows(sample_df)
                            final_keys = find_unique_key_columns_sequential(sample_df, max_cols=8)
                        except FILE_READ_ERRORS:
                            final_keys = []
                keys_cache[(tok.upper(), src.upper())] = final_keys

            key_col_display = ", ".join(final_keys)

            # RULE EVALUATION
            # Rule 1: No copybook and No korg -> directly load it in the bcs schema
            if not cb_found and not has_korg_col:
                tgt_schemas = [{"schema": "bcs", "broker": "BCS"}]
                ready = True
                issues.append("DIRECT_BCS_LOAD:NO_COPYBOOK_NO_KORG")
            # Rule 2: No copybook but korg column is present -> segregate records based on korg values
            elif not cb_found and has_korg_col:
                tgt_schemas = schemas if schemas else [{"schema": "bcs", "broker": "BCS"}]
                ready = True
                issues.append(f"SEGREGATED_BY_KORG:{korg_in_file or korg_mapped_sheet}")
            # Case 3: Copybook present and korg present
            elif cb_found and has_korg_col:
                tgt_schemas = schemas if schemas else [{"schema": "bcs", "broker": "BCS"}]
                ready = True
            # Case 4: Copybook present but No korg
            else:
                tgt_schemas = [{"schema": "bcs", "broker": "BCS"}]
                ready = True
                issues.append("DIRECT_BCS_LOAD:NO_KORG")

            common, tname = tbl_mapped.split(".", 1) if "." in tbl_mapped else (None, tbl_mapped)
            common = common_schema_name(common) if common else None
            if common and ready:
                tgt_schemas = [{"schema": common, "broker": "COMMON"}]

            try:
                st = p.stat()
                size_mb = round(st.st_size / (1024 * 1024), 2)
            except OSError:
                size_mb = 0

            inv.append({
                "folder": folder,
                "version_date": vdate.isoformat() if vdate else "",
                "s3_filename": p.name,
                "company_token": tok,
                "source_company": company or "",
                "source_filename": src,
                "has_header": has_header_str,
                "copybook_map": cb_map_display,
                "korg_map": korg_display,
                "copybook_status": cb_status_yn,
                "broker_name": ", ".join(s.get("broker", "") for s in tgt_schemas if s.get("broker")),
                "schema_name": ", ".join(s["schema"] for s in tgt_schemas),
                "table_name": tname or "",
                "postgres_table": tbl_mapped,
                "key_columns": key_col_display,
                "file_size_mb": size_mb,
                "mapping_status": "READY" if ready else "REVIEW",
                "full_path": str(p),
                "copybook_file": cb_file,
                "issues": "|".join(issues)
            })

            if ready:
                for s in tgt_schemas:
                    loads.append({
                        "folder": folder,
                        "s3_filename": p.name,
                        "full_path": str(p),
                        "source_company": company,
                        "source_filename": src,
                        "has_header": has_header_str,
                        "version_date": vdate.isoformat() if vdate else "",
                        "target_schema": s["schema"].lower(),
                        "broker": s.get("broker", ""),
                        "target_table": tname,
                        "key_columns": key_col_display,
                        "size_mb": size_mb,
                        "copybook_file": cb_file,
                        "copybook_map": cb_map_display,
                        "korg_map": korg_display
                    })

    loads = sorted(loads, key=lambda x: (x.get("target_schema", ""), x.get("target_table", ""), x.get("size_mb", 0)))

    inv_cols = [
        "folder", "version_date", "s3_filename", "company_token", "source_company", "source_filename",
        "has_header", "copybook_map", "korg_map", "copybook_status", "broker_name", "schema_name", "table_name",
        "postgres_table", "key_columns", "file_size_mb", "mapping_status", "full_path", "copybook_file", "issues"
    ]
    lm_cols = [
        "folder", "s3_filename", "full_path", "source_company", "source_filename", "has_header", "version_date",
        "target_schema", "broker", "target_table", "key_columns", "size_mb", "copybook_file", "copybook_map", "korg_map"
    ]

    df = pd.DataFrame(inv) if inv else pd.DataFrame(columns=inv_cols)
    lm = pd.DataFrame(loads) if loads else pd.DataFrame(columns=lm_cols)

    if args.latest_only and not lm.empty:
        orig_count = len(lm)
        lm = filter_latest_versions(lm, ["target_schema", "target_table"])
        logger.info(f"Filtered to latest file versions: {len(lm)} tasks (was {orig_count})")

    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / "vsam_02_inventory.csv", index=False)
    df.to_csv(OUT / "inventory.csv", index=False)
    lm.to_csv(OUT / "vsam_02_load_matrix.csv", index=False)
    lm.to_csv(OUT / "load_matrix.csv", index=False)

    hdr_cols = [
        "company_token", "source_filename", "latest_filename", "has_header",
        "copybook_name", "copybook_status", "column_count", "header_columns",
        "matched_copybook_columns", "unmatched_columns", "full_path"
    ]
    hdf = pd.DataFrame(latest_headers_records) if latest_headers_records else pd.DataFrame(columns=hdr_cols)
    hdf.to_csv(OUT / "vsam_02_latest_file_headers.csv", index=False)
    hdf.to_csv(OUT / "latest_file_headers.csv", index=False)
    logger.info(f"Saved latest file headers: {OUT / 'vsam_02_latest_file_headers.csv'} ({len(hdf)} tables)")

    # Excel report - FileInventory with the exact requested columns
    file_inventory_rows = []
    for r in inv:
        file_inventory_rows.append({
            "version date": r.get("version_date", ""),
            "S3 filename": r.get("s3_filename", ""),
            "company token": r.get("company_token", ""),
            "source filename": r.get("source_filename", ""),
            "has header": r.get("has_header", "NO"),
            "copybook map": r.get("copybook_map", "NO_COPYBOOK"),
            "korg mapping": r.get("korg_map", "NO_KORG"),
            "copybook status": r.get("copybook_status", "NO"),
            "broker name": r.get("broker_name", ""),
            "schema name": r.get("schema_name", ""),
            "table name": r.get("table_name", ""),
            "key columns": r.get("key_columns", ""),
            "file size (MB)": r.get("file_size_mb", 0),
            "mapping status": r.get("mapping_status", "REVIEW"),
            "full S3 file path": r.get("full_path", "")
        })

    summary_rows = []
    if inv:
        grp = df.groupby(["company_token", "source_filename"], dropna=False).agg(
            file_count=("s3_filename", "count"),
            company=("source_company", "first"),
            table=("table_name", "first"),
            schemas=("schema_name", "first"),
            key_columns=("key_columns", "first"),
            status=("mapping_status", "first")
        ).reset_index()
        summary_rows = grp.to_dict(orient="records")

    ts = get_timestamp()
    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_02_inventory_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generate_excel_report(report_file, {
        "Summary": summary_rows,
        "FileInventory": file_inventory_rows,
        "LatestFileHeaders": latest_headers_records
    })

    logger.info(f"Inventory Complete: {len(df)} files discovered, {len(lm)} target table mappings planned.")
    logger.info(f"Output CSVs : {OUT / 'vsam_02_inventory.csv'}, {OUT / 'vsam_02_load_matrix.csv'}, {OUT / 'vsam_02_latest_file_headers.csv'}")
    logger.info(f"Excel Report: {report_file}")

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  files discovered           : {len(df):>10,}",
        f"  ready to load              : {sum(1 for r in inv if r['mapping_status'] == 'READY'):>10,}",
        f"  review / unmapped          : {sum(1 for r in inv if r['mapping_status'] != 'READY'):>10,}",
        f"  target table loads planned : {len(lm):>10,}",
        f"  total volume (MB)          : {round(df['file_size_mb'].sum(), 2) if not df.empty and 'file_size_mb' in df.columns else 0:>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
