#!/usr/bin/env python3
"""
02_s3Inventory.py - S3 Migration Discovery and File-to-Table Mapping
Scans the S3 bucket, pairs objects with Oracle tables, enforces GZIP priority over CSV,
and orders files strictly from SMALLEST to LARGEST file size.

Generates:
  - outputs/s3_file_map.csv
  - reports/02_s3Inventory_report_<timestamp>.xlsx
"""
import os
import re
import sys
import csv
import json
import argparse
import subprocess
from pathlib import Path
from collections import defaultdict
from sweep_common import (
    ORACLE_INPUTS_DIR, ORACLE_OUTPUTS_DIR, ORACLE_REPORTS_DIR, S3_BUCKET, AWS_REGION,
    META_TABLES_CSV, get_logger, get_timestamp, get_db_name, generate_excel_report, norm_col
)

logger = get_logger("oracle_02_inventory", "oracle_02_inventory")


def get_known_tables():
    """Reads oracle_metadata_tables.csv and target PostgreSQL catalog to get master list of known owners and table names."""
    dd_path = META_TABLES_CSV
    known = defaultdict(set)
    if dd_path.exists():
        with open(dd_path, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                o = (r.get("owner") or "").upper().strip()
                t = (r.get("table_name") or "").upper().strip()
                if o and t:
                    known[o].add(t)

    # Also query target PostgreSQL catalog so code schemas (e.g. bss01001_code), errlog tables, and all created tables are known
    try:
        from sweep_common import query_db_dicts
        sql = (
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta')"
        )
        for r in query_db_dicts(sql):
            s = (r.get("table_schema") or "").upper().strip()
            t = (r.get("table_name") or "").upper().strip()
            if s and t:
                known[s].add(t)
    except Exception as e:
        logger.debug(f"Could not load tables from database catalog: {e}")

    return known


def list_s3_objects(s3_uri, region=AWS_REGION):
    """
    Lists all objects under S3 prefix using Boto3 SDK (with fallback to aws s3api).
    Returns list of dicts: {uri, key, size, is_gz, is_csv}
    """
    clean_uri = s3_uri.rstrip("/")
    if not clean_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    parts = clean_uri[5:].split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    logger.info(f"Listing S3 objects from bucket '{bucket}' prefix '{prefix}' (region: {region})...")
    objects = []

    # Attempt 1: Boto3 SDK (fastest, purely in Python, zero subprocess)
    try:
        import boto3
        s3 = boto3.client("s3", region_name=region)
        paginator = s3.get_paginator("list_objects_v2")
        paginate_args = {"Bucket": bucket}
        if prefix:
            paginate_args["Prefix"] = prefix

        for page in paginator.paginate(**paginate_args):
            for item in page.get("Contents", []):
                k = item.get("Key", "")
                sz = int(item.get("Size", 0))
                if not k or k.endswith("/"):
                    continue

                k_lower = k.lower()
                is_gz = k_lower.endswith(".gz")
                is_csv = k_lower.endswith(".csv")
                if is_gz or is_csv:
                    objects.append({
                        "uri": f"s3://{bucket}/{k}",
                        "key": k,
                        "filename": Path(k).name,
                        "size_bytes": sz,
                        "is_gz": is_gz,
                        "is_csv": is_csv
                    })
        logger.info(f"Discovered {len(objects)} candidate S3 file(s) via Boto3 SDK.")
        return objects
    except Exception as be:
        logger.warning(f"Boto3 S3 listing notice: {be}. Attempting AWS CLI fallback...")

    # Attempt 2: Fallback to AWS CLI s3api
    env = os.environ.copy()
    if "/usr/local/lib/python3.9/site-packages" not in env.get("PYTHONPATH", ""):
        env["PYTHONPATH"] = f"/usr/local/lib/python3.9/site-packages:{env.get('PYTHONPATH', '')}"

    continuation_token = None
    while True:
        cmd = [
            "aws", "s3api", "list-objects-v2",
            "--bucket", bucket,
            "--region", region,
            "--output", "json"
        ]
        if prefix:
            cmd.extend(["--prefix", prefix])
        if continuation_token:
            cmd.extend(["--continuation-token", continuation_token])

        res = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if res.returncode != 0:
            raise RuntimeError(f"aws s3api failed: {res.stderr.strip()}")

        data = json.loads(res.stdout)
        contents = data.get("Contents", [])
        for item in contents:
            k = item.get("Key", "")
            sz = int(item.get("Size", 0))
            if not k or k.endswith("/"):
                continue

            k_lower = k.lower()
            is_gz = k_lower.endswith(".gz")
            is_csv = k_lower.endswith(".csv")
            if is_gz or is_csv:
                objects.append({
                    "uri": f"s3://{bucket}/{k}",
                    "key": k,
                    "filename": Path(k).name,
                    "size_bytes": sz,
                    "is_gz": is_gz,
                    "is_csv": is_csv
                })

        if data.get("IsTruncated"):
            continuation_token = data.get("NextContinuationToken")
        else:
            break

    logger.info(f"Discovered {len(objects)} data object(s) in S3.")
    return objects


def match_table(filename, key_path, known_tables):
    """
    Matches an S3 object to an Oracle owner and table name using strict token
    and word-boundary resolution, preventing substring collisions (e.g.
    preventing BSS01105 from matching BSS01105_CODE, or MONTHLYINTEREST from
    matching MONTHLYINTEREST_EXT).
    """
    fname_upper = Path(filename).name.upper()
    stem_upper = Path(filename).stem.upper()
    if stem_upper.endswith(".CSV") or stem_upper.endswith(".GZ"):
        stem_upper = Path(stem_upper).stem.upper()
    path_upper = key_path.upper()

    # Strategy 1: Explicit <OWNER>.<TABLE>.* or <OWNER>.<TABLE>_<TIMESTAMP> / <OWNER>.<TABLE>-<TIMESTAMP>
    dots = stem_upper.split(".")
    if len(dots) >= 2:
        cand_owner = dots[0]
        rest = ".".join(dots[1:])
        base_owner = cand_owner[:-5] if cand_owner.endswith("_CODE") else cand_owner

        # 1a. Exact or prefix match in cand_owner
        if cand_owner in known_tables:
            if rest in known_tables[cand_owner]:
                return cand_owner, rest
            for t in sorted(known_tables[cand_owner], key=len, reverse=True):
                if rest == t or rest.startswith(f"{t}_") or rest.startswith(f"{t}-") or rest.startswith(f"{t}."):
                    return cand_owner, t

        # 1b. Dedicated Error Log Table resolution:
        # e.g. BSS01001_CODE.ERR_ACCOUNTAUDIT_ERRLOG.csv.gz -> bss01001_code, err_accountaudit_errlog
        if "ERRLOG" in rest or rest.startswith("ERR_"):
            m_err = re.match(r"^(ERR_[A-Za-z0-9_$#]+?_ERRLOG)(?:[._-].*)?$", rest)
            err_tbl = m_err.group(1) if m_err else rest
            code_owner = f"{cand_owner}_CODE" if not cand_owner.endswith("_CODE") else cand_owner
            if code_owner in known_tables and err_tbl in known_tables[code_owner]:
                return code_owner, err_tbl
            if cand_owner in known_tables and err_tbl in known_tables[cand_owner]:
                return cand_owner, err_tbl
            if cand_owner in known_tables or base_owner in known_tables:
                return cand_owner, err_tbl

    # Strategy 2: Prefix formatted names: FDICSWP_<OWNER>_<TABLE>_... or <OWNER>_<TABLE>_...
    clean_stem = re.sub(r"^FDICSWP_", "", stem_upper)
    for owner in sorted(known_tables.keys(), key=len, reverse=True):
        if clean_stem.startswith(f"{owner}_"):
            rem = clean_stem[len(owner) + 1:]
            for t in sorted(known_tables[owner], key=len, reverse=True):
                if rem == t or rem.startswith(f"{t}_") or rem.startswith(f"{t}."):
                    return owner, t

    # Strategy 3: S3 folder-path hierarchy: .../<OWNER>/.../<TABLE>...
    tokens = [tok for tok in path_upper.split("/") if tok]
    for tok in tokens:
        if tok in known_tables:
            owner = tok
            for t in sorted(known_tables[owner], key=len, reverse=True):
                tbl_pattern = rf"(?<![A-Z0-9]){re.escape(t)}(?![A-Z0-9])"
                if re.search(tbl_pattern, fname_upper) or f"/{t}/" in path_upper:
                    return owner, t

    # Guard: Error log files must NEVER fuzzy-match to a base table in Strategy 4 or 5
    if "ERRLOG" in fname_upper or re.search(r"(?:^|[._])ERR_", fname_upper):
        return "UNKNOWN", Path(filename).stem.upper()

    # Strategy 4: Strict word-boundary search across all owners (longest owner, longest table first)
    for owner in sorted(known_tables.keys(), key=len, reverse=True):
        owner_pattern = rf"(?<![A-Z0-9_]){re.escape(owner)}(?![A-Z0-9_])"
        if re.search(owner_pattern, path_upper) or re.search(owner_pattern, fname_upper):
            for t in sorted(known_tables[owner], key=len, reverse=True):
                tbl_pattern = rf"(?<![A-Z0-9]){re.escape(t)}(?![A-Z0-9])"
                if re.search(tbl_pattern, fname_upper) or f"/{t}/" in path_upper or f"_{t}_" in fname_upper:
                    return owner, t

    # Strategy 5: Standalone table name in filename (boundary-checked)
    for owner in sorted(known_tables.keys(), key=len, reverse=True):
        for t in sorted(known_tables[owner], key=len, reverse=True):
            tbl_pattern = rf"(?<![A-Z0-9]){re.escape(t)}(?![A-Z0-9])"
            if re.search(tbl_pattern, fname_upper):
                return owner, t

    # Fallback: Parse from filename tokens
    m = re.match(r"^([A-Za-z0-9]+)_([A-Za-z0-9_]+?)_FLAT", fname_upper)
    if m:
        cand_owner, cand_table = m.group(1), m.group(2)
        if cand_owner in known_tables and cand_table in known_tables[cand_owner]:
            return cand_owner, cand_table

    return "UNKNOWN", Path(filename).stem.upper()


def is_chunked_file(filename, table_name=""):
    """
    Determines whether a file is a chunked/split part of a larger table export.
    Matches common chunking patterns:
      - Unix split after .csv: e.g. table.csv_aa.gz, table.csv_ab.gz, table.csv_01.gz
      - Keyword splits: e.g. .part_01.gz, _part1.gz, .chunk01.gz, _split_aa.gz
      - 2-3 letter chunk suffix: e.g. table_aa.gz, table_ab.gz
      - 2-4 digit numeric chunk suffix: e.g. table_00.gz, table_01.gz, table.001.gz
    Table names with short identifiers (e.g. CD, GL, FX) are protected by table boundary checks.
    """
    name = Path(filename).name.lower()
    t_clean = (table_name or "").lower().strip()

    # Strip terminal .gz / .gzip
    stem = re.sub(r'\.gz(?:ip)?$', '', name)

    # 1. Check if it has .csv_aa, .csv_ab, or .csv_01 (Unix split after .csv)
    if re.search(r'\.csv[_-][a-z0-9]+$', stem, re.I):
        return True

    # Strip standard .csv if present
    stem = re.sub(r'\.csv$', '', stem)

    # If the remaining stem ends exactly with the table name, it is the full table, NOT a chunk
    if t_clean and (stem == t_clean or stem.endswith('.' + t_clean) or stem.endswith('_' + t_clean)):
        return False

    # 2. Check part / chunk / split keywords
    if re.search(r'[\._-](?:part|chunk|split)[\._-]?[a-z0-9]+$', stem, re.I):
        return True

    # 3. Check 2-3 letter chunk suffix (_aa, _ab, etc.)
    if re.search(r'[\._-][a-z]{2,3}$', stem, re.I):
        return True

    # 4. Check 2-4 digit numeric chunk suffix (_00, _01, .001)
    if re.search(r'[\._-]\d{2,4}$', stem, re.I):
        return True

    return False


def build_inventory(s3_uri=None, mock_files=None, owners_filter=None, tables_filter=None):
    ts = get_timestamp()
    target_uri = s3_uri or S3_BUCKET
    known = get_known_tables()

    raw_objects = []
    if mock_files:
        raw_objects = mock_files
    elif target_uri:
        raw_objects = list_s3_objects(target_uri)
    else:
        logger.warning("No S3 URI specified and ORACLE_S3_BUCKET not set. Checking local inputs/...")
        # Check local sample files
        for p in ORACLE_INPUTS_DIR.glob("*.*"):
            if p.suffix.lower() in (".gz", ".csv"):
                raw_objects.append({
                    "uri": str(p),
                    "key": p.name,
                    "filename": p.name,
                    "size_bytes": p.stat().st_size,
                    "is_gz": p.suffix.lower() == ".gz",
                    "is_csv": p.suffix.lower() == ".csv"
                })

    if not raw_objects:
        logger.error("No S3 objects found to inventory.")
        return False

    # Group objects by (owner, table) and track ignored / unmapped files
    grouped = defaultdict(list)
    ignored_files = []

    for obj in raw_objects:
        owner, table = match_table(obj["filename"], obj["key"], known)

        owner_base = owner.upper()[:-5] if owner and owner.upper().endswith("_CODE") else (owner.upper() if owner else "")
        owner_is_known = bool(owner and (owner in known or owner_base in known))
        table_is_known = bool(
            owner and (
                table in known.get(owner, set())
                or (owner_is_known and ("ERRLOG" in str(table) or str(table).startswith("ERR_")))
            )
        )

        if not owner or owner == "UNKNOWN" or (known and not owner_is_known) or (known and not table_is_known):
            ignored_files.append({
                "uri": obj["uri"],
                "filename": obj["filename"],
                "size_bytes": obj["size_bytes"],
                "size_mb": round(obj["size_bytes"] / (1024**2), 2),
                "detected_owner": owner or "UNKNOWN",
                "detected_table": table or "UNKNOWN",
                "reason_ignored": "Not mapped to any known table in Oracle metadata or PostgreSQL catalog"
            })
            continue

        if owners_filter and owner.upper() not in owners_filter and owner_base not in owners_filter:
            ignored_files.append({
                "uri": obj["uri"],
                "filename": obj["filename"],
                "size_bytes": obj["size_bytes"],
                "size_mb": round(obj["size_bytes"] / (1024**2), 2),
                "detected_owner": owner,
                "detected_table": table,
                "reason_ignored": f"Excluded by --owners filter ({', '.join(sorted(owners_filter))})"
            })
            continue

        if tables_filter and f"{owner}.{table}".upper() not in tables_filter and table.upper() not in tables_filter:
            ignored_files.append({
                "uri": obj["uri"],
                "filename": obj["filename"],
                "size_bytes": obj["size_bytes"],
                "size_mb": round(obj["size_bytes"] / (1024**2), 2),
                "detected_owner": owner,
                "detected_table": table,
                "reason_ignored": "Excluded by --tables filter"
            })
            continue

        grouped[(owner, table)].append(obj)

    # Apply Rules:
    # Rule 1: The loader should take only the gzip files, decompress it and stream it to load.
    # Rule 2: If there are no matching gzip files, take the matching csv files and stream it.
    final_selected_files = []
    volume_by_table = []

    for (owner, table), file_list in grouped.items():
        gz_files = [f for f in file_list if f["is_gz"]]
        csv_files = [f for f in file_list if f["is_csv"] and not f["is_gz"]]

        if gz_files:
            chosen_format = "gzip"
            consolidated_gz = [f for f in gz_files if not is_chunked_file(f["filename"], table)]
            chunked_gz = [f for f in gz_files if is_chunked_file(f["filename"], table)]

            if consolidated_gz:
                # Priority 1: Consolidated GZIP files present.
                # Deduplicate only files with identical basenames across different S3 prefixes,
                # while preserving all distinct files (e.g. timestamped files, delta snapshots).
                dedup_map = {}
                for f in consolidated_gz:
                    fn = f["filename"]
                    if fn not in dedup_map or f["size_bytes"] > dedup_map[fn]["size_bytes"]:
                        if fn in dedup_map:
                            dup = dedup_map[fn]
                            ignored_files.append({
                                "uri": dup["uri"],
                                "filename": dup["filename"],
                                "size_bytes": dup["size_bytes"],
                                "size_mb": round(dup["size_bytes"] / (1024**2), 2),
                                "detected_owner": owner,
                                "detected_table": table,
                                "reason_ignored": f"Duplicate key across prefixes (superseded by {f['uri']})"
                            })
                        dedup_map[fn] = f
                    else:
                        ignored_files.append({
                            "uri": f["uri"],
                            "filename": f["filename"],
                            "size_bytes": f["size_bytes"],
                            "size_mb": round(f["size_bytes"] / (1024**2), 2),
                            "detected_owner": owner,
                            "detected_table": table,
                            "reason_ignored": f"Duplicate key across prefixes (superseded by {dedup_map[fn]['uri']})"
                        })
                chosen_files = sorted(dedup_map.values(), key=lambda x: x["filename"])

                # Ignore chunked GZIP files
                for unchosen in chunked_gz:
                    ignored_files.append({
                        "uri": unchosen["uri"],
                        "filename": unchosen["filename"],
                        "size_bytes": unchosen["size_bytes"],
                        "size_mb": round(unchosen["size_bytes"] / (1024**2), 2),
                        "detected_owner": owner,
                        "detected_table": table,
                        "reason_ignored": f"Chunked GZIP ignored (single consolidated file selected: {chosen_files[0]['filename']})"
                    })

                # Ignore uncompressed CSV files
                for unchosen in csv_files:
                    ignored_files.append({
                        "uri": unchosen["uri"],
                        "filename": unchosen["filename"],
                        "size_bytes": unchosen["size_bytes"],
                        "size_mb": round(unchosen["size_bytes"] / (1024**2), 2),
                        "detected_owner": owner,
                        "detected_table": table,
                        "reason_ignored": f"Uncompressed CSV ignored (single consolidated GZIP file selected: {chosen_files[0]['filename']})"
                    })
            else:
                # Priority 2: No consolidated GZIP file present -> take ALL chunked GZIP files
                chosen_files = chunked_gz
                for unchosen in csv_files:
                    ignored_files.append({
                        "uri": unchosen["uri"],
                        "filename": unchosen["filename"],
                        "size_bytes": unchosen["size_bytes"],
                        "size_mb": round(unchosen["size_bytes"] / (1024**2), 2),
                        "detected_owner": owner,
                        "detected_table": table,
                        "reason_ignored": f"Uncompressed CSV ignored ({len(chunked_gz)} chunked GZIP files selected)"
                    })
        else:
            # Priority 3: No GZIP files at all -> fall back to matching CSV files
            chosen_format = "csv"
            consolidated_csv = [f for f in csv_files if not is_chunked_file(f["filename"], table)]
            chunked_csv = [f for f in csv_files if is_chunked_file(f["filename"], table)]

            if consolidated_csv:
                dedup_map = {}
                for f in consolidated_csv:
                    fn = f["filename"]
                    if fn not in dedup_map or f["size_bytes"] > dedup_map[fn]["size_bytes"]:
                        if fn in dedup_map:
                            dup = dedup_map[fn]
                            ignored_files.append({
                                "uri": dup["uri"],
                                "filename": dup["filename"],
                                "size_bytes": dup["size_bytes"],
                                "size_mb": round(dup["size_bytes"] / (1024**2), 2),
                                "detected_owner": owner,
                                "detected_table": table,
                                "reason_ignored": f"Duplicate key across prefixes (superseded by {f['uri']})"
                            })
                        dedup_map[fn] = f
                    else:
                        ignored_files.append({
                            "uri": f["uri"],
                            "filename": f["filename"],
                            "size_bytes": f["size_bytes"],
                            "size_mb": round(f["size_bytes"] / (1024**2), 2),
                            "detected_owner": owner,
                            "detected_table": table,
                            "reason_ignored": f"Duplicate key across prefixes (superseded by {dedup_map[fn]['uri']})"
                        })
                chosen_files = sorted(dedup_map.values(), key=lambda x: x["filename"])

                for unchosen in chunked_csv:
                    ignored_files.append({
                        "uri": unchosen["uri"],
                        "filename": unchosen["filename"],
                        "size_bytes": unchosen["size_bytes"],
                        "size_mb": round(unchosen["size_bytes"] / (1024**2), 2),
                        "detected_owner": owner,
                        "detected_table": table,
                        "reason_ignored": f"Chunked CSV ignored (single consolidated CSV selected: {chosen_files[0]['filename']})"
                    })
            else:
                chosen_files = chunked_csv

        table_bytes = sum(f["size_bytes"] for f in chosen_files)
        volume_by_table.append({
            "owner": owner,
            "table_name": table,
            "target_schema": owner.lower(),
            "target_table": norm_col(table),
            "chosen_format": chosen_format,
            "file_count": len(chosen_files),
            "total_bytes": table_bytes,
            "total_mb": round(table_bytes / (1024**2), 2),
            "total_gb": round(table_bytes / (1024**3), 3)
        })

        for f in chosen_files:
            f["owner"] = owner
            f["table_name"] = table
            f["target_schema"] = owner.lower()
            f["target_table"] = norm_col(table)
            f["file_format"] = chosen_format
            final_selected_files.append(f)

    # Rule 3: Load the s3 files in that order (small to large file size)
    # Calculate total volume per table
    table_vols = defaultdict(int)
    for f in final_selected_files:
        table_vols[(f["owner"], f["table_name"])] += f["size_bytes"]

    # Strict sort: small tables first, then alphabetical chunk sequence within table (e.g. _aa -> _ab -> _ac)
    final_selected_files.sort(key=lambda x: (table_vols[(x["owner"], x["table_name"])], x["size_bytes"], x["filename"]))

    # Assign 1-indexed load order rank
    file_map_rows = []
    for rank, f in enumerate(final_selected_files, 1):
        f["load_order_rank"] = rank
        file_map_rows.append({
            "load_order_rank": rank,
            "owner": f["owner"],
            "table_name": f["table_name"],
            "target_schema": f["target_schema"],
            "target_table": f["target_table"],
            "file_format": f["file_format"],
            "size_bytes": f["size_bytes"],
            "size_mb": round(f["size_bytes"] / (1024**2), 2),
            "uri": f["uri"],
            "filename": f["filename"]
        })

    # Write output CSV (both oracle_02_s3_file_map.csv and legacy s3_file_map.csv)
    map_csv_path = ORACLE_OUTPUTS_DIR / "oracle_02_s3_file_map.csv"
    legacy_map_path = ORACLE_OUTPUTS_DIR / "s3_file_map.csv"
    for out_path in (map_csv_path, legacy_map_path):
        with open(out_path, "w", newline="", encoding="utf-8") as out:
            if file_map_rows:
                w = csv.DictWriter(out, fieldnames=list(file_map_rows[0].keys()))
                w.writeheader()
                w.writerows(file_map_rows)
            else:
                w = csv.writer(out)
                w.writerow(["load_order_rank", "owner", "table_name", "target_schema", "target_table", "file_format", "size_bytes", "size_mb", "uri", "filename"])

    logger.info(f"Generated {map_csv_path} and {legacy_map_path} with {len(file_map_rows)} file(s) ordered small to large.")

    # Summary Stats
    total_files = len(file_map_rows)
    total_bytes = sum(r["size_bytes"] for r in file_map_rows)
    total_gb = round(total_bytes / (1024**3), 2)
    summary_data = [
        {"Metric": "Discovery Timestamp", "Value": ts},
        {"Metric": "Target S3 Source", "Value": target_uri or "Local Inputs"},
        {"Metric": "Total Ingestion Files", "Value": total_files},
        {"Metric": "Total Compressed Volume (GB)", "Value": total_gb},
        {"Metric": "Total Tables Identified", "Value": len(volume_by_table)},
        {"Metric": "Total Ignored / Unmapped Files", "Value": len(ignored_files)},
        {"Metric": "Files Format Breakdown", "Value": f"GZIP: {sum(1 for r in file_map_rows if r['file_format'] == 'gzip')}, CSV: {sum(1 for r in file_map_rows if r['file_format'] == 'csv')}"},
        {"Metric": "Load Ordering Strategy", "Value": "Strict File Size (Small to Large)"}
    ]

    # Generate Excel Report
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_02_inventory_report_{ts}.xlsx"
    sheets = {
        "Summary": summary_data,
        "Load_Order_Small_To_Large": file_map_rows,
        "Volume_By_Table": volume_by_table,
        "Ignored_Unmapped_Files": ignored_files if ignored_files else [{"Status": "No ignored or unmapped files"}]
    }
    generate_excel_report(report_file, sheets)
    logger.info(f"Excel inventory report generated: {report_file} (Ignored files: {len(ignored_files)})")

    cons_count = sum(1 for r in file_map_rows if r['file_format'] == 'gzip' and not is_chunked_file(r['filename'], r['table_name']))
    chunk_count = sum(1 for r in file_map_rows if r['file_format'] == 'gzip' and is_chunked_file(r['filename'], r['table_name']))
    csv_count = sum(1 for r in file_map_rows if r['file_format'] == 'csv')

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  tables identified          : {len(volume_by_table):>10,}",
        f"  files selected for load    : {len(file_map_rows):>10,}",
        f"    - consolidated gzip      : {cons_count:>10,}",
        f"    - chunked gzip           : {chunk_count:>10,}",
        f"    - uncompressed csv       : {csv_count:>10,}",
        f"  total volume to load (MB)  : {round(total_bytes / (1024**2), 2):>10,}",
        f"  total volume to load (GB)  : {total_gb:>10,}",
        f"  files ignored / skipped    : {len(ignored_files):>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="02_s3Inventory.py - S3 Inventory and Small-to-Large Load Ordering")
    parser.add_argument("--s3", help="S3 URI prefix (e.g. s3://bucket/path/)")
    parser.add_argument("--owners", help="Comma-separated owners to filter (e.g. BSS01001,BSS01104)")
    parser.add_argument("--tables", help="Comma-separated tables to filter (e.g. BSS01001.BROKERACCOUNTDETAIL)")
    args = parser.parse_args()

    owners = {o.strip().upper() for o in args.owners.split(",")} if args.owners else None
    tables = {t.strip().upper() for t in args.tables.split(",")} if args.tables else None

    ok = build_inventory(s3_uri=args.s3, owners_filter=owners, tables_filter=tables)
    sys.exit(0 if ok else 1)

