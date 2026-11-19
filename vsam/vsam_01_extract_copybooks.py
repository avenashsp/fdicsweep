#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_01_extract_copybooks.py - Step 01: Copybook Metadata Extraction
=============================================================================
Parses all COBOL copybook (.txt) files from vsam/inputs/copybooks/ and
extracts column metadata including name, PIC type, SQL type, length,
and primary key identification.

Key column detection rules:
  - Any field whose PARENT group name contains '-KEY' or '-KEM' is a PK field.
  - Single or composite keys are both supported.

Generates:
  - outputs/vsam/vsam_01_copybook_extracts.csv    (all fields)
  - reports/vsam/<db>_vsam_01_extract_report_<ts>.xlsx
=============================================================================
"""
import sys
import re
from pathlib import Path

import pandas as pd
from sweep_common import (
    log_summary,
    clip_error,
    generate_excel_report,
    get_db_name,
    get_logger,
    get_timestamp,
    norm_col,
    VSAM_REPORTS_DIR,
)
from vsam.common_cobol import read_copybook_tree
from vsam.common_config import COPYBOOK_DIR
from vsam.vsam_common import field_lengths, is_key_group_or_field, norm_usage, OUT, sql_type

logger = get_logger("vsam_01_extract_copybooks", "vsam_01_extract_copybooks")


def _walk(node):
    for child in node["children"]:
        yield child
        yield from _walk(child)

USAGE_RX = re.compile(r"\b(COMPUTATIONAL-[1-5]|COMPUTATIONAL|COMP-[1-5]|COMP|BINARY|PACKED-DECIMAL)\b", re.I)


def parse_copybook_with_keys(path):
    """
    Parse a COBOL copybook file. Returns list of field dicts with:
      name, pic, usage, sql_type, len_display, len_storage, is_key, is_filler
    Key fields are those whose parent group or field name matches KEY/KEM conventions.
    Text handling (sequence areas, comments, :TAG: names, multi-line statements) lives in
    vsam.common_cobol.
    """
    root = read_copybook_tree(path)
    for node in _walk(root):
        node["usage"] = norm_usage(node["usage_raw"]) if node.get("usage_raw") else None
        node["is_key_group"] = is_key_group_or_field(node.get("name", ""))

    # Flatten: carry down key-group status from parent
    def flat(nodes, parent_is_key=False):
        for n in nodes:
            if n["redef"]:
                continue
            # This node is a key group, or its parent was a key group, or this elementary field is a key
            is_key = parent_is_key or n["is_key_group"]
            for i in range(1, (n["occurs"] or 1) + 1):
                sfx = f"_{i}" if n["occurs"] else ""
                if n["children"]:
                    # Pass key status down to children
                    yield from flat(n["children"], parent_is_key=is_key)
                elif n["pic"] or n["usage"] in ("COMP-1", "COMP-2"):
                    ld, ls = field_lengths(n["pic"], n["usage"])
                    st = sql_type(n["pic"], n["usage"])
                    is_fill = bool(re.match(r"^FILLER\b|^FILLER[-_\d]", n["name"], re.I))
                    yield {
                        "field_name": n["name"] + sfx,
                        "col_name": norm_col(n["name"] + sfx),
                        "pic": n["pic"] or "",
                        "usage": n["usage"] or "",
                        "sql_type": st,
                        "len_display": ld,
                        "len_storage": ls,
                        "is_key": is_key and not is_fill,
                        "is_filler": is_fill
                    }

    return list(flat(root["children"]))


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Step 01: Extract column metadata from COBOL copybooks.")
    parser.add_argument("--bg", action="store_true", help="Run in background")
    parser.parse_args()

    logger.info("=" * 75)
    logger.info("SWEEP VSAM MIGRATION - STEP 01: COPYBOOK METADATA EXTRACTION")
    logger.info("=" * 75)

    copybook_dir = COPYBOOK_DIR      # honours the VSAM_COPYBOOK_DIR environment variable
    if not copybook_dir.exists():
        logger.error(f"Copybook directory not found: {copybook_dir}")
        sys.exit(1)

    cb_files = sorted(copybook_dir.glob("*.txt")) + sorted(copybook_dir.glob("*.cbl")) + sorted(copybook_dir.glob("*.cob"))
    logger.info(f"Found {len(cb_files)} copybook files in {copybook_dir}")

    all_rows = []
    summary_rows = []

    for cb in cb_files:
        try:
            fields = parse_copybook_with_keys(cb)
            key_fields = [f for f in fields if f["is_key"] and not f["is_filler"]]
            data_fields = [f for f in fields if not f["is_key"] and not f["is_filler"]]
            filler_fields = [f for f in fields if f["is_filler"]]

            for f in fields:
                all_rows.append({
                    "copybook_file": cb.name,
                    "field_name": f["field_name"],
                    "col_name": f["col_name"],
                    "pic": f["pic"],
                    "usage": f["usage"],
                    "sql_type": f["sql_type"],
                    "len_display": f["len_display"],
                    "len_storage": f["len_storage"],
                    "is_key": "YES" if f["is_key"] else "NO",
                    "is_filler": "YES" if f["is_filler"] else "NO"
                })

            key_col_names = ", ".join(f["col_name"] for f in key_fields)
            key_type = (
                "SINGLE" if len(key_fields) == 1 else
                f"COMPOSITE({len(key_fields)})" if len(key_fields) > 1 else
                "NONE"
            )
            summary_rows.append({
                "copybook_file": cb.name,
                "total_fields": len(fields),
                "key_fields": len(key_fields),
                "data_fields": len(data_fields),
                "filler_fields": len(filler_fields),
                "key_type": key_type,
                "key_columns": key_col_names,
                "status": "OK" if fields else "EMPTY"
            })
            logger.info(f"  [OK] {cb.name}: {len(fields)} fields, key={key_type} ({key_col_names or 'none'})")
        except Exception as e:  # noqa: BLE001 - one bad copybook must not stop the rest; reported + exit code
            logger.warning(f"  [ERROR] {cb.name}: {e}")
            summary_rows.append({
                "copybook_file": cb.name,
                "total_fields": 0, "key_fields": 0, "data_fields": 0, "filler_fields": 0,
                "key_type": "ERROR", "key_columns": "", "status": clip_error(e)
            })

    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=[
        "copybook_file", "field_name", "col_name", "pic", "usage",
        "sql_type", "len_display", "len_storage", "is_key", "is_filler"
    ])
    df.to_csv(OUT / "vsam_01_copybook_extracts.csv", index=False)
    logger.info(f"Saved: {OUT / 'vsam_01_copybook_extracts.csv'} ({len(df)} fields)")

    ts = get_timestamp()
    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_01_extract_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generate_excel_report(report_file, {
        "Summary": summary_rows,
        "FieldExtract": all_rows
    })
    logger.info(f"Excel Report: {report_file}")

    log_summary(logger, Path(__file__).name, [
        ('copybooks processed', len(summary_rows)),
        ('total fields extracted', len(all_rows)),
        ('copybooks with keys', sum(1 for r in summary_rows if r['key_type'] not in ('NONE','ERROR'))),
        ('composite keys', sum(1 for r in summary_rows if 'COMPOSITE' in r['key_type'])),
    ])

    # Any copybook that failed to parse leaves downstream DDL/type mapping incomplete.
    failed = [r["copybook_file"] for r in summary_rows if r["key_type"] == "ERROR"]
    if failed:
        logger.error(f"{len(failed)} copybook(s) failed to parse: {', '.join(failed[:10])}")
    return not failed


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
