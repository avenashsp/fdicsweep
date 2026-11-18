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
    VSAM_COPYBOOK_DIR,
    VSAM_REPORTS_DIR,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    norm_col,
)
from vsam.vsam_common import field_lengths, is_key_group_or_field, norm_usage, OUT, sql_type

logger = get_logger("vsam_01_extract_copybooks", "vsam_01_extract_copybooks")

USAGE_RX = re.compile(r"\b(COMPUTATIONAL-[1-5]|COMPUTATIONAL|COMP-[1-5]|COMP|BINARY|PACKED-DECIMAL)\b", re.I)


def parse_copybook_with_keys(path):
    """
    Parse a COBOL copybook file. Returns list of field dicts with:
      name, pic, usage, sql_type, len_display, len_storage, is_key, is_filler
    Key fields are those whose parent group or field name matches KEY/KEM conventions.
    """
    lines = []
    for ln in Path(path).read_text(errors="ignore").splitlines():
        # Strip 5 to 8 digit sequence numbers at start of line
        ln = re.sub(r"^\d{5,8}\s*", "", ln)
        # Skip comment lines
        if ln[:1] == "*" or ln.strip().startswith(("*", "/")) or (len(ln) > 0 and ln[0] in "*/"):
            continue
        lines.append(ln.strip())

    stmts = [s.strip() for s in re.split(r"\.(?=\s|$)", " ".join(lines)) if s.strip()]

    root = {"level": 0, "name": "__ROOT__", "is_key_group": False, "children": []}
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
        usage_m = USAGE_RX.search(rest)

        node = {
            "level": lvl,
            "name": name,
            "redef": bool(re.search(r"\bREDEFINES\b", rest, re.I)),
            "pic": pic.group(1) if pic else None,
            "usage": norm_usage(usage_m.group(1)) if usage_m else None,
            "occurs": int(occ.group(1)) if occ else None,
            "is_key_group": is_key_group_or_field(name),
            "children": []
        }

        # Pop stack to correct parent level
        while len(stack) > 1 and stack[-1]["level"] >= lvl:
            stack.pop()

        stack[-1]["children"].append(node)
        stack.append(node)

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

    copybook_dir = VSAM_COPYBOOK_DIR
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
                "key_type": "ERROR", "key_columns": "", "status": str(e)[:100]
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

    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  copybooks processed        : {len(summary_rows):>10,}",
        f"  total fields extracted     : {len(all_rows):>10,}",
        f"  copybooks with keys        : {sum(1 for r in summary_rows if r['key_type'] not in ('NONE','ERROR')):>10,}",
        f"  composite keys             : {sum(1 for r in summary_rows if 'COMPOSITE' in r['key_type']):>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)

    # Any copybook that failed to parse leaves downstream DDL/type mapping incomplete.
    failed = [r["copybook_file"] for r in summary_rows if r["key_type"] == "ERROR"]
    if failed:
        logger.error(f"{len(failed)} copybook(s) failed to parse: {', '.join(failed[:10])}")
    return not failed


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
