#!/usr/bin/env python3
"""
03_typeGateway.py - Pre-Flight Structure & Data Type Quality Gate
Reads sampled records directly from S3 files and validates them against the target
PostgreSQL data types and Oracle DDL before any database tables are provisioned.

Enforces:
  - Tables with column mismatches (missing/extra columns) fall under FAIL / BLOCK status.
  - Generates outputs/gate_results.csv and outputs/gate_pass_tables.txt for downstream ingestion.

Generates:
  - outputs/gate_results.csv
  - outputs/gate_pass_tables.txt
  - reports/03_typeGateway_report_<timestamp>.xlsx
"""
import os
import re
import sys
import csv
import argparse
import subprocess
from pathlib import Path
from collections import defaultdict
from sweep_common import (
    ORACLE_OUTPUTS_DIR, ORACLE_REPORTS_DIR, AWS_REGION, STREAM_SCRIPT,
    META_TABLES_CSV, get_logger, get_timestamp, get_db_name, generate_excel_report, norm_col, oracle_to_pg
)

logger = get_logger("oracle_03_type_gate", "oracle_03_type_gate")


def sample_s3_file(uri, sample_rows=1000, region=AWS_REGION):
    """
    Samples decompressed lines from an S3 file.
    Prefers in-process Boto3 streaming for maximum reliability and speed,
    with fallback to sweep_stream.sh.
    Returns: (list_of_dict_records, error_message)
    """
    clean_uri = uri.strip()
    if clean_uri.startswith("s3://"):
        parts = clean_uri[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        # 1. Try Boto3 streaming (pure Python, fast, zero CLI dependency)
        try:
            import boto3, gzip, io
            s3 = boto3.client("s3", region_name=region)
            resp = s3.get_object(Bucket=bucket, Key=key)
            body = resp["Body"]
            lines = []
            if key.lower().endswith(".gz"):
                with gzip.GzipFile(fileobj=body) as gz_f:
                    text_stream = io.TextIOWrapper(gz_f, encoding="utf-8", errors="replace")
                    for _ in range(sample_rows + 1):
                        line = text_stream.readline()
                        if not line:
                            break
                        lines.append(line)
            else:
                text_stream = io.TextIOWrapper(body, encoding="utf-8", errors="replace")
                for _ in range(sample_rows + 1):
                    line = text_stream.readline()
                    if not line:
                        break
                    lines.append(line)

            if lines:
                reader = csv.DictReader(lines)
                records = list(reader)
                if records:
                    return records, ""
        except Exception as e:
            logger.debug(f"Boto3 streaming sample notice for {clean_uri}: {e}")

        # 2. Fallback to sweep_stream.sh
        env = os.environ.copy()
        if "/usr/local/lib/python3.9/site-packages" not in env.get("PYTHONPATH", ""):
            env["PYTHONPATH"] = f"/usr/local/lib/python3.9/site-packages:{env.get('PYTHONPATH', '')}"

        cmd = f"bash \"{STREAM_SCRIPT}\" \"{clean_uri}\" \"{region}\" | head -n {sample_rows + 1}"
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
        if res.stdout.strip():
            lines = res.stdout.splitlines()
            reader = csv.DictReader(lines)
            records = list(reader)
            if records:
                return records, ""

        err_out = res.stderr.strip() if res.stderr else "Empty stream output"
        return [], err_out

    # Local file fallback
    p = Path(clean_uri)
    if p.exists():
        import gzip
        try:
            if p.suffix.lower() == ".gz":
                with gzip.open(p, "rt", encoding="utf-8", errors="replace") as fh:
                    lines = [fh.readline() for _ in range(sample_rows + 1)]
            else:
                with open(p, "r", encoding="utf-8", errors="replace") as fh:
                    lines = [fh.readline() for _ in range(sample_rows + 1)]
            records = list(csv.DictReader([l for l in lines if l]))
            return records, ""
        except Exception as e:
            return [], str(e)

    return [], f"File not found: {clean_uri}"


def check_table_sample(owner, table, target_cols, sample_records, err_msg=""):
    """
    Validates sampled data rows against column definitions.
    Returns list of findings: {check_type, severity, column, message}
    """
    findings = []
    if not sample_records:
        msg = f"Sample returned 0 rows from S3 object ({err_msg})" if err_msg else "Sample returned 0 rows from S3 object."
        findings.append({
            "owner": owner,
            "table_name": table,
            "column_name": "*",
            "check_type": "STREAM_ERROR" if err_msg and "Empty" not in err_msg else "EMPTY_OR_UNREADABLE",
            "severity": "WARN",
            "sample_value": "",
            "message": f"NON-BLOCKING: {msg}"
        })
        return findings

    # Filter out empty or whitespace-only column headers (e.g. from trailing/leading commas)
    valid_raw_headers = [k for k in sample_records[0].keys() if k and k.strip()]
    sample_headers = [norm_col(c) for c in valid_raw_headers if norm_col(c)]
    target_col_names = [c["pg_name"] for c in target_cols if c.get("pg_name")]

    # Check structural column parity - column mismatch results in BLOCK / FAIL
    AUDIT_COLS = {"createddate_m", "createdby_m", "modifieddate", "modifiedby"}
    missing_in_file = (set(target_col_names) - set(sample_headers)) - AUDIT_COLS
    extra_in_file = set(sample_headers) - set(target_col_names)

    if missing_in_file:
        findings.append({
            "owner": owner,
            "table_name": table,
            "column_name": ", ".join(list(missing_in_file)[:5]),
            "check_type": "COLUMNS_MISSING_IN_FILE",
            "severity": "BLOCK",
            "sample_value": "",
            "message": f"COLUMN MISMATCH: {len(missing_in_file)} column(s) defined in Oracle DDL are absent from S3 file header ({', '.join(sorted(missing_in_file)[:5])})."
        })

    if extra_in_file:
        findings.append({
            "owner": owner,
            "table_name": table,
            "column_name": ", ".join(list(extra_in_file)[:5]),
            "check_type": "COLUMNS_EXTRA_IN_FILE",
            "severity": "BLOCK",
            "sample_value": "",
            "message": f"COLUMN MISMATCH: {len(extra_in_file)} extra column(s) present in S3 header are not in Oracle DDL ({', '.join(sorted(extra_in_file)[:5])})."
        })

    if len(sample_headers) != len(target_col_names) and not (missing_in_file or extra_in_file):
        findings.append({
            "owner": owner,
            "table_name": table,
            "column_name": "*",
            "check_type": "COLUMN_COUNT_MISMATCH",
            "severity": "BLOCK",
            "sample_value": f"S3: {len(sample_headers)} cols, DDL: {len(target_col_names)} cols",
            "message": f"COLUMN MISMATCH: Column count mismatch between S3 file ({len(sample_headers)}) and Oracle DDL ({len(target_col_names)})."
        })

    # Validate data values per column
    col_def_map = {c["pg_name"]: c for c in target_cols}

    for norm_h, raw_h in zip(sample_headers, valid_raw_headers):
        c_def = col_def_map.get(norm_h)
        if not c_def:
            continue

        pg_type = c_def["pg_type"].lower()
        non_null_samples = [r[raw_h].strip() for r in sample_records if r.get(raw_h) and r[raw_h].strip()]

        if not non_null_samples:
            continue

        # 1. Numeric / Integer Checks
        if pg_type in ("integer", "smallint", "bigint"):
            for v in non_null_samples[:100]:
                if not re.match(r"^-?\d+$", v):
                    findings.append({
                        "owner": owner,
                        "table_name": table,
                        "column_name": norm_h,
                        "check_type": "INTEGER_TYPE_NON_INTEGER_VALUE",
                        "severity": "BLOCK",
                        "sample_value": v[:50],
                        "message": f"Column typed as '{pg_type}' contains non-integer value '{v}'."
                    })
                    break
                # Range check for 4-byte integer
                if pg_type == "integer" and abs(int(v)) > 2147483647:
                    findings.append({
                        "owner": owner,
                        "table_name": table,
                        "column_name": norm_h,
                        "check_type": "INTEGER_OVERFLOW",
                        "severity": "BLOCK",
                        "sample_value": v,
                        "message": f"Value {v} exceeds 4-byte integer limits (-2.14B to +2.14B). Needs 'bigint'."
                    })
                    break

        # 2. Date / Timestamp Checks
        if "timestamp" in pg_type or pg_type == "date":
            date_re = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?")
            for v in non_null_samples[:100]:
                if not date_re.match(v) and not re.match(r"^\d{2}-[A-Za-z]{3}-\d{2,4}", v):
                    findings.append({
                        "owner": owner,
                        "table_name": table,
                        "column_name": norm_h,
                        "check_type": "DATE_FORMAT_ANOMALY",
                        "severity": "WARN",
                        "sample_value": v[:50],
                        "message": f"Date value '{v}' does not match standard ISO or Oracle timestamp patterns."
                    })
                    break

    return findings


def run_type_gate(sample_rows=1000, max_tables=None, tables_filter=None):
    ts = get_timestamp()
    map_path = ORACLE_OUTPUTS_DIR / "oracle_02_s3_file_map.csv"
    if not map_path.exists():
        map_path = ORACLE_OUTPUTS_DIR / "s3_file_map.csv"
    dd_path = META_TABLES_CSV

    if not map_path.exists():
        logger.error(f"S3 file map not found at {map_path}. Run 03_s3_inventory.py first.")
        return False

    if not dd_path.exists():
        logger.error(f"Oracle metadata tables not found at {dd_path}. Run 01_extractMetadata.py first.")
        return False

    # Read DDL columns
    oracle_cols = defaultdict(list)
    with open(dd_path, "r", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            o = r["owner"].upper()
            t = r["table_name"].upper()
            pg_col = norm_col(r["column_name"])
            pg_type = oracle_to_pg(r["data_type"], r.get("data_length"), r.get("data_precision"), r.get("data_scale"))
            oracle_cols[(o, t)].append({
                "column_name": r["column_name"],
                "pg_name": pg_col,
                "pg_type": pg_type,
                "data_type": r["data_type"]
            })

    # Pick representative files to sample per table (prefer non-empty files)
    files_to_sample = {}
    with open(map_path, "r", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            k = (r["owner"].upper(), r["table_name"].upper())
            if tables_filter and f"{k[0]}.{k[1]}" not in tables_filter and k[1] not in tables_filter:
                continue
            sz = int(r.get("size_bytes") or 0)
            if k not in files_to_sample:
                files_to_sample[k] = (r["uri"], sz)
            elif files_to_sample[k][1] == 0 and sz > 0:
                # Upgrade to non-empty chunk if previous was 0 bytes
                files_to_sample[k] = (r["uri"], sz)

    files_to_sample = {k: v[0] for k, v in files_to_sample.items()}

    if max_tables:
        files_to_sample = dict(list(files_to_sample.items())[:max_tables])

    logger.info(f"Running pre-flight data type gate on {len(files_to_sample)} table sample(s)...")

    all_findings = []
    table_summaries = []

    for (owner, table), uri in files_to_sample.items():
        logger.info(f"Sampling: {owner}.{table} from {Path(uri).name}...")
        records, err_msg = sample_s3_file(uri, sample_rows=sample_rows)
        target_cols = oracle_cols.get((owner, table), [])

        findings = check_table_sample(owner, table, target_cols, records, err_msg=err_msg)
        all_findings.extend(findings)

        blocks = sum(1 for f in findings if f["severity"] == "BLOCK")
        warns = sum(1 for f in findings if f["severity"] == "WARN")
        status = "FAIL" if blocks > 0 else ("WARN" if warns > 0 else "PASS")

        table_summaries.append({
            "owner": owner,
            "table_name": table,
            "sampled_rows": len(records),
            "findings_count": len(findings),
            "blocks": blocks,
            "warns": warns,
            "status": status,
            "sample_file": Path(uri).name
        })

    # Save outputs/oracle_03_gate_results.csv and gate_results.csv
    for p_out in (ORACLE_OUTPUTS_DIR / "oracle_03_gate_results.csv", ORACLE_OUTPUTS_DIR / "gate_results.csv"):
        with open(p_out, "w", newline="", encoding="utf-8") as f:
            if table_summaries:
                w = csv.DictWriter(f, fieldnames=list(table_summaries[0].keys()))
                w.writeheader()
                w.writerows(table_summaries)
            else:
                w = csv.writer(f)
                w.writerow(["owner", "table_name", "sampled_rows", "findings_count", "blocks", "warns", "status", "sample_file"])
    logger.info(f"Saved: {ORACLE_OUTPUTS_DIR / 'oracle_03_gate_results.csv'}")

    # Save outputs/oracle_03_gate_pass_tables.txt and gate_pass_tables.txt (tables with 0 BLOCK findings, ready to load)
    passed_tables = [f"{r['owner']}.{r['table_name']}" for r in table_summaries if r["blocks"] == 0]
    for p_out in (ORACLE_OUTPUTS_DIR / "oracle_03_gate_pass_tables.txt", ORACLE_OUTPUTS_DIR / "gate_pass_tables.txt"):
        with open(p_out, "w", encoding="utf-8") as f:
            for pt in passed_tables:
                f.write(pt + "\n")
    logger.info(f"Saved: {ORACLE_OUTPUTS_DIR / 'oracle_03_gate_pass_tables.txt'} ({len(passed_tables)} passed table(s))")

    # Generate Excel Report
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_03_type_gate_report_{ts}.xlsx"
    total_blocks = sum(r["blocks"] for r in table_summaries)
    total_warns = sum(r["warns"] for r in table_summaries)
    passed_count = sum(1 for r in table_summaries if r["blocks"] == 0)
    failed_count = sum(1 for r in table_summaries if r["blocks"] > 0)

    summary_sheet = [
        {"Metric": "Type Gateway Run Timestamp", "Value": ts},
        {"Metric": "Total Tables Sampled", "Value": len(table_summaries)},
        {"Metric": "Passed Tables (Ready to Load)", "Value": passed_count},
        {"Metric": "Failed / Blocked Tables", "Value": failed_count},
        {"Metric": "Total Findings", "Value": len(all_findings)},
        {"Metric": "BLOCK Findings (Must Resolve)", "Value": total_blocks},
        {"Metric": "WARN Findings (Review)", "Value": total_warns},
        {"Metric": "Gateway Verdict", "Value": "REJECT / FIX REQUIRED" if total_blocks > 0 else "PASS"}
    ]

    sheets = {
        "Summary": summary_sheet,
        "Table_Results": table_summaries,
        "Passed_Tables": [{"owner": r["owner"], "table_name": r["table_name"], "status": r["status"]} for r in table_summaries if r["blocks"] == 0],
        "Failed_Tables": [r for r in table_summaries if r["blocks"] > 0],
        "Detailed_Findings": all_findings if all_findings else [{"Status": "No issues detected"}]
    }
    generate_excel_report(report_file, sheets)
    logger.info(f"Excel report generated: {report_file}")

    if total_blocks > 0:
        logger.warning(f"Type gateway reported {total_blocks} BLOCK finding(s). Review report before applying DDL.")
    else:
        logger.info(f"Type gateway PASSED: {passed_count}/{len(table_summaries)} table(s) ready for ingestion.")
        
    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  tables evaluated           : {len(table_summaries):>10,}",
        f"  tables passed (ready)      : {passed_count:>10,}",
        f"  tables blocked (failed)    : {failed_count:>10,}",
        f"  total findings             : {len(all_findings):>10,}",
        f"    - block findings         : {total_blocks:>10,}",
        f"    - warn findings          : {total_warns:>10,}",
        f"  gateway verdict            : {('PASS' if total_blocks == 0 else 'REJECT / FIX REQUIRED'):>10}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)
        
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="03_typeGateway.py - Pre-Flight Type Gateway")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to sample per file")
    parser.add_argument("--tables", help="Comma-separated tables to check (e.g. BSS01001.BROKERACCOUNTDETAIL)")
    parser.add_argument("--max-tables", type=int, help="Limit number of tables sampled")
    args = parser.parse_args()

    tables = {t.strip().upper() for t in args.tables.split(",")} if args.tables else None
    ok = run_type_gate(sample_rows=args.rows, max_tables=args.max_tables, tables_filter=tables)
    sys.exit(0 if ok else 1)
