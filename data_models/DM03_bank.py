#!/usr/bin/env python3
r"""bank_model.py - Migration and enrichment script for bcs.bank table.
Implements the logic from bank_datamodel.txt:
1. Adds column bank_statement_mnemonic (or bank_statemnt_mnemonic) VARCHAR(10) if not exists.
2. Updates bank_statement_mnemonic from staging table:
   bcs.stg_program_bank_primary_rec_v s
   WHERE UPPER(TRIM(b.displayname)) = UPPER(TRIM(s.bank_name))
3. Applies manual rule overrides for 5 specific institutions:
   - 'First Bank & Trust'       -> 'FRSTBKTR'
   - 'East West Bank'           -> 'ESTWST'
   - 'American Bank of Dallas'  -> 'AMERICAN'
   - 'Union Bank & Trust'       -> 'UNIONBK'
   - 'Metropolitan Commercial'  -> 'METCOMM'

Composite Key & Idempotent Upsert:
  - Natural composite key: (display_name, bank_statement_mnemonic)
  - Pre-checks existing mnemonics against target table:
    * If all rows already have up-to-date mnemonics: logs "no new records found for upsert" and skips redundant updates.
    * If updates are needed: applies updates via staging table and verification overrides.
    * Never halts the migration when rows already exist; runs the entire logic and verification.

Post-load comparison:
   Automatically compares natural key pair (Bank_Name, Bank_Statement_Mnemonic) against expected mappings,
   outputting an Excel-ready comparereport_bank_<datetime>.csv.

Connection settings can be edited directly below in the script or passed as CLI arguments:
  --host, --port, --user, --password, --src-db, --tgt-db (or --db)

Commands:
  python bank_model.py                     # Dry run (safe, read-only preview)
  python bank_model.py --execute           # Execute DDL & updates on target database
"""

SWEEP_MODEL = {
    "name": "dm03_bank",
    "inputs": ["bcs.stg_program_bank_primary_rec_v"],
    "outputs": ["bcs.bank"],
    "depends_on": [],
    "delta_strategy": "update_in_place",
    "update_key": ["bankid"],
    "timeout": 3600,
}


import argparse
import csv
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime

import _dm_support as _dm

# ----------------------------------------------------------------------
# DEFAULT CONNECTION & DATABASE CONFIGURATION (Edit here or pass via CLI)
# ----------------------------------------------------------------------
DEFAULT_HOST = _dm.libpq_defaults()["host"]
DEFAULT_PORT = _dm.libpq_defaults()["port"]
DEFAULT_USER = _dm.libpq_defaults()["user"]
DEFAULT_PASS = _dm.libpq_defaults()["password"]   # empty: ~/.pgpass or peer / trust auth
DEFAULT_SRC_DB = _dm.libpq_defaults()["db"]        # source database ($PGDATABASE)
DEFAULT_TGT_DB = _dm.libpq_defaults()["db"]        # target database ($PGDATABASE)
DEFAULT_TARGET_TABLE = "bcs.bank"
DEFAULT_SRC_SCHEMA = "bcs"
DEFAULT_STAGING_TABLE = "stg_program_bank_primary_rec_v"

# Manual business overrides from spec
MANUAL_MNEMONICS = {
    "FIRST BANK & TRUST": "FRSTBKTR",
    "EAST WEST BANK": "ESTWST",
    "AMERICAN BANK OF DALLAS": "AMERICAN",
    "UNION BANK & TRUST": "UNIONBK",
    "METROPOLITAN COMMERCIAL": "METCOMM",
}

# Column aliases for flexible schema detection
TARGET_ID_ALIASES = ["bankid", "bank_id", "id"]
TARGET_NAME_ALIASES = ["displayname", "display_name", "bank_name", "bankname", "name"]
TARGET_MNEMONIC_ALIASES = ["bank_statement_mnemonic", "bank_statemnt_mnemonic", "statement_mnemonic", "mnemonic"]

STG_NAME_ALIASES = ["bank_name", "bankname", "displayname", "name"]
STG_MNEMONIC_ALIASES = ["bank_statemnt_mnemonic", "bank_statement_mnemonic", "mnemonic"]

SEP = "\x1f"
HERE = os.path.dirname(os.path.abspath(__file__))
RESULT = {}


def q(x):
    """Safely quote SQL identifiers."""
    return '"' + str(x).replace('"', '""') + '"'


def lit(x):
    """Format literal value for SQL strings."""
    return "NULL" if x is None else "'" + str(x).replace("'", "''") + "'"


def run_psql(db, sql, host, port, user, password, readonly=False):
    """Run psql command with connection arguments and environment."""
    env = dict(os.environ, PGDATABASE=db)
    if password:
        env["PGPASSWORD"] = password
    if readonly:
        env["PGOPTIONS"] = (env.get("PGOPTIONS", "") + " -c default_transaction_read_only=on").strip()

    cmd = ["psql", "-X", "-At", "-F", SEP, "-v", "ON_ERROR_STOP=1"]
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-p", str(port)])
    if user:
        cmd.extend(["-U", user])
    cmd.extend(["-c", sql])

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, env=env)
    except FileNotFoundError:
        sys.exit("psql CLI tool not found in PATH. Please ensure PostgreSQL client tools are installed.")
    if r.returncode:
        sys.exit(f"psql on {db} failed: " + (r.stderr or r.stdout).strip()[:600])
    return [ln.split(SEP) for ln in r.stdout.split("\n") if ln]


def find_cols(db, schema, table, host, port, user, password):
    """Case-insensitive catalog lookup: returns (real_schema, real_table, {col_lower: (real_name, type)})"""
    rows = run_psql(
        db,
        "SELECT c.table_schema, c.table_name, c.column_name, format_type(a.atttypid, a.atttypmod) "
        "FROM information_schema.columns c "
        "JOIN pg_namespace n ON n.nspname = c.table_schema "
        "JOIN pg_class k ON k.relnamespace = n.oid AND k.relname = c.table_name "
        "JOIN pg_attribute a ON a.attrelid = k.oid AND a.attname = c.column_name "
        f"WHERE lower(c.table_schema) = {lit(schema.lower())} AND lower(c.table_name) = {lit(table.lower())}",
        host,
        port,
        user,
        password,
        readonly=True,
    )
    if not rows:
        return None, None, {}
    have = {r[2].lower(): (r[2], r[3]) for r in rows}
    return rows[0][0], rows[0][1], have


def run_post_load_comparison(dst_db, target_table, target_name_col, target_mnemonic_col, preview_data, host, port, user, password, out_dir):
    """Executes comparison on displayname (Bank_Name) and bank_statement_mnemonic (Mnemonic), writing an Excel CSV report."""
    print("\n" + "=" * 80)
    print(" [?] AUTOMATIC POST-LOAD VERIFICATION & COMPARISON REPORT")
    print("=" * 80)
    print(f" Target Table:       {target_table}")
    print(f" 2 Composite Fields: {target_name_col} (Bank_Name) & {target_mnemonic_col} (Mnemonic) [Surrogate ID bypassed]")

    tgt_sql = f"""
    SELECT DISTINCT
        UPPER(TRIM({q(target_name_col)}::text)),
        TRIM({q(target_mnemonic_col)}::text)
    FROM {target_table}
    WHERE {q(target_mnemonic_col)} IS NOT NULL AND TRIM({q(target_mnemonic_col)}::text) <> ''
    ORDER BY 1;
    """
    target_data = run_psql(dst_db, tgt_sql, host, port, user, password, readonly=True)

    src_map = {p["displayname"].strip().upper(): p["new_mnemonic"].strip() for p in preview_data if p.get("new_mnemonic")}
    tgt_map = {r[0].strip().upper(): r[1].strip() for r in target_data if len(r) >= 2}

    src_keys = set(src_map.keys())
    tgt_keys = set(tgt_map.keys())

    matched_keys = {k for k in src_keys.intersection(tgt_keys) if src_map[k] == tgt_map[k]}
    mismatched_keys = {k for k in src_keys.intersection(tgt_keys) if src_map[k] != tgt_map[k]}
    missing_in_target = src_keys - tgt_keys

    # Prepare Excel CSV Report named comparereport_modelname_datetime
    dt_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"comparereport_bank_{dt_str}.csv"
    report_csv = os.path.join(out_dir, report_filename)
    with open(report_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Bank_Name", "Bank_Statement_Mnemonic", "Source_Present", "Target_Present", "Match_Status"])
        all_names = sorted(src_keys.union(tgt_keys))
        for bname in all_names:
            in_src = "YES" if bname in src_keys else "NO"
            in_tgt = "YES" if bname in tgt_keys else "NO"
            exp_m = src_map.get(bname, "")
            act_m = tgt_map.get(bname, "")
            mnem = exp_m if in_src == "YES" else act_m
            stat = "MATCH (100%)" if in_src == "YES" and in_tgt == "YES" and exp_m == act_m else (
                "VALUE_MISMATCH" if in_src == "YES" and in_tgt == "YES" else (
                    "MISSING_IN_TARGET" if in_src == "YES" else "EXTRA_IN_TARGET"
                )
            )
            w.writerow([bname, mnem, in_src, in_tgt, stat])

    # Also maintain comparison_report.csv copy for backward compatibility
    try:
        import shutil
        shutil.copyfile(report_csv, os.path.join(out_dir, "comparison_report.csv"))
    except Exception:
        pass

    print(f" Source Distinct Banks: {len(src_keys)}")
    print(f" Target Enriched Banks: {len(tgt_keys)}")
    print(f" Matched Key Pairs:     {len(matched_keys)} / {len(src_keys)}")

    if len(src_keys) <= len(tgt_keys) and len(missing_in_target) == 0 and len(mismatched_keys) == 0:
        print("\n [OK] VERIFICATION RESULT: 100% MATCH! All bank mnemonics successfully verified in target.")
    else:
        print(f"\n [!] VERIFICATION WARNING: {len(missing_in_target)} missing, {len(mismatched_keys)} mismatched.")

    print(f" [REPORT] Excel Comparison Report generated: {os.path.abspath(report_csv)}")
    print("=" * 80 + "\n")


def main():
    _dm.init_session()          # model session: sweep.loader = fe_migration
    parser = argparse.ArgumentParser(
        description="bcs.bank mnemonic enrichment script with dry-run, idempotent upsert, and pipeline automation support."
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Postgres host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", default=DEFAULT_PORT, type=int, help=f"Postgres port (default: {DEFAULT_PORT})")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"Postgres user (default: {DEFAULT_USER})")
    parser.add_argument("--password", default=DEFAULT_PASS, help="Postgres password (default: empty or from env)")
    parser.add_argument("--src-db", default=DEFAULT_SRC_DB, help=f"Source database (default: {DEFAULT_SRC_DB})")
    parser.add_argument("--tgt-db", "--dst-db", "--target-db", dest="tgt_db", default=DEFAULT_TGT_DB, help=f"Target database (default: {DEFAULT_TGT_DB})")
    parser.add_argument("--db", default=None, help="Fallback database for both source & target if specified")
    parser.add_argument("--src-schema", default=DEFAULT_SRC_SCHEMA, help=f"Source staging schema (default: {DEFAULT_SRC_SCHEMA})")
    parser.add_argument("--staging-table", default=DEFAULT_STAGING_TABLE, help=f"Staging table (default: {DEFAULT_STAGING_TABLE})")
    parser.add_argument("--target", default=DEFAULT_TARGET_TABLE, help=f"Target table (default: {DEFAULT_TARGET_TABLE})")
    parser.add_argument("--mnemonic-col", default="auto", help="Target mnemonic column name (default: auto-detect)")
    parser.add_argument("--out-dir", default=None, help="Directory for preview artifacts (default: $VSAM_OUT/bank)")
    parser.add_argument("--execute", action="store_true", help="Execute DDL and updates in target database (dry run by default)")
    parser.add_argument("--mode", choices=["full", "delta"], default=os.environ.get("SWEEP_MODE") or "full",
                        help="full / delta (the runner passes it; only banks whose mnemonic differs are updated, so both are idempotent)")
    args = parser.parse_args()

    src_db = args.src_db
    dst_db = args.tgt_db
    if args.db:
        if args.src_db == DEFAULT_SRC_DB:
            src_db = args.db
        if args.tgt_db == DEFAULT_TGT_DB:
            dst_db = args.db

    base_out = args.out_dir or os.environ.get("SWEEP_MODEL_WORKDIR") or os.path.join(HERE, "work", "dm03_bank", "manual")
    os.makedirs(base_out, exist_ok=True)

    print("=" * 80)
    print(" BCS.BANK MNEMONIC ENRICHMENT PIPELINE")
    print("=" * 80)
    print(f" Connecting to Postgres at {args.host}:{args.port} (User: {args.user})")
    print(f" Source DB      : {src_db} (Staging: {args.src_schema}.{args.staging_table})")
    print(f" Target DB      : {dst_db} (Target: {args.target})")
    print(f" Execution Mode : {'EXECUTE' if args.execute else 'DRY RUN (Preview Only)'}")
    print(" Upsert Mode    : IDEMPOTENT UPSERT (Composite Key: displayname, bank_statement_mnemonic)")
    print(f" Artifacts Dir  : {os.path.abspath(base_out)}")
    print("-" * 80)

    # -------------------------------------------------------------------------
    # STEP 1: Inspect Target Table (bcs.bank)
    # -------------------------------------------------------------------------
    print(f"[*] Inspecting target table {args.target} in {dst_db}...")
    tsch, ttab = args.target.split(".", 1) if "." in args.target else ("public", args.target)
    rts, rtt, all_tgt_cols = find_cols(dst_db, tsch, ttab, args.host, args.port, args.user, args.password)
    if rts is None:
        sys.exit(f"Target table {args.target} not found in database {dst_db}.")

    # Resolve bankid column
    id_col_match = next((all_tgt_cols[a] for a in TARGET_ID_ALIASES if a in all_tgt_cols), None)
    if not id_col_match:
        sys.exit(f"Could not find bank ID column in {rts}.{rtt}. Looked for: {', '.join(TARGET_ID_ALIASES)}")
    target_id_col = id_col_match[0]

    # Resolve displayname column
    name_col_match = next((all_tgt_cols[a] for a in TARGET_NAME_ALIASES if a in all_tgt_cols), None)
    if not name_col_match:
        sys.exit(f"Could not find display name column in {rts}.{rtt}. Looked for: {', '.join(TARGET_NAME_ALIASES)}")
    target_name_col = name_col_match[0]

    # Resolve or determine mnemonic column
    if args.mnemonic_col != "auto":
        target_mnemonic_col = args.mnemonic_col
        column_exists = target_mnemonic_col.lower() in all_tgt_cols
    else:
        mnemonic_match = next((all_tgt_cols[a] for a in TARGET_MNEMONIC_ALIASES if a in all_tgt_cols), None)
        if mnemonic_match:
            target_mnemonic_col = mnemonic_match[0]
            column_exists = True
        else:
            # Default to the standard spelling if not present
            target_mnemonic_col = "bank_statement_mnemonic"
            column_exists = False

    print(f"    Target ID column       : {target_id_col}")
    print(f"    Target Name column     : {target_name_col}")
    print(f"    Target Mnemonic column : {target_mnemonic_col} (Exists in table: {column_exists})")

    # -------------------------------------------------------------------------
    # STEP 2: Inspect Staging Table (bcs.stg_program_bank_primary_rec_v)
    # -------------------------------------------------------------------------
    print(f"[*] Inspecting staging table {args.src_schema}.{args.staging_table} in {src_db}...")
    rss, rst, all_stg_cols = find_cols(src_db, args.src_schema, args.staging_table, args.host, args.port, args.user, args.password)
    if rss is None:
        sys.exit(f"Staging table {args.src_schema}.{args.staging_table} not found in database {src_db}.")

    stg_name_match = next((all_stg_cols[a] for a in STG_NAME_ALIASES if a in all_stg_cols), None)
    stg_mnemonic_match = next((all_stg_cols[a] for a in STG_MNEMONIC_ALIASES if a in all_stg_cols), None)

    if not stg_name_match or not stg_mnemonic_match:
        sys.exit(
            f"Staging table {rss}.{rst} missing required columns. "
            f"Found: {', '.join(sorted(all_stg_cols.keys()))}"
        )
    stg_name_col = stg_name_match[0]
    stg_mnemonic_col = stg_mnemonic_match[0]
    print(f"    Staging Name column    : {stg_name_col}")
    print(f"    Staging Mnemonic column: {stg_mnemonic_col}")

    # Extract staging rows
    stg_query = f"""
    SELECT DISTINCT
        TRIM({q(stg_name_col)}) AS bank_name,
        TRIM({q(stg_mnemonic_col)}) AS mnemonic
    FROM {q(rss)}.{q(rst)}
    WHERE {q(stg_name_col)} IS NOT NULL AND TRIM({q(stg_name_col)}) <> ''
      AND {q(stg_mnemonic_col)} IS NOT NULL AND TRIM({q(stg_mnemonic_col)}) <> ''
    ORDER BY 1;
    """
    raw_stg_rows = run_psql(src_db, stg_query, args.host, args.port, args.user, args.password, readonly=True)
    print(f"    Extracted {len(raw_stg_rows)} valid mnemonic mapping rows from staging.")

    stg_map = {}
    for b_name, mnem in raw_stg_rows:
        stg_map[b_name.strip().upper()] = mnem.strip()

    stg_rows_file = os.path.join(base_out, "staging_rows.csv")
    with open(stg_rows_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["bank_name", "bank_statemnt_mnemonic"])
        w.writerows(raw_stg_rows)

    # -------------------------------------------------------------------------
    # STEP 3: Extract Current Bank Records from Target Table
    # -------------------------------------------------------------------------
    print(f"[*] Extracting current bank records from {args.target}...")
    mnemonic_select = q(target_mnemonic_col) if column_exists else "NULL::text"
    bank_query = f"""
    SELECT
        {q(target_id_col)}::text AS bank_id,
        COALESCE(TRIM({q(target_name_col)}), '') AS display_name,
        COALESCE(TRIM({mnemonic_select}::text), '') AS current_mnemonic
    FROM {q(rts)}.{q(rtt)}
    ORDER BY {q(target_id_col)};
    """
    bank_rows = run_psql(dst_db, bank_query, args.host, args.port, args.user, args.password, readonly=True)
    print(f"    Found {len(bank_rows)} banks in {args.target}.")

    # -------------------------------------------------------------------------
    # STEP 4: Resolve Mnemonics & Detect Changes
    # -------------------------------------------------------------------------
    preview_data = []
    to_update = []
    stats = Counter()

    for r in bank_rows:
        b_id = r[0].strip()
        d_name = r[1].strip()
        curr_mnem = r[2].strip() if len(r) > 2 and r[2] else ""
        norm_name = d_name.upper()

        new_mnem = None
        source_rule = "NONE"

        # Rule 1: Manual overrides take highest priority
        if norm_name in MANUAL_MNEMONICS:
            new_mnem = MANUAL_MNEMONICS[norm_name]
            source_rule = "MANUAL_OVERRIDE"
        # Rule 2: Staging table mapping
        elif norm_name in stg_map:
            new_mnem = stg_map[norm_name]
            source_rule = "STAGING_MATCH"
        else:
            new_mnem = curr_mnem if curr_mnem else ""
            source_rule = "UNMATCHED"

        # Check if update is required
        if new_mnem and new_mnem != curr_mnem:
            action = "PENDING_UPDATE"
            stats[source_rule] += 1
            stats["TOTAL_UPDATES"] += 1
            to_update.append((b_id, new_mnem))
        elif new_mnem and new_mnem == curr_mnem:
            action = "ALREADY_UP_TO_DATE"
            stats["ALREADY_CORRECT"] += 1
        else:
            action = "NO_CHANGE"
            stats["UNMATCHED"] += 1

        preview_data.append({
            "bank_id": b_id,
            "displayname": d_name,
            "current_mnemonic": curr_mnem,
            "new_mnemonic": new_mnem,
            "source_rule": source_rule,
            "status": action,
        })

    # Composite Key Check & Message Logging
    if stats["TOTAL_UPDATES"] == 0 and len(bank_rows) > 0:
        print(f" [i] composite key check -> all {len(bank_rows)} bank records already exist / up-to-date in target ({stats['ALREADY_CORRECT']} matched) | no new records found for upsert")
    elif stats["TOTAL_UPDATES"] > 0:
        print(f" [+] composite key check -> banks evaluated = {len(bank_rows)} | banks to update = {stats['TOTAL_UPDATES']} | already up-to-date = {stats['ALREADY_CORRECT']}")

    print("-" * 80)
    print(" ENRICHMENT SUMMARY:")
    print(f"    Total banks evaluated      : {len(bank_rows)}")
    print(f"    Updates via Manual Override: {stats['MANUAL_OVERRIDE']}")
    print(f"    Updates via Staging Match  : {stats['STAGING_MATCH']}")
    print(f"    Already up to date         : {stats['ALREADY_CORRECT']}")
    print(f"    Unmatched / No change      : {stats['UNMATCHED']}")
    print(f"    TOTAL ROWS TO BE UPDATED   : {stats['TOTAL_UPDATES']}")
    print("-" * 80)

    # Write preview_updates.csv
    preview_file = os.path.join(base_out, "preview_updates.csv")
    with open(preview_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["bank_id", "displayname", "current_mnemonic", "new_mnemonic", "source_rule", "status"]
        )
        writer.writeheader()
        writer.writerows(preview_data)

    # Write stage.csv for \copy
    stage_file = os.path.join(base_out, "stage.csv")
    with open(stage_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for b_id, mnem in to_update:
            writer.writerow([b_id, mnem])

    # -------------------------------------------------------------------------
    # STEP 5: Construct Transactional load.sql
    # -------------------------------------------------------------------------
    tgt = f"{q(rts)}.{q(rtt)}"
    # an UPDATE sets modifiedby / modifieddate and flips createdby_m to 'fe_migration' (createddate_m stays as loaded)
    audit_set = ", ".join(f"{q(c)} = {e}" for c, e in _dm.update_audit({}, flip_created_by_m=True))
    stage_path_sql = stage_file.replace("\\", "/")

    sql_statements = [
        "\\set ON_ERROR_STOP on",
        "BEGIN;",
        "SET LOCAL lock_timeout = '60s';",
        _dm.ensure_audit_sql(f"{rts}.{rtt}"),
    ]

    # If all bank records already have up-to-date mnemonics, skip update logic cleanly
    if stats["TOTAL_UPDATES"] == 0 and len(bank_rows) > 0:
        sql_statements.append(f"-- Composite key check: All {len(bank_rows)} bank records already have up-to-date mnemonics.")
        sql_statements.append(f"SELECT '{args.target}' AS target_table, 'no new records found for upsert' AS status, count(*) AS total_count FROM {tgt} WHERE {q(target_mnemonic_col)} IS NOT NULL;")
    else:
        sql_statements.extend([
            f"LOCK TABLE {tgt} IN SHARE ROW EXCLUSIVE MODE;",
            "",
            "-- Step 1: Ensure column exists in target table",
            f"ALTER TABLE {tgt} ADD COLUMN IF NOT EXISTS {q(target_mnemonic_col)} VARCHAR(10);",
            "",
            "-- Step 2: Apply updates via temporary staging table",
            "CREATE TEMP TABLE bank_mnemonic_stage (",
            "    bank_id bigint,",
            "    mnemonic varchar(10)",
            ") ON COMMIT DROP;",
            f"\\copy bank_mnemonic_stage FROM '{stage_path_sql}' WITH (FORMAT csv)",
            "",
            f"UPDATE {tgt} b",
            f"SET {q(target_mnemonic_col)} = s.mnemonic, {audit_set}",
            "FROM bank_mnemonic_stage s",
            f"WHERE b.{q(target_id_col)} = s.bank_id",
            f"  AND (b.{q(target_mnemonic_col)} IS DISTINCT FROM s.mnemonic);",
            "",
            "-- Step 3: Verification of manual rule overrides from spec",
            f"UPDATE {tgt}",
            f"SET {audit_set}, {q(target_mnemonic_col)} = CASE UPPER(TRIM({q(target_name_col)}))",
            "    WHEN 'FIRST BANK & TRUST' THEN 'FRSTBKTR'",
            "    WHEN 'EAST WEST BANK' THEN 'ESTWST'",
            "    WHEN 'AMERICAN BANK OF DALLAS' THEN 'AMERICAN'",
            "    WHEN 'UNION BANK & TRUST' THEN 'UNIONBK'",
            "    WHEN 'METROPOLITAN COMMERCIAL' THEN 'METCOMM'",
            "END",
            f"WHERE UPPER(TRIM({q(target_name_col)})) IN (",
            "    'FIRST BANK & TRUST',",
            "    'EAST WEST BANK',",
            "    'AMERICAN BANK OF DALLAS',",
            "    'UNION BANK & TRUST',",
            "    'METROPOLITAN COMMERCIAL'",
            f") AND ({q(target_mnemonic_col)} IS DISTINCT FROM CASE UPPER(TRIM({q(target_name_col)}))",
            "    WHEN 'FIRST BANK & TRUST' THEN 'FRSTBKTR'",
            "    WHEN 'EAST WEST BANK' THEN 'ESTWST'",
            "    WHEN 'AMERICAN BANK OF DALLAS' THEN 'AMERICAN'",
            "    WHEN 'UNION BANK & TRUST' THEN 'UNIONBK'",
            "    WHEN 'METROPOLITAN COMMERCIAL' THEN 'METCOMM'",
            "END);",
            "",
            f"SELECT 'UPDATED_BANKS_COUNT' AS status, count(*) AS count FROM {tgt} WHERE {q(target_mnemonic_col)} IS NOT NULL;",
        ])

    sql_statements.append("COMMIT;")

    sql_file = os.path.join(base_out, "load.sql")
    with open(sql_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sql_statements) + "\n")

    print(f"[*] Generated transactional SQL script: {sql_file}")
    print("[*] Artifacts generated:")
    print(f"    - Staging Rows: {stg_rows_file}")
    print(f"    - Preview CSV : {preview_file}")
    print(f"    - Stage CSV   : {stage_file}")
    print(f"    - Load SQL    : {sql_file}")

    # Preview sample pending updates in console
    pending_samples = [p for p in preview_data if p["status"] == "PENDING_UPDATE"][:5]
    if pending_samples:
        print("-" * 80)
        print(" SAMPLE PENDING UPDATES (Top 5):")
        for p in pending_samples:
            print(f"   ID: {p['bank_id']:<6} | Name: {p['displayname']:<30} | {p['current_mnemonic'] or '(null)':<10} -> {p['new_mnemonic']:<10} ({p['source_rule']})")

    RESULT.update(updated=stats["TOTAL_UPDATES"], banks=len(bank_rows), dry_run=not args.execute)

    # -------------------------------------------------------------------------
    # STEP 6: Execute or Dry Run Exit
    # -------------------------------------------------------------------------
    print("-" * 80)
    if not args.execute:
        print("[!] DRY RUN COMPLETED SUCCESSFULLY: No database mutations were made.")
        print(f"    Review {preview_file} and verify mappings.")
        print("    To apply changes to the target table, rerun with: --execute")
        return

    print(f"[*] Executing SQL transaction on database {dst_db}...")
    cmd = ["psql", "-X"]
    if args.host:
        cmd.extend(["-h", args.host])
    if args.port:
        cmd.extend(["-p", str(args.port)])
    if args.user:
        cmd.extend(["-U", args.user])
    cmd.extend(["-f", sql_file])

    env = dict(os.environ, PGDATABASE=dst_db)
    if args.password:
        env["PGPASSWORD"] = args.password

    run_result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    log_file = os.path.join(base_out, "run.log")
    with open(log_file, "w", encoding="utf-8") as f:
        f.write(run_result.stdout + "\n" + run_result.stderr)

    if run_result.returncode != 0:
        print("[-] EXECUTION FAILED! Transaction has rolled back automatically.")
        err_msg = (run_result.stderr or run_result.stdout).strip()[-600:]
        print(f"    Error details: {err_msg}")
        sys.exit(1)

    print("[+] TRANSACTION COMMITTED SUCCESSFULLY!")
    print(run_result.stdout.strip())
    print(f"    Log written to: {log_file}")
    if stats["TOTAL_UPDATES"] > 0:
        print(f"    Done: Updated {stats['TOTAL_UPDATES']} bank records in {rts}.{rtt} in database {dst_db}!")
    else:
        print(f"    Done: All {len(bank_rows)} bank records already up-to-date in {rts}.{rtt} in database {dst_db} (no new records found for upsert).")

    # ---------------- Step 7: Automatic In-Line Post-Load Comparison
    run_post_load_comparison(dst_db, tgt, target_name_col, target_mnemonic_col, preview_data, args.host, args.port, args.user, args.password, base_out)


if __name__ == "__main__":
    main()
    print(_dm.result_line(dict(RESULT, model=SWEEP_MODEL["name"])))
