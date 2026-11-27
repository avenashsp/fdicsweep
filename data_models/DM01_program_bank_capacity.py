#!/usr/bin/env python3
r"""program_bank_capacity_model.py - Program Bank Capacity migration with Delta Detection & Upsert:
Source:
    bcs.capacityplan cp
    JOIN bcs.cusipcusipgroup ccg ON cp.cusipgroupid = ccg.cusipgroupid
    JOIN bcs.cusip c ON ccg.cusipid = c.cusipid
    JOIN bcs.bank b ON c.bankid = b.bankid

Target:
    bcs.program_bank_capacity (default, overridable via --target)

Delta Detection Logic:
    - On every run, source rows are compared against existing target rows.
    - Each source row is classified as:
        NEW       : (broker_id, bank_id) does not exist in target → INSERT
        CHANGED   : (broker_id, bank_id) exists but bank_capacity differs → UPDATE
        UNCHANGED : (broker_id, bank_id) exists and bank_capacity is the same → SKIP
    - preview.csv shows ALL source rows with the row_type column.
    - stage.csv (for \copy) contains ONLY NEW + CHANGED rows.
    - load.sql performs INSERT ... ON CONFLICT DO UPDATE (UPSERT) for delta rows only.
    - comparison_report.csv covers ONLY the delta rows (NEW + CHANGED).

DRY RUN unless --execute.

Connection settings can be edited directly below in the script or passed as CLI arguments:
    --host, --port, --user, --password, --db (or --src-db / --dst-db)

Post-load comparison:
    Automatically compares broker_id and bank_id for DELTA rows only,
    generating comparison_report.csv for Excel verification.

Commands:
    python program_bank_capacity_model.py                     # Dry run (safe, read-only)
    python program_bank_capacity_model.py --execute           # Delta upsert into target table
    python program_bank_capacity_model.py --execute --replace # Delete all SYSTEM rows first, then full insert
"""

SWEEP_MODEL = {
    "name": "dm01_program_bank_capacity",
    "inputs": ['bcs.capacityplan', 'bcs.cusipcusipgroup', 'bcs.cusip', 'bcs.bank'],
    "outputs": ['bcs.program_bank_capacity'],
    "depends_on": [],
    "delta_strategy": "update_in_place",
    "update_key": ['broker_id', 'bank_id'],
    "timeout": 3600,
}


import argparse
import csv
import os
import subprocess
import sys
from collections import Counter

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
DEFAULT_TARGET_TABLE = "bcs.program_bank_capacity"
DEFAULT_SRC_SCHEMA = "bcs"

# Source tables in schema bcs
SRC_TABLE_CAPACITY_PLAN = "capacityplan"
SRC_TABLE_CUSIP_GROUP   = "cusipcusipgroup"
SRC_TABLE_CUSIP         = "cusip"
SRC_TABLE_BANK          = "bank"

BUSINESS_COLS = [
    "broker_id",
    "bank_id",
    "bank_capacity",
    "overflow_capacity_percent",
    "capacity_percent_low_limit",
    "capacity_percent_high_limit",
    "is_active",
]

COLUMN_ALIASES = {
    "broker_id":                  ["broker_id", "brokerid"],
    "bank_id":                    ["bank_id", "bankid"],
    "bank_capacity":              ["bank_capacity", "bankcapacity"],
    "overflow_capacity_percent":  ["overflow_capacity_percent", "overflowcapacitypercent"],
    "capacity_percent_low_limit": ["capacity_percent_low_limit", "capacitypercentlowlimit"],
    "capacity_percent_high_limit":["capacity_percent_high_limit", "capacitypercenthighlimit"],
    "is_active":                  ["is_active", "isactive"],
}

# Audit & metadata columns: the six canonical audit columns (createdby, createddate, modifiedby, modifieddate,
# createddate_m, createdby_m) are handled by _dm_support / sweep_audit - see docs/audit_columns.md

# Business Defaults
DEFAULT_OVERFLOW_PCT = "10.000"
DEFAULT_LOW_LIMIT    = "75.000"
DEFAULT_HIGH_LIMIT   = "90.000"
DEFAULT_IS_ACTIVE    = "true"

# Specific override: bank_id = 1707 AND broker_id = 108 -> bank_capacity = 25000000.0000
SPECIFIC_OVERRIDES = {
    (108, 1707): "25000000.0000"
}

SEP = "\x1f"


# ===========================================================================
# HELPERS
# ===========================================================================

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
        host, port, user, password, readonly=True,
    )
    if not rows:
        return None, None, {}
    have = {r[2].lower(): (r[2], r[3]) for r in rows}
    return rows[0][0], rows[0][1], have


# ===========================================================================
# DELTA DETECTION
# ===========================================================================

def fetch_existing_target_keys(dst_db, tgt_expr, broker_col, bank_col, cap_col, host, port, user, password):
    """
    Fetches all existing (broker_id, bank_id, bank_capacity) from the target table.
    Returns a dict: {(broker_id_str, bank_id_str): bank_capacity_str}
    """
    sql = f"""
    SELECT
        TRIM({q(broker_col)}::text),
        TRIM({q(bank_col)}::text),
        TRIM({q(cap_col)}::text)
    FROM {tgt_expr}
    WHERE {q(broker_col)} IS NOT NULL AND {q(bank_col)} IS NOT NULL
    ORDER BY 1, 2;
    """
    rows = run_psql(dst_db, sql, host, port, user, password, readonly=True)
    return {(r[0], r[1]): r[2] for r in rows}


def classify_rows(transformed, existing_target):
    """
    Classifies each transformed source row as:
        NEW       - key not in target
        CHANGED   - key exists, bank_capacity differs
        UNCHANGED - key exists, bank_capacity same
    Returns:
        classified_rows : list of dicts with added 'row_type' field
        delta_rows      : list of dicts for NEW + CHANGED only
    """
    classified_rows = []
    delta_rows = []

    for row in transformed:
        key = (str(row["broker_id"]), str(row["bank_id"]))
        tgt_cap = existing_target.get(key)

        if tgt_cap is None:
            row_type = "NEW"
        else:
            # Normalize capacity for comparison (strip trailing zeros)
            try:
                src_cap_norm = str(float(row["bank_capacity"]))
                tgt_cap_norm = str(float(tgt_cap))
                row_type = "CHANGED" if src_cap_norm != tgt_cap_norm else "UNCHANGED"
            except (ValueError, TypeError):
                row_type = "CHANGED" if str(row["bank_capacity"]) != str(tgt_cap) else "UNCHANGED"

        classified = dict(row, row_type=row_type)
        classified_rows.append(classified)

        if row_type in ("NEW", "CHANGED"):
            delta_rows.append(classified)

    return classified_rows, delta_rows


# ===========================================================================
# POST-LOAD COMPARISON (DELTA ROWS ONLY)
# ===========================================================================

def run_post_load_comparison(dst_db, tgt_expr, delta_rows, broker_col, bank_col,
                              host, port, user, password, out_dir):
    """
    Compares broker_id and bank_id for DELTA rows only (NEW + CHANGED),
    writing an Excel-ready comparison_report.csv.
    """
    print("\n" + "=" * 80)
    print(" 🔍 AUTOMATIC POST-LOAD DELTA VERIFICATION & COMPARISON REPORT")
    print("=" * 80)
    print(f" Target Table:       {tgt_expr}")
    print(" 2 Natural Fields:   broker_id & bank_id (surrogate sequence ID bypassed)")
    print(f" Scope:              DELTA rows only (NEW + CHANGED) — {len(delta_rows)} rows")

    if not delta_rows:
        print(" ℹ️  No delta rows to compare (all source rows were UNCHANGED).")
        print("=" * 80 + "\n")
        return

    # Fetch target state and filter to delta keys in Python (avoids long IN (...) CLI limits on fresh loads)
    tgt_sql = f"""
    SELECT DISTINCT
        TRIM({q(broker_col)}::text),
        TRIM({q(bank_col)}::text)
    FROM {tgt_expr}
    WHERE {q(broker_col)} IS NOT NULL AND {q(bank_col)} IS NOT NULL
    ORDER BY 1, 2;
    """
    target_data = run_psql(dst_db, tgt_sql, host, port, user, password, readonly=True)
    all_tgt_keys = set((r[0].strip(), r[1].strip()) for r in target_data)
    src_keys  = set((str(r["broker_id"]), str(r["bank_id"])) for r in delta_rows)
    tgt_keys  = all_tgt_keys & src_keys

    matched_keys       = src_keys & tgt_keys
    missing_in_target  = src_keys - tgt_keys
    extra_in_target    = tgt_keys - src_keys

    report_csv = os.path.join(out_dir, "comparison_report.csv")
    with open(report_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Broker_ID", "Bank_ID", "Row_Type", "Source_Present", "Target_Present", "Match_Status"])
        all_keys = sorted(src_keys | tgt_keys,
                          key=lambda x: (int(x[0]) if x[0].isdigit() else x[0],
                                         int(x[1]) if x[1].isdigit() else x[1]))
        # Build a lookup of row_type for quick access
        row_type_map = {(str(r["broker_id"]), str(r["bank_id"])): r["row_type"] for r in delta_rows}
        for b_id, bk_id in all_keys:
            in_src  = "YES" if (b_id, bk_id) in src_keys else "NO"
            in_tgt  = "YES" if (b_id, bk_id) in tgt_keys else "NO"
            rtype   = row_type_map.get((b_id, bk_id), "EXTRA")
            stat    = (
                "MATCH (100%)"       if in_src == "YES" and in_tgt == "YES"
                else "MISSING_IN_TARGET" if in_src == "YES"
                else "EXTRA_IN_TARGET"
            )
            w.writerow([b_id, bk_id, rtype, in_src, in_tgt, stat])

    print(f" Delta Source Keys  : {len(src_keys)} (NEW + CHANGED)")
    print(f" Confirmed in Target: {len(tgt_keys)}")
    print(f" Matched Key Pairs  : {len(matched_keys)} / {len(src_keys)}")

    if len(missing_in_target) == 0 and len(src_keys) > 0:
        print("\n 🎉 DELTA VERIFICATION: 100% MATCH! All delta rows verified in target.")
    else:
        print(f"\n ⚠️ DELTA VERIFICATION WARNING: {len(missing_in_target)} missing, {len(extra_in_target)} extra.")

    print(f" 📊 Delta Comparison Report: {os.path.abspath(report_csv)}")
    print("=" * 80 + "\n")


# ===========================================================================
# MAIN
# ===========================================================================

HERE = os.path.dirname(os.path.abspath(__file__))
RESULT = {}


def main():
    _dm.init_session()          # model session: sweep.loader = fe_migration
    parser = argparse.ArgumentParser(
        description="Program Bank Capacity Migration with Delta Detection & Upsert"
    )
    parser.add_argument("--host",       default=DEFAULT_HOST,         help=f"Postgres host (default: {DEFAULT_HOST})")
    parser.add_argument("--port",       default=DEFAULT_PORT, type=int,help=f"Postgres port (default: {DEFAULT_PORT})")
    parser.add_argument("--user",       default=DEFAULT_USER,         help=f"Postgres user (default: {DEFAULT_USER})")
    parser.add_argument("--password",   default=DEFAULT_PASS,         help="Postgres password (default: empty or from env)")
    parser.add_argument("--src-db",     default=DEFAULT_SRC_DB,       help=f"Source database (default: {DEFAULT_SRC_DB})")
    parser.add_argument("--tgt-db", "--dst-db", "--target-db", dest="tgt_db",
                        default=DEFAULT_TGT_DB,                       help=f"Target database (default: {DEFAULT_TGT_DB})")
    parser.add_argument("--db",         default=None,                 help="Fallback database for both source & target if specified")
    parser.add_argument("--src-schema", default=DEFAULT_SRC_SCHEMA,   help=f"Source schema (default: {DEFAULT_SRC_SCHEMA})")
    parser.add_argument("--target",     default=DEFAULT_TARGET_TABLE, help=f"Target table (default: {DEFAULT_TARGET_TABLE})")
    parser.add_argument("--replace",    action="store_true",           help="Delete all earlier SYSTEM rows before inserting (full reload)")
    parser.add_argument("--out-dir",    default=None,                 help="Directory for preview/staging artifacts (default: $VSAM_OUT/program_bank_capacity)")
    parser.add_argument("--execute",    action="store_true",           help="Execute delta upsert into target database (dry run by default)")
    parser.add_argument("--mode", choices=["full", "delta"], default=os.environ.get("SWEEP_MODE") or "full",
                        help="full / delta (the runner passes it; the model compares source and target itself, so both are idempotent)")
    args = parser.parse_args()

    src_db = args.src_db
    dst_db = args.tgt_db
    if args.db:
        if args.src_db == DEFAULT_SRC_DB:
            src_db = args.db
        if args.tgt_db == DEFAULT_TGT_DB:
            dst_db = args.db

    # Setup output artifact directory
    base_out = args.out_dir or os.environ.get("SWEEP_MODEL_WORKDIR") or os.path.join(HERE, "work", "dm01_program_bank_capacity", "manual")
    os.makedirs(base_out, exist_ok=True)

    print("=" * 80)
    print(" PROGRAM BANK CAPACITY — DELTA MIGRATION PIPELINE")
    print("=" * 80)
    print(f" Connecting to Postgres at {args.host}:{args.port} (User: {args.user})")
    print(f" Source DB: {src_db} (Schema: {args.src_schema}) | Target DB: {dst_db} | Target Table: {args.target}")
    print(f" Execution Mode: {'EXECUTE (Delta Upsert)' if args.execute else 'DRY RUN (Preview Only)'}")
    print(f" Full Replace Mode: {args.replace}")
    print(f" Artifacts Dir: {os.path.abspath(base_out)}")
    print("-" * 80)

    # -------------------------------------------------------------------------
    # STEP 1: Verify source tables exist in source database
    # -------------------------------------------------------------------------
    print("[*] STEP 1: Verifying source tables...")
    for tbl_name in [SRC_TABLE_CAPACITY_PLAN, SRC_TABLE_CUSIP_GROUP, SRC_TABLE_CUSIP, SRC_TABLE_BANK]:
        rs, rt, cols = find_cols(src_db, args.src_schema, tbl_name, args.host, args.port, args.user, args.password)
        if rs is None:
            sys.exit(f"Source table {args.src_schema}.{tbl_name} not found in database {src_db}")
        print(f"    ✓ {args.src_schema}.{tbl_name}")

    # -------------------------------------------------------------------------
    # STEP 2: Extract all source records
    # -------------------------------------------------------------------------
    print(f"\n[*] STEP 2: Extracting capacity records from {src_db}...")
    src_query = f"""
    SELECT DISTINCT
        b.brokerid::text AS broker_id,
        c.bankid::text AS bank_id,
        CAST(cp.bankcapacity AS NUMERIC(19,4))::text AS bank_capacity
    FROM {q(args.src_schema)}.{q(SRC_TABLE_CAPACITY_PLAN)} cp
    INNER JOIN {q(args.src_schema)}.{q(SRC_TABLE_CUSIP_GROUP)} ccg
        ON cp.cusipgroupid = ccg.cusipgroupid
    INNER JOIN {q(args.src_schema)}.{q(SRC_TABLE_CUSIP)} c
        ON ccg.cusipid = c.cusipid
    INNER JOIN {q(args.src_schema)}.{q(SRC_TABLE_BANK)} b
        ON c.bankid = b.bankid
    WHERE b.brokerid IS NOT NULL AND c.bankid IS NOT NULL
    ORDER BY 1, 2;
    """
    raw_rows = run_psql(src_db, src_query, args.host, args.port, args.user, args.password, readonly=True)
    print(f"    Extracted {len(raw_rows)} joined rows from {src_db}.")

    # Save full source extraction
    src_rows_file = os.path.join(base_out, "source_rows.csv")
    with open(src_rows_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["broker_id", "bank_id", "bank_capacity"])
        writer.writerows(raw_rows)

    if not raw_rows:
        print("[-] Warning: No eligible source rows found.")
        if args.execute:
            sys.exit("NOT EXECUTED: Source query returned 0 rows.")

    # -------------------------------------------------------------------------
    # STEP 3: Apply transformations & business override rules
    # -------------------------------------------------------------------------
    print("\n[*] STEP 3: Applying transformations & business overrides...")
    transformed = []
    overrides_hit = []
    pair_counts = Counter()

    for idx, r in enumerate(raw_rows, start=1):
        try:
            broker_id = int(r[0].strip())
            bank_id   = int(r[1].strip())
        except (ValueError, TypeError):
            continue

        orig_capacity  = r[2].strip() if r[2] else "0.0000"
        pair_counts[(broker_id, bank_id)] += 1

        override_key = (broker_id, bank_id)
        if override_key in SPECIFIC_OVERRIDES:
            final_capacity = SPECIFIC_OVERRIDES[override_key]
            overrides_hit.append({
                "broker_id":         broker_id,
                "bank_id":           bank_id,
                "original_capacity": orig_capacity,
                "override_capacity": final_capacity,
            })
            override_applied = "YES"
        else:
            final_capacity   = orig_capacity
            override_applied = "NO"

        transformed.append({
            "stage_id":                   idx,
            "broker_id":                  broker_id,
            "bank_id":                    bank_id,
            "bank_capacity":              final_capacity,
            "overflow_capacity_percent":  DEFAULT_OVERFLOW_PCT,
            "capacity_percent_low_limit": DEFAULT_LOW_LIMIT,
            "capacity_percent_high_limit":DEFAULT_HIGH_LIMIT,
            "is_active":                  DEFAULT_IS_ACTIVE,
            "override_applied":           override_applied,
        })

    duplicates = {k: v for k, v in pair_counts.items() if v > 1}
    if duplicates:
        print(f"    NOTE: {len(duplicates)} (broker_id, bank_id) pairs have multiple capacity records in source.")
    print(f"    Unique (broker_id, bank_id) pairs: {len(pair_counts)}")
    print(f"    Business overrides detected: {len(overrides_hit)}")
    for ov in overrides_hit:
        print(f"      -> Rule Applied: broker_id={ov['broker_id']}, bank_id={ov['bank_id']} | capacity: {ov['original_capacity']} -> {ov['override_capacity']}")

    # -------------------------------------------------------------------------
    # STEP 4: Inspect target table & resolve columns
    # -------------------------------------------------------------------------
    print(f"\n[*] STEP 4: Inspecting target table {args.target} in {dst_db}...")
    tsch, ttab = args.target.split(".", 1) if "." in args.target else ("public", args.target)
    rts, rtt, all_tgt = find_cols(dst_db, tsch, ttab, args.host, args.port, args.user, args.password)
    if rts is None:
        sys.exit(f"Target table {args.target} does not exist in database {dst_db}.")

    # Resolve business columns
    tcols, tmiss = {}, []
    for c in BUSINESS_COLS:
        aliases   = COLUMN_ALIASES.get(c, [c])
        found_col = next((all_tgt[a] for a in aliases if a in all_tgt), None)
        if found_col:
            tcols[c] = found_col
        else:
            tmiss.append(c)

    if tmiss:
        sys.exit(f"Target table {rts}.{rtt} in {dst_db} is missing required business columns: {', '.join(tmiss)}")

    # Resolve audit/metadata columns
    # the six canonical audit columns (+ legacy variants the table carries): ensured in the load transaction, filled here
    audit_insert_fields = _dm.insert_audit(all_tgt)

    print(f"    Detected audit columns: {', '.join(a[0] for a in audit_insert_fields)}")

    tgt              = f"{q(rts)}.{q(rtt)}"
    broker_col_name  = tcols["broker_id"][0]
    bank_col_name    = tcols["bank_id"][0]
    cap_col_name     = tcols["bank_capacity"][0]
    createdby_real   = "createdby_m"      # own rows of the model family: createdby_m = 'fe_migration'

    # Count target rows
    count_all    = run_psql(dst_db, f"SELECT count(*) FROM {tgt}", args.host, args.port, args.user, args.password, readonly=True)[0][0]
    count_system = "0"
    if "createdby_m" in all_tgt:
        count_system = run_psql(
            dst_db,
            f"SELECT count(*) FROM {tgt} WHERE {q(createdby_real)} = {lit(_dm.TAG)}",
            args.host, args.port, args.user, args.password, readonly=True,
        )[0][0]
    print(f"    Target current state: {count_all} total rows ({count_system} rows by '{_dm.TAG}')")

    # -------------------------------------------------------------------------
    # STEP 5: DELTA DETECTION — Compare source vs target
    # -------------------------------------------------------------------------
    print("\n[*] STEP 5: Running delta detection (source vs target)...")

    if args.replace:
        # Full replace: treat everything as NEW (bypass delta detection)
        print("    --replace flag set: treating all source rows as NEW (full reload).")
        classified_rows = [dict(r, row_type="NEW") for r in transformed]
        delta_rows      = classified_rows
    else:
        existing_target = fetch_existing_target_keys(
            dst_db, tgt,
            broker_col_name, bank_col_name, cap_col_name,
            args.host, args.port, args.user, args.password,
        )
        classified_rows, delta_rows = classify_rows(transformed, existing_target)

    new_count       = sum(1 for r in classified_rows if r["row_type"] == "NEW")
    changed_count   = sum(1 for r in classified_rows if r["row_type"] == "CHANGED")
    unchanged_count = sum(1 for r in classified_rows if r["row_type"] == "UNCHANGED")

    print(f"    Total Source Rows : {len(classified_rows)}")
    print(f"    NEW (to INSERT)   : {new_count}")
    print(f"    CHANGED (to UPDATE): {changed_count}")
    print(f"    UNCHANGED (skip)  : {unchanged_count}")
    RESULT.update(inserted=new_count, updated=changed_count, unchanged=unchanged_count, dry_run=not args.execute)
    print(f"    Delta rows total  : {len(delta_rows)}")

    # -------------------------------------------------------------------------
    # STEP 6: Write output files
    # -------------------------------------------------------------------------

    # preview.csv — ALL source rows with row_type classification
    preview_file = os.path.join(base_out, "preview.csv")
    preview_fieldnames = [
        "stage_id", "broker_id", "bank_id", "bank_capacity",
        "overflow_capacity_percent", "capacity_percent_low_limit",
        "capacity_percent_high_limit", "is_active",
        "override_applied", "row_type",
    ]
    with open(preview_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=preview_fieldnames)
        writer.writeheader()
        writer.writerows(classified_rows)

    # stage.csv — DELTA rows only (NEW + CHANGED), used for \copy
    stage_file = os.path.join(base_out, "stage.csv")
    with open(stage_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for idx, t in enumerate(delta_rows, start=1):
            writer.writerow([
                idx,                              # re-sequence stage_id for delta only
                t["broker_id"],
                t["bank_id"],
                t["bank_capacity"],
                t["overflow_capacity_percent"],
                t["capacity_percent_low_limit"],
                t["capacity_percent_high_limit"],
                t["is_active"],
                t["row_type"],                    # NEW or CHANGED
            ])

    # delta_summary.csv — Concise delta-only summary
    delta_summary_file = os.path.join(base_out, "delta_summary.csv")
    with open(delta_summary_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["broker_id", "bank_id", "bank_capacity", "row_type", "override_applied"])
        for t in delta_rows:
            writer.writerow([t["broker_id"], t["bank_id"], t["bank_capacity"], t["row_type"], t["override_applied"]])

    # -------------------------------------------------------------------------
    # STEP 7: Construct UPSERT load.sql (INSERT ... ON CONFLICT DO UPDATE)
    # -------------------------------------------------------------------------
    stage_path_sql = stage_file.replace("\\", "/")

    def cast(col, expr):
        return f"({expr})::{tcols[col][1]}"

    # Build column lists for INSERT
    ins_cols     = [q(tcols[c][0]) for c in BUSINESS_COLS] + [q(a[0]) for a in audit_insert_fields]
    ins_cols_sql = ", ".join(ins_cols)

    sel_expressions = [
        cast("broker_id",                  "s.broker_id"),
        cast("bank_id",                    "s.bank_id"),
        cast("bank_capacity",              "s.bank_capacity"),
        cast("overflow_capacity_percent",  "s.overflow_pct"),
        cast("capacity_percent_low_limit", "s.low_limit"),
        cast("capacity_percent_high_limit","s.high_limit"),
        cast("is_active",                  "s.is_active"),
    ]
    for real_name, data_type, expr_val in audit_insert_fields:
        sel_expressions.append(f"({expr_val})::{data_type}")
    sel_sql = ", ".join(sel_expressions)

    # Set-based UPDATE for CHANGED rows — works without requiring a UNIQUE constraint on target
    update_set_parts = [
        f"{q(tcols['bank_capacity'][0])} = {cast('bank_capacity', 's.bank_capacity')}",
        f"{q(tcols['overflow_capacity_percent'][0])} = {cast('overflow_capacity_percent', 's.overflow_pct')}",
        f"{q(tcols['capacity_percent_low_limit'][0])} = {cast('capacity_percent_low_limit', 's.low_limit')}",
        f"{q(tcols['capacity_percent_high_limit'][0])} = {cast('capacity_percent_high_limit', 's.high_limit')}",
        f"{q(tcols['is_active'][0])} = {cast('is_active', 's.is_active')}",
    ]
    # an UPDATE sets only the data columns + modifiedby / modifieddate (created* and the _m columns are never touched)
    for col_, expr_ in _dm.update_audit(all_tgt):
        update_set_parts.append(f"{q(col_)} = {expr_}")
    update_set_sql = ",\n        ".join(update_set_parts)

    key_match_sql = (
        f"t.{q(broker_col_name)} = {cast('broker_id', 's.broker_id')} "
        f"AND t.{q(bank_col_name)} = {cast('bank_id', 's.bank_id')}"
    )

    sql_statements = [
        "\\set ON_ERROR_STOP on",
        "BEGIN;",
        "SET LOCAL lock_timeout = '60s';",
        f"LOCK TABLE {tgt} IN SHARE ROW EXCLUSIVE MODE;",
        _dm.ensure_audit_sql(f"{rts}.{rtt}"),
    ]

    if args.replace and createdby_real:
        sql_statements.append(f"DELETE FROM {tgt} WHERE {q(createdby_real)} = {lit(_dm.TAG)};")

    sql_statements += [
        "",
        "-- Temp staging table for delta rows only",
        "CREATE TEMP TABLE pbc_stage (",
        "    rn           bigint,",
        "    broker_id    bigint,",
        "    bank_id      bigint,",
        "    bank_capacity    numeric(19,4),",
        "    overflow_pct     numeric(10,3),",
        "    low_limit        numeric(10,3),",
        "    high_limit       numeric(10,3),",
        "    is_active        boolean,",
        "    row_type         text",
        ") ON COMMIT DROP;",
        f"\\copy pbc_stage FROM '{stage_path_sql}' WITH (FORMAT csv)",
        "",
        "-- 1. UPDATE changed rows (no UNIQUE constraint required on target)",
        f"UPDATE {tgt} t",
        "SET",
        f"    {update_set_sql}",
        "FROM pbc_stage s",
        "WHERE s.row_type = 'CHANGED'",
        f"  AND {key_match_sql};",
        "",
        "-- 2. INSERT new rows (fresh load & delta inserts)",
        f"INSERT INTO {tgt} ({ins_cols_sql})",
        f"SELECT {sel_sql}",
        "FROM pbc_stage s",
        "WHERE s.row_type = 'NEW'",
        "  AND NOT EXISTS (",
        f"      SELECT 1 FROM {tgt} t",
        f"      WHERE {key_match_sql}",
        "  )",
        "ORDER BY s.rn;",
        "",
        "-- Explicit business override update (spec requirement)",
        f"UPDATE {tgt} SET {q(cap_col_name)} = 25000000.0000",
        f"WHERE {q(bank_col_name)} = 1707 AND {q(broker_col_name)} = 108;",
        "",
        f"SELECT 'DELTA_UPSERT_COMPLETE' AS status, count(*) AS total_rows FROM {tgt}" +
        (f" WHERE {q(createdby_real)} = {lit(_dm.TAG)}" if createdby_real else "") + ";",
        "COMMIT;",
    ]

    sql_file = os.path.join(base_out, "load.sql")
    with open(sql_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sql_statements) + "\n")

    print("\n[*] Artifacts generated:")
    print(f"    - Source Rows CSV   : {src_rows_file}         (all {len(raw_rows)} source rows)")
    print(f"    - Preview CSV       : {preview_file}  (all rows + row_type: NEW/CHANGED/UNCHANGED)")
    print(f"    - Stage CSV         : {stage_file}    (delta rows only: {len(delta_rows)} rows)")
    print(f"    - Delta Summary CSV : {delta_summary_file}")
    print(f"    - Load SQL (UPSERT) : {sql_file}")

    # Console sample
    print("-" * 80)
    print(" SAMPLE DELTA RECORDS (First 5):")
    for item in delta_rows[:5]:
        print(f"   [{item['row_type']:<9}] Broker: {item['broker_id']:<6} | Bank: {item['bank_id']:<6} | "
              f"Capacity: {item['bank_capacity']:<16} | Override: {item['override_applied']}")
    if not delta_rows:
        print("   (No delta rows — all source rows already exist unchanged in target)")
    print("-" * 80)

    # -------------------------------------------------------------------------
    # STEP 8: Execute or Dry Run exit
    # -------------------------------------------------------------------------
    if not args.execute:
        print("[!] DRY RUN COMPLETED SUCCESSFULLY: No database mutations were made.")
        print(f"    Review {preview_file} for full classification.")
        print(f"    Review {delta_summary_file} for delta rows only.")
        print("    To apply delta upsert to target, rerun with: --execute")
        if unchanged_count == len(classified_rows) and not args.replace:
            print("    ℹ️  All source rows are UNCHANGED — no upsert needed.")
        return

    if not delta_rows and not args.replace:
        print("[!] No delta rows detected. Nothing to upsert. Exiting without changes.")
        return

    print(f"[*] Executing delta UPSERT transaction on database {dst_db}...")
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

    print("[+] DELTA UPSERT TRANSACTION COMMITTED SUCCESSFULLY!")
    print(run_result.stdout.strip())
    print(f"    Log written to: {log_file}")
    print(f"    Done: {new_count} rows INSERTED, {changed_count} rows UPDATED in {rts}.{rtt}")

    # -------------------------------------------------------------------------
    # STEP 9: Automatic In-Line Post-Load Delta Comparison
    # -------------------------------------------------------------------------
    run_post_load_comparison(
        dst_db, tgt, delta_rows,
        broker_col_name, bank_col_name,
        args.host, args.port, args.user, args.password,
        base_out,
    )


if __name__ == "__main__":
    main()
    print(_dm.result_line(dict(RESULT, model=SWEEP_MODEL["name"])))
