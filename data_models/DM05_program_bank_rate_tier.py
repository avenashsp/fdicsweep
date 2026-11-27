#!/usr/bin/env python3
r"""program_bank_rate_tier_model.py - Program Bank Rate Tier migration with Delta Detection & Upsert:
Source tables (PostgreSQL, default bcs):
    bcs.program_bank_rate pbr
    JOIN bcs.cusip c ON c.bankid = pbr.bank_id AND c.productid = pbr.product_id
    JOIN bcs.cusipcusipgroup ccg ON ccg.cusipid = c.cusipid
    JOIN bcs.rate r ON r.cusipgroupid = ccg.cusipgroupid AND r.ratetypeid = pbr.rate_type_id
    JOIN bcs.ratetier rt ON rt.rateid = r.rateid
    LEFT JOIN bcs.flattier ft ON ft.tierid = rt.tierid
    LEFT JOIN bcs.indexedtier it ON it.tierid = rt.tierid

Target table (PostgreSQL, default bcs):
    bcs.program_bank_rate_tier (default, overridable via --target)

Delta Detection Logic:
    - Each source row is classified against existing target rows using natural key
      (program_bank_rate_id, tier_type):
        NEW       : key does not exist in target → INSERT
        CHANGED   : key exists but value fields differ (flat_rate, lower_limit,
                    upper_limit, effective_date, expiration_date, published_index_id) → UPDATE
        UNCHANGED : all fields match → SKIP
    - preview.csv shows ALL source rows with the row_type column.
    - stage.csv (for \copy) contains ONLY NEW + CHANGED rows.
    - load.sql performs INSERT ... ON CONFLICT DO UPDATE (UPSERT) for delta rows only.
    - comparison_report.csv covers ONLY the delta rows (NEW + CHANGED).

DRY RUN unless --execute.

Commands:
    python program_bank_rate_tier_model.py                     # Dry run (safe, read-only)
    python program_bank_rate_tier_model.py --execute           # Delta upsert into target table
    python program_bank_rate_tier_model.py --execute --replace # Delete all SYSTEM rows first, then full insert
"""

SWEEP_MODEL = {
    "name": "dm05_program_bank_rate_tier",
    "inputs": ['bcs.program_bank_rate', 'bcs.cusip', 'bcs.cusipcusipgroup', 'bcs.rate', 'bcs.ratetier', 'bcs.flattier', 'bcs.indexedtier'],
    "outputs": ['bcs.program_bank_rate_tier'],
    "depends_on": ['dm04_program_bank_rate'],
    "delta_strategy": "update_in_place",
    "update_key": ['program_bank_rate_id', 'tier_type'],
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
DEFAULT_TARGET_TABLE = "bcs.program_bank_rate_tier"
DEFAULT_SRC_SCHEMA = "bcs"
DEFAULT_PBR_TABLE  = "auto"        # "auto" = bcs.program_bank_rate (the output of DM04)

SRC_BASE_TABLES = ["cusip", "cusipcusipgroup", "rate", "ratetier", "flattier", "indexedtier"]

BUSINESS_COLS = [
    "program_bank_rate_id",
    "tier_type",
    "lower_limit",
    "upper_limit",
    "effective_date",
    "expiration_date",
    "flat_rate",
    "published_index_id",
    "trailing_days",
    "modifier_number",
    "modifier_percent",
    "floor_rate",
    "floor_rate_unit",
]

COLUMN_ALIASES = {
    "program_bank_rate_id": ["program_bank_rate_id", "programbankrateid"],
    "tier_type":            ["tier_type", "tiertype"],
    "lower_limit":          ["lower_limit", "lowerlimit", "tierlowerlimit"],
    "upper_limit":          ["upper_limit", "upperlimit", "tierupperlimit"],
    "effective_date":       ["effective_date", "effectivedate"],
    "expiration_date":      ["expiration_date", "expirationdate"],
    "flat_rate":            ["flat_rate", "flatrate", "rate"],
    "published_index_id":   ["published_index_id", "publishedindexid"],
    "trailing_days":        ["trailing_days", "trailingdays"],
    "modifier_number":      ["modifier_number", "modifiernumber", "ratemodifierincrementnumber"],
    "modifier_percent":     ["modifier_percent", "modifierpercent", "ratemodifierpercentagenumber"],
    "floor_rate":           ["floor_rate", "floorrate"],
    "floor_rate_unit":      ["floor_rate_unit", "floorrateunit"],
}

# Audit & metadata columns: the six canonical audit columns (createdby, createddate, modifiedby, modifieddate,
# createddate_m, createdby_m) are handled by _dm_support / sweep_audit - see docs/audit_columns.md

# Value fields used to detect CHANGED rows (beyond the natural key)
CHANGE_DETECT_FIELDS = [
    "flat_rate", "lower_limit", "upper_limit",
    "effective_date", "expiration_date", "published_index_id",
]

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

def fetch_existing_target_keys(dst_db, tgt_expr, pbr_id_col, tier_col,
                                flat_col, low_col, up_col, eff_col, exp_col, pub_col,
                                host, port, user, password):
    """
    Fetches existing (program_bank_rate_id, tier_type) + value fields from target.
    Returns dict: {(pbr_id_str, tier_type_str): (flat_rate, lower_limit, upper_limit,
                                                   effective_date, expiration_date, pub_idx_id)}
    """
    sql = f"""
    SELECT
        TRIM({q(pbr_id_col)}::text),
        COALESCE(TRIM({q(tier_col)}::text), ''),
        COALESCE(TRIM({q(flat_col)}::text), ''),
        COALESCE(TRIM({q(low_col)}::text), ''),
        COALESCE(TRIM({q(up_col)}::text), ''),
        COALESCE(TRIM({q(eff_col)}::text), ''),
        COALESCE(TRIM({q(exp_col)}::text), ''),
        COALESCE(TRIM({q(pub_col)}::text), '')
    FROM {tgt_expr}
    WHERE {q(pbr_id_col)} IS NOT NULL
    ORDER BY 1, 2;
    """
    rows = run_psql(dst_db, sql, host, port, user, password, readonly=True)
    return {(r[0], r[1]): (r[2], r[3], r[4], r[5], r[6], r[7]) for r in rows}


def classify_rows(transformed, existing_target):
    """
    Classifies each transformed source row as NEW, CHANGED, or UNCHANGED.
    Natural key: (program_bank_rate_id, tier_type)
    Change fields: flat_rate, lower_limit, upper_limit, effective_date,
                   expiration_date, published_index_id
    """
    classified_rows = []
    delta_rows      = []

    for row in transformed:
        key      = (str(row["program_bank_rate_id"]), str(row["tier_type"]))
        tgt_vals = existing_target.get(key)

        if tgt_vals is None:
            row_type = "NEW"
        else:
            tgt_flat, tgt_low, tgt_up, tgt_eff, tgt_exp, tgt_pub = tgt_vals
            src_flat = str(row["flat_rate"]).strip()
            src_low  = str(row["lower_limit"]).strip()
            src_up   = str(row["upper_limit"]).strip()
            src_eff  = str(row["effective_date"]).strip()
            src_exp  = str(row["expiration_date"]).strip()
            src_pub  = str(row["published_index_id"]).strip()

            # Normalize numeric comparisons
            def norm_num(v):
                try:
                    return str(float(v)) if v else ""
                except (ValueError, TypeError):
                    return str(v).strip()

            row_type = "CHANGED" if (
                norm_num(src_flat) != norm_num(tgt_flat)
                or norm_num(src_low)  != norm_num(tgt_low)
                or norm_num(src_up)   != norm_num(tgt_up)
                or src_eff  != tgt_eff
                or src_exp  != tgt_exp
                or src_pub  != tgt_pub
            ) else "UNCHANGED"

        classified = dict(row, row_type=row_type)
        classified_rows.append(classified)
        if row_type in ("NEW", "CHANGED"):
            delta_rows.append(classified)

    return classified_rows, delta_rows


# ===========================================================================
# POST-LOAD COMPARISON (DELTA ROWS ONLY)
# ===========================================================================

def run_post_load_comparison(dst_db, tgt_expr, delta_rows, pbr_id_col, tier_col,
                              host, port, user, password, out_dir):
    """
    Compares program_bank_rate_id and tier_type for DELTA rows only (NEW + CHANGED),
    writing an Excel-ready comparison_report.csv.
    """
    print("\n" + "=" * 80)
    print(" 🔍 AUTOMATIC POST-LOAD DELTA VERIFICATION & COMPARISON REPORT")
    print("=" * 80)
    print(f" Target Table:       {tgt_expr}")
    print(" 2 Natural Fields:   program_bank_rate_id & tier_type (surrogate sequence ID bypassed)")
    print(f" Scope:              DELTA rows only (NEW + CHANGED) — {len(delta_rows)} rows")

    if not delta_rows:
        print(" ℹ️  No delta rows to compare (all source rows were UNCHANGED).")
        print("=" * 80 + "\n")
        return

    tgt_sql = f"""
    SELECT DISTINCT
        TRIM({q(pbr_id_col)}::text),
        COALESCE(TRIM({q(tier_col)}::text), '')
    FROM {tgt_expr}
    WHERE {q(pbr_id_col)} IS NOT NULL
    ORDER BY 1, 2;
    """
    target_data = run_psql(dst_db, tgt_sql, host, port, user, password, readonly=True)
    all_tgt_keys = set((r[0].strip(), r[1].strip()) for r in target_data)
    src_keys  = set((str(r["program_bank_rate_id"]), str(r["tier_type"])) for r in delta_rows)
    tgt_keys  = all_tgt_keys & src_keys

    matched_keys      = src_keys & tgt_keys
    missing_in_target = src_keys - tgt_keys
    extra_in_target   = tgt_keys - src_keys

    row_type_map = {
        (str(r["program_bank_rate_id"]), str(r["tier_type"])): r["row_type"]
        for r in delta_rows
    }

    report_csv = os.path.join(out_dir, "comparison_report.csv")
    with open(report_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Program_Bank_Rate_ID", "Tier_Type", "Row_Type",
                    "Source_Present", "Target_Present", "Match_Status"])
        all_keys = sorted(src_keys | tgt_keys,
                          key=lambda x: (int(x[0]) if x[0].isdigit() else x[0], x[1]))
        for pbr_id, tt in all_keys:
            in_src = "YES" if (pbr_id, tt) in src_keys else "NO"
            in_tgt = "YES" if (pbr_id, tt) in tgt_keys else "NO"
            rtype  = row_type_map.get((pbr_id, tt), "EXTRA")
            stat   = (
                "MATCH (100%)"       if in_src == "YES" and in_tgt == "YES"
                else "MISSING_IN_TARGET" if in_src == "YES"
                else "EXTRA_IN_TARGET"
            )
            w.writerow([pbr_id, tt, rtype, in_src, in_tgt, stat])

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
        description="Program Bank Rate Tier Migration with Delta Detection & Upsert"
    )
    parser.add_argument("--host",       default=DEFAULT_HOST,          help=f"Postgres host (default: {DEFAULT_HOST})")
    parser.add_argument("--port",       default=DEFAULT_PORT, type=int, help=f"Postgres port (default: {DEFAULT_PORT})")
    parser.add_argument("--user",       default=DEFAULT_USER,          help=f"Postgres user (default: {DEFAULT_USER})")
    parser.add_argument("--password",   default=DEFAULT_PASS,          help="Postgres password (default: empty or from env)")
    parser.add_argument("--src-db",     default=DEFAULT_SRC_DB,        help=f"Source database (default: {DEFAULT_SRC_DB})")
    parser.add_argument("--tgt-db", "--dst-db", "--target-db", dest="tgt_db",
                        default=DEFAULT_TGT_DB,                        help=f"Target database (default: {DEFAULT_TGT_DB})")
    parser.add_argument("--db",         default=None,                  help="Fallback database for both source & target")
    parser.add_argument("--src-schema", default=DEFAULT_SRC_SCHEMA,    help=f"Source schema (default: {DEFAULT_SRC_SCHEMA})")
    parser.add_argument("--pbr-table",  default=DEFAULT_PBR_TABLE,     help="Source program_bank_rate table (default: auto)")
    parser.add_argument("--target",     default=DEFAULT_TARGET_TABLE,  help=f"Target table (default: {DEFAULT_TARGET_TABLE})")
    parser.add_argument("--replace",    action="store_true",            help="Delete all earlier SYSTEM rows before inserting (full reload)")
    parser.add_argument("--out-dir",    default=None,                  help="Directory for preview/staging artifacts (default: $VSAM_OUT/program_bank_rate_tier)")
    parser.add_argument("--execute",    action="store_true",            help="Execute delta upsert into target database (dry run by default)")
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

    base_out = args.out_dir or os.environ.get("SWEEP_MODEL_WORKDIR") or os.path.join(HERE, "work", "dm05_program_bank_rate_tier", "manual")
    os.makedirs(base_out, exist_ok=True)

    print("=" * 80)
    print(" PROGRAM BANK RATE TIER — DELTA MIGRATION PIPELINE")
    print("=" * 80)
    print(f" Connecting to Postgres at {args.host}:{args.port} (User: {args.user})")
    print(f" Source DB: {src_db} (Schema: {args.src_schema}) | Target DB: {dst_db} | Target Table: {args.target}")
    print(f" Execution Mode: {'EXECUTE (Delta Upsert)' if args.execute else 'DRY RUN (Preview Only)'}")
    print(f" Full Replace Mode: {args.replace}")
    print(f" Artifacts Dir: {os.path.abspath(base_out)}")
    print("-" * 80)

    # -------------------------------------------------------------------------
    # STEP 1: Verify source tables & resolve program_bank_rate table
    # -------------------------------------------------------------------------
    print("[*] STEP 1: Verifying source tables...")
    for tbl_name in SRC_BASE_TABLES:
        rs, rt, cols = find_cols(src_db, args.src_schema, tbl_name, args.host, args.port, args.user, args.password)
        if rs is None:
            sys.exit(f"Source table {args.src_schema}.{tbl_name} not found in database {src_db}")
        print(f"    ✓ {args.src_schema}.{tbl_name}")

    # Resolve the program_bank_rate source table (the live bcs.program_bank_rate unless --pbr-table names another)
    if args.pbr_table != "auto":
        pbr_tsch, pbr_ttab = args.pbr_table.split(".", 1) if "." in args.pbr_table else (args.src_schema, args.pbr_table)
        r_pbr_sch, r_pbr_tab, _ = find_cols(src_db, pbr_tsch, pbr_ttab, args.host, args.port, args.user, args.password)
        if r_pbr_sch is None:
            sys.exit(f"Specified program_bank_rate table {args.pbr_table} not found in {src_db}")
        chosen_pbr_table = f"{q(r_pbr_sch)}.{q(r_pbr_tab)}"
    else:
        r_std_sch, r_std_tab, _ = find_cols(src_db, args.src_schema, "program_bank_rate",
                                              args.host, args.port, args.user, args.password)
        if not r_std_sch:
            sys.exit(f"program_bank_rate not found in {src_db}.{args.src_schema}")
        chosen_pbr_table = f"{q(r_std_sch)}.{q(r_std_tab)}"

    print(f"    ✓ Using program_bank_rate source: {chosen_pbr_table}")

    # -------------------------------------------------------------------------
    # STEP 2: Extract all source records
    # -------------------------------------------------------------------------
    print(f"\n[*] STEP 2: Extracting program bank rate tier records from {src_db}...")
    src_query = f"""
    WITH selected_ratetier AS (
        SELECT
            pbr.program_bank_rate_id,
            rt.tierid,
            rt.tiertype,
            rt.tierlowerlimit,
            rt.tierupperlimit,
            rt.effectivedate,
            rt.expirationdate,
            ROW_NUMBER() OVER (
                PARTITION BY pbr.program_bank_rate_id
                ORDER BY
                    CASE WHEN rt.expirationdate IS NULL THEN 0 ELSE 1 END,
                    rt.modifieddate DESC NULLS LAST,
                    rt.createddate DESC NULLS LAST
            ) AS rn
        FROM {chosen_pbr_table} pbr
        JOIN {q(args.src_schema)}.cusip c
            ON c.bankid = pbr.bank_id AND c.productid = pbr.product_id
        JOIN {q(args.src_schema)}.cusipcusipgroup ccg
            ON ccg.cusipid = c.cusipid
        JOIN {q(args.src_schema)}.rate r
            ON r.cusipgroupid = ccg.cusipgroupid AND r.ratetypeid = pbr.rate_type_id
        JOIN {q(args.src_schema)}.ratetier rt
            ON rt.rateid = r.rateid
    )
    SELECT
        srt.program_bank_rate_id::text,
        COALESCE(srt.tiertype::text, ''),
        COALESCE(srt.tierlowerlimit, 0)::text,
        COALESCE(srt.tierupperlimit::text, ''),
        COALESCE(srt.effectivedate::date::text, ''),
        COALESCE(srt.expirationdate::date::text, ''),
        COALESCE(ft.rate::text, ''),
        COALESCE(it.publishedindexid::text, ''),
        COALESCE(it.trailingdays::text, ''),
        COALESCE(it.ratemodifierincrementnumber::text, ''),
        COALESCE(it.ratemodifierpercentagenumber::text, ''),
        COALESCE(it.floorrate::text, ''),
        'percent'
    FROM selected_ratetier srt
    LEFT JOIN {q(args.src_schema)}.flattier ft ON ft.tierid = srt.tierid
    LEFT JOIN {q(args.src_schema)}.indexedtier it ON it.tierid = srt.tierid
    WHERE srt.rn = 1
    ORDER BY srt.program_bank_rate_id;
    """
    raw_rows = run_psql(src_db, src_query, args.host, args.port, args.user, args.password, readonly=True)
    print(f"    Extracted {len(raw_rows)} joined rows from {src_db}.")

    src_rows_file = os.path.join(base_out, "source_rows.csv")
    with open(src_rows_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(BUSINESS_COLS)
        writer.writerows(raw_rows)

    if not raw_rows:
        print("[-] Warning: No eligible source rows found.")
        if args.execute:
            sys.exit("NOT EXECUTED: Source query returned 0 rows.")

    # -------------------------------------------------------------------------
    # STEP 3: Apply transformations & format records
    # -------------------------------------------------------------------------
    print("\n[*] STEP 3: Applying transformations...")
    transformed    = []
    pbr_id_counts  = Counter()

    for idx, r in enumerate(raw_rows, start=1):
        if len(r) < 13:
            continue
        pbr_id          = r[0].strip()
        tier_type       = r[1].strip()
        lower_limit     = r[2].strip() or "0"
        upper_limit     = r[3].strip()
        effective_date  = r[4].strip()
        expiration_date = r[5].strip()
        flat_rate       = r[6].strip()
        pub_idx_id      = r[7].strip()
        trailing_days   = r[8].strip()
        mod_num         = r[9].strip()
        mod_pct         = r[10].strip()
        floor_rate      = r[11].strip()
        floor_unit      = r[12].strip() or "percent"

        pbr_id_counts[pbr_id] += 1

        transformed.append({
            "stage_id":             idx,
            "program_bank_rate_id": pbr_id,
            "tier_type":            tier_type,
            "lower_limit":          lower_limit,
            "upper_limit":          upper_limit,
            "effective_date":       effective_date,
            "expiration_date":      expiration_date,
            "flat_rate":            flat_rate,
            "published_index_id":   pub_idx_id,
            "trailing_days":        trailing_days,
            "modifier_number":      mod_num,
            "modifier_percent":     mod_pct,
            "floor_rate":           floor_rate,
            "floor_rate_unit":      floor_unit,
            "is_active":            "true",
        })

    print(f"    Unique program_bank_rate_ids: {len(pbr_id_counts)}")

    # -------------------------------------------------------------------------
    # STEP 4: Inspect target table & resolve columns
    # -------------------------------------------------------------------------
    print(f"\n[*] STEP 4: Inspecting target table {args.target} in {dst_db}...")
    tsch, ttab = args.target.split(".", 1) if "." in args.target else ("public", args.target)
    rts, rtt, all_tgt = find_cols(dst_db, tsch, ttab, args.host, args.port, args.user, args.password)
    if rts is None:
        sys.exit(f"Target table {args.target} does not exist in database {dst_db}.")

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

    # the six canonical audit columns (+ legacy variants the table carries): ensured in the load transaction, filled here
    audit_insert_fields = _dm.insert_audit(all_tgt)

    print(f"    Detected audit columns: {', '.join(a[0] for a in audit_insert_fields)}")

    tgt            = f"{q(rts)}.{q(rtt)}"
    pbr_id_col     = tcols["program_bank_rate_id"][0]
    tier_col       = tcols["tier_type"][0]
    flat_col       = tcols["flat_rate"][0]
    low_col        = tcols["lower_limit"][0]
    up_col         = tcols["upper_limit"][0]
    eff_col        = tcols["effective_date"][0]
    exp_col        = tcols["expiration_date"][0]
    pub_col        = tcols["published_index_id"][0]
    createdby_real   = "createdby_m"      # own rows of the model family: createdby_m = 'fe_migration'

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
    # STEP 5: DELTA DETECTION
    # -------------------------------------------------------------------------
    print("\n[*] STEP 5: Running delta detection (source vs target)...")

    if args.replace:
        print("    --replace flag set: treating all source rows as NEW (full reload).")
        classified_rows = [dict(r, row_type="NEW") for r in transformed]
        delta_rows      = classified_rows
    else:
        existing_target = fetch_existing_target_keys(
            dst_db, tgt,
            pbr_id_col, tier_col, flat_col, low_col, up_col, eff_col, exp_col, pub_col,
            args.host, args.port, args.user, args.password,
        )
        classified_rows, delta_rows = classify_rows(transformed, existing_target)

    new_count       = sum(1 for r in classified_rows if r["row_type"] == "NEW")
    changed_count   = sum(1 for r in classified_rows if r["row_type"] == "CHANGED")
    unchanged_count = sum(1 for r in classified_rows if r["row_type"] == "UNCHANGED")

    print(f"    Total Source Rows  : {len(classified_rows)}")
    print(f"    NEW (to INSERT)    : {new_count}")
    print(f"    CHANGED (to UPDATE): {changed_count}")
    print(f"    UNCHANGED (skip)   : {unchanged_count}")
    RESULT.update(inserted=new_count, updated=changed_count, unchanged=unchanged_count, dry_run=not args.execute)
    print(f"    Delta rows total   : {len(delta_rows)}")

    # -------------------------------------------------------------------------
    # STEP 6: Write output files
    # -------------------------------------------------------------------------

    # preview.csv — ALL rows with row_type classification
    preview_file = os.path.join(base_out, "preview.csv")
    preview_fieldnames = [
        "stage_id", "program_bank_rate_id", "tier_type", "lower_limit", "upper_limit",
        "effective_date", "expiration_date", "flat_rate", "published_index_id",
        "trailing_days", "modifier_number", "modifier_percent", "floor_rate",
        "floor_rate_unit", "is_active", "row_type",
    ]
    with open(preview_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=preview_fieldnames)
        writer.writeheader()
        writer.writerows(classified_rows)

    # stage.csv — DELTA rows only (NEW + CHANGED)
    stage_file = os.path.join(base_out, "stage.csv")
    with open(stage_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for idx, t in enumerate(delta_rows, start=1):
            writer.writerow([
                idx,
                t["program_bank_rate_id"], t["tier_type"], t["lower_limit"],
                t["upper_limit"], t["effective_date"], t["expiration_date"],
                t["flat_rate"], t["published_index_id"], t["trailing_days"],
                t["modifier_number"], t["modifier_percent"], t["floor_rate"],
                t["floor_rate_unit"], t["row_type"],
            ])

    # delta_summary.csv — concise delta-only view
    delta_summary_file = os.path.join(base_out, "delta_summary.csv")
    with open(delta_summary_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["program_bank_rate_id", "tier_type", "flat_rate",
                          "lower_limit", "upper_limit", "effective_date",
                          "expiration_date", "row_type"])
        for t in delta_rows:
            writer.writerow([
                t["program_bank_rate_id"], t["tier_type"], t["flat_rate"],
                t["lower_limit"], t["upper_limit"], t["effective_date"],
                t["expiration_date"], t["row_type"],
            ])

    # -------------------------------------------------------------------------
    # STEP 7: Construct UPSERT load.sql
    # -------------------------------------------------------------------------
    stage_path_sql = stage_file.replace("\\", "/")

    def cast(col, expr):
        return f"({expr})::{tcols[col][1]}"

    ins_cols     = [q(tcols[c][0]) for c in BUSINESS_COLS] + [q(a[0]) for a in audit_insert_fields]
    ins_cols_sql = ", ".join(ins_cols)

    sel_expressions = [
        cast("program_bank_rate_id", "s.pbr_id"),
        cast("tier_type",            "s.tier_type"),
        cast("lower_limit",          "s.lower_limit"),
        f"NULLIF(s.upper_limit, '')::{tcols['upper_limit'][1]}",
        f"NULLIF(s.effective_date, '')::{tcols['effective_date'][1]}",
        f"NULLIF(s.expiration_date, '')::{tcols['expiration_date'][1]}",
        f"NULLIF(s.flat_rate, '')::{tcols['flat_rate'][1]}",
        f"NULLIF(s.pub_idx_id, '')::{tcols['published_index_id'][1]}",
        f"NULLIF(s.trailing_days, '')::{tcols['trailing_days'][1]}",
        f"NULLIF(s.mod_num, '')::{tcols['modifier_number'][1]}",
        f"NULLIF(s.mod_pct, '')::{tcols['modifier_percent'][1]}",
        f"NULLIF(s.floor_rate, '')::{tcols['floor_rate'][1]}",
        cast("floor_rate_unit", "s.floor_unit"),
    ]
    for real_name, data_type, expr_val in audit_insert_fields:
        sel_expressions.append(f"({expr_val})::{data_type}")
    sel_sql = ", ".join(sel_expressions)

    # Set-based UPDATE for CHANGED rows — works without requiring a UNIQUE constraint on target
    update_set_parts = [
        f"{q(tcols['lower_limit'][0])} = {cast('lower_limit', 's.lower_limit')}",
        f"{q(tcols['upper_limit'][0])} = NULLIF(s.upper_limit, '')::{tcols['upper_limit'][1]}",
        f"{q(tcols['effective_date'][0])} = NULLIF(s.effective_date, '')::{tcols['effective_date'][1]}",
        f"{q(tcols['expiration_date'][0])} = NULLIF(s.expiration_date, '')::{tcols['expiration_date'][1]}",
        f"{q(tcols['flat_rate'][0])} = NULLIF(s.flat_rate, '')::{tcols['flat_rate'][1]}",
        f"{q(tcols['published_index_id'][0])} = NULLIF(s.pub_idx_id, '')::{tcols['published_index_id'][1]}",
        f"{q(tcols['trailing_days'][0])} = NULLIF(s.trailing_days, '')::{tcols['trailing_days'][1]}",
        f"{q(tcols['modifier_number'][0])} = NULLIF(s.mod_num, '')::{tcols['modifier_number'][1]}",
        f"{q(tcols['modifier_percent'][0])} = NULLIF(s.mod_pct, '')::{tcols['modifier_percent'][1]}",
        f"{q(tcols['floor_rate'][0])} = NULLIF(s.floor_rate, '')::{tcols['floor_rate'][1]}",
    ]
    # an UPDATE sets only the data columns + modifiedby / modifieddate (created* and the _m columns are never touched)
    for col_, expr_ in _dm.update_audit(all_tgt):
        update_set_parts.append(f"{q(col_)} = {expr_}")
    update_set_sql = ",\n        ".join(update_set_parts)

    key_match_sql = (
        f"t.{q(pbr_id_col)} = {cast('program_bank_rate_id', 's.pbr_id')} "
        f"AND COALESCE(TRIM(t.{q(tier_col)}::text), '') = COALESCE(TRIM(s.tier_type), '')"
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
        "-- Temp staging table for delta rows only (NEW + CHANGED)",
        "CREATE TEMP TABLE pbrt_stage (",
        "    rn              bigint,",
        "    pbr_id          bigint,",
        "    tier_type       text,",
        "    lower_limit     numeric,",
        "    upper_limit     text,",
        "    effective_date  text,",
        "    expiration_date text,",
        "    flat_rate       text,",
        "    pub_idx_id      text,",
        "    trailing_days   text,",
        "    mod_num         text,",
        "    mod_pct         text,",
        "    floor_rate      text,",
        "    floor_unit      text,",
        "    row_type        text",
        ") ON COMMIT DROP;",
        f"\\copy pbrt_stage FROM '{stage_path_sql}' WITH (FORMAT csv)",
        "",
        "-- 1. UPDATE changed rows (no UNIQUE constraint required on target)",
        f"UPDATE {tgt} t",
        "SET",
        f"    {update_set_sql}",
        "FROM pbrt_stage s",
        "WHERE s.row_type = 'CHANGED'",
        f"  AND {key_match_sql};",
        "",
        "-- 2. INSERT new rows (fresh load & delta inserts)",
        f"INSERT INTO {tgt} ({ins_cols_sql})",
        f"SELECT {sel_sql}",
        "FROM pbrt_stage s",
        "WHERE s.row_type = 'NEW'",
        "  AND NOT EXISTS (",
        f"      SELECT 1 FROM {tgt} t",
        f"      WHERE {key_match_sql}",
        "  )",
        "ORDER BY s.rn;",
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
    print(f"    - Stage CSV         : {stage_file}    (delta rows only: {len(delta_rows)})")
    print(f"    - Delta Summary CSV : {delta_summary_file}")
    print(f"    - Load SQL (UPSERT) : {sql_file}")

    print("-" * 80)
    print(" SAMPLE DELTA RECORDS (First 5):")
    for item in delta_rows[:5]:
        print(f"   [{item['row_type']:<9}] RateID: {item['program_bank_rate_id']:<6} | "
              f"Type: {item['tier_type']:<8} | Low: {item['lower_limit']:<6} | "
              f"FlatRate: {item['flat_rate'] or 'NULL':<8} | PubIdx: {item['published_index_id'] or 'NULL'}")
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
        pbr_id_col, tier_col,
        args.host, args.port, args.user, args.password,
        base_out,
    )


if __name__ == "__main__":
    main()
    print(_dm.result_line(dict(RESULT, model=SWEEP_MODEL["name"])))
