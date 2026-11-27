#!/usr/bin/env python3
r"""ledger_config_model.py - Ledger Config Multi-Schema Migration Script:
Source table:
    <schema>.stg_transcodedescptrec_v
Target table:
    <schema>.ledger_config (default; another table name per schema via --target-table)

Performs insertion across all specified schemas in a single run:
    bcs, bss01001, bss01104, bss01105, bss01108, bss01110, bss01112, bss01115, bss01116

Mapping:
    - broker_id         <- m405_korg (cast to numeric/target type)
    - tran_code         <- m405_ktrancd
    - trans_description <- m405_divdescr
    - gl_trans_code     <- m405_inttc

Composite Key & Idempotent Upsert:
    - Unique composite key: (broker_id, tran_code)
    - Checks for existing keys before insertion:
      * If all rows already exist: prints "no new records found for upsert" and moves to next schema.
      * If new rows exist: inserts only new rows using WHERE NOT EXISTS and ON CONFLICT DO NOTHING.
    - Prevents duplicate key errors (pk_ledger_config1).

Post-load comparison:
    Automatically compares (broker_id, tran_code) composite keys against source staging,
    bypassing surrogate auto-generated IDs, generating comparison_report.csv for Excel verification.

DRY RUN unless --execute.

Connection settings can be edited directly below or passed via CLI:
    --host, --port, --user, --password, --src-db, --tgt-db (or --db)

Commands:
    python ledger_config_model.py                     # Dry run (all 9 schemas preview)
    python ledger_config_model.py --execute           # Execute inserts across all schemas in one run
    python ledger_config_model.py --execute --replace # Delete earlier SYSTEM rows before insert
    python ledger_config_model.py --target-table ledger_config --execute
"""

SWEEP_MODEL = {
    "name": "dm06_ledger_config",
    "inputs": ['bcs.stg_transcodedescptrec_v', 'bss01001.stg_transcodedescptrec_v', 'bss01104.stg_transcodedescptrec_v', 'bss01105.stg_transcodedescptrec_v', 'bss01108.stg_transcodedescptrec_v', 'bss01110.stg_transcodedescptrec_v', 'bss01112.stg_transcodedescptrec_v', 'bss01115.stg_transcodedescptrec_v', 'bss01116.stg_transcodedescptrec_v'],
    "outputs": ['bcs.ledger_config', 'bss01001.ledger_config', 'bss01104.ledger_config', 'bss01105.ledger_config', 'bss01108.ledger_config', 'bss01110.ledger_config', 'bss01112.ledger_config', 'bss01115.ledger_config', 'bss01116.ledger_config'],
    "depends_on": [],
    "delta_strategy": "insert_missing",
    "update_key": ["broker_id", "tran_code"],
    "timeout": 3600,
}


import argparse
import csv
import os
import subprocess
import sys
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
DEFAULT_SOURCE_TABLE = "stg_transcodedescptrec_v"
DEFAULT_TARGET_TABLE = "ledger_config"

# Default 11 schemas from ledger_config
DEFAULT_SCHEMAS = [
    "bcs",
    "bss01001",
    "bss01104",
    "bss01105",
    "bss01108",
    "bss01110",
    "bss01112",
    "bss01115",
    "bss01116",
]

# Source column mappings
SRC_MAPPINGS = {
    "broker_id": "m405_korg",
    "tran_code": "m405_ktrancd",
    "trans_description": "m405_divdescr",
    "gl_trans_code": "m405_inttc",
}

SRC_COLS = ["m405_korg", "m405_ktrancd", "m405_divdescr", "m405_inttc"]
BUSINESS_COLS = ["broker_id", "tran_code", "trans_description", "gl_trans_code"]

COLUMN_ALIASES = {
    "broker_id": ["broker_id", "brokerid"],
    "tran_code": ["tran_code", "trancode", "transaction_code"],
    "trans_description": ["trans_description", "transdescription", "description"],
    "gl_trans_code": ["gl_trans_code", "gltranscode", "gl_tran_code"],
}

# Business default (not an audit column): a new row is active
BUSINESS_DEFAULTS = {"is_active": ("TRUE", ["is_active", "isactive"])}
# The six canonical audit columns (createdby, createddate, modifiedby, modifieddate, createddate_m, createdby_m) are handled by
# _dm_support / sweep_audit - see docs/audit_columns.md

SEP = "\x1f"
HERE = os.path.dirname(os.path.abspath(__file__))
RESULT = {}


def q(x):
    """Safely quote SQL identifiers."""
    return '"' + str(x).replace('"', '""') + '"'


def lit(x):
    """Format literal value for SQL strings."""
    return "NULL" if x is None else "'" + str(x).replace("'", "''") + "'"


def norm_key(b, t):
    """Normalize composite key (broker_id, tran_code) for comparison."""
    if b is None or t is None:
        return ("", "")
    b_s = str(b).strip()
    t_s = str(t).strip()
    if b_s.replace(".", "", 1).isdigit():
        try:
            b_s = str(int(float(b_s)))
        except Exception:
            pass
    return (b_s, t_s)


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


def discover_schemas_with_table(db, table_name, host, port, user, password):
    """Auto-discover all schemas that contain the given table."""
    rows = run_psql(
        db,
        f"SELECT table_schema FROM information_schema.tables WHERE lower(table_name) = {lit(table_name.lower())} ORDER BY 1",
        host,
        port,
        user,
        password,
        readonly=True,
    )
    return [r[0] for r in rows if r and r[0] not in ("information_schema", "pg_catalog")]


def run_post_load_comparison(dst_db, valid_schemas_data, host, port, user, password, out_dir):
    """Executes multi-schema natural composite key comparison (broker_id, tran_code) against target tables, writing an Excel CSV report."""
    print("\n" + "=" * 80)
    print(" [?] AUTOMATIC POST-LOAD VERIFICATION & COMPARISON REPORT")
    print("=" * 80)
    print(f" Schemas Checked:    {len(valid_schemas_data)}")
    print(" 2 Composite Fields: broker_id & tran_code (pk_ledger_config1, surrogate ID bypassed)")

    dt_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"comparereport_ledger_config_{dt_str}.csv"
    report_csv = os.path.join(out_dir, report_filename)
    report_rows = []
    total_src_keys = 0
    total_tgt_keys = 0
    total_matched = 0
    total_missing = 0
    total_extra = 0

    for item in valid_schemas_data:
        sch = item["schema"]
        tgt_table = f"{q(item['rs_tgt'])}.{q(item['rt_tgt'])}"
        b_col = q(item["tcols"]["broker_id"][0])
        t_col = q(item["tcols"]["tran_code"][0])

        # Query inserted rows in target
        tgt_sql = f"""
        SELECT DISTINCT
            TRIM({b_col}::text),
            TRIM({t_col}::text)
        FROM {tgt_table}
        WHERE {b_col} IS NOT NULL AND {t_col} IS NOT NULL;
        """
        tgt_data = run_psql(dst_db, tgt_sql, host, port, user, password, readonly=True)

        src_keys = set(norm_key(r[0], r[1]) for r in item["rows"])
        tgt_keys = set(norm_key(r[0], r[1]) for r in tgt_data if len(r) >= 2)

        matched = src_keys.intersection(tgt_keys)
        missing = src_keys - tgt_keys
        extra = tgt_keys - src_keys

        total_src_keys += len(src_keys)
        total_tgt_keys += len(tgt_keys)
        total_matched += len(matched)
        total_missing += len(missing)
        total_extra += len(extra)

        all_keys = sorted(src_keys.union(tgt_keys))
        for b_id, t_cd in all_keys:
            in_src = "YES" if (b_id, t_cd) in src_keys else "NO"
            in_tgt = "YES" if (b_id, t_cd) in tgt_keys else "NO"
            stat = "MATCH (100%)" if in_src == "YES" and in_tgt == "YES" else ("MISSING_IN_TARGET" if in_src == "YES" else "EXTRA_IN_TARGET")
            report_rows.append([sch, b_id, t_cd, in_src, in_tgt, stat])

    with open(report_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Schema", "Broker_ID", "Tran_Code", "Source_Present", "Target_Present", "Match_Status"])
        w.writerows(report_rows)

    # Also maintain comparison_report.csv copy for backward compatibility
    try:
        import shutil
        shutil.copyfile(report_csv, os.path.join(out_dir, "comparison_report.csv"))
    except Exception:
        pass

    print(f" Total Source Records : {total_src_keys}")
    print(f" Total Target Records : {total_tgt_keys}")
    print(f" Matched Key Pairs    : {total_matched} / {total_src_keys}")

    if total_missing == 0 and total_src_keys > 0:
        print("\n [OK] VERIFICATION RESULT: 100% MATCH! All ledger config records successfully verified in target.")
    else:
        print(f"\n [!] VERIFICATION NOTICE: {total_missing} missing, {total_extra} extra across schemas.")

    print(f" [REPORT] Excel Comparison Report generated: {os.path.abspath(report_csv)}")
    print("=" * 80 + "\n")


def main():
    _dm.init_session()          # model session: sweep.loader = fe_migration
    parser = argparse.ArgumentParser(
        description="Multi-schema Ledger Config migration script with dry-run, idempotent upsert, and pipeline automation support."
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Postgres host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", default=DEFAULT_PORT, type=int, help=f"Postgres port (default: {DEFAULT_PORT})")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"Postgres user (default: {DEFAULT_USER})")
    parser.add_argument("--password", default=DEFAULT_PASS, help="Postgres password (default: empty or from env)")
    parser.add_argument("--src-db", default=DEFAULT_SRC_DB, help=f"Source database (default: {DEFAULT_SRC_DB})")
    parser.add_argument("--tgt-db", "--dst-db", "--target-db", dest="tgt_db", default=DEFAULT_TGT_DB, help=f"Target database (default: {DEFAULT_TGT_DB})")
    parser.add_argument("--db", default=None, help="Fallback database for both source & target if specified")
    parser.add_argument("--schemas", default=",".join(DEFAULT_SCHEMAS),
                        help="Comma-separated schemas (or 'all' to discover all matching schemas)")
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE,
                        help=f"Source staging table (default: {DEFAULT_SOURCE_TABLE})")
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE,
                        help=f"Target table name per schema (default: {DEFAULT_TARGET_TABLE}, e.g. ledger_config or ledger_config)")
    parser.add_argument("--replace", action="store_true", help="Delete earlier SYSTEM rows from target tables before inserting")
    parser.add_argument("--truncate", action="store_true", help="Truncate target tables before inserting")
    parser.add_argument("--out-dir", default=None, help="Directory for preview/staging artifacts (default: $VSAM_OUT/ledger_config)")
    parser.add_argument("--execute", action="store_true", help="Execute insert in target database (dry run by default)")
    parser.add_argument("--mode", choices=["full", "delta"], default=os.environ.get("SWEEP_MODE") or "full",
                        help="full / delta (the runner passes it; the composite-key check makes both idempotent)")
    args = parser.parse_args()

    src_db = args.src_db
    dst_db = args.tgt_db
    if args.db:
        if args.src_db == DEFAULT_SRC_DB:
            src_db = args.db
        if args.tgt_db == DEFAULT_TGT_DB:
            dst_db = args.db

    base_out = args.out_dir or os.environ.get("SWEEP_MODEL_WORKDIR") or os.path.join(HERE, "work", "dm06_ledger_config", "manual")
    os.makedirs(base_out, exist_ok=True)

    # Resolve schema list
    if args.schemas.strip().lower() == "all":
        target_schemas = discover_schemas_with_table(src_db, args.source_table, args.host, args.port, args.user, args.password)
        if not target_schemas:
            sys.exit(f"No schemas found containing table {args.source_table} in database {src_db}")
    else:
        target_schemas = [s.strip() for s in args.schemas.split(",") if s.strip()]

    print("=" * 80)
    print(" LEDGER_CONFIG MULTI-SCHEMA ETL PIPELINE")
    print("=" * 80)
    print(f" Connecting to Postgres at {args.host}:{args.port} (User: {args.user})")
    print(f" Source DB      : {src_db}")
    print(f" Target DB      : {dst_db}")
    print(f" Source Table   : <schema>.{args.source_table}")
    print(f" Target Table   : <schema>.{args.target_table}")
    print(f" Schemas ({len(target_schemas):<2})   : {', '.join(target_schemas)}")
    print(f" Execution Mode : {'EXECUTE' if args.execute else 'DRY RUN (Preview Only)'}")
    print(f" Upsert Mode    : {'TRUNCATE' if args.truncate else ('DELETE SYSTEM ROWS' if args.replace else 'IDEMPOTENT UPSERT (Composite Key pk_ledger_config1)')}")
    print(f" Artifacts Dir  : {os.path.abspath(base_out)}")
    print("-" * 80)

    # -------------------------------------------------------------------------
    # STEP 1: Schema-by-Schema Inspection & Extraction
    # -------------------------------------------------------------------------
    schema_stats = []
    all_preview_rows = []
    valid_schemas_data = []
    total_source_records = 0
    total_new_records = 0

    is_same_db = (src_db == dst_db)

    for sch in target_schemas:
        # Check source table
        rs_src, rt_src, src_cols = find_cols(src_db, sch, args.source_table, args.host, args.port, args.user, args.password)
        if rs_src is None:
            print(f" [!] {sch}: source table {args.source_table} NOT FOUND in {src_db} - SKIPPED")
            schema_stats.append({
                "schema": sch,
                "status": "SRC_NOT_FOUND",
                "source_rows": 0,
                "new_rows": 0,
                "already_exist": 0,
                "target_rows": 0,
                "system_rows": 0,
                "audit_columns": "",
            })
            continue

        # Check target table
        rs_tgt, rt_tgt, all_tgt_cols = find_cols(dst_db, sch, args.target_table, args.host, args.port, args.user, args.password)
        if rs_tgt is None:
            print(f" [!] {sch}: target table {args.target_table} NOT FOUND in {dst_db} - SKIPPED")
            schema_stats.append({
                "schema": sch,
                "status": "TGT_NOT_FOUND",
                "source_rows": 0,
                "new_rows": 0,
                "already_exist": 0,
                "target_rows": 0,
                "system_rows": 0,
                "audit_columns": "",
            })
            continue

        # Check source columns
        missing_src_cols = [c for c in SRC_COLS if c not in src_cols]
        if missing_src_cols:
            print(f" [!] {sch}.{rt_src}: missing source columns: {', '.join(missing_src_cols)} - SKIPPED")
            schema_stats.append({
                "schema": sch,
                "status": "SRC_COL_MISSING",
                "source_rows": 0,
                "new_rows": 0,
                "already_exist": 0,
                "target_rows": 0,
                "system_rows": 0,
                "audit_columns": "",
            })
            continue

        # Resolve target business columns
        tcols = {}
        missing_tgt_cols = []
        for c in BUSINESS_COLS:
            aliases = COLUMN_ALIASES.get(c, [c])
            found_col = next((all_tgt_cols[a] for a in aliases if a in all_tgt_cols), None)
            if found_col:
                tcols[c] = found_col
            else:
                missing_tgt_cols.append(c)

        if missing_tgt_cols:
            print(f" [!] {sch}.{rt_tgt}: missing target columns: {', '.join(missing_tgt_cols)} - SKIPPED")
            schema_stats.append({
                "schema": sch,
                "status": "TGT_COL_MISSING",
                "source_rows": 0,
                "new_rows": 0,
                "already_exist": 0,
                "target_rows": 0,
                "system_rows": 0,
                "audit_columns": "",
            })
            continue

        # Resolve target audit columns
        audit_fields = []
        for std_name, (expr_val, aliases) in BUSINESS_DEFAULTS.items():
            matched_audit = next((all_tgt_cols[a] for a in aliases if a in all_tgt_cols), None)
            if matched_audit:
                audit_fields.append((matched_audit[0], matched_audit[1], expr_val))
        audit_fields += _dm.insert_audit(all_tgt_cols)      # the six canonical audit columns (+ legacy variants the table carries)

        # Query source count and sample rows
        src_query = f"""
        SELECT
            COALESCE(TRIM({q(src_cols['m405_korg'][0])}::text), '') AS broker_id,
            COALESCE(TRIM({q(src_cols['m405_ktrancd'][0])}::text), '') AS tran_code,
            COALESCE(TRIM({q(src_cols['m405_divdescr'][0])}::text), '') AS trans_description,
            COALESCE(TRIM({q(src_cols['m405_inttc'][0])}::text), '') AS gl_trans_code
        FROM {q(rs_src)}.{q(rt_src)}
        ORDER BY 1, 2;
        """
        raw_rows = run_psql(src_db, src_query, args.host, args.port, args.user, args.password, readonly=True)
        src_row_count = len(raw_rows)
        total_source_records += src_row_count

        # Query existing target rows and composite keys
        tgt_qname = f"{q(rs_tgt)}.{q(rt_tgt)}"
        count_tgt = run_psql(dst_db, f"SELECT count(*) FROM {tgt_qname}", args.host, args.port, args.user, args.password, readonly=True)[0][0]
        createdby_real = "createdby_m"      # own rows of the model family: createdby_m = 'fe_migration'

        count_sys = "0"
        if "createdby_m" in all_tgt_cols:
            count_sys = run_psql(
                dst_db,
                f"SELECT count(*) FROM {tgt_qname} WHERE {q(createdby_real)} = {lit(_dm.TAG)}",
                args.host,
                args.port,
                args.user,
                args.password,
                readonly=True,
            )[0][0]

        # Composite Key Check: (broker_id, tran_code) against target table
        b_tgt_col = q(tcols["broker_id"][0])
        t_tgt_col = q(tcols["tran_code"][0])
        existing_keys_query = f"""
        SELECT DISTINCT
            COALESCE(TRIM({b_tgt_col}::text), ''),
            COALESCE(TRIM({t_tgt_col}::text), '')
        FROM {tgt_qname}
        WHERE {b_tgt_col} IS NOT NULL AND {t_tgt_col} IS NOT NULL;
        """
        existing_tgt_rows = run_psql(dst_db, existing_keys_query, args.host, args.port, args.user, args.password, readonly=True)
        existing_keys_set = {norm_key(r[0], r[1]) for r in existing_tgt_rows if len(r) >= 2}

        # Partition raw_rows into new vs already existing
        new_rows = []
        seen_keys = set()
        already_existing_count = 0
        for r in raw_rows:
            k = norm_key(r[0], r[1])
            if k in existing_keys_set:
                already_existing_count += 1
            elif k in seen_keys:
                already_existing_count += 1
            else:
                seen_keys.add(k)
                new_rows.append(r)

        effective_new_count = len(new_rows) if (not args.replace and not args.truncate) else src_row_count
        effective_exist_count = already_existing_count if (not args.replace and not args.truncate) else 0
        total_new_records += effective_new_count

        # Schema status reporting
        if not args.replace and not args.truncate and src_row_count > 0 and already_existing_count == src_row_count:
            status_desc = "ALL_EXIST"
            print(f" [i] {sch:<10}: composite key check -> all {src_row_count} rows already exist in target ({already_existing_count} matched) | no new records found for upsert")
        elif not args.replace and not args.truncate and already_existing_count > 0:
            status_desc = "PARTIAL_EXIST"
            print(f" [+] {sch:<10}: source rows = {src_row_count:<5} | new rows to insert = {len(new_rows):<5} | already exist = {already_existing_count:<5} | target total = {count_tgt}")
        else:
            status_desc = "READY"
            print(f" [+] {sch:<10}: source rows = {src_row_count:<5} | target rows = {count_tgt:<5} ({count_sys} SYSTEM) | audit cols: {len(audit_fields)}")

        schema_stats.append({
            "schema": sch,
            "status": status_desc,
            "source_rows": src_row_count,
            "new_rows": effective_new_count,
            "already_exist": effective_exist_count,
            "target_rows": int(count_tgt),
            "system_rows": int(count_sys),
            "audit_columns": ";".join(a[0] for a in audit_fields),
        })

        # Store for preview
        for r in raw_rows:
            exists = norm_key(r[0], r[1]) in existing_keys_set
            all_preview_rows.append({
                "schema": sch,
                "broker_id": r[0],
                "tran_code": r[1],
                "trans_description": r[2],
                "gl_trans_code": r[3],
                "is_active": "TRUE",
                "action": "SKIP (Already Exists)" if (exists and not args.replace and not args.truncate) else "INSERT",
            })

        valid_schemas_data.append({
            "schema": sch,
            "rs_src": rs_src,
            "rt_src": rt_src,
            "src_cols": src_cols,
            "rs_tgt": rs_tgt,
            "rt_tgt": rt_tgt,
            "tcols": tcols,
            "audit_fields": audit_fields,
            "createdby_real": createdby_real,
            "rows": raw_rows,
            "new_rows": new_rows,
            "already_existing_count": already_existing_count,
        })

    # Summary table
    print("-" * 80)
    print(" INSPECTION SUMMARY:")
    print(f"    Total Schemas Checked  : {len(target_schemas)}")
    print(f"    Valid Schemas Ready    : {len(valid_schemas_data)}")
    print(f"    Total Source Records   : {total_source_records}")
    print(f"    New Records to Insert  : {total_new_records}")
    print(f"    Already Existing Rows  : {total_source_records - total_new_records}")
    RESULT.update(inserted=total_new_records, source_rows=total_source_records, schemas=len(valid_schemas_data), dry_run=not args.execute)
    print("-" * 80)

    # -------------------------------------------------------------------------
    # STEP 2: Write Output Preview Artifacts
    # -------------------------------------------------------------------------
    # 1. Summary CSV
    summary_file = os.path.join(base_out, "source_summary.csv")
    with open(summary_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["schema", "status", "source_rows", "new_rows", "already_exist", "target_rows", "system_rows", "audit_columns"])
        w.writeheader()
        w.writerows(schema_stats)

    # 2. Preview CSV
    preview_file = os.path.join(base_out, "preview.csv")
    with open(preview_file, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["schema", "broker_id", "tran_code", "trans_description", "gl_trans_code", "is_active", "action"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_preview_rows)

    # -------------------------------------------------------------------------
    # STEP 3: Construct Single Consolidated Transaction SQL (load.sql)
    # -------------------------------------------------------------------------
    sql_statements = [
        "\\set ON_ERROR_STOP on",
        "BEGIN;",
        "SET LOCAL lock_timeout = '60s';",
        "",
        "-- ==========================================================================",
        "-- MULTI-SCHEMA LEDGER_CONFIG TRANSACTIONAL LOAD SCRIPT",
        "-- ==========================================================================",
    ]

    for item in valid_schemas_data:
        sch = item["schema"]
        rs_tgt = item["rs_tgt"]
        rt_tgt = item["rt_tgt"]
        tcols = item["tcols"]
        audit_fields = item["audit_fields"]
        createdby_real = item["createdby_real"]
        already_existing_count = item["already_existing_count"]
        src_row_count = len(item["rows"])
        new_rows = item["new_rows"]

        tgt_table = f"{q(rs_tgt)}.{q(rt_tgt)}"

        sql_statements.append(f"\n-- --- Schema: {sch} ({tgt_table}) ---")

        # If all rows already exist and neither replace nor truncate is requested, skip cleanly
        if not args.truncate and not args.replace and already_existing_count == src_row_count and src_row_count > 0:
            sql_statements.append(f"-- Composite key check: All {src_row_count} records already exist in target.")
            sql_statements.append(f"SELECT '{sch}' AS schema, 'no new records found for upsert' AS status, count(*) AS total_count FROM {tgt_table};")
            continue

        sql_statements.append(f"LOCK TABLE {tgt_table} IN SHARE ROW EXCLUSIVE MODE;")
        sql_statements.append(_dm.ensure_audit_sql(f"{rs_tgt}.{rt_tgt}"))      # six audit columns + trigger, idempotent

        # Replace or truncate
        if args.truncate:
            sql_statements.append(f"TRUNCATE TABLE {tgt_table};")
        elif args.replace and createdby_real:
            sql_statements.append(f"DELETE FROM {tgt_table} WHERE {q(createdby_real)} = {lit(_dm.TAG)};")

        # Destination columns
        b_col_name = tcols["broker_id"][0]
        t_col_name = tcols["tran_code"][0]
        ins_cols = [
            q(b_col_name),
            q(t_col_name),
            q(tcols["trans_description"][0]),
            q(tcols["gl_trans_code"][0]),
        ]
        ins_cols += [q(a[0]) for a in audit_fields]
        ins_cols_str = ", ".join(ins_cols)

        def cast_expr(col_name, raw_expr):
            col_type = tcols[col_name][1] if (col_name in tcols and tcols[col_name][1]) else ("numeric" if col_name == "broker_id" else "text")
            # If target column is numeric/integer or broker_id, cast safely from trimmed text to target type
            if "numeric" in col_type.lower() or "int" in col_type.lower() or col_name == "broker_id":
                return f"NULLIF(TRIM(({raw_expr})::text), '')::{col_type}"
            return f"TRIM(({raw_expr})::text)"

        if is_same_db:
            # High-performance server-side SQL INSERT ... SELECT with idempotent WHERE NOT EXISTS & ON CONFLICT
            rs_src = item["rs_src"]
            rt_src = item["rt_src"]
            src_cols = item["src_cols"]
            src_table = f"{q(rs_src)}.{q(rt_src)}"

            b_cast = cast_expr("broker_id", q(src_cols['m405_korg'][0]))
            t_cast = cast_expr("tran_code", q(src_cols['m405_ktrancd'][0]))

            sel_exprs = [
                b_cast,
                t_cast,
                cast_expr("trans_description", q(src_cols['m405_divdescr'][0])),
                cast_expr("gl_trans_code", q(src_cols['m405_inttc'][0])),
            ]
            sel_exprs += [a[2] for a in audit_fields]
            sel_exprs_str = ", ".join(sel_exprs)

            sql_statements.append(f"INSERT INTO {tgt_table} ({ins_cols_str})")
            sql_statements.append(f"SELECT {sel_exprs_str}")
            sql_statements.append(f"FROM {src_table} s")
            if not args.truncate and not args.replace:
                sql_statements.append("WHERE NOT EXISTS (")
                sql_statements.append(f"    SELECT 1 FROM {tgt_table} t")
                sql_statements.append(f"    WHERE t.{q(b_col_name)} = {b_cast} AND t.{q(t_col_name)} = {t_cast}")
                sql_statements.append(")")
            sql_statements.append(f"ON CONFLICT ({q(b_col_name)}, {q(t_col_name)}) DO NOTHING;")
        else:
            # Staging table via \copy for cross-db
            rows_to_stage = new_rows if (not args.replace and not args.truncate) else item["rows"]
            stage_file = os.path.join(base_out, f"stage_{sch}.csv")
            with open(stage_file, "w", newline="", encoding="utf-8") as sf:
                w = csv.writer(sf)
                w.writerows(rows_to_stage)

            stage_path_sql = stage_file.replace("\\", "/")
            temp_tbl = f"lc_stage_{sch.replace('.', '_')}"

            sql_statements.append(f"CREATE TEMP TABLE {temp_tbl} (")
            sql_statements.append("    broker_id text, tran_code text, trans_description text, gl_trans_code text")
            sql_statements.append(") ON COMMIT DROP;")
            sql_statements.append(f"\\copy {temp_tbl} FROM '{stage_path_sql}' WITH (FORMAT csv)")

            b_cast = cast_expr("broker_id", "s.broker_id")
            t_cast = cast_expr("tran_code", "s.tran_code")

            sel_exprs = [
                b_cast,
                t_cast,
                cast_expr("trans_description", "s.trans_description"),
                cast_expr("gl_trans_code", "s.gl_trans_code"),
            ]
            sel_exprs += [a[2] for a in audit_fields]
            sel_exprs_str = ", ".join(sel_exprs)

            sql_statements.append(f"INSERT INTO {tgt_table} ({ins_cols_str})")
            sql_statements.append(f"SELECT {sel_exprs_str}")
            sql_statements.append(f"FROM {temp_tbl} s")
            if not args.truncate and not args.replace:
                sql_statements.append("WHERE NOT EXISTS (")
                sql_statements.append(f"    SELECT 1 FROM {tgt_table} t")
                sql_statements.append(f"    WHERE t.{q(b_col_name)} = {b_cast} AND t.{q(t_col_name)} = {t_cast}")
                sql_statements.append(")")
            sql_statements.append(f"ON CONFLICT ({q(b_col_name)}, {q(t_col_name)}) DO NOTHING;")

        sql_statements.append(f"SELECT '{sch}' AS schema, 'upserted ({len(new_rows)} new)' AS status, count(*) AS total_count FROM {tgt_table};")

    sql_statements.append("\nCOMMIT;")

    sql_file = os.path.join(base_out, "load.sql")
    with open(sql_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sql_statements) + "\n")

    print(f"[*] Generated single multi-schema SQL transaction script: {sql_file}")
    print("[*] Artifacts generated:")
    print(f"    - Summary CSV : {summary_file}")
    print(f"    - Preview CSV : {preview_file}")
    print(f"    - Load SQL    : {sql_file}")

    # Preview sample records in console
    print("-" * 80)
    print(" SAMPLE PREVIEW OF RECORDS (First 5):")
    for item in all_preview_rows[:5]:
        print(f"   Schema: {item['schema']:<10} | Broker: {item['broker_id']:<6} | TranCode: {item['tran_code']:<6} | GLCode: {item['gl_trans_code']:<6} | Action: {item['action']}")

    # -------------------------------------------------------------------------
    # STEP 4: Execute or Dry Run Exit
    # -------------------------------------------------------------------------
    print("-" * 80)
    if not args.execute:
        print("[!] DRY RUN COMPLETED SUCCESSFULLY: No database mutations were made.")
        print(f"    Review {summary_file} and {preview_file}.")
        print("    To execute the inserts across all schemas in a single transaction, rerun with: --execute")
        return

    if not valid_schemas_data or total_source_records == 0:
        print("[-] EXECUTION HALTED: No valid schemas or source rows found in source database.")
        sys.exit(1)

    print(f"[*] Executing multi-schema transaction on database {dst_db}...")
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
    if total_new_records > 0:
        print(f"    Done: Successfully migrated {total_new_records} new records across {len(valid_schemas_data)} schemas!")
    else:
        print(f"    Done: All records already exist in target across {len(valid_schemas_data)} schemas (no new records found for upsert).")

    # ---------------- Step 5: Automatic In-Line Post-Load Comparison
    run_post_load_comparison(dst_db, valid_schemas_data, args.host, args.port, args.user, args.password, base_out)


if __name__ == "__main__":
    main()
    print(_dm.result_line(dict(RESULT, model=SWEEP_MODEL["name"])))
