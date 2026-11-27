#!/usr/bin/env python3
r"""DM02_brokerglpair.py - BrokerGLPair migration: STG_GENLEDGEINTERFACE_V -> BCS.BROKERGLPAIR_BACKUP.
DRY RUN unless --execute.

Target & Source:
  Default DB:     postgres (both source and destination, configurable via --src-db / --tgt-db)
  Source table:   <schema>.stg_genledgeinterface_v (default schema: bss00098)
  Target table:   bcs.brokerglpair
  Broker table:   bcs.broker

Natural Key Comparison Fields (Instead of sequence IDs):
  Key 1: hogan_product_code (811, 812, 826, 827, 818, 819)
  Key 2: total_number (m921_ktotal::INTEGER::VARCHAR(10))

Composite Key & Idempotent Upsert:
  - Unique composite key: (hogan_product_code, total_number)
  - Checks for existing keys in target table before insertion:
    * If all rows already exist: prints "no new records found for upsert" and safely skips duplicate inserts.
    * If new rows exist: inserts only new rows using WHERE NOT EXISTS.
    * Never halts the migration when rows already exist; runs the entire logic and verification.

Self-Verifying Execution:
  After --execute commits the rows, it automatically runs an in-line comparison
  between source staging and target table, generating an Excel-compatible
  comparison report (comparison_report.csv) confirming 100% source-to-target match!
"""

SWEEP_MODEL = {
    "name": "dm02_brokerglpair",
    "inputs": ["bss00098.stg_genledgeinterface_v", "bcs.broker"],
    "outputs": ["bcs.brokerglpair"],
    "depends_on": [],
    "delta_strategy": "insert_missing",
    "update_key": ["hogan_product_code", "total_number"],
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
DEFAULT_TARGET_TABLE = "bcs.brokerglpair"
DEFAULT_SRC_SCHEMA   = "bss00098"

PRODUCT_CODES = "811,812,826,827,818,819"
SRC_TABLE = "stg_genledgeinterface_v"

SRC_COLS = [
    "m921_kfund_kproduct",
    "m921_ktotal",
    "m921_gldrorgcd",
    "m921_gldrorg",
    "m921_gldracct",
    "m921_gldrcost",
    "m921_gldrkeycd",
    "m921_gldrcostc1",
    "m921_gldrtrancd",
    "m921_gldrconcd",
    "m921_gldrdesc",
    "m921_glcrorgcd",
    "m921_glcrorg",
    "m921_glcracct",
    "m921_glcrcost",
    "m921_glcrkeycd",
    "m921_glcrcostc1",
    "m921_glcrtrancd",
    "m921_glcrconcd",
    "m921_glcrdesc"
]

BUSINESS_COLS = [
    "brokerid",
    "product_id",
    "hogan_product_code",
    "total_number",
    "debit_orgcode",
    "debit_org",
    "debit_acctnr",
    "debit_costctr",
    "debit_keycd",
    "debit_costcds",
    "debit_trancd",
    "debit_concd",
    "debit_descr",
    "credit_orgcode",
    "credit_org",
    "credit_acctnr",
    "credit_costctr",
    "credit_keycd",
    "credit_costcds",
    "credit_trancd",
    "credit_concd",
    "credit_descr",
    "is_active"
]

COLUMN_ALIASES = {
    "brokerid": ["brokerid", "broker_id"],
    "product_id": ["product_id", "productid"],
    "hogan_product_code": ["hogan_product_code", "hoganproductcode"],
    "total_number": ["total_number", "totalnumber"],
    "debit_orgcode": ["debit_orgcode", "debit_org_code", "debitorgcode"],
    "debit_org": ["debit_org", "debitorg"],
    "debit_acctnr": ["debit_acctnr", "debit_acct_nr", "debitacctnr"],
    "debit_costctr": ["debit_costctr", "debit_cost_ctr", "debitcostctr"],
    "debit_keycd": ["debit_keycd", "debit_key_cd", "debitkeycd"],
    "debit_costcds": ["debit_costcds", "debit_cost_cds", "debitcostcds"],
    "debit_trancd": ["debit_trancd", "debit_tran_cd", "debittrancd"],
    "debit_concd": ["debit_concd", "debit_con_cd", "debitconcd"],
    "debit_descr": ["debit_descr", "debit_desc", "debitdescr"],
    "credit_orgcode": ["credit_orgcode", "credit_org_code", "creditorgcode"],
    "credit_org": ["credit_org", "creditorg"],
    "credit_acctnr": ["credit_acctnr", "credit_acct_nr", "creditacctnr"],
    "credit_costctr": ["credit_costctr", "credit_cost_ctr", "creditcostctr"],
    "credit_keycd": ["credit_keycd", "credit_key_cd", "creditkeycd"],
    "credit_costcds": ["credit_costcds", "credit_cost_cds", "creditcostcds"],
    "credit_trancd": ["credit_trancd", "credit_tran_cd", "credittrancd"],
    "credit_concd": ["credit_concd", "credit_con_cd", "creditconcd"],
    "credit_descr": ["credit_descr", "credit_desc", "creditdescr"],
    "is_active": ["is_active", "isactive"]
}

SEP = "\x1f"
HERE = os.path.dirname(os.path.abspath(__file__))
RESULT = {}


def q(x):
    return '"' + str(x).replace('"', '""') + '"'


def lit(x):
    return "NULL" if x is None else "'" + str(x).replace("'", "''") + "'"


def clean_total(x):
    """Replicates m921_ktotal::INTEGER::VARCHAR(10) (strips leading zeroes)."""
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    try:
        return str(int(s))
    except ValueError:
        return s[:10]


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


def find_cols(db, schema, table, wanted, host, port, user, password):
    """case-insensitive: returns (real_schema, real_table, {wanted_lower: (real_name, type)})"""
    rows = run_psql(db, "select c.table_schema, c.table_name, c.column_name, format_type(a.atttypid, a.atttypmod) from information_schema.columns c "
    "join pg_namespace n on n.nspname=c.table_schema join pg_class k on k.relnamespace=n.oid and k.relname=c.table_name "
    "join pg_attribute a on a.attrelid=k.oid and a.attname=c.column_name "
    f"where lower(c.table_schema)={lit(schema.lower())} and lower(c.table_name)={lit(table.lower())}",
    host, port, user, password, readonly=True)
    if not rows:
        return None, None, {}
    have = {r[2].lower(): (r[2], r[3]) for r in rows}
    return rows[0][0], rows[0][1], {w: have[w] for w in wanted if w in have} | {"__all__": have}


def run_post_load_comparison(dst_db, target_table, src_rows, host, port, user, password, out_dir):
    """Executes natural composite key comparison between source and target, writing an Excel CSV report."""
    print("\n" + "=" * 80)
    print(" [?] AUTOMATIC POST-LOAD VERIFICATION & COMPARISON REPORT")
    print("=" * 80)
    print(f" Target Table:       {target_table}")
    print(" 2 Composite Fields: hogan_product_code & total_number (surrogate ID bypassed)")

    codes = ["811", "812", "826", "827", "818", "819"]

    # Target records
    tgt_sql = f"""
    SELECT DISTINCT
        TRIM(hogan_product_code::text),
        TRIM(total_number::text)
    FROM {target_table}
    WHERE TRIM(hogan_product_code::text) IN ({','.join(lit(c) for c in codes)})
    ORDER BY 1, 2;
    """
    target_data = run_psql(dst_db, tgt_sql, host, port, user, password, readonly=True)

    src_map = {(str(r["m921_kfund_kproduct"]).strip(), clean_total(r["m921_ktotal"])): r for r in src_rows}
    tgt_keys = set((str(r[0]).strip(), clean_total(r[1])) for r in target_data if len(r) >= 2)

    src_keys = set(src_map.keys())

    matched_keys = src_keys.intersection(tgt_keys)
    missing_in_target = src_keys - tgt_keys
    extra_in_target = tgt_keys - src_keys

    # Prepare Excel CSV Report named comparereport_modelname_datetime
    dt_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"comparereport_brokerglpair_{dt_str}.csv"
    report_csv = os.path.join(out_dir, report_filename)
    with open(report_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Hogan_Product_Code", "Total_Number", "Source_Present", "Target_Present", "Match_Status"])
        all_keys = sorted(src_keys.union(tgt_keys))
        for hpc, tot in all_keys:
            in_src = "YES" if (hpc, tot) in src_keys else "NO"
            in_tgt = "YES" if (hpc, tot) in tgt_keys else "NO"
            stat = "MATCH (100%)" if in_src == "YES" and in_tgt == "YES" else ("MISSING_IN_TARGET" if in_src == "YES" else "EXTRA_IN_TARGET")
            w.writerow([hpc, tot, in_src, in_tgt, stat])

    # Also maintain comparison_report.csv copy for backward compatibility
    try:
        import shutil
        shutil.copyfile(report_csv, os.path.join(out_dir, "comparison_report.csv"))
    except Exception:
        pass

    print(f" Source Distinct Keys: {len(src_keys)}")
    print(f" Target Inserted Keys: {len(tgt_keys)}")
    print(f" Matched Key Pairs:    {len(matched_keys)} / {len(src_keys)}")

    if len(src_keys) == len(tgt_keys) and len(missing_in_target) == 0 and len(extra_in_target) == 0:
        print("\n [OK] VERIFICATION RESULT: 100% MATCH! All source records successfully verified in target.")
    else:
        print(f"\n [!] VERIFICATION WARNING: {len(missing_in_target)} missing, {len(extra_in_target)} extra.")

    print(f" [REPORT] Excel Comparison Report generated: {os.path.abspath(report_csv)}")
    print("=" * 80 + "\n")


def main():
    _dm.init_session()          # model session: sweep.loader = fe_migration
    ap = argparse.ArgumentParser(description="BrokerGLPair Migration to BCS.BROKERGLPAIR_BACKUP with Idempotent Upsert Support")
    ap.add_argument("--host", default=DEFAULT_HOST, help=f"Postgres host (default: {DEFAULT_HOST})")
    ap.add_argument("--port", default=DEFAULT_PORT, type=int, help=f"Postgres port (default: {DEFAULT_PORT})")
    ap.add_argument("--user", default=DEFAULT_USER, help=f"Postgres user (default: {DEFAULT_USER})")
    ap.add_argument("--password", default=DEFAULT_PASS, help="Postgres password (default: empty or from env)")
    ap.add_argument("--src-db", default=DEFAULT_SRC_DB, help=f"Source database (default: {DEFAULT_SRC_DB})")
    ap.add_argument("--tgt-db", "--dst-db", "--target-db", dest="tgt_db", default=DEFAULT_TGT_DB, help=f"Target database (default: {DEFAULT_TGT_DB})")
    ap.add_argument("--db", default=None, help="Fallback database for both source & target if specified")
    ap.add_argument("--schemas", default=DEFAULT_SRC_SCHEMA, help=f"Source schema (default: {DEFAULT_SRC_SCHEMA})")
    ap.add_argument("--target", default=DEFAULT_TARGET_TABLE, help=f"Target table (default: {DEFAULT_TARGET_TABLE})")
    ap.add_argument("--product-codes", default=PRODUCT_CODES, help="Hogan product codes (default: 811,812,826,827,818,819)")
    ap.add_argument("--broker-table", default="bcs.broker", help="Broker table (default: bcs.broker)")
    ap.add_argument("--broker-name-col", default="auto")
    ap.add_argument("--map", help="Hogan code to broker name mapping file")
    ap.add_argument("--replace", action="store_true", help="LEGACY, by hand only: delete the earlier rows of these codes created by the models (createdby_m = 'fe_migration') before inserting")
    ap.add_argument("--allow-null-broker", action="store_true", help="Allow records with unresolved broker to insert with NULL brokerid")
    ap.add_argument("--execute", action="store_true", help="Execute insert in target database (dry run by default)")
    ap.add_argument("--mode", choices=["full", "delta"], default=os.environ.get("SWEEP_MODE") or "full",
                    help="full / delta (the runner passes it; the composite-key check makes both idempotent)")
    a = ap.parse_args()
    if a.replace and os.environ.get("SWEEP_RUN_ID"):
        sys.exit("--replace deletes rows and is refused when the script runs under the SWEEP runner (SWEEP_RUN_ID is set)")

    src_db = a.src_db
    dst_db = a.tgt_db
    if a.db:
        if a.src_db == DEFAULT_SRC_DB:
            src_db = a.db
        if a.tgt_db == DEFAULT_TGT_DB:
            dst_db = a.db

    print("=" * 80)
    print(" BROKERGLPAIR MIGRATION PIPELINE")
    print("=" * 80)
    print(f" Connecting to Postgres at {a.host}:{a.port} (User: {a.user})")
    print(f" Source DB      : {src_db}")
    print(f" Target DB      : {dst_db}")
    print(f" Target Table   : {a.target}")
    print(f" Source Schemas : {a.schemas}")
    print(f" Product Codes  : {a.product_codes}")
    print(f" Execution Mode : {'EXECUTE' if a.execute else 'DRY RUN (Preview Only)'}")
    print(f" Upsert Mode    : {'DELETE & REPLACE' if a.replace else 'IDEMPOTENT UPSERT (Composite Key: hogan_product_code, total_number)'}")
    print("-" * 80)

    od = os.environ.get("SWEEP_MODEL_WORKDIR") or os.path.join(HERE, "work", "dm02_brokerglpair", "manual")      # generated files
    cfg = os.environ.get("SWEEP_MODEL_CONFIG") or os.path.join(HERE, "config", "dm02_brokerglpair")           # files people edit
    os.makedirs(od, exist_ok=True)
    product_codes = [c.strip() for c in a.product_codes.split(",") if c.strip()]
    tsch, ttab = a.target.split(".", 1)

    # ---------------- Step 1: Read source rows (read-only)
    src, counts = [], Counter()
    for s in [x.strip() for x in a.schemas.split(",") if x.strip()]:
        rs, rt, cols = find_cols(src_db, s, SRC_TABLE, SRC_COLS, a.host, a.port, a.user, a.password)
        if rs is None:
            print(f" [!] {s}: {SRC_TABLE} not found in {src_db}: skipped")
            continue
        miss = [c for c in SRC_COLS if c not in cols]
        if miss:
            sys.exit(f"{rs}.{rt} in {src_db} has no column {', '.join(miss)}")

        prod_c = q(cols["m921_kfund_kproduct"][0])
        tot_c = q(cols["m921_ktotal"][0])
        sel_cols = ", ".join(f"coalesce(trim({q(cols[c][0])}::text), '')" for c in SRC_COLS)

        rows = run_psql(src_db, f"select {sel_cols} from {q(rs)}.{q(rt)} "
                                 f"where trim({prod_c}::text) in ({','.join(lit(c) for c in product_codes)}) "
                                 f"order by trim({prod_c}::text), {tot_c}",
                        a.host, a.port, a.user, a.password, readonly=True)

        for r in rows:
            rec = {"schema": rs}
            for col_name, val in zip(SRC_COLS, r):
                rec[col_name] = val if val != "" else None
            src.append(rec)
            counts[(rs, rec["m921_kfund_kproduct"])] += 1

        print(f" [+] {rs}.{rt}: {len(rows)} eligible rows read from {src_db}")

    with open(os.path.join(od, "source_rows.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["schema"] + SRC_COLS)
        w.writerows([[r["schema"]] + [r[c] for c in SRC_COLS] for r in src])

    found = {c for _, c in counts}
    print(f"source: {len(src)} rows, Product Codes found {len(found)}/{len(product_codes)}" +
          (f", not found: {', '.join(c for c in product_codes if c not in found)}" if len(found) < len(product_codes) else ""))

    # ---------------- Step 2: Broker lookup
    bs, bt = a.broker_table.split(".", 1)
    rbs, rbt, bcols = find_cols(dst_db, bs, bt, [], a.host, a.port, a.user, a.password)
    if rbs is None:
        sys.exit(f"{a.broker_table} not found in {dst_db}")
    allb = bcols["__all__"]
    idcol = next((allb[c][0] for c in ("brokerid", "broker_id") if c in allb), None)
    namecol = allb.get(a.broker_name_col.lower(), (None,))[0] if a.broker_name_col != "auto" else \
        next((allb[c][0] for c in ("brokername", "broker_name", "name", "brokerdescription", "description") if c in allb), None)
    if not idcol or not namecol:
        sys.exit(f"{a.broker_table}: id column {idcol} / name column {namecol} not found; columns: {', '.join(sorted(allb))}")

    brokers = run_psql(dst_db, f"select {q(idcol)}, {q(namecol)} from {q(rbs)}.{q(rbt)} order by 2",
                       a.host, a.port, a.user, a.password, readonly=True)
    with open(os.path.join(od, "broker_list.csv"), "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([["brokerid", "broker_name"]] + brokers)

    by_name, by_exact = {}, {}
    for bid, bn in brokers:
        by_name.setdefault((bn or "").strip().upper(), []).append(bid)
        by_exact.setdefault((bn or "").strip(), []).append(bid)

    mp = a.map or os.path.join(cfg, "hogan_broker_map.csv")
    if not os.path.exists(mp):
        sys.exit(f"hogan_broker_map.csv not found at {mp}: the Hogan product code -> broker name map is a maintained file "
                 f"(data_models/config/dm02_brokerglpair/hogan_broker_map.csv, or --map); nothing is inserted without it "
                 f"(broker names as in {od}/broker_list.csv)")

    cmap = {}
    with open(mp, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            cmap[(r.get("hogan_product_code") or "").strip()] = (r.get("broker_name") or "").strip()

    broker_of, problems = {}, []
    for code in sorted(found):
        nm = cmap.get(code, "")
        ids = (by_exact.get(nm) or by_name.get(nm.upper(), [])) if nm else []
        if not nm:
            problems.append(f"Code {code}: no broker_name in the map")
        elif len(ids) != 1:
            problems.append(f"Code {code}: broker name {nm!r} matches {len(ids)} rows in {a.broker_table}")
        else:
            broker_of[code] = ids[0]

    print(f"broker lookup ({rbs}.{rbt}.{namecol} -> {idcol}): {len(broker_of)}/{len(found)} Product Codes resolved")
    for p in problems:
        print("  WARNING: " + p)

    # ---------------- Step 3: Inspect target table and audit columns
    rts, rtt, raw_tcols = find_cols(dst_db, tsch, ttab, [], a.host, a.port, a.user, a.password)
    if rts is None:
        sys.exit(f"Target table {a.target} not found in {dst_db}. Please check table name.")
    all_tgt = raw_tcols["__all__"]

    # 1. Resolve Business Columns
    tcols, tmiss = {}, []
    for c in BUSINESS_COLS:
        aliases = COLUMN_ALIASES.get(c, [c])
        found_col = next((all_tgt[a] for a in aliases if a in all_tgt), None)
        if found_col:
            tcols[c] = found_col
        else:
            tmiss.append(c)

    if tmiss:
        sys.exit(f"Target {rts}.{rtt} in {dst_db} is missing required business columns: {', '.join(tmiss)}")

    # 2. The six canonical audit columns (+ legacy variants the table carries): ensured in the load transaction, filled here
    audit_insert_fields = _dm.insert_audit(all_tgt)

    print(f"target {rts}.{rtt}: detected audit columns: {', '.join(a[0] for a in audit_insert_fields)}")

    hpc_c = q(tcols["hogan_product_code"][0])
    tot_c = q(tcols["total_number"][0])
    createdby_real = "createdby_m"      # own rows of the model family: createdby_m = 'fe_migration'

    tgt = f"{q(rts)}.{q(rtt)}"
    have, other, mx = [], [], ["0"]
    incl = f"{hpc_c}::text in ({','.join(lit(c) for c in product_codes)})"
    if "createdby_m" in all_tgt:
        cb_c = q(createdby_real)
        have = run_psql(dst_db, f"select {hpc_c}, count(*) from {tgt} where {incl} and {cb_c} = {lit(_dm.TAG)} group by 1 order by 1",
                        a.host, a.port, a.user, a.password, readonly=True)
        other = run_psql(dst_db, f"select {hpc_c}, count(*) from {tgt} where {incl} and {cb_c} is distinct from {lit(_dm.TAG)} group by 1 order by 1",
                         a.host, a.port, a.user, a.password, readonly=True)
    mx = run_psql(dst_db, f"select count(*) from {tgt}",
                  a.host, a.port, a.user, a.password, readonly=True)[0]

    print(f"target {rts}.{rtt}: {mx[0]} total existing rows; previously loaded by {_dm.TAG}: " + (", ".join(f"Code {c} x{n}" for c, n in have) or "none"))
    if other:
        print(" NOTE: rows with these product codes not created by the models (createdby_m is not 'fe_migration'; kept untouched): " + ", ".join(f"Code {c} x{n}" for c, n in other))

    # ---------------- Step 4: Preview + Deduplication (SELECT DISTINCT)
    prev = []
    seen = set()
    for r in src:
        c = r["m921_kfund_kproduct"]
        tot = clean_total(r["m921_ktotal"])
        row = dict(
            brokerid=broker_of.get(c),
            product_id=2,
            hogan_product_code=c,
            total_number=tot,
            debit_orgcode=r["m921_gldrorgcd"],
            debit_org=r["m921_gldrorg"],
            debit_acctnr=r["m921_gldracct"],
            debit_costctr=r["m921_gldrcost"],
            debit_keycd=r["m921_gldrkeycd"],
            debit_costcds=r["m921_gldrcostc1"],
            debit_trancd=r["m921_gldrtrancd"],
            debit_concd=r["m921_gldrconcd"],
            debit_descr=r["m921_gldrdesc"],
            credit_orgcode=r["m921_glcrorgcd"],
            credit_org=r["m921_glcrorg"],
            credit_acctnr=r["m921_glcracct"],
            credit_costctr=r["m921_glcrcost"],
            credit_keycd=r["m921_glcrkeycd"],
            credit_costcds=r["m921_glcrcostc1"],
            credit_trancd=r["m921_glcrtrancd"],
            credit_concd=r["m921_glcrconcd"],
            credit_descr=r["m921_glcrdesc"],
            is_active="true",
            src_schema=r["schema"]
        )

        dedup_key = tuple(row[k] for k in BUSINESS_COLS)
        if dedup_key not in seen:
            seen.add(dedup_key)
            prev.append(row)

    print(f"records: {len(src)} source rows -> {len(prev)} distinct rows ({len(src)-len(prev)} duplicate rows collapsed)")

    # ---------------- Composite Key Pre-check against target table
    existing_tgt_rows = run_psql(dst_db, f"""
        SELECT DISTINCT
            COALESCE(TRIM({hpc_c}::text), ''),
            COALESCE(TRIM({tot_c}::text), '')
        FROM {tgt}
        WHERE {hpc_c} IS NOT NULL AND {tot_c} IS NOT NULL;
    """, a.host, a.port, a.user, a.password, readonly=True)
    existing_keys_set = {(str(r[0]).strip(), clean_total(r[1])) for r in existing_tgt_rows if len(r) >= 2}

    new_rows = []
    already_existing_count = 0
    for p in prev:
        k = (str(p["hogan_product_code"]).strip(), clean_total(p["total_number"]))
        if k in existing_keys_set:
            already_existing_count += 1
            p["action"] = "SKIP (Already Exists)" if not a.replace else "INSERT (Replace)"
        else:
            new_rows.append(p)
            p["action"] = "INSERT"

    effective_new_count = len(prev) if a.replace else len(new_rows)
    RESULT.update(inserted=effective_new_count, already_existing=already_existing_count, dry_run=not a.execute)

    # Composite Key Check Message Logging
    if not a.replace and len(prev) > 0 and already_existing_count == len(prev):
        print(f" [i] composite key check -> all {len(prev)} rows already exist in target ({already_existing_count} matched) | no new records found for upsert")
    elif not a.replace and already_existing_count > 0:
        print(f" [+] composite key check -> source distinct = {len(prev)} | new rows to insert = {len(new_rows)} | already exist in target = {already_existing_count}")
    else:
        print(f" [+] composite key check -> all {len(prev)} rows are new and ready to insert")

    with open(os.path.join(od, "preview.csv"), "w", newline="", encoding="utf-8") as f:
        fieldnames = list(prev[0].keys()) if prev else BUSINESS_COLS + ["action"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(prev)

    # Stage CSV contains rows to insert
    rows_to_stage = prev if a.replace else new_rows
    stage_cols = [c for c in BUSINESS_COLS if c not in ("is_active", "product_id")]
    stage = os.path.join(od, "stage.csv")
    with open(stage, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for i, p in enumerate(rows_to_stage, 1):
            w.writerow([i] + [p[col] or "" for col in stage_cols])

    # ---------------- Step 5: SQL Construction
    def cast(col, expr):
        return f"({expr})::{tcols[col][1]}"

    ins_cols = [q(tcols[c][0]) for c in BUSINESS_COLS] + [q(a[0]) for a in audit_insert_fields]
    ins_cols_sql = ", ".join(ins_cols)

    sel_expressions = [
        cast("brokerid", "nullif(trim(s.brokerid), '')"),
        cast("product_id", "2"),
        cast("hogan_product_code", "nullif(trim(s.hogan_code), '')"),
        cast("total_number", "nullif(trim(s.total_number), '')"),
        cast("debit_orgcode", "nullif(trim(s.debit_orgcode), '')"),
        cast("debit_org", "nullif(trim(s.debit_org), '')"),
        cast("debit_acctnr", "nullif(trim(s.debit_acctnr), '')"),
        cast("debit_costctr", "nullif(trim(s.debit_costctr), '')"),
        cast("debit_keycd", "nullif(trim(s.debit_keycd), '')"),
        cast("debit_costcds", "nullif(trim(s.debit_costcds), '')"),
        cast("debit_trancd", "nullif(trim(s.debit_trancd), '')"),
        cast("debit_concd", "nullif(trim(s.debit_concd), '')"),
        cast("debit_descr", "nullif(trim(s.debit_descr), '')"),
        cast("credit_orgcode", "nullif(trim(s.credit_orgcode), '')"),
        cast("credit_org", "nullif(trim(s.credit_org), '')"),
        cast("credit_acctnr", "nullif(trim(s.credit_acctnr), '')"),
        cast("credit_costctr", "nullif(trim(s.credit_costctr), '')"),
        cast("credit_keycd", "nullif(trim(s.credit_keycd), '')"),
        cast("credit_costcds", "nullif(trim(s.credit_costcds), '')"),
        cast("credit_trancd", "nullif(trim(s.credit_trancd), '')"),
        cast("credit_concd", "nullif(trim(s.credit_concd), '')"),
        cast("credit_descr", "nullif(trim(s.credit_descr), '')"),
        cast("is_active", "true")
    ]

    for real_name, data_type, expr_val in audit_insert_fields:
        sel_expressions.append(f"({expr_val})::{data_type}")

    sel_sql = ", ".join(sel_expressions)
    stage_path_sql = stage.replace("\\", "/")

    sql = [
        "\\set ON_ERROR_STOP on",
        "BEGIN;",
        "SET LOCAL lock_timeout = '60s';",
        _dm.ensure_audit_sql(f"{rts}.{rtt}"),
    ]

    # If all records already exist and replace is not set, skip insertion to prevent duplicates
    if not a.replace and already_existing_count == len(prev) and len(prev) > 0:
        sql.append(f"-- Composite key check: All {len(prev)} records already exist in target table.")
        sql.append(f"SELECT '{a.target}' AS target_table, 'no new records found for upsert' AS status, count(*) AS total_count FROM {tgt} WHERE {incl};")
    else:
        sql.append(f"LOCK TABLE {tgt} IN SHARE ROW EXCLUSIVE MODE;")
        if a.replace and createdby_real:
            sql.append(f"DELETE FROM {tgt} WHERE {incl} AND {q(createdby_real)} = {lit(_dm.TAG)};")

        sql += [
            "CREATE TEMP TABLE bgl_stage (",
            "  rn bigint, brokerid text, hogan_code text, total_number text,",
            "  debit_orgcode text, debit_org text, debit_acctnr text, debit_costctr text, debit_keycd text, debit_costcds text, debit_trancd text, debit_concd text, debit_descr text,",
            "  credit_orgcode text, credit_org text, credit_acctnr text, credit_costctr text, credit_keycd text, credit_costcds text, credit_trancd text, credit_concd text, credit_descr text",
            ") ON COMMIT DROP;",
            f"\\copy bgl_stage FROM '{stage_path_sql}' WITH (FORMAT csv)",
            f"INSERT INTO {tgt} ({ins_cols_sql})",
            f"SELECT {sel_sql}",
            "FROM bgl_stage s"
        ]
        if not a.replace:
            sql.append("WHERE NOT EXISTS (")
            sql.append(f"    SELECT 1 FROM {tgt} t")
            sql.append(f"    WHERE t.{hpc_c} = nullif(trim(s.hogan_code), '')::{tcols['hogan_product_code'][1]}")
            sql.append(f"      AND t.{tot_c} = nullif(trim(s.total_number), '')::{tcols['total_number'][1]}")
            sql.append(")")
        sql.append("ORDER BY s.rn;")
        sql.append(f"SELECT 'upserted ({len(rows_to_stage)} new)' AS status, count(*) AS count FROM {tgt} WHERE {incl}" + (f" AND {q(createdby_real)} = {lit(_dm.TAG)}" if createdby_real else "") + ";")

    sql.append("COMMIT;")

    sqlf = os.path.join(od, "load.sql")
    with open(sqlf, "w", encoding="utf-8") as f:
        f.write("\n".join(sql) + "\n")

    print(f"files written: {od}/source_rows.csv, preview.csv, broker_list.csv, {os.path.basename(mp)}, load.sql")

    # ---------------- Step 6: Dry Run / Execute checks
    if not a.execute:
        print("\n[!] DRY RUN COMPLETED: No changes made to database. Review preview.csv and load.sql.")
        print("    To execute the migration and run automatic post-load verification, rerun with: --execute")
        return

    stop = []
    if not src:
        stop.append("no eligible source rows found in source table")
    if problems and not a.allow_null_broker:
        stop.append("broker lookup incomplete (check hogan_broker_map.csv or specify --allow-null-broker)")

    if stop:
        sys.exit("NOT EXECUTED:\n  " + "\n  ".join(stop))

    cmd = ["psql", "-X"]
    if a.host:
        cmd.extend(["-h", a.host])
    if a.port:
        cmd.extend(["-p", str(a.port)])
    if a.user:
        cmd.extend(["-U", a.user])
    cmd.extend(["-f", sqlf])

    env = dict(os.environ, PGDATABASE=dst_db)
    if a.password:
        env["PGPASSWORD"] = a.password

    r = subprocess.run(cmd, capture_output=True, text=True, env=env)
    with open(os.path.join(od, "run.log"), "w", encoding="utf-8") as f:
        f.write(r.stdout + "\n" + r.stderr)

    if r.returncode:
        sys.exit("[-] EXECUTION FAILED, transaction rolled back: " + (r.stderr or r.stdout).strip()[-600:])

    print("[+] TRANSACTION COMMITTED SUCCESSFULLY!")
    print(r.stdout.strip())
    print(f"    Log written to: {os.path.join(od, 'run.log')}")
    if effective_new_count > 0:
        print(f"    Done: Successfully migrated {effective_new_count} records into {rts}.{rtt} in database {dst_db}!")
    else:
        print(f"    Done: All {len(prev)} records already exist in {rts}.{rtt} in database {dst_db} (no new records found for upsert).")

    # ---------------- Step 7: Automatic In-Line Post-Load Comparison
    run_post_load_comparison(dst_db, tgt, src, a.host, a.port, a.user, a.password, od)


if __name__ == "__main__":
    main()
    print(_dm.result_line(dict(RESULT, model=SWEEP_MODEL["name"])))
