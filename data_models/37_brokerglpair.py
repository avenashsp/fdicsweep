#!/usr/bin/env python3
"""37_brokerglpair.py - BrokerGLPair migration: STG_GENLEDGEINTERFACE_V -> BCS.BROKERGLPAIR.
DRY RUN unless --execute.

Runs inside the SWEEP data-model runner (python3 sweep.py models run) in the silver database, or by hand.

Target & Source:
  Default DB:     $PGDATABASE (both source and destination)
  Source table:   <schema>.stg_genledgeinterface_v (default schema: bss00098)
  Target table:   bcs.brokerglpair
  Broker table:   bcs.broker

Connection: libpq environment (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE), overridable on the command line:
  --host, --port, --user, --password, --db (or --src-db / --dst-db)

Runner contract (environment variables set by `sweep.py models run`):
  SWEEP_MODEL_WORKDIR  generated files (source_rows.csv, preview.csv, broker_list.csv, stage.csv, load.sql, run.log)
  SWEEP_MODEL_CONFIG   files people edit (hogan_broker_map.csv)
  SWEEP_INPUT_DELTAS   per input table {"full": true} or {"full": false, "delta_table", "seq_from", "seq_to"}
  The script ends with one line:  SWEEP_MODEL_RESULT {json}   and exits non-zero on error.

Update in place (never destructive). The output key is (hogan_product_code, total_number):
  - no output row with the key            -> INSERT
  - row created by DATA_MIGRATION         -> UPDATE the non-key business columns only where a value differs
                                             (modifiedby = DATA_MIGRATION, modifieddate = CURRENT_TIMESTAMP)
  - row created by anyone else            -> untouched (counted as skipped_foreign)
  id, createddate and createdby never change; nothing is deleted. Running the same delta again changes nothing.

Audit & Metadata columns handled:
  - createddate    = CURRENT_TIMESTAMP
  - createdby      = 'DATA_MIGRATION'
  - modifieddate   = CURRENT_TIMESTAMP
  - modifiedby     = 'DATA_MIGRATION'
  - createddate_m  = CURRENT_TIMESTAMP
  - createdby_m    = 'DATA_MIGRATION'
  (The script dynamically detects which of these columns exist in the target table).

Commands:
  python3 37_brokerglpair.py                         # Dry run (safe): reports what would be inserted / updated
  python3 37_brokerglpair.py --execute               # Insert / update in bcs.brokerglpair
  python3 37_brokerglpair.py --execute --mode delta  # Only the source rows named by SWEEP_INPUT_DELTAS
"""

SWEEP_MODEL = {
    "name": "brokerglpair",
    "inputs": ["bss00098.stg_genledgeinterface_v", "bcs.broker"],
    "outputs": ["bcs.brokerglpair"],
    "depends_on": [],
    "delta_strategy": "update_in_place",
    "update_key": ["hogan_product_code", "total_number"],
    "timeout": 3600,
}

import argparse
import csv
import json
import os
import subprocess
import sys
from collections import Counter

# ----------------------------------------------------------------------
# DEFAULT CONNECTION & DATABASE CONFIGURATION (Edit here or pass via CLI)
# ----------------------------------------------------------------------
DEFAULT_HOST = os.environ.get("PGHOST") or "localhost"
DEFAULT_PORT = int(os.environ.get("PGPORT") or 5432)
DEFAULT_USER = os.environ.get("PGUSER") or "postgres"
DEFAULT_PASS = os.environ.get("PGPASSWORD") or ""   # leave empty if using ~/.pgpass or peer/trust auth
DEFAULT_DB   = os.environ.get("PGDATABASE") or "postgres"   # default database for both source & target
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

CREATED_BY = "DATA_MIGRATION"
SEP = "\x1f"
RESULT_PREFIX = "SWEEP_MODEL_RESULT"
UPDATE_KEY = SWEEP_MODEL["update_key"]
HERE = os.path.dirname(os.path.abspath(__file__))

DEFAULT_BROKERS = {
    "811": "Edelman",
    "812": "Edelman",
    "826": "Empower",
    "827": "Empower",
    "818": "LPL",
    "819": "Access Alternative Investments"
}


def q(x):
    return '"' + str(x).replace('"', '""') + '"'


def lit(x):
    return "NULL" if x is None else "'" + str(x).replace("'", "''") + "'"


def clean_total(x):
    """Replicates m921_ktotal::INTEGER::VARCHAR(10) (strips leading zeroes)."""
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
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

    r = subprocess.run(cmd, capture_output=True, text=True, env=env)
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


def main():
    ap = argparse.ArgumentParser(description="BrokerGLPair Migration to BCS.BROKERGLPAIR in postgres")
    ap.add_argument("--host", default=DEFAULT_HOST, help=f"Postgres host (default: {DEFAULT_HOST})")
    ap.add_argument("--port", default=DEFAULT_PORT, type=int, help=f"Postgres port (default: {DEFAULT_PORT})")
    ap.add_argument("--user", default=DEFAULT_USER, help=f"Postgres user (default: {DEFAULT_USER})")
    ap.add_argument("--password", default=DEFAULT_PASS, help="Postgres password (default: empty or from env)")
    ap.add_argument("--db", default=DEFAULT_DB, help=f"Postgres database for both source & target (default: {DEFAULT_DB})")
    ap.add_argument("--src-db", help="Source DB if different from --db")
    ap.add_argument("--dst-db", help="Target DB if different from --db")
    ap.add_argument("--schemas", default=DEFAULT_SRC_SCHEMA, help=f"Source schema (default: {DEFAULT_SRC_SCHEMA})")
    ap.add_argument("--target", default=DEFAULT_TARGET_TABLE, help=f"Target table (default: {DEFAULT_TARGET_TABLE})")
    ap.add_argument("--product-codes", default=PRODUCT_CODES, help="Hogan product codes (default: 811,812,826,827,818,819)")
    ap.add_argument("--broker-table", default="bcs.broker", help="Broker table (default: bcs.broker)")
    ap.add_argument("--broker-name-col", default="auto")
    ap.add_argument("--map", help="Hogan code to broker name mapping file")
    ap.add_argument("--replace", action="store_true",
                    help="LEGACY, by hand only: delete earlier DATA_MIGRATION rows of these codes before inserting (refused under the runner)")
    ap.add_argument("--mode", choices=["full", "delta"], default=os.environ.get("SWEEP_MODE") or "full",
                    help="full: every eligible source row; delta: only the rows named by SWEEP_INPUT_DELTAS (default: $SWEEP_MODE or full)")
    ap.add_argument("--allow-null-broker", action="store_true", help="Allow records with unresolved broker to insert with NULL brokerid")
    ap.add_argument("--execute", action="store_true", help="Execute insert in target database")
    a = ap.parse_args()
    if a.replace and os.environ.get("SWEEP_RUN_ID"):
        sys.exit("--replace deletes rows and is refused when the script runs under the SWEEP runner (SWEEP_RUN_ID is set): "
                 "the model updates in place instead")
    if any(k not in BUSINESS_COLS for k in UPDATE_KEY):
        sys.exit(f"SWEEP_MODEL update_key {UPDATE_KEY} must name business columns")

    src_db = a.src_db or a.db
    dst_db = a.dst_db or a.db

    print(f"Connecting to Postgres at {a.host}:{a.port} (User: {a.user})")
    print(f"Source DB: {src_db} | Target DB: {dst_db} | Target Table: {a.target}")

    od = os.environ.get("SWEEP_MODEL_WORKDIR") or os.path.join(HERE, "work", "brokerglpair", "manual")      # generated files
    cfg = os.environ.get("SWEEP_MODEL_CONFIG") or os.path.join(HERE, "config", "brokerglpair")              # files people edit
    os.makedirs(od, exist_ok=True)
    os.makedirs(cfg, exist_ok=True)
    deltas = json.loads(os.environ.get("SWEEP_INPUT_DELTAS") or "{}")
    product_codes = [c.strip() for c in a.product_codes.split(",") if c.strip()]
    tsch, ttab = a.target.split(".", 1)

    # ---------------- Step 1: Read source rows (read-only)
    src, counts = [], Counter()
    for s in [x.strip() for x in a.schemas.split(",") if x.strip()]:
        rs, rt, cols = find_cols(src_db, s, SRC_TABLE, SRC_COLS, a.host, a.port, a.user, a.password)
        if rs is None:
            print(f" {s}: {SRC_TABLE} not found in {src_db}: skipped")
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

        print(f" {rs}.{rt}: {len(rows)} eligible rows read from {src_db}")

    # delta mode: the source rows that changed (rows of the silver delta table of the input). None = everything is affected
    affected_src, source_delta_rows = None, 0
    if a.mode == "delta":
        affected_src = []
        for s_ in [x.strip() for x in a.schemas.split(",") if x.strip()]:
            spec = deltas.get(f"{s_}.{SRC_TABLE}".lower())
            if not spec or spec.get("full"):
                affected_src = None
                break
            rs_, rt_, cols_ = find_cols(src_db, s_, SRC_TABLE, SRC_COLS, a.host, a.port, a.user, a.password)
            if rs_ is None:
                continue
            dschema, dtable = spec["delta_table"].split(".", 1)
            sel_ = ", ".join(f"coalesce(trim({q(cols_[c][0])}::text), '')" for c in SRC_COLS)
            drows = run_psql(src_db, f"select {sel_} from {q(dschema)}.{q(dtable)} where _seq > {int(spec['seq_from'])} and _seq <= {int(spec['seq_to'])} "
                                     f"and trim({q(cols_['m921_kfund_kproduct'][0])}::text) in ({','.join(lit(c) for c in product_codes)})",
                             a.host, a.port, a.user, a.password, readonly=True)
            source_delta_rows += len(drows)
            for r in drows:
                affected_src.append({"schema": rs_, **{c: (v if v != "" else None) for c, v in zip(SRC_COLS, r)}})
        # the broker lookup is shared by every row: a changed broker table affects everything
        bspec = deltas.get(a.broker_table.lower())
        if affected_src is not None:
            if not bspec or bspec.get("full"):
                affected_src = None
            else:
                bs_, bt_ = bspec["delta_table"].split(".", 1)
                nb = run_psql(src_db, f"select count(*) from {q(bs_)}.{q(bt_)} where _seq > {int(bspec['seq_from'])} and _seq <= {int(bspec['seq_to'])}",
                              a.host, a.port, a.user, a.password, readonly=True)[0][0]
                if int(nb) > 0:
                    affected_src = None
        print("delta mode: " + ("every row is affected (an input is read in full or the broker table changed)" if affected_src is None
                                 else f"{source_delta_rows} changed source row(s)"))

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
        with open(mp, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows([["hogan_product_code", "broker_name"]] + [[c, DEFAULT_BROKERS.get(c, "")] for c in product_codes])
        print(f"wrote {mp} with the mapping of the insert query (edit it to change a broker; names as in {od}/broker_list.csv)")

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

    keyreal = sorted(tcols[k][0] for k in UPDATE_KEY)
    has_unique = run_psql(dst_db, f"select 1 from pg_index i where i.indrelid = {lit(q(rts) + '.' + q(rtt))}::regclass and i.indisunique and "
                                  f"(select array_agg(a.attname::text order by a.attname::text) from pg_attribute a where a.attrelid = i.indrelid "
                                  f"and a.attnum = any(i.indkey)) = ARRAY[{','.join(lit(k) for k in keyreal)}]::text[]",
                          a.host, a.port, a.user, a.password, readonly=True)
    if not has_unique:
        print(f"  WARNING: {rts}.{rtt} has no unique index on the update key ({', '.join(keyreal)}): the key is honoured by the script only")

    # 2. Dynamically Resolve Audit & Metadata Columns
    # Supports: createddate, createdby, modifiedby, modifieddate, createddate_m, createdby_m
    AUDIT_CANDIDATES = {
        "createddate": ("CURRENT_TIMESTAMP", ["createddate", "created_date"]),
        "createdby": (lit(CREATED_BY), ["createdby", "created_by"]),
        "modifieddate": ("CURRENT_TIMESTAMP", ["modifieddate", "modified_date"]),
        "modifiedby": (lit(CREATED_BY), ["modifiedby", "modified_by"]),
        "createddate_m": ("CURRENT_TIMESTAMP", ["createddate_m", "created_date_m"]),
        "createdby_m": (lit(CREATED_BY), ["createdby_m", "created_by_m"])
    }

    audit_insert_fields = []
    for std_name, (expr_val, aliases) in AUDIT_CANDIDATES.items():
        matched_audit = next((all_tgt[a] for a in aliases if a in all_tgt), None)
        if matched_audit:
            # (real_col_name, data_type, sql_expression)
            audit_insert_fields.append((matched_audit[0], matched_audit[1], expr_val))

    print(f"target {rts}.{rtt}: detected audit columns: {', '.join(a[0] for a in audit_insert_fields)}")

    # Check for existing migration rows
    hpc_c = q(tcols["hogan_product_code"][0])
    createdby_real = next((a[0] for a in audit_insert_fields if "createdby" in a[0].lower()), None)

    have, other, mx = [], [], ["0"]
    incl = f"{hpc_c}::text in ({','.join(lit(c) for c in product_codes)})"
    if createdby_real:
        cb_c = q(createdby_real)
        have = run_psql(dst_db, f"select {hpc_c}, count(*) from {q(rts)}.{q(rtt)} where {incl} and {cb_c} = {lit(CREATED_BY)} group by 1 order by 1",
                        a.host, a.port, a.user, a.password, readonly=True)
        other = run_psql(dst_db, f"select {hpc_c}, count(*) from {q(rts)}.{q(rtt)} where {incl} and {cb_c} is distinct from {lit(CREATED_BY)} group by 1 order by 1",
                         a.host, a.port, a.user, a.password, readonly=True)
    mx = run_psql(dst_db, f"select count(*) from {q(rts)}.{q(rtt)}",
                  a.host, a.port, a.user, a.password, readonly=True)[0]

    print(f"target {rts}.{rtt}: {mx[0]} total existing rows; previously loaded by {CREATED_BY}: " + (", ".join(f"Code {c} x{n}" for c, n in have) or "none"))
    if other:
        print(" NOTE rows with these product codes not created by DATA_MIGRATION (kept untouched): " + ", ".join(f"Code {c} x{n}" for c, n in other))

    # ---------------- Step 4: Preview + Deduplication (SELECT DISTINCT)
    prev = []
    seen = set()

    def business_row(r):
        c = r["m921_kfund_kproduct"]
        tot = clean_total(r["m921_ktotal"])
        return dict(
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

    for r in src:
        row = business_row(r)
        dedup_key = tuple(row[k] for k in BUSINESS_COLS)
        if dedup_key not in seen:
            seen.add(dedup_key)
            prev.append(row)

    print(f"records: {len(src)} source rows -> {len(prev)} distinct rows ({len(src)-len(prev)} duplicate rows collapsed)")

    # the update key must identify one output row: two different business rows with the same key stop the run (dry run too)
    key_of = lambda p: tuple(p[k] for k in UPDATE_KEY)
    by_key = {}
    for p in prev:
        by_key.setdefault(key_of(p), []).append(p)
    clashes = {k: v for k, v in by_key.items() if len(v) > 1}
    if clashes:
        shown = "; ".join(f"{dict(zip(UPDATE_KEY, k))} x{len(v)}" for k, v in list(clashes.items())[:5])
        sys.exit(f"UPDATE KEY NOT UNIQUE: {len(clashes)} key value(s) ({', '.join(UPDATE_KEY)}) belong to more than one different source row, e.g. {shown}. "
                 f"Nothing was written. Decide the real key of {a.target} before running again.")
    if affected_src is None:
        todo, affected_n = prev, len(src)
    else:
        wanted = {key_of(business_row(r)) for r in affected_src}
        todo, affected_n = [p for p in prev if key_of(p) in wanted], source_delta_rows
    print(f"rows to process: {len(todo)} of {len(prev)} (mode {a.mode}, {affected_n} affected source row(s))")

    with open(os.path.join(od, "preview.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(todo[0].keys()) if todo else BUSINESS_COLS)
        w.writeheader()
        w.writerows(todo)

    stage_cols = [c for c in BUSINESS_COLS if c not in ("is_active", "product_id")]
    stage = os.path.join(od, "stage.csv")
    with open(stage, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for i, p in enumerate(todo, 1):
            w.writerow([i] + [p[col] or "" for col in stage_cols])

    # ---------------- Step 5: SQL Construction (update in place: insert missing keys, update rows DATA_MIGRATION created)
    def cast(col, expr):
        return f"({expr})::{tcols[col][1]}"

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
    new_select = ", ".join(f"{e} AS {q(c)}" for c, e in zip(BUSINESS_COLS, sel_expressions))      # typed values, named by the canonical column

    tgt = f"{q(rts)}.{q(rtt)}"
    stage_path_sql = stage.replace("\\", "/")
    ins_cols_sql = ", ".join([q(tcols[c][0]) for c in BUSINESS_COLS] + [q(x[0]) for x in audit_insert_fields])
    ins_select = ", ".join([f"n.{q(c)}" for c in BUSINESS_COLS] + [f"({expr})::{dtype}" for _, dtype, expr in audit_insert_fields])
    key_match = " AND ".join(f"t.{q(tcols[k][0])} IS NOT DISTINCT FROM n.{q(k)}" for k in UPDATE_KEY)
    nonkey = [c for c in BUSINESS_COLS if c not in UPDATE_KEY]
    differ = " OR ".join(f"t.{q(tcols[c][0])} IS DISTINCT FROM n.{q(c)}" for c in nonkey)
    dm_pred = f"t.{q(createdby_real)} = {lit(CREATED_BY)}" if createdby_real else "false"
    set_sql = ", ".join([f"{q(tcols[c][0])} = n.{q(c)}" for c in nonkey] +
                        [f"{q(real)} = ({expr})::{dtype}" for real, dtype, expr in audit_insert_fields
                         if real.lower() in ("modifiedby", "modified_by", "modifieddate", "modified_date")])

    sql = [
        "\\set ON_ERROR_STOP on",
        "BEGIN;",
        "SET LOCAL lock_timeout = '60s';",
        f"LOCK TABLE {tgt} IN SHARE ROW EXCLUSIVE MODE;"
    ]

    if a.replace and createdby_real:                      # legacy, by hand only (refused under the runner)
        sql.append(f"DELETE FROM {tgt} WHERE {incl} AND {q(createdby_real)} = {lit(CREATED_BY)};")

    sql += [
        "CREATE TEMP TABLE bgl_stage ("
        "  rn bigint, brokerid text, hogan_code text, total_number text,"
        "  debit_orgcode text, debit_org text, debit_acctnr text, debit_costctr text, debit_keycd text, debit_costcds text, debit_trancd text, debit_concd text, debit_descr text,"
        "  credit_orgcode text, credit_org text, credit_acctnr text, credit_costctr text, credit_keycd text, credit_costcds text, credit_trancd text, credit_concd text, credit_descr text"
        ") ON COMMIT DROP;",
        f"\\copy bgl_stage FROM '{stage_path_sql}' WITH (FORMAT csv)",
        f"CREATE TEMP TABLE bgl_new ON COMMIT DROP AS SELECT s.rn, {new_select} FROM bgl_stage s;",
        # what happens to each row: insert (no output row with the key), dm (created by DATA_MIGRATION: update where different),
        # foreign (created by someone else: untouched)
        f"CREATE TEMP TABLE bgl_cls ON COMMIT DROP AS SELECT n.rn, CASE "
        f"WHEN NOT EXISTS (SELECT 1 FROM {tgt} t WHERE {key_match}) THEN 'insert' "
        f"WHEN EXISTS (SELECT 1 FROM {tgt} t WHERE {key_match} AND {dm_pred}) THEN 'dm' ELSE 'foreign' END AS cls FROM bgl_new n;",
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM bgl_new n JOIN " + tgt + " t ON " + key_match + " GROUP BY n.rn HAVING count(*) > 1) THEN "
        f"RAISE EXCEPTION 'update key ({', '.join(UPDATE_KEY)}) matches more than one existing row of {rts}.{rtt}: nothing is updated'; END IF; END $$;",
        "SELECT '@@COUNTS@@' || json_build_object("
        "'insert', (SELECT count(*) FROM bgl_cls WHERE cls = 'insert'), 'foreign', (SELECT count(*) FROM bgl_cls WHERE cls = 'foreign'), "
        "'dm', (SELECT count(*) FROM bgl_cls WHERE cls = 'dm'), "
        f"'update', (SELECT count(*) FROM bgl_new n JOIN bgl_cls c USING (rn) WHERE c.cls = 'dm' AND EXISTS "
        f"(SELECT 1 FROM {tgt} t WHERE {key_match} AND {dm_pred} AND ({differ}))));",
    ]
    apply_sql = [
        f"INSERT INTO {tgt} ({ins_cols_sql}) SELECT {ins_select} FROM bgl_new n JOIN bgl_cls c USING (rn) WHERE c.cls = 'insert' ORDER BY n.rn;",
        f"UPDATE {tgt} t SET {set_sql} FROM bgl_new n JOIN bgl_cls c USING (rn) WHERE c.cls = 'dm' AND {key_match} AND {dm_pred} AND ({differ});",
        "COMMIT;"
    ]

    sqlf = os.path.join(od, "load.sql")
    open(sqlf, "w", encoding="utf-8").write("\n".join(sql + apply_sql) + "\n")
    previewf = os.path.join(od, "preview.sql")                      # the same classification, rolled back: used by the dry run
    open(previewf, "w", encoding="utf-8").write("\n".join(sql + ["ROLLBACK;"]) + "\n")

    print(f"files written: {od}/source_rows.csv, preview.csv, broker_list.csv, {os.path.basename(mp)}, load.sql")
    for p in todo[:5]:
        print(f"  sample: Code {p['hogan_product_code']} broker {p['brokerid']} total {p['total_number']!r} debit_trancd {p['debit_trancd']!r}")

    def psql_file(path, db):
        cmd = ["psql", "-X", "-At"]
        if a.host:
            cmd.extend(["-h", a.host])
        if a.port:
            cmd.extend(["-p", str(a.port)])
        if a.user:
            cmd.extend(["-U", a.user])
        cmd.extend(["-f", path])
        env = dict(os.environ, PGDATABASE=db)
        if a.password:
            env["PGPASSWORD"] = a.password
        return subprocess.run(cmd, capture_output=True, text=True, env=env)

    def counts_of(out):
        for ln in out.splitlines():
            if ln.startswith("@@COUNTS@@"):
                return json.loads(ln[len("@@COUNTS@@"):])
        return None

    def result_line(status, c):
        res = {"status": status, "mode": a.mode, "inserted": c["insert"], "updated": c["update"], "unchanged": c["dm"] - c["update"],
               "skipped_foreign": c["foreign"], "affected_source_rows": affected_n, "rows_considered": len(todo)}
        print(f"{RESULT_PREFIX} {json.dumps(res, sort_keys=True)}")

    # ---------------- Step 6: Dry Run / Execute checks
    if not a.execute:
        if not todo:
            result_line("DRY_RUN", {"insert": 0, "update": 0, "dm": 0, "foreign": 0})
            print("\ndry run complete: nothing to process")
            return
        r = psql_file(previewf, dst_db)
        if r.returncode:
            sys.exit("dry run check failed: " + (r.stderr or r.stdout).strip()[-600:])
        c = counts_of(r.stdout)
        print(f"would insert {c['insert']}, update {c['update']}, leave {c['dm'] - c['update']} unchanged, skip {c['foreign']} row(s) not created by {CREATED_BY}")
        result_line("DRY_RUN", c)
        print("\ndry run complete: nothing changed in database. Review preview.csv and load.sql, then run with --execute")
        return

    stop = []
    if not src:
        stop.append("no eligible source rows found")
    if problems and not a.allow_null_broker:
        stop.append("broker lookup incomplete (check hogan_broker_map.csv or specify --allow-null-broker)")

    if stop:
        sys.exit("NOT EXECUTED:\n  " + "\n  ".join(stop))

    if not todo:
        result_line("SUCCESS", {"insert": 0, "update": 0, "dm": 0, "foreign": 0})
        print("nothing to process")
        return

    r = psql_file(sqlf, dst_db)
    open(os.path.join(od, "run.log"), "w", encoding="utf-8").write(r.stdout + r.stderr)

    if r.returncode:
        sys.exit("FAILED, rolled back: " + (r.stderr or r.stdout).strip()[-600:])

    c = counts_of(r.stdout)
    print(f"done in {rts}.{rtt} (database {dst_db}): inserted {c['insert']}, updated {c['update']}, unchanged {c['dm'] - c['update']}, "
          f"skipped (not created by {CREATED_BY}) {c['foreign']}")
    result_line("SUCCESS", c)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        if e.code not in (None, 0):
            print(f"{RESULT_PREFIX} {json.dumps({'status': 'FAILED', 'error': str(e.code)[:500]})}")
        raise
