#!/usr/bin/env python3
"""
sweep_reconcile.py - `sweep.py layers reconcile` and `sweep.py layers check` (F6).

Day-1 was loaded into each database separately, so before the first chained delta the layers must be proven to agree:

  reconcile --from A --to B   per-table row counts plus a key-hash comparison (exact = every row, default = a 1 percent
                              sample of the keys), Excel report, exit code. Bronze vs silver vs gold_backup must match for
                              the Oracle tables (and model outputs); for gold the report is informational only (the
                              application changes data there); VSAM tables are informational.
  check [--layer X]           database reachable, PostgreSQL >= 16, privilege to set session_replication_role, sweep_ctl
                              migrated, schema drift summary, sequence status.
"""
from sweep_common import (MIN_PG_VERSION, ORACLE_REPORTS_DIR, clip_error, generate_excel_report, get_logger, get_timestamp, lit, q,
                          query_db_dicts)
from sweep_delta import TOOL_AUDIT_COLS
from sweep_layers import LayerError, chain, get_layer, layer_conn, read_manifest, same_credentials, use_layer
from sweep_targeting import Selector, SelectorError, parse_table_ref

logger = get_logger("sweep_reconcile", "sweep_reconcile")

SAMPLE_MOD = 100          # sampled comparison: keys whose hash is 0 modulo 100 (about 1 percent)


# -----------------------------------------------------------------------------
# reconcile
# -----------------------------------------------------------------------------
def _columns(schema, table, dbname):
    rows = query_db_dicts(
        f"SELECT a.attname AS n, a.attgenerated AS g FROM pg_attribute a WHERE a.attrelid = to_regclass({lit(q(schema) + '.' + q(table))}) "
        f"AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", dbname=dbname)
    return [r["n"] for r in rows if not r["g"]]


def _key_columns(schema, table, dbname):
    rows = query_db_dicts(
        f"SELECT a.attname AS n FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        f"WHERE i.indrelid = to_regclass({lit(q(schema) + '.' + q(table))}) AND i.indisprimary ORDER BY array_position(i.indkey, a.attnum)",
        dbname=dbname)
    return [r["n"] for r in rows]


def fingerprint(schema, table, dbname, cols, key, exact):
    """(row count, hash of the compared rows). The hash is the sum of per-row hashes, so row order does not matter."""
    tbl = f"{q(schema)}.{q(table)}"
    row_text = f"ROW({', '.join(q(c) for c in cols)})::text"
    key_text = f"ROW({', '.join(q(c) for c in key)})::text" if key else row_text
    where = "" if exact else f"WHERE mod(abs(hashtextextended({key_text}, 0)), {SAMPLE_MOD}) = 0"
    r = query_db_dicts(
        f"SELECT (SELECT count(*) FROM {tbl}) AS n, (SELECT count(*) FROM {tbl} {where}) AS compared, "
        f"(SELECT COALESCE(sum(hashtextextended({row_text}, 0)::numeric), 0) FROM {tbl} {where}) AS h", dbname=dbname)[0]
    return int(r["n"]), int(r["compared"]), r["h"]


def reconcile_tables(src, tgt, tables=None, exact=False):
    """Compares the tables both layers own. Returns the result rows (dicts)."""
    sel = Selector(tables=[parse_table_ref(t) for t in (tables or [])])
    src_rows = {(r["schema"], r["table"]): r for r in read_manifest(src)}
    tgt_rows = read_manifest(tgt)
    try:
        sel.validate_tables([(r["schema"], r["table"]) for r in tgt_rows], what=f"the {tgt.name} manifest")
    except SelectorError as e:
        raise LayerError(str(e))
    out = []
    for r in tgt_rows:
        key = (r["schema"], r["table"])
        if not sel.matches(*key) or key not in src_rows:
            continue
        row = {"table": f"{key[0]}.{key[1]}", "track": r["track"], "mode": "exact" if exact else f"sample 1/{SAMPLE_MOD}"}
        # gold is informational (the application changes data there); VSAM tables are informational too
        row["enforced"] = tgt.name != "gold" and r["track"] in ("oracle", "model_output")
        try:
            scols, tcols = _columns(*key, src.database), _columns(*key, tgt.database)
            if not tcols:
                raise RuntimeError(f"table missing in {tgt.database}")
            if not scols:
                raise RuntimeError(f"table missing in {src.database}")
            common = [c for c in scols if c in tcols and c not in TOOL_AUDIT_COLS]
            keycols = [k for k in _key_columns(*key, tgt.database) if k in common]
            sn, sc, sh = fingerprint(*key, src.database, common, keycols, exact)
            tn, tc, th = fingerprint(*key, tgt.database, common, keycols, exact)
            only_src, only_tgt = [c for c in scols if c not in tcols], [c for c in tcols if c not in scols]
            row.update(src_rows=sn, tgt_rows=tn, rows_match=sn == tn, src_hash=sh, tgt_hash=th, compared_src=sc, compared_tgt=tc,
                       hash_match=(sh == th and sc == tc), columns_only_in_source=", ".join(only_src),
                       columns_only_in_target=", ".join(only_tgt), message="")
            row["status"] = "MATCH" if row["rows_match"] and row["hash_match"] else "MISMATCH"
        except RuntimeError as e:
            row.update(status="ERROR", message=clip_error(e), rows_match=False, hash_match=False)
        out.append(row)
    return out


def reconcile_cmd(from_layer, to_layer, tables=None, exact=False):
    """CLI entry: prints the result, writes the Excel report, returns the exit code (non-zero on an enforced mismatch)."""
    src, tgt = get_layer(from_layer), get_layer(to_layer)
    if src.name == tgt.name:
        raise LayerError("--from and --to must be different layers")
    if not same_credentials(layer_conn(src), layer_conn(tgt)):
        raise LayerError("both layers must be reachable with the same host, port, user and password for the comparison")
    with use_layer(tgt):
        rows = reconcile_tables(src, tgt, tables, exact)
    bad = [r for r in rows if r["status"] != "MATCH" and r["enforced"]]
    info = [r for r in rows if r["status"] != "MATCH" and not r["enforced"]]
    for r in rows:
        mark = r["status"] if r["enforced"] or r["status"] == "MATCH" else r["status"] + " (informational)"
        print(f"{mark:<26} {r['table']:<50} {r.get('src_rows', '-'):>10} {r.get('tgt_rows', '-'):>10}  {r['message']}")
    report = ORACLE_REPORTS_DIR / f"{tgt.database.lower()}_layers_reconcile_{src.name}_to_{tgt.name}_{get_timestamp()}.xlsx"
    generate_excel_report(report, {
        "Summary": [{"Metric": "From", "Value": src.name}, {"Metric": "To", "Value": tgt.name}, {"Metric": "Comparison", "Value": "exact" if exact else f"sampled 1/{SAMPLE_MOD}"},
                    {"Metric": "Tables compared", "Value": len(rows)}, {"Metric": "Mismatches (enforced)", "Value": len(bad)},
                    {"Metric": "Mismatches (informational)", "Value": len(info)},
                    {"Metric": "Verdict", "Value": "MATCH" if not bad else "DIFFERENT - do not chain deltas yet"}],
        "Tables": rows or [{"status": "no tables in common"}]})
    logger.info(f"reconcile {src.name} -> {tgt.name}: {len(rows)} table(s), {len(bad)} enforced mismatch(es), {len(info)} informational; report {report}")
    return 1 if bad else 0


# -----------------------------------------------------------------------------
# check
# -----------------------------------------------------------------------------
def _check_layer(lay):
    """List of (check, status, detail) for one layer. status: OK / WARN / FAIL."""
    from sweep_loadpipe import probe_trigger_mode
    from sweep_migrate import LATEST_VERSION
    res = []
    add = lambda name, status, detail="": res.append((name, status, detail))
    try:
        with use_layer(lay):
            actual = query_db_dicts("SELECT current_database() AS d, current_setting('server_version_num')::int AS v")[0]
            add("database reachable", "OK", actual["d"])
            if actual["d"].lower() != lay.database.lower():
                add("database name", "FAIL", f"connected to {actual['d']}, layer {lay.name} is configured as {lay.database}")
            major = int(actual["v"]) // 10000
            add(f"PostgreSQL >= {MIN_PG_VERSION}", "OK" if major >= MIN_PG_VERSION else "FAIL", f"server version {major}")
            add("session_replication_role", "OK" if probe_trigger_mode() == "replica" else "WARN",
                "can be set (triggers are switched off per transaction)" if probe_trigger_mode() == "replica"
                else "not allowed: falls back to disabling the user triggers of each table inside the transaction")
            try:
                done = {int(r["version"]) for r in query_db_dicts("SELECT version FROM sweep_ctl.schema_version")}
            except RuntimeError:
                done = set()
            add("sweep_ctl migrated", "OK" if LATEST_VERSION in done else "FAIL",
                f"schema version {max(done) if done else 'none'} (current {LATEST_VERSION}); run: python3 sweep.py migrate --layer {lay.name}")
            tables = read_manifest(lay)
            add("manifest", "OK" if tables else "WARN", f"{len(tables)} enabled table(s) in {lay.manifest_path.name}")
            if lay.upstream and tables:
                up = get_layer(lay.upstream)
                drift, missing_tables = [], []
                for r in tables:
                    mine = _columns(r["schema"], r["table"], lay.database)
                    theirs = _columns(r["schema"], r["table"], up.database)
                    if not mine:
                        missing_tables.append(f"{r['schema']}.{r['table']}")
                    elif theirs:
                        gap = [c for c in theirs if c not in mine and c not in TOOL_AUDIT_COLS]
                        if gap:
                            drift.append(f"{r['schema']}.{r['table']}: {', '.join(gap[:4])}")
                add("schema drift vs " + up.name, "OK" if not drift and not missing_tables else "WARN",
                    "none" if not drift and not missing_tables else
                    f"{len(drift)} table(s) lack source columns ({'; '.join(drift[:3])}); {len(missing_tables)} table(s) missing")
            behind = []
            for r in tables:
                for row in query_db_dicts(
                        f"SELECT a.attname AS c FROM pg_attribute a WHERE a.attrelid = to_regclass({lit(q(r['schema']) + '.' + q(r['table']))}) "
                        f"AND a.attnum > 0 AND NOT a.attisdropped AND (a.attidentity <> '' OR a.atthasdef) "
                        f"AND a.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype)"):
                    seq = query_db_dicts(f"SELECT pg_get_serial_sequence({lit(q(r['schema']) + '.' + q(r['table']))}, {lit(row['c'])}) AS s")[0]["s"]
                    if not seq:
                        continue
                    pos = query_db_dicts(f"SELECT last_value AS lv, is_called AS ic FROM {seq}")[0]
                    mx = query_db_dicts(f"SELECT max({q(row['c'])}) AS m FROM {q(r['schema'])}.{q(r['table'])}")[0]["m"]
                    if mx not in ("", None) and (int(mx) > int(pos["lv"]) or (int(mx) == int(pos["lv"]) and pos["ic"] in ("f", "false"))):
                        behind.append(f"{r['schema']}.{r['table']}.{row['c']}")
            add("sequences", "OK" if not behind else "WARN",
                "all sequences are at or beyond max(column)" if not behind else f"{len(behind)} behind max(column): {', '.join(behind[:3])}")
    except (RuntimeError, LayerError) as e:
        add("database reachable", "FAIL", clip_error(e))
    return res


def check_cmd(layer=None):
    """CLI entry: prints a status line per check and layer; returns 1 when a check FAILED."""
    layers = [get_layer(layer)] if layer else chain()
    failed = 0
    for lay in layers:
        print(f"== {lay.name} ({lay.database}) ==")
        for name, status, detail in _check_layer(lay):
            print(f"  [{status:<4}] {name}: {detail}")
            failed += status == "FAIL"
    print("All checks passed." if not failed else f"{failed} check(s) FAILED.")
    return 1 if failed else 0
