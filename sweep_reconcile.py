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
from sweep_fwd_ddl import load_fe_changes
from sweep_layers import LayerError, chain, get_layer, layer_conn, read_manifest, same_credentials, use_layer
from sweep_targeting import Selector, SelectorError, parse_table_ref
from sweep_validate import FE_STATUSES, FeLog

logger = get_logger("sweep_reconcile", "sweep_reconcile")

SAMPLE_MOD = 100          # sampled comparison: keys whose hash is 0 modulo 100 (about 1 percent)


# -----------------------------------------------------------------------------
# reconcile
# -----------------------------------------------------------------------------
def _columns(schema, table, dbname):
    return list(_column_types(schema, table, dbname))


def missing_audit_columns(schema, table, dbname):
    """The canonical audit columns (sweep_audit) a table lacks ([] when it carries all six; [] too when the table does not exist)."""
    have = set(_column_types(schema, table, dbname))
    return [] if not have else [c for c in sorted(TOOL_AUDIT_COLS) if c not in have]


def _column_types(schema, table, dbname):
    """{column: type} (generated columns left out), in column order."""
    rows = query_db_dicts(
        f"SELECT a.attname AS n, a.attgenerated AS g, format_type(a.atttypid, a.atttypmod) AS t FROM pg_attribute a "
        f"WHERE a.attrelid = to_regclass({lit(q(schema) + '.' + q(table))}) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", dbname=dbname)
    return {r["n"]: r["t"] for r in rows if not r["g"]}


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


def superset_missing(src, tgt, schema, table, cols, key, exact):
    """
    Gold check: how many of the (sampled) keys of the source table do NOT exist in the target. The keys are streamed from the
    source database into the target (the same cross-database pipe promote uses) and compared there in one read-only transaction.
    Returns (keys checked, keys missing).
    """
    from sweep_promote import STREAM_MARK, run_with_stream, source_argv
    key_text = lambda alias="": f"ROW({', '.join((alias + q(c)) for c in (key or cols))})::text"
    where = "" if exact else f"WHERE mod(abs(hashtextextended({key_text()}, 0)), {SAMPLE_MOD}) = 0"
    copy_sql = f"COPY (SELECT {key_text()} FROM {q(schema)}.{q(table)} {where}) TO STDOUT WITH (FORMAT csv, ENCODING 'UTF8')"
    script = ("\\set ON_ERROR_STOP on\n\\pset tuples_only on\n\\pset format unaligned\nBEGIN;\n"
              "CREATE TEMP TABLE _k (k text) ON COMMIT DROP;\n"
              "\\copy _k FROM STDIN WITH (FORMAT csv, ENCODING 'UTF8')\n" + STREAM_MARK + "\n"
              f"SELECT '@@SUP@@' || (SELECT count(*) FROM _k) || '|' || (SELECT count(*) FROM _k LEFT JOIN "
              f"(SELECT {key_text()} AS k FROM {q(schema)}.{q(table)}) t USING (k) WHERE t.k IS NULL);\nROLLBACK;\n")
    res = run_with_stream(script, source_argv(layer_conn(src), copy_sql), "-c statement_timeout=0", 0)
    if res.returncode != 0:
        raise RuntimeError(clip_error(res.stderr or res.stdout or "psql failed"))
    for line in res.stdout.splitlines():
        if line.startswith("@@SUP@@"):
            n, miss = line[7:].split("|")
            return int(n), int(miss)
    raise RuntimeError("superset check returned no result")


OK_STATUSES = ("MATCH",) + FE_STATUSES


def reconcile_tables(src, tgt, tables=None, exact=False, felog=None):
    """Compares the tables both layers own. Returns the result rows (dicts)."""
    sel = Selector(tables=[parse_table_ref(t) for t in (tables or [])])
    src_rows = {(r["schema"], r["table"]): r for r in read_manifest(src)}
    tgt_rows = read_manifest(tgt)
    try:
        sel.validate_tables([(r["schema"], r["table"]) for r in tgt_rows], what=f"the {tgt.name} manifest")
    except SelectorError as e:
        raise LayerError(str(e))
    fe = load_fe_changes(tgt.database)
    felog = felog if felog is not None else FeLog(fe)
    superset = tgt.name == "gold"
    out = []
    for r in tgt_rows:
        key = (r["schema"], r["table"])
        if not sel.matches(*key) or key not in src_rows:
            continue
        mode = (f"superset, {'exact' if exact else f'sample 1/{SAMPLE_MOD}'}" if superset else "exact" if exact else f"sample 1/{SAMPLE_MOD}")
        row = {"table": f"{key[0]}.{key[1]}", "track": r["track"], "mode": mode}
        # gold: every key of gold_backup must still exist (the application adds and changes rows there, it never removes them).
        # VSAM tables are enforced in silver (bronze must equal silver) and informational elsewhere.
        row["enforced"] = r["track"] in ("oracle", "model_output") or (r["track"] == "vsam" and tgt.name == "silver")
        try:
            stypes, ttypes = _column_types(*key, src.database), _column_types(*key, tgt.database)
            if not ttypes:
                if fe.table_dropped(*key):
                    felog.table(key[0], key[1], "MISSING_IN_PG")
                    row.update(status="FE_DROPPED", message=f"table dropped by {fe.dropped_tables[key]}", rows_match=True, hash_match=True)
                    out.append(row)
                    continue
                raise RuntimeError(f"table missing in {tgt.database}")
            if not stypes:
                raise RuntimeError(f"table missing in {src.database}")
            both = [c for c in stypes if c in ttypes and c not in TOOL_AUDIT_COLS]
            common = [c for c in both if stypes[c] == ttypes[c]]                    # only columns of the same type are compared
            type_diff = [c for c in both if stypes[c] != ttypes[c]]
            keycols = [k for k in _key_columns(*key, tgt.database) if k in common]
            sn, sc, sh = fingerprint(*key, src.database, common, keycols, exact)
            tn, tc, th = fingerprint(*key, tgt.database, common, keycols, exact)
            only_src, only_tgt = [c for c in stypes if c not in ttypes], [c for c in ttypes if c not in stypes]
            row.update(src_rows=sn, tgt_rows=tn, src_hash=sh, tgt_hash=th, compared_src=sc, compared_tgt=tc,
                       columns_only_in_source=", ".join(only_src), columns_only_in_target=", ".join(only_tgt), message="")
            if superset:
                checked, missing = superset_missing(src, tgt, *key, common, keycols, exact)
                row.update(rows_match=tn >= sn, hash_match=missing == 0, keys_checked=checked, keys_missing=missing)
                if missing or tn < sn:
                    row["message"] = f"{missing} key(s) of {src.name} missing in {tgt.name}; rows {sn} -> {tn}"
            else:
                row.update(rows_match=sn == tn, hash_match=(sh == th and sc == tc))
            # type differences are reported on their own; the ones an applied FE file explains count as PASS
            unexplained = []
            for c in type_diff:
                if not felog.column(key[0], key[1], c, "TYPE_MISMATCH"):
                    unexplained.append(c)
            for c in only_src:
                felog.column(key[0], key[1], c, "MISSING_IN_PG")
            for c in only_tgt:
                felog.column(key[0], key[1], c, "EXTRA_IN_PG")
            row["type_differences"] = "; ".join(f"{c} ({stypes[c]} vs {ttypes[c]})" for c in type_diff)
            row["status"] = "MATCH" if row["rows_match"] and row["hash_match"] else "MISMATCH"
            if row["status"] == "MATCH" and unexplained:
                row["status"] = "TYPE_DIFFERENCE"
                row["message"] = f"column type differs (not compared): {', '.join(unexplained)}"
            elif row["status"] == "MATCH" and type_diff:
                row["status"] = "FE_CHANGED"
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
        felog = FeLog(load_fe_changes(tgt.database))
        rows = reconcile_tables(src, tgt, tables, exact, felog)
    bad = [r for r in rows if r["status"] not in OK_STATUSES and r["enforced"]]
    info = [r for r in rows if r["status"] not in OK_STATUSES and not r["enforced"]]
    for r in rows:
        mark = r["status"] if r["enforced"] or r["status"] in OK_STATUSES else r["status"] + " (informational)"
        print(f"{mark:<26} {r['table']:<50} {r.get('src_rows', '-'):>10} {r.get('tgt_rows', '-'):>10}  {r['message']}")
    report = ORACLE_REPORTS_DIR / f"{tgt.database.lower()}_layers_reconcile_{src.name}_to_{tgt.name}_{get_timestamp()}.xlsx"
    generate_excel_report(report, {
        "Summary": [{"Metric": "From", "Value": src.name}, {"Metric": "To", "Value": tgt.name},
                    {"Metric": "Comparison", "Value": ("superset check, " if tgt.name == "gold" else "") + ("exact" if exact else f"sampled 1/{SAMPLE_MOD}")},
                    {"Metric": "Tables compared", "Value": len(rows)}, {"Metric": "Mismatches (enforced)", "Value": len(bad)},
                    {"Metric": "Mismatches (informational)", "Value": len(info)},
                    {"Metric": "Differences explained by FE files", "Value": len(felog.rows)},
                    {"Metric": "Verdict", "Value": "MATCH" if not bad else "DIFFERENT - do not chain deltas yet"}],
        "Tables": rows or [{"status": "no tables in common"}],
        "FE_Changes": felog.sheet()})
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
            lacking = [f"{r['schema']}.{r['table']} ({', '.join(m)})" for r in tables
                       for m in [missing_audit_columns(r["schema"], r["table"], lay.database)] if m]
            add("audit columns", "OK" if not lacking else "WARN",
                "every table carries the six canonical audit columns" if not lacking else
                f"{len(lacking)} table(s) lack audit column(s): {'; '.join(lacking[:3])}" + (" ..." if len(lacking) > 3 else ""))
            if lay.upstream and tables:
                up = get_layer(lay.upstream)
                drift, missing_tables = [], []
                fe = load_fe_changes(lay.database)       # differences an applied FE file explains are not drift
                for r in tables:
                    mine = _columns(r["schema"], r["table"], lay.database)
                    theirs = _columns(r["schema"], r["table"], up.database)
                    if not mine:
                        if not fe.table_dropped(r["schema"], r["table"]):
                            missing_tables.append(f"{r['schema']}.{r['table']}")
                    elif theirs:
                        explained = fe.dropped_columns.get((r["schema"], r["table"]), {})
                        gap = [c for c in theirs if c not in mine and c not in TOOL_AUDIT_COLS and c not in explained]
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


# -----------------------------------------------------------------------------
# validate (I5): one report and one verdict per layer
# -----------------------------------------------------------------------------
def schema_comparison(src, tgt, tables, felog):
    """Column by column comparison (name, type) of the manifest tables of a layer with its upstream; FE explained differences are FE_*."""
    sel = Selector(tables=[parse_table_ref(t) for t in (tables or [])])
    src_rows = {(r["schema"], r["table"]) for r in read_manifest(src)}
    rows = []
    for r in read_manifest(tgt):
        key = (r["schema"], r["table"])
        if key not in src_rows or not sel.matches(*key):
            continue
        stypes, ttypes = _column_types(*key, src.database), _column_types(*key, tgt.database)
        base = {"target_schema": key[0], "target_table": key[1]}
        if not ttypes:
            st = felog.table(key[0], key[1], "MISSING_IN_TARGET") or "MISSING_IN_TARGET"
            rows.append({**base, "column_name": "", "source_type": "", "target_type": "", "status": st})
            continue
        for c in missing_audit_columns(*key, tgt.database):         # the six canonical audit columns are expected in every table
            rows.append({**base, "column_name": c, "source_type": stypes.get(c, ""), "target_type": "", "status": "MISSING_AUDIT_COLUMN"})
        for c in sorted(set(stypes) | set(ttypes)):
            if c in TOOL_AUDIT_COLS:
                continue
            a, b = stypes.get(c, ""), ttypes.get(c, "")
            if a and b:
                st = "MATCH" if a == b else (felog.column(key[0], key[1], c, "TYPE_MISMATCH") or "TYPE_DIFFERENCE")
            elif a:
                st = felog.column(key[0], key[1], c, "MISSING_IN_TARGET") or "MISSING_IN_TARGET"
            else:
                st = felog.column(key[0], key[1], c, "EXTRA_IN_TARGET") or "EXTRA_IN_TARGET"
            rows.append({**base, "column_name": c, "source_type": a, "target_type": b, "status": st})
    return rows


def validate_layer_cmd(layer_name, tables=None, exact=False, tracks=("oracle", "vsam")):
    """
    `sweep.py validate --layer L`: bronze = Oracle Step 08 + VSAM Step 07 (their own reports) + the load ledger; every other layer =
    reconcile with its upstream + schema comparison + ledger. One Excel report with a verdict per component and for the layer.
    Returns 0 on PASS, 1 otherwise.
    """
    import subprocess
    import sys

    from sweep_common import BASE_DIR
    from sweep_validate import ledger_check
    lay = get_layer(layer_name)
    components, sheets = [], {}
    fe_rows = []
    sel = Selector(tables=[parse_table_ref(t) for t in (tables or [])])
    with use_layer(lay):
        fe = load_fe_changes(lay.database)
        felog = FeLog(fe)
        if lay.upstream is None:
            env = dict(__import__("os").environ, PYTHONPATH=str(BASE_DIR))
            for track, mod in (("oracle", "oracle.oracle_08_validate"), ("vsam", "vsam.vsam_07_validate")):
                if track not in tracks:
                    continue
                cmd = [sys.executable, "-m", mod, "--layer", lay.name]
                if exact:
                    cmd.append("--exact-counts")
                if tables:
                    cmd += ["--tables", ",".join(tables)]
                r = subprocess.run(cmd, cwd=str(BASE_DIR), env=env, capture_output=True, text=True)
                verdict = "PASS" if r.returncode == 0 else "FAIL" if r.returncode == 1 else f"ERROR (exit {r.returncode})"
                tail = (r.stdout + r.stderr).strip().splitlines()[-1:] or [""]
                components.append({"component": f"{track} validation ({mod.split('.')[-1]})", "verdict": verdict,
                                   "detail": clip_error(tail[0])})
        else:
            up = get_layer(lay.upstream)
            if not same_credentials(layer_conn(up), layer_conn(lay)):
                raise LayerError("both layers must be reachable with the same host, port, user and password for the comparison")
            rec = reconcile_tables(up, lay, tables, exact, felog)
            bad = [x for x in rec if x["status"] not in OK_STATUSES and x["enforced"]]
            info = [x for x in rec if x["status"] not in OK_STATUSES and not x["enforced"]]
            sheets["Reconcile"] = rec or [{"status": "no tables in common"}]
            components.append({"component": f"reconcile with {up.name} ({'superset' if lay.name == 'gold' else 'equal'})",
                               "verdict": "PASS" if not bad else "FAIL",
                               "detail": f"{len(rec)} table(s), {len(bad)} enforced difference(s), {len(info)} informational"})
            sch = schema_comparison(up, lay, tables, FeLog(fe))
            for x in sch:                                  # the schema comparison explains FE differences through the same overlay
                if x["status"] in FE_STATUSES:
                    felog.add(x["status"], x["target_schema"], x["target_table"], x["column_name"], "", "", "schema comparison")
            sheets["Schema"] = sch or [{"status": "no tables in common"}]
            unexplained = [x for x in sch if x["status"] in ("MISSING_IN_TARGET", "TYPE_DIFFERENCE")]
            components.append({"component": f"schema comparison with {up.name}", "verdict": "PASS" if not unexplained else "FAIL",
                               "detail": f"{len(sch)} column row(s), {len(unexplained)} unexplained difference(s)"})
        ledger = ledger_check(None, sel if sel.table_selectors else None, fe)
        sheets["Ledger"] = ledger or [{"ledger_status": "no load_history rows"}]
        led_bad = [x for x in ledger if x["ledger_status"] == "LEDGER_MISMATCH"]
        components.append({"component": "ledger (rows vs sweep_ctl.load_history)", "verdict": "PASS" if not led_bad else "FAIL",
                           "detail": f"{len(ledger)} table(s), {len(led_bad)} mismatch(es)"})
        fe_rows = felog.sheet()
    overall = "PASS" if all(c["verdict"] == "PASS" for c in components) else "FAIL"
    for c in components:
        print(f"[{c['verdict']:<5}] {c['component']}: {c['detail']}")
    print(f"VERDICT {lay.name} ({lay.database}): {overall}")
    report = ORACLE_REPORTS_DIR / f"{lay.database.lower()}_validate_{lay.name}_{get_timestamp()}.xlsx"
    generate_excel_report(report, {
        "Summary": [{"component": "layer", "verdict": overall, "detail": f"{lay.name} ({lay.database})"}] + components,
        **sheets, "FE_Changes": fe_rows})
    logger.info(f"validate {lay.name}: {overall}; report {report}")
    return 0 if overall == "PASS" else 1
