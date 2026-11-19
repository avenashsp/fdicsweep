#!/usr/bin/env python3
"""
vsam/vsam_06_pipeline.py - SQL pipeline of the VSAM Step 06 loader.

Same workflow as the Oracle track (see oracle/oracle_06_pipeline.py): temp tables, bulk \\copy, set-based
validation, ONE transaction per unit of work.

A unit is one file of a keyed table, or ALL files of one table without a primary key (they replace the table
together, so they share one transaction and one connection):

    BEGIN
      \\copy every file into an all-text temp table (columns named after the table columns, via the Step 02 map)
      rows are routed to a destination schema from the korg column (last 3 digits -> active schema, else bcs)
      keyed tables:  ALL_NULL / PK_BLANK / PK_SENTINEL rows are rejected
      every table:   BAD_VALUE (pg_input_is_valid), NOT_NULL
      typed temp table, duplicates inside the file on the key (last row wins, DUP_IN_FILE)
      keyed:    INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE (target) IS DISTINCT FROM (file)
      keyless:  TRUNCATE the destination tables, INSERT the new snapshot
      row-count check: read = inserted + updated + unchanged + rejected + duplicates, else roll back
      rejected rows -> sweep_ctl.load_rejects, load_log rows (with the S3 metadata), COMMIT
    END
"""
from sweep_common import CTL_SCHEMA, lit, q
from sweep_loadpipe import MAX_DUP_ROWS_PRINTED, MAX_REJECT_ROWS_PRINTED, REJECT_TABLE, TEXT_TYPES, upsert_statements
from sweep_s3meta import meta_sql_values

WS = "E' \\t\\r\\n'"          # SQL constant holding the whitespace characters that are trimmed

HELPERS = """
CREATE FUNCTION pg_temp.sw_blank(t text) RETURNS boolean LANGUAGE sql IMMUTABLE AS
  $f$ SELECT t IS NULL OR lower(btrim(t, @WS@)) IN ('', 'nan', 'none', 'null') $f$;
CREATE FUNCTION pg_temp.sw_nines(t text) RETURNS boolean LANGUAGE sql IMMUTABLE AS
  $f$ SELECT btrim(t, @WS@) ~ '^9+$' OR btrim(t, @WS@) ~ '^9{4}[-_]9{2}[-_]9{2}$' $f$;
CREATE FUNCTION pg_temp.sw_sent(t text) RETURNS boolean LANGUAGE sql IMMUTABLE AS
  $f$ SELECT pg_temp.sw_blank(t) OR pg_temp.sw_nines(t) OR btrim(t, @WS@) ~ '^0+$' OR btrim(t, @WS@) ~ '^0{4}[-_]0{2}[-_]0{2}$' $f$;
CREATE FUNCTION pg_temp.sw_last3(t text) RETURNS text LANGUAGE sql IMMUTABLE AS
  $f$ SELECT CASE WHEN length(d) >= 3 THEN right(d, 3) ELSE lpad(d, 3, '0') END FROM (SELECT (regexp_match(split_part(btrim(t, @WS@), '.', 1), '(\\d+)\\D*$'))[1] AS d) x $f$;
""".replace("@WS@", WS)


def _is_text(type_name):
    return type_name in TEXT_TYPES


def dest_expr(route_col, alias="r"):
    """SQL expression for the destination schema of a raw row (last 3 digits of the korg value, else bcs)."""
    if not route_col:
        return "'bcs'"
    return (f"COALESCE((SELECT m.schema FROM _route m WHERE m.last3 = pg_temp.sw_last3({alias}.{q(route_col)})), 'bcs')")


def _reject_inserts(i, f, meta_cols, arbiter, keyed):
    """Statements that reject rows of file i: PK rules (keyed), BAD_VALUE, NOT_NULL. Returns (sql list, typed stage sql)."""
    out = []
    raw = f"_raw{i}"
    dest = dest_expr(f["route_col"])
    if keyed:
        all_cols = [c for c in f["raw_cols"] if not c.startswith("_skip")]
        allsent = " AND ".join(f"pg_temp.sw_sent(r.{q(c)})" for c in all_cols) or "false"
        blank_any = " OR ".join(f"pg_temp.sw_blank(r.{q(c)})" for c in arbiter)
        nines_any = " OR ".join(f"pg_temp.sw_nines(r.{q(c)})" for c in arbiter)
        blank_detail = ", ".join(
            f"CASE WHEN pg_temp.sw_blank(r.{q(c)}) THEN {lit('PK column ' + c + ' is NULL / empty / blank')} END" for c in arbiter)
        nines_detail = ", ".join(
            f"CASE WHEN pg_temp.sw_nines(r.{q(c)}) THEN {lit('PK column ' + c + ' contains sentinel value ')} || btrim(r.{q(c)}) END"
            for c in arbiter)
        out.append(
            f"INSERT INTO _rej SELECT {i}, x._dest, x._row_no, x._reason, x._detail, "
            f"to_jsonb(x) - '_row_no' - '_reason' - '_detail' - '_dest' FROM ("
            f"SELECT r.*, {dest} AS _dest, "
            f"CASE WHEN {allsent} THEN 'ALL_NULL' WHEN {blank_any} THEN 'PK_BLANK' WHEN {nines_any} THEN 'PK_SENTINEL' END AS _reason, "
            f"CASE WHEN {allsent} THEN 'All columns of the record are empty, blank or placeholder (0s / 9s) values' "
            f"WHEN {blank_any} THEN array_to_string(ARRAY[{blank_detail}], '; ') "
            f"WHEN {nines_any} THEN array_to_string(ARRAY[{nines_detail}], '; ') END AS _detail "
            f"FROM {raw} r) x WHERE x._reason IS NOT NULL;\n")

    typed = [c for c in f["present"] if not _is_text(meta_cols[c]["type"])]
    if typed:
        tests = ", ".join(
            f"CASE WHEN NOT pg_input_is_valid(NULLIF(btrim(r.{q(c)}, {WS}), ''), {lit(meta_cols[c]['type'])}) "
            f"THEN {lit(c + ' = ')} || quote_literal(left(r.{q(c)}, 100)) END" for c in typed)
        out.append(
            f"INSERT INTO _rej SELECT {i}, x._dest, x._row_no, 'BAD_VALUE', array_to_string(x.bad, '; '), "
            f"to_jsonb(x) - '_row_no' - 'bad' - '_dest' FROM ("
            f"SELECT r.*, {dest} AS _dest, array_remove(ARRAY[{tests}], NULL) AS bad FROM {raw} r "
            f"WHERE NOT EXISTS (SELECT 1 FROM _rej b WHERE b.f = {i} AND b.row_no = r._row_no)) x WHERE cardinality(x.bad) > 0;\n")

    sel = ", ".join(f"NULLIF(btrim(r.{q(c)}, {WS}), '')::{meta_cols[c]['type']} AS {q(c)}" for c in f["present"])
    stage = (f"CREATE TEMP TABLE _s{i} ON COMMIT DROP AS SELECT r._row_no, {dest} AS _dest, {sel} FROM {raw} r "
             f"WHERE NOT EXISTS (SELECT 1 FROM _rej b WHERE b.f = {i} AND b.row_no = r._row_no);\nDROP TABLE {raw};\n")
    return out, stage


def build_unit_script(unit):
    """
    unit: {table, files, dests, cols (live column meta), arbiter (list, [] = keyless), route [(last3, schema)],
           max_rejects, replica (bool), keyless_replace (bool)}
    file: {i, uri, name, stream_cmd, raw_cols, present, route_col, header, delim_sql, quote_sql, size_bytes,
           s3_meta, snapshot_msg, worker_id}
    """
    table, files, dests, cols = unit["table"], unit["files"], unit["dests"], unit["cols"]
    arbiter = unit["arbiter"]
    keyed = bool(arbiter)
    s = []
    add = s.append
    add("\\set ON_ERROR_STOP on\n\\pset tuples_only on\n\\pset format unaligned\nBEGIN;\n")
    if unit.get("replica"):
        add("SET LOCAL session_replication_role = replica;\n")
    add(HELPERS)
    add("CREATE TEMP TABLE _c (k text PRIMARY KEY, n bigint NOT NULL DEFAULT 0) ON COMMIT DROP;\n"
        "CREATE TEMP TABLE _rej (f int, dest text, row_no bigint, reason text, detail text, row_data jsonb) ON COMMIT DROP;\n"
        "CREATE INDEX ON _rej (f, row_no);\n"
        "CREATE TEMP TABLE _route (last3 text PRIMARY KEY, schema text NOT NULL) ON COMMIT DROP;\n")
    if unit["route"]:
        add("INSERT INTO _route VALUES " + ", ".join(f"({lit(a)}, {lit(b)})" for a, b in unit["route"]) + ";\n")

    for f in files:
        i = f["i"]
        raw_defs = ", ".join(f"{q(c)} text" for c in f["raw_cols"])
        col_list = ", ".join(q(c) for c in f["raw_cols"])
        add(f"CREATE TEMP TABLE _raw{i} (_row_no bigint GENERATED ALWAYS AS IDENTITY, {raw_defs}) ON COMMIT DROP;\n")
        add(f"\\copy _raw{i} ({col_list}) FROM PROGRAM '{f['stream_cmd']}' WITH (FORMAT csv, HEADER {'true' if f['header'] else 'false'}, "
            f"DELIMITER {f['delim_sql']}{f['quote_sql']}, ENCODING '{unit['encoding']}');\n")
        add(f"INSERT INTO _c SELECT 'raw|{i}', count(*) FROM _raw{i};\n")
        stmts, stage = _reject_inserts(i, f, cols, arbiter, keyed)
        for st in stmts:
            add(st)
        add(stage)
        nn = [c for c in f["present"] if cols[c]["notnull"] and c not in arbiter]
        if nn:
            tests = ", ".join(f"CASE WHEN s.{q(c)} IS NULL THEN {lit(c)} END" for c in nn)
            add(f"INSERT INTO _rej SELECT {i}, s._dest, s._row_no, 'NOT_NULL', col, to_jsonb(s) - '_row_no' - '_dest' "
                f"FROM _s{i} s CROSS JOIN LATERAL unnest(array_remove(ARRAY[{tests}], NULL)) AS col;\n"
                f"DELETE FROM _s{i} s WHERE EXISTS (SELECT 1 FROM _rej r WHERE r.f = {i} AND r.row_no = s._row_no);\n")
        if keyed:
            kc = ", ".join(q(c) for c in arbiter)
            add(f"WITH d AS (DELETE FROM _s{i} s USING (SELECT _row_no FROM (SELECT _row_no, row_number() OVER "
                f"(PARTITION BY _dest, {kc} ORDER BY _row_no DESC) AS rn FROM _s{i}) w WHERE rn > 1) l "
                f"WHERE s._row_no = l._row_no RETURNING s._row_no, s._dest, to_jsonb(s) - '_row_no' - '_dest' AS js) "
                f"INSERT INTO _rej SELECT {i}, _dest, _row_no, 'DUP_IN_FILE', {lit('key (' + ','.join(arbiter) + ')')}, js FROM d;\n")
        add(f"INSERT INTO _c SELECT 'rejected|{i}', count(*) FROM _rej WHERE f = {i} AND reason <> 'DUP_IN_FILE';\n"
            f"INSERT INTO _c SELECT 'dup|{i}', count(*) FROM _rej WHERE f = {i} AND reason = 'DUP_IN_FILE';\n")
        for d in dests:
            add(f"INSERT INTO _c SELECT 'staged|{i}|{d}', count(*) FROM _s{i} WHERE _dest = {lit(d)};\n")

    # report what was found (echoed even when the unit then fails), then the reject limit
    add("SELECT '@@COUNTS@@' || json_object_agg(k, n) FROM _c;\n"
        "SELECT '@@REJ_SUMMARY@@' || json_build_object('f', f, 'reason', reason, 'detail', d, 'n', n) FROM "
        "(SELECT f, reason, min(detail) AS d, count(*) AS n FROM _rej GROUP BY f, reason ORDER BY f, n DESC LIMIT 2000) g;\n"
        "SELECT '@@REJ_ROW@@' || json_build_object('f', f, 'dest', dest, 'row_no', row_no, 'reason', reason, 'detail', detail, "
        "'row', row_data) FROM (SELECT *, row_number() OVER (PARTITION BY f ORDER BY row_no) AS rn FROM _rej "
        f"WHERE reason <> 'DUP_IN_FILE') a WHERE rn <= {MAX_REJECT_ROWS_PRINTED};\n"
        "SELECT '@@REJ_ROW@@' || json_build_object('f', f, 'dest', dest, 'row_no', row_no, 'reason', reason, 'detail', detail, "
        "'row', row_data) FROM (SELECT *, row_number() OVER (PARTITION BY f ORDER BY row_no) AS rn FROM _rej "
        f"WHERE reason = 'DUP_IN_FILE') a WHERE rn <= {MAX_DUP_ROWS_PRINTED};\n")
    limit = int(unit.get("max_rejects") or 0)
    if limit > 0:
        names = ", ".join(f"({f['i']}, {lit(f['name'])})" for f in files)
        add("DO $$ DECLARE r record; msg text; BEGIN "
            f"FOR r IN SELECT v.i, v.name, c.n FROM (VALUES {names}) AS v(i, name) "
            "JOIN _c c ON c.k = 'rejected|' || v.i LOOP "
            f"IF r.n >= {limit} THEN "
            "SELECT string_agg('record ' || row_no || ' ' || reason || ' ' || coalesce(detail, ''), '; ') INTO msg "
            "FROM (SELECT * FROM _rej WHERE f = r.i AND reason <> 'DUP_IN_FILE' ORDER BY row_no LIMIT 5) z; "
            f"RAISE EXCEPTION 'Too many rejected rows in %: % (limit {limit}). First: %', r.name, r.n, msg; END IF; END LOOP; END $$;\n")

    # apply
    if keyed:
        a = {"cols": arbiter}
        for f in files:
            for d in dests:
                for stmt in upsert_statements(f"{q(d)}.{q(table)}", f["present"], cols, a, set(f["present"]), False,
                                              stg=f"_s{f['i']}", where=f"WHERE s._dest = {lit(d)}",
                                              sfx=f"|{f['i']}|{d}", existing=f"_ex{f['i']}"):
                    add(stmt)
    else:
        for d in dests:       # snapshot replace: a destination that receives rows is emptied first
            add("DO $$ BEGIN IF (SELECT coalesce(sum(n), 0) FROM _c WHERE split_part(k, '|', 1) = 'staged' "
                f"AND split_part(k, '|', 3) = {lit(d)}) > 0 THEN TRUNCATE TABLE {q(d)}.{q(table)}; END IF; END $$;\n")
        for f in files:
            for d in dests:
                for stmt in upsert_statements(f"{q(d)}.{q(table)}", f["present"], cols, None, set(f["present"]), False,
                                              stg=f"_s{f['i']}", where=f"WHERE s._dest = {lit(d)}", sfx=f"|{f['i']}|{d}"):
                    add(stmt)

    # nothing may disappear silently
    add("DO $$ DECLARE raw_n bigint; acc bigint; BEGIN "
        "SELECT sum(n) INTO raw_n FROM _c WHERE split_part(k, '|', 1) = 'raw'; "
        "SELECT sum(n) INTO acc FROM _c WHERE split_part(k, '|', 1) IN ('inserted', 'updated', 'unchanged', 'rejected', 'dup'); "
        "IF raw_n IS DISTINCT FROM acc THEN RAISE EXCEPTION 'Row count check failed: % rows read but % accounted for "
        "(inserted + updated + unchanged + rejected + duplicates)', raw_n, acc; END IF; END $$;\n"
        "SELECT '@@COUNTS@@' || json_object_agg(k, n) FROM _c;\n")

    # rejects + load_log rows (inside the transaction: they exist only if the data does)
    done_kinds = "('inserted', 'updated', 'unchanged')"
    for f in files:
        i, uri = f["i"], f["uri"]
        add(f"DELETE FROM {REJECT_TABLE} WHERE uri = {lit(uri)} AND target_table = {lit(table)};\n"
            f"INSERT INTO {REJECT_TABLE} (uri, target_schema, target_table, row_no, reason, detail, row_data, file_status) "
            f"SELECT {lit(uri)}, dest, {lit(table)}, row_no, reason, detail, row_data, 'LOADED' FROM "
            f"(SELECT * FROM _rej WHERE f = {i} AND reason <> 'DUP_IN_FILE' UNION ALL "
            f"(SELECT * FROM _rej WHERE f = {i} AND reason = 'DUP_IN_FILE' ORDER BY row_no LIMIT {MAX_REJECT_ROWS_PRINTED})) z;\n")
        lm, etag, sz = meta_sql_values(f["s3_meta"])
        add(f"DELETE FROM {CTL_SCHEMA}.load_log WHERE uri = {lit(uri)} OR starts_with(uri, {lit(uri + '#')});\n"
            f"INSERT INTO {CTL_SCHEMA}.load_log (uri, target_schema, target_table, size_bytes, loaded_rows, seconds, worker_id, "
            f"status, message, s3_last_modified, s3_etag, s3_size, loaded_at) "
            f"SELECT {lit(uri + '#')} || d, d, {lit(table)}, {int(f['size_bytes'])}, n, 0, {int(f['worker_id'])}, 'SUCCESS', "
            f"{lit(f['snapshot_msg'])}, {lm}, {etag}, {sz}, now() FROM ("
            f"SELECT d, n FROM (SELECT split_part(k, '|', 3) AS d, sum(n) AS n FROM _c WHERE split_part(k, '|', 1) IN {done_kinds} "
            f"AND split_part(k, '|', 2) = '{i}' GROUP BY 1) a WHERE n > 0 "
            f"UNION ALL SELECT 'bcs', 0 WHERE NOT EXISTS (SELECT 1 FROM _c WHERE split_part(k, '|', 1) IN {done_kinds} "
            f"AND split_part(k, '|', 2) = '{i}' AND n > 0)) z;\n")
    add("COMMIT;\n")
    return "".join(s)


def parse_counts(counts):
    """
    {'raw|1': 5, 'inserted|1|bcs': 3, ...} -> {file index: {raw, rejected, dup, by_dest: {dest: {inserted, updated,
    unchanged, staged}}}}
    """
    out = {}
    for k, n in counts.items():
        parts = k.split("|")
        kind, i = parts[0], int(parts[1])
        rec = out.setdefault(i, {"raw": 0, "rejected": 0, "dup": 0, "by_dest": {}})
        if kind in ("raw", "rejected", "dup"):
            rec[kind] = n
        elif len(parts) == 3:
            rec["by_dest"].setdefault(parts[2], {"inserted": 0, "updated": 0, "unchanged": 0, "staged": 0})[kind] = n
    return out
