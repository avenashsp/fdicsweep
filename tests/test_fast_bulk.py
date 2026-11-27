"""--fast-bulk is for BARE tables (no key / FK / CHECK / index / trigger), empty or not: registry, ledger and promote (6.2.3)."""
import threading

import pytest

import sweep_common as c
import sweep_layers as sl
import sweep_promote as sp
from chain_support import LAYERS, psql
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_pipeline as pl
from sweep_loadpipe import load_table_meta
from sweep_validate import ledger_check

# ---------------------------------------------------------------- the rule itself (pure)
META = {"columns": {"id": {"identity": ""}, "v": {"identity": ""}}, "keys": [], "checks": [], "fks": [], "triggers": [], "indexes": [], "all_triggers": []}


def blockers(**over):
    return pl.fast_bulk_blockers({**META, **over}, ["id", "v"], over.pop("has_rules", False) if False else False)


def test_a_bare_table_has_no_blocker_and_not_null_identity_by_default_and_serial_defaults_do_not_block():
    assert blockers() == []
    cols = {"id": {"identity": "d", "notnull": True, "hasdef": True}, "v": {"identity": "", "notnull": True, "hasdef": True}}
    assert pl.fast_bulk_blockers({**META, "columns": cols}, ["id", "v"]) == []


@pytest.mark.parametrize("over,needle", [
    ({"keys": [{"name": "pk_t", "primary": True, "cols": ["id"]}]}, "primary / unique key (pk_t)"),
    ({"fks": [{"name": "fk_t", "parent": "p", "self_ref": False, "cols": ["id"], "parent_cols": ["id"]}]}, "foreign key(s) (fk_t)"),
    ({"checks": [{"name": "ck_t", "expr": "id > 0", "cols": ["id"]}]}, "CHECK constraint(s) (ck_t)"),
    ({"indexes": [{"name": "ix_plain", "on": "s.t", "unique": False, "partial": False, "expr": False, "valid": True}]}, "ix_plain"),
    ({"indexes": [{"name": "ix_part", "on": "s.t", "unique": False, "partial": True, "expr": False, "valid": True}]}, "ix_part (partial"),
    ({"indexes": [{"name": "ix_expr", "on": "s.t", "unique": False, "partial": False, "expr": True, "valid": True}]}, "ix_expr (expression"),
    ({"indexes": [{"name": "ix_bad", "on": "s.t", "unique": False, "partial": False, "expr": False, "valid": False}]}, "ix_bad (invalid"),
    ({"indexes": [{"name": "ix_child", "on": "s.t_p1", "unique": False, "partial": False, "expr": False, "valid": True}]}, "ix_child (on s.t_p1)"),
    ({"all_triggers": [{"name": "trg_on", "on": "s.t", "enabled": True}]}, "user trigger(s): trg_on on s.t"),
    ({"all_triggers": [{"name": "trg_off", "on": "s.t", "enabled": False}]}, "trg_off (disabled) on s.t"),
])
def test_each_blocker_alone_sends_the_table_to_the_staged_path_and_is_named(over, needle):
    why = blockers(**over)
    assert len(why) == 1 and needle in why[0], why


def test_generated_always_in_the_file_and_fe_rules_block_and_the_old_empty_table_rule_is_gone():
    cols = {"id": {"identity": "a"}, "v": {"identity": ""}}
    assert "GENERATED ALWAYS" in pl.fast_bulk_blockers({**META, "columns": cols}, ["id", "v"])[0]
    assert pl.fast_bulk_blockers({**META, "columns": cols}, ["v"]) == []                       # the column is not in the file
    assert "FE column rules" in pl.fast_bulk_blockers(META, ["id"], True)[0]
    import inspect
    assert list(inspect.signature(pl.fast_bulk_blockers).parameters) == ["meta", "header_cols", "has_rules"]       # no table_is_empty any more


# ---------------------------------------------------------------- against PostgreSQL
pytestmark_db = pytest.mark.integration


@pytest.fixture(autouse=True)
def _fresh(pg_db):
    o6.META_CACHE.clear()
    o6.PK_CACHE.clear()
    pl.ensure_reject_table()
    yield


def _t(table, ddl, schema="fb"):
    c.run_psql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"; DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE; CREATE TABLE "{schema}"."{table}" ({ddl});')


def _q(sql):
    return c.run_psql(sql, tuples_only=True)


def _job(path, table, schema="fb"):
    return {"uri": str(path), "target_schema": schema, "target_table": table, "size_bytes": path.stat().st_size, "size_mb": 0.01,
            "filename": path.name, "file_format": "gzip"}


def _load(gz_file, name, text, table, worker=1, **kw):
    o6.META_CACHE.clear()                       # the table layout is cached per run: DDL done by the test must be seen
    p = gz_file(name, text)
    return o6.load_single_file(_job(p, table), worker_id=worker, fast_bulk=True, **kw)


def _body(a, b):
    return "id,v\n" + "".join(f"{i},v{i}\n" for i in range(a, b))


@pytest.mark.integration
def test_every_blocker_on_a_real_table_uses_the_staged_path_and_the_log_names_the_object(gz_file, grab, monkeypatch):
    cases = {
        "pk": ("id int PRIMARY KEY, v text", "", "pk"),
        "uq": ("id int, v text, CONSTRAINT uq_c UNIQUE (id)", "", "uq_c"),
        "fk": ("id int, v text, CONSTRAINT fk_c FOREIGN KEY (id) REFERENCES fb.parent(id)", "", "fk_c"),
        "ck": ("id int, v text, CONSTRAINT ck_c CHECK (id > 0)", "", "ck_c"),
        "ix": ("id int, v text", "CREATE INDEX ix_plain ON fb.ix (id)", "ix_plain"),
        "ixp": ("id int, v text", "CREATE INDEX ix_partial ON fb.ixp (id) WHERE id > 0", "ix_partial"),
        "ixe": ("id int, v text", "CREATE INDEX ix_expr ON fb.ixe ((id + 1))", "ix_expr"),
        "trg": ("id int, v text", "CREATE FUNCTION fb.f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; "
                                  "CREATE TRIGGER trg_user BEFORE INSERT ON fb.trg FOR EACH ROW EXECUTE FUNCTION fb.f()", "trg_user"),
        "trgoff": ("id int, v text", "CREATE FUNCTION fb.f2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; "
                                     "CREATE TRIGGER trg_off BEFORE INSERT ON fb.trgoff FOR EACH ROW EXECUTE FUNCTION fb.f2(); "
                                     "ALTER TABLE fb.trgoff DISABLE TRIGGER trg_off", "trg_off"),
    }
    _t("parent", "id int PRIMARY KEY")
    c.run_psql("INSERT INTO fb.parent VALUES (1), (2), (3)")
    for name, (ddl, extra, needle) in cases.items():
        _t(name, ddl)
        if extra:
            c.run_psql(extra)
        o6.META_CACHE.clear()
        msgs = grab(o6.logger)
        o = _load(gz_file, f"{name}.gz", _body(1, 3), name)
        assert (o.status, o.path) == ("SUCCESS", "staged"), (name, o.error)
        assert any("--fast-bulk skipped" in m and needle in m and "table is not empty" not in m for m in msgs), (name, msgs)
    # an invalid index and an index on a child partition (the parent itself is bare)
    _t("part", "id int, v text) PARTITION BY RANGE (id")
    c.run_psql("CREATE TABLE fb.part_1 PARTITION OF fb.part FOR VALUES FROM (0) TO (100); CREATE TABLE fb.part_2 PARTITION OF fb.part FOR VALUES FROM (100) TO (200)")
    o = _load(gz_file, "part0.gz", _body(1, 3), "part")
    assert o.path == "fast bulk", o.error                                                         # a bare partitioned parent takes the fast path
    c.run_psql("CREATE INDEX ix_child ON fb.part_2 (v)")
    msgs = grab(o6.logger)
    o = _load(gz_file, "part1.gz", _body(3, 5), "part")
    assert o.path == "staged" and any("ix_child" in m and "fb.part_2" in m for m in msgs)
    _t("bad", "id int, v text")
    c.run_psql("CREATE INDEX ix_bad ON fb.bad (id); UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'fb.ix_bad'::regclass")
    msgs = grab(o6.logger)
    assert _load(gz_file, "bad.gz", _body(1, 3), "bad").path == "staged" and any("ix_bad (invalid" in m for m in msgs)
    # FE column rules and GENERATED ALWAYS in the file
    _t("fe", "id int, v text")
    monkeypatch.setattr(o6, "fwd_rules", lambda: [("ignore-column", "fb", "fe", "old", "", "")])
    msgs = grab(o6.logger)
    assert _load(gz_file, "fe.gz", "id,v,old\n1,a,x\n", "fe").path == "staged" and any("FE column rules" in m for m in msgs)
    monkeypatch.setattr(o6, "fwd_rules", lambda: [])
    _t("ga", "id bigint GENERATED ALWAYS AS IDENTITY, v text")
    msgs = grab(o6.logger)
    o = _load(gz_file, "ga.gz", "id,v\n1,a\n", "ga")
    assert (o.status, o.path) == ("SUCCESS", "staged"), o.error                                   # the staged path loads it (OVERRIDING SYSTEM VALUE)
    assert any("GENERATED ALWAYS" in m and "--fast-bulk skipped" in m for m in msgs)


@pytest.mark.integration
def test_not_null_identity_by_default_and_serial_defaults_do_not_block(gz_file, grab):
    _t("nn", "id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL, n serial, v text NOT NULL DEFAULT 'x', w text")
    msgs = grab(o6.logger)
    o = _load(gz_file, "a.gz", "id,v,w\n1,a,b\n2,c,d\n", "nn")
    assert (o.status, o.path, o.rows) == ("SUCCESS", "fast bulk", 2), o.error
    assert not any("--fast-bulk skipped" in m for m in msgs)


@pytest.mark.integration
def test_load_table_meta_knows_every_index_and_trigger_and_keeps_the_old_fields(pg_db):
    _t("m", "id int PRIMARY KEY, v text")
    c.run_psql("CREATE INDEX ix_a ON fb.m (v); CREATE INDEX ix_b ON fb.m (v) WHERE id > 1; CREATE INDEX ix_c ON fb.m ((lower(v)))")
    meta = load_table_meta("fb", "m")
    assert {i["name"] for i in meta["indexes"]} == {"m_pkey", "ix_a", "ix_b", "ix_c"}
    flags = {i["name"]: (i["unique"], i["partial"], i["expr"], i["valid"]) for i in meta["indexes"]}
    assert flags["m_pkey"] == (True, False, False, True) and flags["ix_b"][1] and flags["ix_c"][2]
    assert meta["keys"] and meta["keys"][0]["name"] == "m_pkey" and set(meta["columns"]) == {"id", "v"}      # existing fields still work
    assert load_table_meta("fb", "nosuch")["indexes"] == []


@pytest.mark.integration
def test_bare_non_empty_table_fast_path_counts_the_file_registers_a_full_copy_and_never_counts_the_target(gz_file, monkeypatch):
    _t("ne", "id int, v text")
    c.run_psql("INSERT INTO fb.ne SELECT g, 'old' FROM generate_series(1, 5) g")
    scripts = []
    real = o6.run_psql_script
    monkeypatch.setattr(o6, "run_psql_script", lambda script, **kw: (scripts.append(script), real(script, **kw))[1])
    o = _load(gz_file, "a.gz", _body(10, 14), "ne")
    assert (o.status, o.path, o.rows, o.inserted) == ("SUCCESS", "fast bulk", 4, 4), o.error
    assert _q("SELECT count(*) FROM fb.ne") == "9"
    assert _q("SELECT count(*) || ':' || max(seq) || ':' || bool_and(full_copy) || ':' || max(rows_inserted) || ':' || max(rows_updated) || ':' || "
              "max(rows_total) || ':' || bool_and(delta_table IS NULL) FROM sweep_ctl.delta_registry WHERE target_table = 'ne'") == "1:1:true:4:0:4:true"
    assert _q("SELECT rows_read || ':' || rows_inserted || ':' || coalesce(rows_before::text, 'NULL') FROM sweep_ctl.load_history WHERE target_table = 'ne'") == "4:4:NULL"
    assert _q(f"SELECT loaded_rows FROM {c.ORACLE_LOAD_LOG_TABLE} WHERE target_table = 'ne'") == "4"                # the rows of the file
    assert [(r["ledger_status"], r["ledger_rows"]) for r in ledger_check()] == [("LEDGER_BASELINE_UNKNOWN", "")]    # informational, not LEDGER_MISMATCH
    sql = scripts[-1]
    assert 'count(*) FROM "fb"."ne"' not in sql and "SELECT count(*) FROM fb" not in sql and ":n" in sql and "ROW_COUNT" in sql
    assert _q("SELECT count(*) FROM fb.ne") == "9"


@pytest.mark.integration
def test_baseline_is_zero_only_for_an_empty_table_and_the_ledger_matches(gz_file):
    _t("em", "id int, v text")
    assert _load(gz_file, "a.gz", _body(1, 4), "em").path == "fast bulk"
    assert _load(gz_file, "b.gz", _body(4, 9), "em").path == "fast bulk"                                    # part 2 into the now non-empty table
    assert _q("SELECT string_agg(coalesce(rows_before::text, 'NULL') || '/' || rows_inserted, ',' ORDER BY id) FROM sweep_ctl.load_history WHERE target_table = 'em'") == "0/3,NULL/5"
    assert _q("SELECT string_agg(seq || ':' || rows_inserted || ':' || full_copy, ',' ORDER BY seq) FROM sweep_ctl.delta_registry WHERE target_table = 'em'") == "1:3:true,2:5:true"
    assert [(r["ledger_status"], r["ledger_rows"], r["actual_rows"]) for r in ledger_check()] == [("LEDGER_MATCH", 8, 8)]
    c.run_psql("TRUNCATE fb.em")                                                                            # emptied by hand: a new baseline
    assert _load(gz_file, "c.gz", _body(20, 22), "em").path == "fast bulk"
    assert _q("SELECT string_agg(coalesce(rows_before::text, 'NULL'), ',' ORDER BY id) FROM sweep_ctl.load_history WHERE target_table = 'em'") == "0,NULL,0"
    assert [(r["ledger_status"], r["ledger_rows"], r["actual_rows"]) for r in ledger_check()] == [("LEDGER_MATCH", 2, 2)]


@pytest.mark.integration
@pytest.mark.parametrize("round_", range(3))
def test_two_part_files_loaded_by_two_workers_both_work_with_their_own_registry_and_ledger_rows(gz_file, round_):
    table = f"pp{round_}"
    _t(table, "id int, v text")
    out = {}

    def run(name, a, b, worker):
        out[name] = _load(gz_file, name + ".gz", _body(a, b), table, worker=worker)
    threads = [threading.Thread(target=run, args=("p1", 1, 30001, 1)), threading.Thread(target=run, args=("p2", 100001, 130001, 2))]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert [(o.status, o.path, o.rows) for o in out.values()] == [("SUCCESS", "fast bulk", 30000)] * 2, {k: v.error for k, v in out.items()}
    assert _q(f"SELECT count(*) FROM fb.{table}") == "60000"
    assert _q(f"SELECT string_agg(seq || ':' || rows_inserted || ':' || full_copy, ',' ORDER BY seq) FROM sweep_ctl.delta_registry WHERE target_table = '{table}'") == "1:30000:true,2:30000:true"
    assert _q(f"SELECT count(*) || ':' || count(rows_before) FROM sweep_ctl.load_history WHERE target_table = '{table}'") == "2:1"      # ONE baseline (0), two ledger rows
    assert _q(f"SELECT string_agg(rows_read || '/' || rows_inserted, ',') FROM sweep_ctl.load_history WHERE target_table = '{table}'") == "30000/30000,30000/30000"
    assert [(r["target_table"], r["ledger_status"], r["ledger_rows"], r["actual_rows"]) for r in ledger_check() if r["target_table"] == table] == [(table, "LEDGER_MATCH", 60000, 60000)]


@pytest.mark.integration
def test_a_bad_row_fails_only_its_file_and_leaves_table_registry_and_ledger_unchanged(gz_file):
    _t("bd", "id int NOT NULL, v text")
    assert _load(gz_file, "a.gz", _body(1, 4), "bd").path == "fast bulk"
    snap = lambda: (_q("SELECT count(*) FROM fb.bd"), _q("SELECT count(*) FROM sweep_ctl.delta_registry WHERE target_table = 'bd'"),
                    _q("SELECT count(*) FROM sweep_ctl.load_history WHERE target_table = 'bd' AND status = 'SUCCESS'"))
    before = snap()
    for bad in ("id,v\n10,a\nnot-int,b\n", "id,v\n11,a\n,b\n"):                                              # a bad value, a NULL in a NOT NULL column
        o = _load(gz_file, "bad.gz", bad, "bd")
        assert o.status == "FAILED" and o.error, bad
        assert snap() == before                                                                              # rolled back: no rows, no registry row, no ledger row
    assert _q("SELECT count(*) FROM sweep_ctl.load_history WHERE target_table = 'bd' AND status = 'FAILED'") == "2"
    o = _load(gz_file, "retry.gz", _body(10, 12), "bd")                                                      # a retry works
    assert (o.status, o.rows) == ("SUCCESS", 2) and snap() == ("5", "2", "2")
    assert _q("SELECT string_agg(seq::text, ',' ORDER BY seq) FROM sweep_ctl.delta_registry WHERE target_table = 'bd'") == "1,2"


@pytest.mark.integration
def test_partitioned_parent_counts_the_rows_copied_over_the_leaf_partitions(gz_file):
    _t("pt", "id int, v text) PARTITION BY RANGE (id")
    c.run_psql("CREATE TABLE fb.pt_1 PARTITION OF fb.pt FOR VALUES FROM (0) TO (10); CREATE TABLE fb.pt_2 PARTITION OF fb.pt FOR VALUES FROM (10) TO (1000)")
    c.run_psql("INSERT INTO fb.pt VALUES (1, 'x'), (500, 'y')")
    o = _load(gz_file, "a.gz", _body(2, 30), "pt")
    assert (o.status, o.path, o.rows) == ("SUCCESS", "fast bulk", 28), o.error                              # rows of the file, over both leaves
    assert _q("SELECT rows_inserted FROM sweep_ctl.delta_registry WHERE target_table = 'pt'") == "28"
    assert _q("SELECT rows_read || ':' || rows_inserted FROM sweep_ctl.load_history WHERE target_table = 'pt'") == "28:28"
    assert _q("SELECT count(*) FROM fb.pt_1") == "9" and _q("SELECT count(*) FROM fb.pt_2") == "21"


@pytest.mark.integration
def test_the_rows_copied_equal_the_transaction_local_insert_counters(gz_file):
    """The in-transaction ROW_COUNT is verified against pg_stat_xact_user_tables summed over the leaf partitions."""
    _t("sx", "id int, v text) PARTITION BY RANGE (id")
    c.run_psql("CREATE TABLE fb.sx_1 PARTITION OF fb.sx FOR VALUES FROM (0) TO (10); CREATE TABLE fb.sx_2 PARTITION OF fb.sx FOR VALUES FROM (10) TO (1000)")
    p = gz_file("a.gz", _body(1, 40))
    csv = p.parent / "a.csv"
    import gzip
    csv.write_bytes(gzip.decompress(p.read_bytes()))
    out = c.run_psql_script(f"""\\set ON_ERROR_STOP on
BEGIN;
\\copy fb.sx FROM '{csv}' WITH (FORMAT csv, HEADER true)
\\set n :ROW_COUNT
SELECT :n AS row_count, (SELECT sum(n_tup_ins) FROM pg_stat_xact_user_tables WHERE relid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = 'fb.sx'::regclass)) AS xact_ins;
COMMIT;
""")
    assert out.returncode == 0 and "39" in out.stdout.split("row_count")[-1].split("(1 row)")[0].replace(" ", "") and out.stdout.count("39") >= 2, out.stdout


# ---------------------------------------------------------------- promote: several full-copy entries are ONE copy
@pytest.fixture()
def parts_env(chain_env, monkeypatch):
    env = chain_env
    env.everywhere("CREATE SCHEMA app; CREATE TABLE app.parts (a int, b text)")
    for l in LAYERS:
        env.manifest(l, [{"schema": "app", "table": "parts", "promote_mode": "insert_only"}])
    return env


def _bronze_part(env, tmp_path, gz, name, a, b):
    p = tmp_path / name
    import gzip
    with gzip.open(p, "wt", newline="") as fh:
        fh.write("a,b\n" + "".join(f"{i},p{i}\n" for i in range(a, b)))
    from sweep_s3meta import head_object
    job = {"uri": str(p), "target_schema": "app", "target_table": "parts", "size_bytes": p.stat().st_size, "size_mb": 0.01, "filename": name, "file_format": "gzip"}
    head_object(str(p))
    with sl.use_layer("bronze"):
        o6.META_CACHE.clear()
        o = o6.load_single_file(job, worker_id=1, fast_bulk=True)
    assert (o.status, o.path) == ("SUCCESS", "fast bulk"), o.error


@pytest.mark.integration
def test_promote_copies_a_table_with_several_full_copy_entries_once_and_the_next_hop_still_works(parts_env, tmp_path, gz_file, monkeypatch):
    env = parts_env
    for i, (a, b) in enumerate(((1, 5), (5, 9), (9, 13)), 1):
        _bronze_part(env, tmp_path, gz_file, f"part{i}.csv.gz", a, b)
    assert psql(env.dbs["bronze"], "SELECT string_agg(seq || ':' || full_copy, ',' ORDER BY seq) FROM sweep_ctl.delta_registry") == "1:true,2:true,3:true"
    calls = []
    real = sp.apply_one
    monkeypatch.setattr(sp, "apply_one", lambda *a, **kw: (calls.append(a[3]["seq"]), real(*a, **kw))[1])
    results, code = env.promote("bronze", "silver")
    assert code == 0 and results[0]["status"] == "SUCCESS" and results[0]["deltas"] == 1, results           # ONE apply for three parts
    assert calls == [1] and results[0]["inserted"] == 12
    assert psql(env.dbs["silver"], "SELECT count(*) FROM app.parts") == "12"
    assert psql(env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.promote_log") == "1"
    assert psql(env.dbs["silver"], "SELECT string_agg(seq || ':' || rows_inserted, ',' ORDER BY seq) FROM sweep_ctl.delta_registry") == "1:12,2:0,3:0"      # every covered sequence is registered
    assert env.promote("bronze", "silver")[0][0]["status"] == "UP_TO_DATE"                                   # and nothing is pending any more
    # the next hop sees the same three sequence numbers in order (no gap)
    calls.clear()
    results, code = env.promote("silver", "gold_backup")
    assert code == 0 and results[0]["status"] == "SUCCESS" and calls == [1] and psql(env.dbs["gold_backup"], "SELECT count(*) FROM app.parts") == "12"
    results, code = env.promote("gold_backup", "gold")
    assert code == 0 and psql(env.dbs["gold"], "SELECT count(*) FROM app.parts") == "12"
    # a later part is a new full copy: copied once, appended
    _bronze_part(env, tmp_path, gz_file, "part4.csv.gz", 13, 15)
    calls.clear()
    results, code = env.promote("bronze", "silver")
    assert code == 0 and calls == [4] and psql(env.dbs["silver"], "SELECT count(*) FROM app.parts") == "14"


def test_collapse_keeps_captured_deltas_and_a_lone_full_copy_untouched():
    mk = lambda seq, full: {"seq": seq, "full": full, "columns": ["a"], "delta_table": None if full else "d", "origin_layer": "bronze"}
    out = sp.collapse_full_copies([mk(1, True), mk(2, True), mk(3, False), mk(4, True), mk(5, False), mk(6, False)])
    assert [(d["seq"], d["full"], d.get("covers")) for d in out] == [(1, True, [1, 2]), (3, False, None), (4, True, [4]), (5, False, None), (6, False, None)]
    assert sp.collapse_full_copies([]) == []
    assert "covered by the full copy of delta 1" in sp.covered_registry_sql("s", "t", "r", "u", ["a"], "bronze", 1, [1, 2, 3])
    assert sp.covered_registry_sql("s", "t", "r", "u", ["a"], "bronze", 1, [1]) == ""
