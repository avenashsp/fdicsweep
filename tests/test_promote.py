"""Delta capture in bronze and the promote chain bronze -> silver -> gold_backup -> gold (needs PostgreSQL; four temp databases)."""
import subprocess
import time

import pytest

import sweep_common as c
import sweep_layers as sl
import sweep_promote as sp
from chain_support import DELTA1, LAYERS, day1, psql, rows
from conftest import write_layers_file

pytestmark = pytest.mark.integration


def test_bronze_load_captures_only_the_applied_delta_and_registers_it(chain_env):
    env = chain_env
    day1(env)
    env.bronze_load(DELTA1)
    reg = psql(env.dbs["bronze"], "SELECT seq || '|' || rows_inserted || '|' || rows_updated || '|' || full_copy || '|' || columns || '|' || delta_table "
                                  "FROM sweep_ctl.delta_registry")
    assert reg == '1|1|1|false|id,name,qty|"sweep_delta"."app__customer"'
    ops = psql(env.dbs["bronze"], "SELECT string_agg(_op || id, ',' ORDER BY id) FROM sweep_delta.app__customer WHERE _seq = 1")
    assert ops == "U2,I4"                                                         # the unchanged row 1 is not in the delta
    assert psql(env.dbs["bronze"], "SELECT relpersistence FROM pg_class WHERE oid = 'sweep_delta.app__customer'::regclass") == "u"   # unlogged
    env.bronze_load("id,name,qty\n4,d,41\n")
    assert psql(env.dbs["bronze"], "SELECT string_agg(seq::text, ',' ORDER BY seq) FROM sweep_ctl.delta_registry") == "1,2"   # strictly increasing


def test_empty_table_load_registers_a_full_copy_without_capturing_rows(chain_env):
    env = chain_env
    env.everywhere("CREATE SCHEMA app; CREATE TABLE app.customer (id int PRIMARY KEY, name text, qty int, modifieddate timestamp);")
    env.bronze_load(DELTA1)
    assert psql(env.dbs["bronze"], "SELECT full_copy || '|' || coalesce(delta_table, 'none') || '|' || rows_inserted FROM sweep_ctl.delta_registry") == "true|none|3"
    assert psql(env.dbs["bronze"], "SELECT count(*) FROM sweep_delta.app__customer") == "0"          # no second copy of the data


def test_promote_chain_applies_the_delta_in_every_layer_and_is_idempotent(chain_env):
    env = chain_env
    day1(env)
    env.bronze_load(DELTA1)
    for a, b in zip(LAYERS, LAYERS[1:]):
        results, code = env.promote(a, b)
        assert code == 0 and [(r["status"], r["inserted"], r["updated"]) for r in results] == [("SUCCESS", 1, 1)]
        assert rows(env, b) == "1:a:10,2:b2:20,3:c:30,4:d:40"
    for l in LAYERS[1:]:
        assert psql(env.dbs[l], "SELECT seq || '|' || origin_layer FROM sweep_ctl.delta_registry") == "1|bronze"
    assert psql(env.dbs["silver"], "SELECT count(*) FROM sweep_delta.app__customer WHERE _seq = 1") == "2"        # silver recorded its own delta
    assert psql(env.dbs["gold"], "SELECT delta_table IS NULL FROM sweep_ctl.delta_registry") == "t"               # gold has no next hop
    assert psql(env.dbs["gold"], "SELECT to_regclass('sweep_delta.app__customer') IS NULL") == "t"
    log = psql(env.dbs["silver"], "SELECT source_layer || '>' || target_layer || ' ' || inserted || '/' || updated || '/' || unchanged || ' ' || status "
                                  "FROM sweep_ctl.promote_log")
    assert log == "bronze>silver 1/1/0 SUCCESS"
    # a second run changes nothing and logs nothing new
    results, code = env.promote("bronze", "silver")
    assert code == 0 and results[0]["status"] == "UP_TO_DATE"
    assert psql(env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.promote_log") == "1"


def test_deltas_are_applied_in_strict_sequence_order(chain_env):
    env = chain_env
    day1(env)
    env.bronze_load("id,name,qty\n2,first,20\n")
    env.bronze_load("id,name,qty\n2,second,20\n")
    results, code = env.promote("bronze", "silver")
    assert code == 0 and results[0]["deltas"] == 2 and rows(env, "silver") == "1:a:10,2:second:20,3:c:30"
    assert psql(env.dbs["silver"], "SELECT string_agg(seq::text, ',' ORDER BY delta_id) FROM sweep_ctl.delta_registry") == "1,2"
    assert psql(env.dbs["silver"], "SELECT string_agg(seq::text, ',' ORDER BY id) FROM sweep_ctl.promote_log") == "1,2"
    # a delta that skips a number cannot be applied (N+1 never before N)
    psql(env.dbs["bronze"], "INSERT INTO sweep_ctl.delta_registry (run_id, seq, target_schema, target_table, rows_inserted, columns, delta_table) "
                            "VALUES ('x', 4, 'app', 'customer', 1, 'id,name,qty', 'sweep_delta.app__customer')")
    results, code = env.promote("bronze", "silver")
    assert code == 1 and "sequence gap: expected delta 3" in results[0]["message"]


def test_a_failure_stops_that_tables_chain_and_gold_is_only_reached_through_gold_backup(chain_env):
    env = chain_env
    day1(env)
    psql(env.dbs["silver"], "ALTER TABLE app.customer DROP COLUMN qty")            # schema drift in silver
    env.bronze_load(DELTA1)
    results, code = env.promote("bronze", "silver")
    assert code == 1 and results[0]["status"] == "FAILED" and "schema drift: column(s) qty" in results[0]["message"]
    assert rows(env, "silver", cols="id, name") == "1:a,2:b,3:c"                    # nothing applied
    assert psql(env.dbs["silver"], "SELECT status FROM sweep_ctl.promote_log") == "FAILED"
    for a, b in (("silver", "gold_backup"), ("gold_backup", "gold")):               # the chain stops: nothing to take
        results, code = env.promote(a, b)
        assert code == 0 and results[0]["status"] == "UP_TO_DATE"
    assert rows(env, "gold") == "1:a:10,2:b:20,3:c:30"
    matrix, _ = sp.promote_status()
    assert matrix == [{"table": "app.customer", "seq": 1, "bronze": "SUCCESS", "silver": "FAILED", "gold_backup": "BLOCKED", "gold": "BLOCKED"}]
    # repair silver, re-run: resumes and the chain continues
    psql(env.dbs["silver"], "ALTER TABLE app.customer ADD COLUMN qty int; UPDATE app.customer SET qty = id * 10")
    assert env.promote("bronze", "silver")[1] == 0
    assert env.promote("silver", "gold_backup")[1] == 0 and env.promote("gold_backup", "gold")[1] == 0
    assert rows(env, "gold") == "1:a:10,2:b2:20,3:c:30,4:d:40"
    assert [r["silver"] for r in sp.promote_status()[0]] == ["SUCCESS"]


def test_promote_refuses_to_skip_a_layer(chain_env):
    env = chain_env
    day1(env)
    with pytest.raises(sl.LayerError, match="ONE hop"):
        sp.promote("bronze", "gold")
    with pytest.raises(sl.LayerError, match="first layer"):
        sp.promote("silver", "bronze")


def test_dry_run_reports_counts_and_writes_nothing(chain_env):
    env = chain_env
    day1(env)
    env.bronze_load(DELTA1)
    results, code = env.promote("bronze", "silver", dry_run=True)
    assert code == 0 and (results[0]["status"], results[0]["inserted"], results[0]["updated"], results[0]["unchanged"]) == ("DRY_RUN", 1, 1, 0)
    assert rows(env, "silver") == "1:a:10,2:b:20,3:c:30"
    assert psql(env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.delta_registry") == "0"
    assert psql(env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.promote_log") == "0"


def test_keep_target_inserts_new_keys_only(chain_env):
    env = chain_env
    day1(env)
    env.manifest("silver", [{"schema": "app", "table": "customer", "conflict_policy": "keep_target"}])
    env.bronze_load(DELTA1)
    results, code = env.promote("bronze", "silver")
    assert code == 0 and (results[0]["inserted"], results[0]["updated"]) == (1, 0)
    assert rows(env, "silver") == "1:a:10,2:b:20,3:c:30,4:d:40"               # id 2 kept its target value
    assert psql(env.dbs["silver"], "SELECT rows_inserted || '/' || rows_updated FROM sweep_ctl.delta_registry") == "1/0"


def test_drift_check_missing_and_required_extra_columns_fail_that_table_only(chain_env):
    env = chain_env
    day1(env, "CREATE TABLE app.other (id int PRIMARY KEY, v text); INSERT INTO app.other VALUES (1,'x');")
    for l in LAYERS:
        env.manifest(l, [{"schema": "app", "table": "customer"}, {"schema": "app", "table": "other"}])
    psql(env.dbs["silver"], "ALTER TABLE app.customer ADD COLUMN must_have text NOT NULL DEFAULT 'x'; ALTER TABLE app.customer ALTER COLUMN must_have DROP DEFAULT")
    env.bronze_load(DELTA1)
    env.bronze_load("id,v\n1,y\n", table="other")
    results, code = env.promote("bronze", "silver")
    by = {r["table"]: r for r in results}
    assert code == 1 and by["app.customer"]["status"] == "FAILED" and "must_have" in by["app.customer"]["message"]
    assert by["app.other"]["status"] == "SUCCESS" and psql(env.dbs["silver"], "SELECT v FROM app.other") == "y"      # other tables continue
    # an extra NOT NULL column WITH a default is fine
    psql(env.dbs["silver"], "ALTER TABLE app.customer ALTER COLUMN must_have SET DEFAULT 'x'")
    assert env.promote("bronze", "silver")[1] == 0 and rows(env, "silver") == "1:a:10,2:b2:20,3:c:30,4:d:40"


def test_sequences_advance_but_never_move_backwards(chain_env):
    env = chain_env
    ddl = ("CREATE SCHEMA app; CREATE TABLE app.ident (id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text);"
           "INSERT INTO app.ident (v) VALUES ('seed');")
    env.everywhere(ddl)
    for l in LAYERS:
        env.manifest(l, [{"schema": "app", "table": "ident"}])
    psql(env.dbs["gold"], "SELECT setval(pg_get_serial_sequence('app.ident', 'id'), 500)")        # the application inserts a lot in gold
    psql(env.dbs["silver"], "SELECT setval(pg_get_serial_sequence('app.ident', 'id'), 1, false)")
    env.bronze_load("id,v\n1,seed\n7,seven\n9,nine\n", table="ident")
    assert env.promote("bronze", "silver")[1] == 0
    assert psql(env.dbs["silver"], "SELECT last_value FROM app.ident_id_seq") == "9"                # advanced to max(id)
    assert env.promote("silver", "gold_backup")[1] == 0 and env.promote("gold_backup", "gold")[1] == 0
    assert psql(env.dbs["gold"], "SELECT last_value FROM app.ident_id_seq") == "500"                # never moved backwards
    assert psql(env.dbs["gold"], "WITH x AS (INSERT INTO app.ident (v) VALUES ('app') RETURNING id) SELECT id FROM x") == "501"


def test_replace_is_for_silver_and_refused_in_gold(chain_env):
    env = chain_env
    ddl = "CREATE SCHEMA vs; CREATE TABLE vs.snap (a int, b text);"
    env.everywhere(ddl)
    psql(env.dbs["silver"], "INSERT INTO vs.snap VALUES (9, 'old'), (8, 'older')")
    psql(env.dbs["gold_backup"], "INSERT INTO vs.snap VALUES (9, 'old')")
    for l in LAYERS:
        env.manifest(l, [{"track": "vsam", "schema": "vs", "table": "snap", "promote_mode": "replace"}])
    env.bronze_load("a,b\n1,x\n2,y\n", table="snap", schema="vs")                     # empty table -> registered as a full copy
    results, code = env.promote("bronze", "silver")
    assert code == 0 and psql(env.dbs["silver"], "SELECT string_agg(a || ':' || b, ',' ORDER BY a) FROM vs.snap") == "1:x,2:y"     # old rows gone, atomically
    results, code = env.promote("silver", "gold_backup")
    assert code == 1 and "replace is refused on layer gold_backup" in results[0]["message"]
    assert psql(env.dbs["gold_backup"], "SELECT count(*) FROM vs.snap") == "1"      # nothing was truncated


def test_model_outputs_upsert_changed_rows_only_and_keyless_outputs_are_skipped(chain_env):
    env = chain_env
    ddl = ("CREATE SCHEMA models; CREATE TABLE models.summary (id int PRIMARY KEY, total int); CREATE TABLE models.keyless (a int, b int);")
    env.everywhere(ddl)
    for l in LAYERS[1:]:
        env.manifest(l, [{"track": "model_output", "schema": "models", "table": "summary"},
                         {"track": "model_output", "schema": "models", "table": "keyless"}])
    env.manifest("bronze", [])
    psql(env.dbs["silver"], "INSERT INTO models.summary VALUES (1, 100), (2, 200), (3, 300); INSERT INTO models.keyless VALUES (1, 1)")
    psql(env.dbs["gold_backup"], "INSERT INTO models.summary VALUES (1, 100), (2, 999), (5, 500)")       # 5 exists only in the target
    results, code = env.promote("silver", "gold_backup")
    by = {r["table"]: r for r in results}
    assert code == 0 and by["models.summary"]["status"] == "SUCCESS" and by["models.keyless"]["status"] == "SKIPPED"
    assert (by["models.summary"]["inserted"], by["models.summary"]["updated"], by["models.summary"]["unchanged"]) == (1, 1, 1)
    assert rows(env, "gold_backup", "models.summary", "id, total") == "1:100,2:200,3:300,5:500"     # nothing deleted
    psql(env.dbs["gold"], "INSERT INTO models.summary VALUES (1, 100), (2, 999), (3, 3), (5, 500)")  # gold is in step; the application changed row 3
    results, code = env.promote("gold_backup", "gold")
    by = {r["table"]: r for r in results}
    assert code == 0 and by["models.summary"]["status"] == "SUCCESS"
    assert rows(env, "gold", "models.summary", "id, total") == "1:100,2:200,3:300,5:500"             # only the changed rows were applied (via the delta)


def test_stream_transport_gives_the_same_result(chain_env):
    env = chain_env
    day1(env)
    env.bronze_load(DELTA1)
    results, code = env.promote("bronze", "silver", transport="stream")
    assert code == 0 and (results[0]["inserted"], results[0]["updated"]) == (1, 1)
    assert rows(env, "silver") == "1:a:10,2:b2:20,3:c:30,4:d:40"


def test_gold_lock_timeout_is_retried_then_fails_cleanly(chain_env, monkeypatch):
    env = chain_env
    day1(env)
    env.bronze_load(DELTA1)
    assert env.promote("bronze", "silver")[1] == 0 and env.promote("silver", "gold_backup")[1] == 0
    monkeypatch.setattr(sp, "LOCK_RETRIES", 2)
    holder = subprocess.Popen([c.PSQL_BIN, "-X", "-w", "-h", c.PGHOST, "-p", str(c.PGPORT), "-U", c.PGUSER, "-d", env.dbs["gold"], "-c",
                               "BEGIN; LOCK TABLE app.customer IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(6); COMMIT;"],
                              env=c.psql_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5)
    t0 = time.time()
    results, code = env.promote("gold_backup", "gold")
    assert code == 1 and "lock timeout" in results[0]["message"].lower() and time.time() - t0 < 5.5     # short lock_timeout, not a hang
    assert rows(env, "gold") == "1:a:10,2:b:20,3:c:30"                                                  # gold untouched
    holder.wait()
    assert env.promote("gold_backup", "gold")[1] == 0 and rows(env, "gold") == "1:a:10,2:b2:20,3:c:30,4:d:40"


def test_purge_removes_deltas_only_after_every_layer_applied_them(chain_env):
    env = chain_env
    day1(env)
    env.bronze_load(DELTA1)
    env.promote("bronze", "silver")
    assert sp.purge_deltas(retention_days=0) == []                                # gold_backup / gold have not applied it yet
    env.promote("silver", "gold_backup")
    env.promote("gold_backup", "gold")
    assert sp.purge_deltas(retention_days=14) == []                               # retention not over
    purged = sp.purge_deltas(retention_days=0)
    assert {p[0] for p in purged} == {"bronze", "silver", "gold_backup"}
    assert psql(env.dbs["bronze"], "SELECT count(*) FROM sweep_delta.app__customer") == "0"
    assert psql(env.dbs["bronze"], "SELECT purged_at IS NOT NULL FROM sweep_ctl.delta_registry") == "t"
    assert rows(env, "gold") == "1:a:10,2:b2:20,3:c:30,4:d:40"                    # data never touched by a purge


def test_wrong_database_for_a_layer_is_refused_by_promote(chain_env, tmp_path, monkeypatch):
    env = chain_env
    day1(env)
    f = write_layers_file(tmp_path / "bad.json", {**env.dbs, "silver": "no_such_db"})
    monkeypatch.setattr(sl, "LAYERS_FILE", f)
    with pytest.raises((SystemExit, RuntimeError, sl.LayerError)):
        sp.promote("bronze", "silver")
