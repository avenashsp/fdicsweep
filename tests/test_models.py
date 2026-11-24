"""Data-model runner (part K): header, order, preconditions, run contract, delta mode, output checks, promote gate, chain."""
import json
import re
import textwrap
import time
from pathlib import Path

import pytest

import sweep_fwd_ddl as fd
import sweep_layers as sl
import sweep_models as sm
import sweep_promote as sp
from chain_support import DELTA1, day1, psql

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------- header, order (no database)
def _script(tmp_path, name, header, body="pass\n"):
    d = tmp_path / "data_models"
    d.mkdir(exist_ok=True)
    (d / name).write_text(f"SWEEP_MODEL = {header!r}\n{body}" if header is not None else body)
    return d / name


def test_the_header_is_read_with_ast_and_the_script_is_never_executed(tmp_path):
    p = _script(tmp_path, "10_a.py", {"name": "a", "inputs": ["s.t"], "outputs": ["bcs.o"], "delta_strategy": "update_in_place", "update_key": ["k"]},
                body="open('executed.txt', 'w').write('x')\nraise SystemExit('must not run')\n")
    m = sm.discover(tmp_path / "data_models")[0]
    assert (m.name, m.inputs, m.outputs, m.delta_strategy, m.update_key, m.prefix, m.source) == ("a", ["s.t"], ["bcs.o"], "update_in_place", ["k"], 10, "header")
    assert not (tmp_path / "executed.txt").exists() and p.exists()


def test_an_expression_in_the_header_is_refused_and_a_bad_value_is_named(tmp_path):
    (tmp_path / "data_models").mkdir()
    (tmp_path / "data_models" / "10_a.py").write_text("SWEEP_MODEL = {'name': 'a', 'inputs': [x for x in 'ab']}\n")
    with pytest.raises(sm.ModelError, match="literal dict"):
        sm.discover(tmp_path / "data_models")
    _script(tmp_path, "10_a.py", {"name": "a", "delta_strategy": "replace"})
    with pytest.raises(sm.ModelError, match="delta_strategy must be one of insert_missing, update_in_place"):
        sm.discover(tmp_path / "data_models")
    _script(tmp_path, "10_a.py", {"name": "a", "inputs": ["no_schema"]})
    with pytest.raises(sm.ModelError, match="needs schema.table"):
        sm.discover(tmp_path / "data_models")


def test_a_script_without_a_header_falls_back_to_the_static_scan_with_a_warning(tmp_path):
    _script(tmp_path, "20_old.py", None, body='SQL = "INSERT INTO bcs.res SELECT * FROM bss00098.src JOIN bcs.other o ON true"\n')
    m = sm.discover(tmp_path / "data_models")[0]
    assert m.source == "scan" and m.inputs == ["bcs.other", "bss00098.src"] and m.outputs == ["bcs.res"]
    assert any("no SWEEP_MODEL header" in w for w in m.warnings)


def test_models_json_overrides_the_header(tmp_path):
    _script(tmp_path, "10_a.py", {"name": "a", "inputs": ["s.t"], "outputs": ["bcs.o"]})
    (tmp_path / "data_models" / "models.json").write_text(json.dumps({"models": {"a": {"depends_on": [], "timeout": 5, "outputs": ["bcs.o2"]}}}))
    m = sm.discover(tmp_path / "data_models")[0]
    assert m.outputs == ["bcs.o2"] and m.timeout == 5 and m.source == "models.json"
    (tmp_path / "data_models" / "models.json").write_text(json.dumps({"models": {"nobody": {}}}))
    with pytest.raises(sm.ModelError, match="no script"):
        sm.discover(tmp_path / "data_models")


def test_order_follows_depends_on_then_the_numeric_prefix_and_cycles_are_found(tmp_path):
    _script(tmp_path, "30_c.py", {"name": "c", "depends_on": ["a"]})
    _script(tmp_path, "20_b.py", {"name": "b"})
    _script(tmp_path, "10_a.py", {"name": "a", "depends_on": ["b"]})
    assert [m.name for m in sm.order_models(sm.discover(tmp_path / "data_models"))] == ["b", "a", "c"]          # a needs b although 10 < 20
    _script(tmp_path, "20_b.py", {"name": "b", "depends_on": ["c"]})
    with pytest.raises(sm.ModelError, match="dependency cycle between data models: .*->.*->"):
        sm.order_models(sm.discover(tmp_path / "data_models"))
    _script(tmp_path, "20_b.py", {"name": "b", "depends_on": ["ghost"]})
    with pytest.raises(sm.ModelError, match="depends_on 'ghost'"):
        sm.order_models(sm.discover(tmp_path / "data_models"))


def test_with_deps_adds_the_models_a_selection_needs(tmp_path):
    _script(tmp_path, "10_a.py", {"name": "a"})
    _script(tmp_path, "20_b.py", {"name": "b", "depends_on": ["a"]})
    _script(tmp_path, "30_c.py", {"name": "c"})
    ms = sm.order_models(sm.discover(tmp_path / "data_models"))
    assert [m.name for m in sm.select_models(ms, ["b"])] == ["b"]
    assert [m.name for m in sm.select_models(ms, ["b"], with_deps=True)] == ["a", "b"]
    with pytest.raises(sm.ModelError, match="unknown model"):
        sm.select_models(ms, ["zzz"])


# ---------------------------------------------------------------- hygiene (A1, A2, A6)
def test_no_old_schema_and_no_backup_output_names_in_the_models_folder():
    for path in (ROOT / "data_models").rglob("*"):
        if path.is_file() and path.suffix in (".py", ".md", ".csv", ".json"):
            text = path.read_text(encoding="utf-8", errors="replace")
            assert "bss00099" not in text, f"{path}: the source schema is bss00098"
            assert not re.search(r"\b\w+\.\w+_backup\b", text), f"{path}: an output / target name ends in _backup (a testing table)"
    for m in sm.discover(ROOT / "data_models"):
        assert not [o for o in m.outputs if o.endswith("_backup")], f"{m.name}: output {m.outputs} ends in _backup"
        assert not [i for i in m.inputs if "bss00099" in i]


def test_the_shipped_brokerglpair_header_declares_what_the_decisions_say():
    m = {x.name: x for x in sm.discover(ROOT / "data_models")}["brokerglpair"]
    assert m.outputs == ["bcs.brokerglpair"] and m.delta_strategy == "update_in_place"
    assert m.update_key == ["hogan_product_code", "total_number"] and "bss00098.stg_genledgeinterface_v" in m.inputs


# ---------------------------------------------------------------- runner against the layer databases
BODY = textwrap.dedent('''
    import json, os, subprocess, sys, time
    def sql(q):
        r = subprocess.run(["psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", q], capture_output=True, text=True)
        if r.returncode:
            sys.exit(r.stderr.strip())
        return r.stdout.strip()
    {work}
    env = {{k: v for k, v in os.environ.items() if k.startswith(("PG", "SWEEP_"))}}
    print("SWEEP_MODEL_RESULT " + json.dumps({{"status": "SUCCESS", "env": env, "db": sql("select current_database()"), "argv": sys.argv[1:]}}))
''')


@pytest.fixture()
def mdl(chain_env, tmp_path, monkeypatch):
    d = tmp_path / "data_models"
    d.mkdir()
    monkeypatch.setenv("SWEEP_MODELS_DIR", str(d))
    monkeypatch.setattr(fd, "FWD_DIR", tmp_path / "fwd_ddl")
    monkeypatch.setenv("SWEEP_BRONZE_SECRET", "must-not-leak")
    day1(chain_env)
    chain_env.everywhere("CREATE TABLE app.m1_out (id int PRIMARY KEY, n int)")

    class M:
        env = chain_env
        dir = d

        @staticmethod
        def write(name, header, work="pass"):
            (d / name).write_text(f"SWEEP_MODEL = {header!r}\n" + BODY.format(work=work))

        @staticmethod
        def runs(model=None):
            w = f"WHERE model = '{model}'" if model else ""
            return psql(chain_env.dbs["silver"], f"SELECT coalesce(string_agg(model || ':' || status || ':' || mode, ' ' ORDER BY id), '') FROM sweep_ctl.model_run_log {w}")

        @staticmethod
        def last_result(model):
            return json.loads(psql(chain_env.dbs["silver"], f"SELECT result FROM sweep_ctl.model_run_log WHERE model = '{model}' AND status = 'SUCCESS' ORDER BY id DESC LIMIT 1"))

    M.hdr = lambda name="m1", **kw: {"name": name, "inputs": ["app.customer"], "outputs": ["app.m1_out"], "delta_strategy": "update_in_place", **kw}
    return M


INSERT_ONE = 'sql("INSERT INTO app.m1_out VALUES (1, 1) ON CONFLICT DO NOTHING")'


def test_a_model_runs_with_only_the_silver_environment_and_the_contract_variables(mdl, tmp_path):
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    assert sm.run_models_cmd() == 0
    r = mdl.last_result("m1")
    env = r["env"]
    assert r["db"] == mdl.env.dbs["silver"] and env["PGDATABASE"] == mdl.env.dbs["silver"]
    assert env["SWEEP_LAYER"] == "silver" and env["SWEEP_MODEL"] == "m1" and env["SWEEP_MODE"] == "full" and env["SWEEP_RUN_ID"]
    assert env["SWEEP_MODEL_WORKDIR"].endswith(f"data_models/work/m1/{env['SWEEP_RUN_ID']}") or env["SWEEP_MODEL_WORKDIR"].endswith(f"m1/{env['SWEEP_RUN_ID']}")
    assert env["SWEEP_MODEL_CONFIG"].endswith("config/m1/") and (tmp_path / "data_models" / "config" / "m1").is_dir()
    assert json.loads(env["SWEEP_INPUT_DELTAS"]) == {"app.customer": {"full": True}}
    assert "SWEEP_BRONZE_SECRET" not in env and "SWEEP_LAYERS_FILE" not in env                      # nothing else of the parent leaks
    assert r["argv"] == ["--execute", "--mode", "full"]
    assert psql(mdl.env.dbs["silver"], "SELECT count(*) FROM app.m1_out") == "1" and psql(mdl.env.dbs["bronze"], "SELECT count(*) FROM app.m1_out") == "0"
    assert list((tmp_path / "data_models" / "logs").glob(f"{mdl.env.dbs['silver']}_m1_*.log"))


def test_a_model_is_not_run_again_until_an_input_receives_a_delta(mdl):
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    assert sm.run_models_cmd() == 0 and mdl.runs() == "m1:SUCCESS:full"
    assert sm.run_models_cmd() == 0 and mdl.runs() == "m1:SUCCESS:full"                              # up to date: not started
    mdl.env.bronze_load(DELTA1)
    assert sm.run_models_cmd(skip_promote_check=False) == 1                                          # (b) the delta is not in silver yet
    assert mdl.env.promote("bronze", "silver")[1] == 0
    assert sm.run_models_cmd() == 0 and mdl.runs() == "m1:SUCCESS:full m1:SUCCESS:delta"
    spec = json.loads(mdl.last_result("m1")["env"]["SWEEP_INPUT_DELTAS"])["app.customer"]
    assert spec == {"full": False, "delta_table": "sweep_delta.app__customer", "seq_from": 0, "seq_to": 1}
    assert sm.run_models_cmd() == 0 and mdl.runs() == "m1:SUCCESS:full m1:SUCCESS:delta"             # and quiet again
    assert sm.run_models_cmd(force=True) == 0 and mdl.runs().endswith("m1:SUCCESS:delta")           # --force
    assert sm.run_models_cmd(full=True) == 0 and mdl.runs().endswith("m1:SUCCESS:full")             # --full


def test_a_failed_run_does_not_advance_the_input_sequences(mdl):
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    assert sm.run_models_cmd() == 0
    mdl.env.bronze_load(DELTA1)
    assert mdl.env.promote("bronze", "silver")[1] == 0
    mdl.write("10_m1.py", mdl.hdr(), "sys.exit('boom')")
    assert sm.run_models_cmd() == 1
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    assert sm.run_models_cmd() == 0
    spec = json.loads(mdl.last_result("m1")["env"]["SWEEP_INPUT_DELTAS"])["app.customer"]
    assert (spec["seq_from"], spec["seq_to"]) == (0, 1)                                              # the failed attempt consumed nothing
    assert "m1:FAILED:delta" in mdl.runs()
    assert "exit code 1" in psql(mdl.env.dbs["silver"], "SELECT message FROM sweep_ctl.model_run_log WHERE status = 'FAILED'")


def test_a_failed_model_blocks_only_its_dependents(mdl):
    mdl.write("10_m1.py", mdl.hdr(), "sys.exit('boom')")
    mdl.write("20_m2.py", mdl.hdr("m2", depends_on=["m1"], outputs=["app.m1_out"]), INSERT_ONE)
    mdl.env.everywhere("CREATE TABLE app.m3_out (id int PRIMARY KEY)")
    mdl.write("30_m3.py", mdl.hdr("m3", outputs=["app.m3_out"]), 'sql("INSERT INTO app.m3_out VALUES (1)")')
    assert sm.run_models_cmd() == 1
    assert set(mdl.runs().split()) == {"m1:FAILED:full", "m2:SKIPPED:full", "m3:SUCCESS:full"}
    assert "dependency m1 failed" in psql(mdl.env.dbs["silver"], "SELECT message FROM sweep_ctl.model_run_log WHERE model = 'm2'")
    assert psql(mdl.env.dbs["silver"], "SELECT count(*) FROM app.m3_out") == "1"


def test_independent_models_run_in_parallel_with_workers(mdl):
    for i in (1, 2, 3):
        mdl.env.everywhere(f"CREATE TABLE app.p{i}_out (id int PRIMARY KEY)")
        mdl.write(f"{i}0_p{i}.py", mdl.hdr(f"p{i}", outputs=[f"app.p{i}_out"]), f'time.sleep(1.5); sql("INSERT INTO app.p{i}_out VALUES (1)")')
    t0 = time.time()
    assert sm.run_models_cmd(workers=3) == 0
    assert time.time() - t0 < 4.2                                                                    # three 1.5 s models, not 4.5 s
    assert len(mdl.runs().split()) == 3


def test_preconditions_pending_fe_files_and_missing_tables_refuse_the_whole_run(mdl, tmp_path, capsys):
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    (tmp_path / "fwd_ddl").mkdir()
    (tmp_path / "fwd_ddl" / "V3__x.sql").write_text("ALTER TABLE app.customer ADD COLUMN x int;\n")
    assert sm.run_models_cmd() == 1 and mdl.runs() == ""                                              # (a)
    (tmp_path / "fwd_ddl" / "V3__x.sql").unlink()
    psql(mdl.env.dbs["silver"], "DROP TABLE app.m1_out")
    assert sm.run_models_cmd() == 1 and mdl.runs() == ""                                              # (d)
    psql(mdl.env.dbs["silver"], "CREATE TABLE app.m1_out (id int PRIMARY KEY, n int)")
    mdl.write("10_m1.py", mdl.hdr(inputs=["app.nowhere"]), INSERT_ONE)
    assert sm.run_models_cmd() == 1 and mdl.runs() == ""                                              # (c)


def test_a_pending_promote_refuses_the_run_unless_skip_promote_check(mdl):
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    mdl.env.bronze_load(DELTA1)
    with sl.use_layer("silver"):
        problems = sm.check_preconditions(sm.discover())
        assert problems and "PENDING bronze -> silver delta(s) 1..1" in problems[0]
        assert sm.check_preconditions(sm.discover(), skip_promote_check=True) == []
    assert sm.run_models_cmd(skip_promote_check=True) == 0


def test_dry_run_checks_and_plans_but_starts_nothing(mdl, capsys):
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    assert sm.run_models_cmd(dry_run=True) == 0 and mdl.runs() == ""
    assert psql(mdl.env.dbs["silver"], "SELECT count(*) FROM app.m1_out") == "0"


def test_an_output_with_the_name_of_a_loaded_table_fails_that_model_and_writes_nothing(mdl):
    mdl.write("10_m1.py", mdl.hdr(outputs=["app.customer"]), 'sql("DELETE FROM app.customer")')
    assert sm.run_models_cmd() == 1
    assert "also tables the loaders fill" in psql(mdl.env.dbs["silver"], "SELECT message FROM sweep_ctl.model_run_log WHERE model = 'm1'")
    assert psql(mdl.env.dbs["silver"], "SELECT count(*) FROM app.customer") == "3"                    # the script never started


def test_timeout_missing_result_line_and_a_bad_exit_code_are_failures(mdl):
    mdl.write("10_m1.py", mdl.hdr(timeout=1), "time.sleep(30)")
    t0 = time.time()
    assert sm.run_models_cmd() == 1 and time.time() - t0 < 15
    assert "timeout: killed after 1s" in psql(mdl.env.dbs["silver"], "SELECT message FROM sweep_ctl.model_run_log WHERE model = 'm1'")
    (mdl.dir / "10_m1.py").write_text(f"SWEEP_MODEL = {mdl.hdr()!r}\nprint('done without a result line')\n")
    assert sm.run_models_cmd() == 1
    assert "printed no `SWEEP_MODEL_RESULT" in psql(mdl.env.dbs["silver"], "SELECT message FROM sweep_ctl.model_run_log ORDER BY id DESC LIMIT 1")


def test_an_output_without_primary_key_needs_insert_only_in_the_manifest(mdl):
    mdl.env.everywhere("CREATE TABLE app.keyless (a int)")
    mdl.write("10_m1.py", mdl.hdr(outputs=["app.keyless"]), 'sql("INSERT INTO app.keyless VALUES (1)")')
    assert sm.run_models_cmd() == 1
    assert "has no primary key and is not promote_mode=insert_only" in psql(mdl.env.dbs["silver"], "SELECT message FROM sweep_ctl.model_run_log")
    mdl.env.manifest("silver", [{"schema": "app", "table": "customer"}, {"track": "model_output", "schema": "app", "table": "keyless", "promote_mode": "insert_only"}])
    assert sm.run_models_cmd() == 0


def test_promote_refuses_a_model_output_whose_model_has_not_succeeded_on_the_current_inputs(mdl):
    for l in ("silver", "gold_backup"):
        mdl.env.manifest(l, [{"schema": "app", "table": "customer"}, {"track": "model_output", "schema": "app", "table": "m1_out"}])
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    with pytest.raises(sl.LayerError, match="not ready to promote.*app.m1_out.*model m1 has no SUCCESS"):
        mdl.env.promote("silver", "gold_backup")
    assert sm.run_models_cmd() == 0
    assert mdl.env.promote("silver", "gold_backup")[1] == 0
    assert psql(mdl.env.dbs["gold_backup"], "SELECT count(*) FROM app.m1_out") == "1"
    mdl.env.bronze_load(DELTA1)
    assert mdl.env.promote("bronze", "silver")[1] == 0                                               # a new silver input delta: the output is stale
    with pytest.raises(sl.LayerError, match="received delta"):
        mdl.env.promote("silver", "gold_backup")
    assert mdl.env.promote("silver", "gold_backup", ignore_model_status=True)[1] == 0               # explicit override
    assert sm.run_models_cmd() == 0 and mdl.env.promote("silver", "gold_backup")[1] == 0


def test_chain_runs_the_models_in_silver_before_gold_backup_and_stops_when_one_fails(mdl):
    for l in ("silver", "gold_backup"):
        mdl.env.manifest(l, [{"schema": "app", "table": "customer"}, {"track": "model_output", "schema": "app", "table": "m1_out"}])
    mdl.write("10_m1.py", mdl.hdr(), 'sql("INSERT INTO app.m1_out SELECT id, qty FROM app.customer ON CONFLICT DO NOTHING")')
    mdl.env.bronze_load(DELTA1)
    assert sp.run_chain(until="models", transport="program") == 0
    assert mdl.runs() == "m1:SUCCESS:full"
    assert psql(mdl.env.dbs["silver"], "SELECT count(*) FROM app.m1_out") == "4" and psql(mdl.env.dbs["gold_backup"], "SELECT count(*) FROM app.m1_out") == "0"
    assert sp.run_chain(until="gold_backup", transport="program") == 0                                # models up to date, outputs go on
    assert psql(mdl.env.dbs["gold_backup"], "SELECT count(*) FROM app.m1_out") == "4"
    mdl.write("10_m1.py", mdl.hdr(), "sys.exit('boom')")
    mdl.env.bronze_load("id,name,qty\n7,g,70\n")
    assert sp.run_chain(until="gold", transport="program", models_args={"force": True}) == 1          # a failed model stops the chain
    assert psql(mdl.env.dbs["gold"], "SELECT count(*) FROM app.customer WHERE id = 7") == "0"
    assert psql(mdl.env.dbs["gold_backup"], "SELECT count(*) FROM app.customer WHERE id = 7") == "0"   # nothing was promoted to gold_backup
    assert sp.run_chain(until="gold_backup", skip_models=True, transport="program") == 0               # --skip-models promotes without them
    assert psql(mdl.env.dbs["gold_backup"], "SELECT count(*) FROM app.customer WHERE id = 7") == "1"


def test_status_and_list_commands(mdl, capsys):
    mdl.write("10_m1.py", mdl.hdr(), INSERT_ONE)
    assert sm.models_list() == 0
    out = capsys.readouterr().out
    assert "m1" in out and "app.customer -> app.m1_out" in out and "update_in_place" in out
    assert sm.models_status() == 0
    out = capsys.readouterr().out
    assert "never run" in out and "DUE: never succeeded" in out
    assert sm.run_models_cmd() == 0
    assert sm.models_status() == 0
    assert "SUCCESS" in capsys.readouterr().out
