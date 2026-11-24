"""layers reconcile / check, `sweep.py migrate`, the promote CLI and the gold DDL safeguards (needs PostgreSQL)."""
import argparse
import subprocess
import sys

import pytest

import sweep
import sweep_common as c
import sweep_layers as sl
import sweep_reconcile as rc
from chain_support import DELTA1, LAYERS, day1, psql, rows

pytestmark = pytest.mark.integration


def test_reconcile_matches_when_layers_agree_and_flags_a_difference(chain_env, capsys):
    env = chain_env
    day1(env)
    assert rc.reconcile_cmd("bronze", "silver") == 0 and rc.reconcile_cmd("bronze", "silver", exact=True) == 0
    assert "MATCH" in capsys.readouterr().out
    psql(env.dbs["silver"], "UPDATE app.customer SET name = 'changed' WHERE id = 2")
    assert rc.reconcile_cmd("bronze", "silver", exact=True) == 1                       # exact comparison sees a changed value
    out = capsys.readouterr().out
    assert "MISMATCH" in out
    psql(env.dbs["silver"], "UPDATE app.customer SET name = 'b' WHERE id = 2; DELETE FROM app.customer WHERE id = 3")
    assert rc.reconcile_cmd("bronze", "silver") == 1                                   # row counts differ
    with pytest.raises(sl.LayerError, match="unknown table"):
        rc.reconcile_cmd("bronze", "silver", tables=["app.nosuch"])


def test_gold_is_a_superset_check_application_changes_pass_and_a_removed_key_fails(chain_env, capsys, monkeypatch):
    env = chain_env
    monkeypatch.setattr(rc, "ORACLE_REPORTS_DIR", env.tmp / "reports")
    day1(env)
    psql(env.dbs["gold"], "UPDATE app.customer SET name = 'application change' WHERE id = 1; INSERT INTO app.customer VALUES (9, 'app', 1)")
    assert rc.reconcile_cmd("gold_backup", "gold", exact=True) == 0                 # changed values and extra rows are fine
    capsys.readouterr()
    psql(env.dbs["gold"], "DELETE FROM app.customer WHERE id = 2")
    assert rc.reconcile_cmd("gold_backup", "gold", exact=True) == 1                 # a key of gold_backup is gone (and one row fewer is masked by the extra row)
    out = capsys.readouterr().out
    assert "MISMATCH" in out and "key(s) of gold_backup missing in gold" in out
    psql(env.dbs["silver"], "ALTER TABLE app.customer ADD COLUMN createddate_m timestamp DEFAULT now()")
    assert rc.reconcile_cmd("bronze", "silver", exact=True) == 0                    # tool audit columns are not compared
    assert len(list((env.tmp / "reports").glob("*layers_reconcile*.xlsx"))) >= 2     # an Excel report per run


def test_reconcile_compares_only_same_type_columns_and_reports_type_differences(chain_env, capsys, monkeypatch):
    env = chain_env
    monkeypatch.setattr(rc, "ORACLE_REPORTS_DIR", env.tmp / "reports")
    day1(env)
    psql(env.dbs["silver"], "ALTER TABLE app.customer ALTER COLUMN qty TYPE bigint")
    assert rc.reconcile_cmd("bronze", "silver", exact=True) == 1                    # not hidden: unexplained type difference
    out = capsys.readouterr().out
    assert "TYPE_DIFFERENCE" in out and "qty" in out
    # an applied FE file that changed the column explains it: FE_CHANGED counts as PASS and is listed in FE_Changes
    psql(env.dbs["silver"], "INSERT INTO sweep_ctl.fwd_ddl_changes (file_name, kind, action, schema_name, object_name) "
                            "VALUES ('V5__qty.sql', 'column', 'changed', 'app', 'customer.qty')")
    assert rc.reconcile_cmd("bronze", "silver", exact=True) == 0
    assert "FE_CHANGED" in capsys.readouterr().out
    import openpyxl
    wb = openpyxl.load_workbook(sorted((env.tmp / "reports").glob("*layers_reconcile_bronze_to_silver*.xlsx"))[-1])
    assert "FE_Changes" in wb.sheetnames
    sheet = [[c.value for c in r] for r in wb["FE_Changes"].iter_rows()]
    assert any("V5__qty.sql" in r for r in sheet)


def test_reconcile_a_table_dropped_by_an_fe_file_is_fe_dropped_not_an_error(chain_env, capsys, monkeypatch):
    env = chain_env
    monkeypatch.setattr(rc, "ORACLE_REPORTS_DIR", env.tmp / "reports")
    day1(env)
    psql(env.dbs["silver"], "DROP TABLE app.customer")
    assert rc.reconcile_cmd("bronze", "silver") == 1
    assert "ERROR" in capsys.readouterr().out
    psql(env.dbs["silver"], "INSERT INTO sweep_ctl.fwd_ddl_changes (file_name, kind, action, schema_name, object_name) "
                            "VALUES ('V6__drop.sql', 'table', 'dropped', 'app', 'customer')")
    assert rc.reconcile_cmd("bronze", "silver") == 0
    assert "FE_DROPPED" in capsys.readouterr().out


def test_vsam_tables_are_enforced_in_silver_and_informational_in_gold_backup(chain_env, capsys, monkeypatch):
    env = chain_env
    monkeypatch.setattr(rc, "ORACLE_REPORTS_DIR", env.tmp / "reports")
    env.everywhere("CREATE SCHEMA bcs; CREATE TABLE bcs.v (a int PRIMARY KEY);")
    for l in ("bronze", "silver", "gold_backup"):
        env.manifest(l, [{"schema": "bcs", "table": "v", "track": "vsam"}])
    psql(env.dbs["bronze"], "INSERT INTO bcs.v VALUES (1), (2)")
    assert rc.reconcile_cmd("bronze", "silver", exact=True) == 1                    # silver must equal bronze
    psql(env.dbs["silver"], "INSERT INTO bcs.v VALUES (1)")
    assert rc.reconcile_cmd("silver", "gold_backup", exact=True) == 0               # informational there
    assert "informational" in capsys.readouterr().out


def test_layers_check_reports_every_layer_and_fails_when_a_layer_is_not_migrated(chain_env, capsys):
    env = chain_env
    day1(env)
    assert rc.check_cmd() == 0
    out = capsys.readouterr().out
    assert out.count("[OK  ] database reachable") == 4 and "All checks passed." in out
    psql(env.dbs["gold"], "DROP TABLE sweep_ctl.schema_version")
    assert rc.check_cmd("gold") == 1
    assert "[FAIL] sweep_ctl migrated" in capsys.readouterr().out
    psql(env.dbs["silver"], "ALTER TABLE app.customer DROP COLUMN qty")
    assert rc.check_cmd("silver") == 0                                                              # drift is a WARN, not a failure
    assert "[WARN] schema drift vs bronze" in capsys.readouterr().out


def test_layers_check_sequence_status(chain_env, capsys):
    env = chain_env
    env.everywhere("CREATE SCHEMA app; CREATE TABLE app.ident (id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text); "
                   "INSERT INTO app.ident (id, v) VALUES (50, 'x')")
    for l in LAYERS:
        env.manifest(l, [{"schema": "app", "table": "ident"}])
    assert rc.check_cmd("silver") == 0
    assert "[WARN] sequences: 1 behind max(column)" in capsys.readouterr().out


def test_migrate_command_runs_layers_in_chain_order_and_is_idempotent(chain_env, capsys):
    env = chain_env
    assert sweep.cmd_migrate(argparse.Namespace(layer=None, all_layers=True, dry_run=False)) == 0
    out = capsys.readouterr().out.splitlines()
    assert [l.split(" ")[0] for l in out] == ["bronze", "silver", "gold_backup", "gold"] and all(l.endswith("up to date") for l in out)
    psql(env.dbs["gold"], "DELETE FROM sweep_ctl.schema_version")
    assert sweep.cmd_migrate(argparse.Namespace(layer="gold", all_layers=False, dry_run=True)) == 0
    assert "would apply 1" in capsys.readouterr().out
    assert psql(env.dbs["gold"], "SELECT count(*) FROM sweep_ctl.schema_version") == "0"           # dry run changed nothing
    assert sweep.cmd_migrate(argparse.Namespace(layer="gold", all_layers=False, dry_run=False)) == 0
    assert sweep.cmd_migrate(argparse.Namespace(layer=None, all_layers=False, dry_run=False)) == 2   # a layer is required


def test_promote_cli_status_and_errors(chain_env, capsys):
    env = chain_env
    day1(env)
    env.bronze_load("id,name,qty\n2,b2,20\n")
    ns = dict(from_layer="bronze", to_layer="silver", tables=None, full=False, dry_run=False, workers=2, transport="program", status=False,
              purge=False, limit=10)
    assert sweep.cmd_promote(argparse.Namespace(**ns)) == 0
    assert "SUCCESS" in capsys.readouterr().out
    assert sweep.cmd_promote(argparse.Namespace(**{**ns, "status": True})) == 0
    out = capsys.readouterr().out
    assert "app.customer" in out and "SUCCESS" in out and "PENDING" in out
    assert sweep.cmd_promote(argparse.Namespace(**{**ns, "from_layer": None})) == 2                # --from / --to are required
    assert sweep.cmd_promote(argparse.Namespace(**{**ns, "from_layer": "bronze", "to_layer": "gold"})) == 2


def test_chain_stops_at_the_first_failure_and_until_stops_early(chain_env):
    env = chain_env
    day1(env)
    env.bronze_load("id,name,qty\n2,b2,20\n")
    import sweep_promote as sp
    assert sp.run_chain(until="gold_backup", transport="program") == 0
    assert rows(env, "gold_backup") == "1:a:10,2:b2:20,3:c:30" and rows(env, "gold") == "1:a:10,2:b:20,3:c:30"     # reviewed at gold_backup
    psql(env.dbs["gold"], "ALTER TABLE app.customer DROP COLUMN qty")
    assert sp.run_chain(transport="program") == 1                                                    # gold drift: the chain reports the failure
    with pytest.raises(sl.LayerError, match="unknown --until"):
        sp.run_chain(until="platinum")


def test_gold_refuses_ddl_steps_without_allow_ddl_and_stays_inside_its_manifest(chain_env, tmp_path):
    env = chain_env
    day1(env)
    import os
    envvars = {**os.environ, "SWEEP_LAYERS_FILE": str(sl.LAYERS_FILE), "PGDATABASE": env.dbs["gold"]}
    for module, extra in (("oracle.oracle_05_apply_ddl", []), ("oracle.oracle_07_load_objects", []), ("vsam.vsam_05_apply_ddl", [])):
        r = subprocess.run([sys.executable, "-m", module, "--layer", "gold", *extra], cwd=c.BASE_DIR, env=envvars, capture_output=True, text=True)
        assert r.returncode == 2 and "allow_ddl=false" in (r.stdout + r.stderr), (module, r.stdout[-300:], r.stderr[-300:])
    r = subprocess.run([sys.executable, "-m", "oracle.oracle_06_load_data", "--layer", "gold"], cwd=c.BASE_DIR, env=envvars, capture_output=True, text=True)
    assert r.returncode == 2 and "promote --from gold_backup --to gold" in (r.stdout + r.stderr)    # S3 is read into bronze only
    r = subprocess.run([sys.executable, "-m", "oracle.oracle_08_validate", "--layer", "gold", "--dry-run"], cwd=c.BASE_DIR, env=envvars,
                       capture_output=True, text=True)
    assert r.returncode != 3                                                                       # the guard accepts the right database
    r = subprocess.run([sys.executable, "-m", "oracle.oracle_08_validate", "--dry-run"], cwd=c.BASE_DIR, env=envvars, capture_output=True, text=True)
    assert r.returncode == 2 and "--layer is required" in (r.stdout + r.stderr)


def test_validate_gives_one_report_and_a_verdict_per_layer_with_fe_differences_explained(chain_env, capsys, monkeypatch):
    env = chain_env
    monkeypatch.setattr(rc, "ORACLE_REPORTS_DIR", env.tmp / "reports")
    day1(env)
    assert rc.validate_layer_cmd("silver") == 0
    out = capsys.readouterr().out
    assert "VERDICT silver" in out and "PASS" in out and "schema comparison with bronze" in out
    psql(env.dbs["silver"], "ALTER TABLE app.customer DROP COLUMN qty")
    assert rc.validate_layer_cmd("silver") == 1                                    # column missing: not explained
    assert "FAIL" in capsys.readouterr().out
    psql(env.dbs["silver"], "INSERT INTO sweep_ctl.fwd_ddl_changes (file_name, kind, action, schema_name, object_name) "
                            "VALUES ('V7__qty.sql', 'column', 'dropped', 'app', 'customer.qty')")
    assert rc.validate_layer_cmd("silver") == 0                                    # explained by an applied FE file
    reports = sorted((env.tmp / "reports").glob("*_validate_silver_*.xlsx"))
    import openpyxl
    wb = openpyxl.load_workbook(reports[-1])
    assert {"Summary", "Reconcile", "Schema", "Ledger", "FE_Changes"} <= set(wb.sheetnames)
    assert any("V7__qty.sql" in [c.value for c in r] for r in wb["FE_Changes"].iter_rows())
    assert rc.validate_layer_cmd("gold") == 0                                      # superset check for gold


def test_monitor_header_has_the_layer_database_last_promote_and_pending_fe_files(chain_env):
    from sweep_dashboard import render_dashboard
    env = chain_env
    day1(env)
    assert "last promote: none" in render_dashboard(use_color=False, layer=sl.get_layer("silver"))
    env.bronze_load(DELTA1)
    assert env.promote("bronze", "silver")[1] == 0
    text = render_dashboard(use_color=False, layer=sl.get_layer("silver"))
    assert f"LAYER: silver    DATABASE: {env.dbs['silver']}" in text
    assert "last promote: bronze>silver" in text and "1 table(s)  +1 inserted  ~1 updated  0 failed" in text
    assert "pending FE files: 0" in text
    assert "LAYER:" not in render_dashboard(use_color=False)                       # without --layer the dashboard is unchanged


def test_layers_check_does_not_call_an_fe_dropped_column_drift(chain_env, capsys):
    env = chain_env
    day1(env)
    psql(env.dbs["silver"], "ALTER TABLE app.customer DROP COLUMN qty")
    rc.check_cmd("silver")
    assert "lack source columns" in capsys.readouterr().out
    psql(env.dbs["silver"], "INSERT INTO sweep_ctl.fwd_ddl_changes (file_name, kind, action, schema_name, object_name) "
                            "VALUES ('V7__qty.sql', 'column', 'dropped', 'app', 'customer.qty')")
    rc.check_cmd("silver")
    out = capsys.readouterr().out
    assert "schema drift vs bronze: none" in out
