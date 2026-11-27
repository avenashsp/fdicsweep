"""FE SQL files (`sweep.py fwd-ddl`): order, immutability, transactions, data refusal, pragmas, compatibility check, promote."""
import argparse
import csv

import pytest

import sweep_common as c
import sweep_fwd_ddl as fd
import sweep_layers as sl
from chain_support import day1, psql

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------- parsing (no database)
def test_splitter_respects_quotes_comments_and_dollar_quotes():
    text = "-- c; x\nCREATE TABLE a (x text DEFAULT 'a;b'); /* ; */ ALTER TABLE a ADD y int;\nDO $$ BEGIN PERFORM 1; END $$;\nSELECT 1"
    assert [s for s, _ in fd.split_statements(text)] == [
        "-- c; x\nCREATE TABLE a (x text DEFAULT 'a;b');", "/* ; */ ALTER TABLE a ADD y int;", "DO $$ BEGIN PERFORM 1; END $$;", "SELECT 1"]


@pytest.mark.parametrize("sql,kind", [
    ("INSERT INTO a VALUES (1)", "data"), ("update a set x = 1", "data"), ("DELETE FROM a", "data"), ("TRUNCATE a", "data"),
    ("COPY a FROM stdin", "data"), ("WITH x AS (SELECT 1) INSERT INTO a SELECT * FROM x", "data"), ("CREATE TABLE b AS SELECT * FROM a", "data"),
    ("DO $$ BEGIN INSERT INTO a VALUES (1); END $$", "data"), ("BEGIN", "transaction"), ("COMMIT", "transaction"),
    ("ALTER TABLE a ADD COLUMN x int", "ddl"), ("CREATE TABLE b (x int)", "ddl"), ("CREATE TABLE b AS SELECT 1 WITH NO DATA", "ddl"),
    ("SELECT 'insert into a' AS x", "ddl"), ("COMMENT ON TABLE a IS 'delete from a'", "ddl")])
def test_statement_classification(sql, kind):
    assert fd.classify_statement(sql)[0] == kind


def test_destructive_statements_produce_warnings():
    assert fd.classify_statement("DROP TABLE a")[1][0].startswith("destructive: DROP table")
    assert "destructive" in fd.classify_statement("ALTER TABLE a DROP COLUMN x")[1][0]
    assert "type change" in fd.classify_statement("ALTER TABLE a ALTER COLUMN x TYPE bigint")[1][0]
    assert fd.classify_statement("ALTER TABLE a ADD COLUMN x int")[1] == []


def test_pragmas_and_file_name_rules(tmp_path):
    p = tmp_path / "V12__demo.sql"
    p.write_text("-- sweep:layers Bronze, silver\n-- sweep:ignore-column app.t.old\n-- sweep:rename-column app.t.a -> b\n"
                 "-- sweep:default-column app.t.c = now() + interval '1 day'\n-- sweep:skip-table app.gone\n-- sweep:no-transaction\nALTER TABLE app.t ADD x int;\n")
    f = fd.parse_file(p)
    assert f.order == 12 and f.layers == ("bronze", "silver") and f.no_transaction
    assert sorted(f.rules) == sorted([("ignore-column", "app", "t", "old", None, None), ("rename-column", "app", "t", "a", "b", None),
                                      ("default-column", "app", "t", "c", None, "now() + interval '1 day'"), ("skip-table", "app", "gone", None, None, None)])
    for bad, msg in (("-- sweep:layers platinum\n", "unknown layer"), ("-- sweep:rename-column app.t.a b\n", "rename-column"),
                     ("-- sweep:nothing x\n", "unknown pragma"), ("\\i other.sql\n", "meta-commands"), ("BEGIN;\nSELECT 1;\n", "transaction control")):
        p.write_text(bad)
        with pytest.raises(fd.FwdError, match=msg):
            fd.parse_file(p)
    with pytest.raises(fd.FwdError, match="V<number>__"):
        fd.parse_file(tmp_path / "bad_name.sql")


# ---------------------------------------------------------------- against databases
@pytest.fixture()
def fwd(chain_env, tmp_path, monkeypatch):
    d = tmp_path / "fwd_ddl"
    d.mkdir()
    monkeypatch.setattr(fd, "FWD_DIR", d)
    day1(chain_env)
    meta = tmp_path / "tables.csv"
    with open(meta, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["owner", "table_name", "column_name"])
        for col in ("ID", "NAME", "QTY", "MODIFIEDDATE"):
            w.writerow(["APP", "CUSTOMER", col])
    monkeypatch.setattr(c, "META_TABLES_CSV", meta)

    class F:
        env = chain_env
        dir = d

        @staticmethod
        def write(name, text):
            (d / name).write_text(text)

        @staticmethod
        def args(layer="silver", **kw):
            ns = dict(layer=layer, dry_run=False, allow_ddl=True, allow_data=False, force=False, file=None, filename=None, tables=None,
                      schemas=None, exclude_tables=None, from_layer=None, to_layer=None)
            ns.update(kw)
            return argparse.Namespace(**ns)

    return F


def _cols(env, layer):
    return psql(env.dbs[layer], "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns "
                                "WHERE table_schema = 'app' AND table_name = 'customer' "
                                "AND column_name NOT IN ('createdby', 'createddate', 'modifiedby', 'createddate_m', 'createdby_m')")      # 6.3.0 post-step columns


def test_files_apply_in_numeric_order_once_and_are_immutable(fwd, capsys):
    fwd.write("V2__second.sql", "ALTER TABLE app.customer ADD COLUMN b int;\n")
    fwd.write("V10__tenth.sql", "ALTER TABLE app.customer ADD COLUMN c int;\n")
    fwd.write("V1__first.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    assert fd.cmd_apply(fwd.args()) == 0
    assert _cols(fwd.env, "silver") == "id,name,qty,modifieddate,a,b,c"                        # V1, V2, V10 (numeric, not alphabetical)
    log = psql(fwd.env.dbs["silver"], "SELECT string_agg(file_name || ':' || status, ',' ORDER BY order_no) FROM sweep_ctl.fwd_ddl_log")
    assert log == "V1__first.sql:APPLIED,V2__second.sql:APPLIED,V10__tenth.sql:APPLIED"
    assert fd.cmd_apply(fwd.args()) == 0 and "no pending FE files" in capsys.readouterr().out    # idempotent
    fwd.write("V2__second.sql", "ALTER TABLE app.customer ADD COLUMN b bigint;\n")              # an applied file was edited
    assert fd.cmd_apply(fwd.args()) == 1
    assert "immutable" in capsys.readouterr().out and _cols(fwd.env, "silver").endswith("a,b,c")
    assert fd.cmd_status(argparse.Namespace(layer="silver")) == 1                                # status reports it too
    fwd.write("V2__second.sql", "ALTER TABLE app.customer ADD COLUMN b int;\n")                 # restored
    fwd.write("V3__fix.sql", "ALTER TABLE app.customer ALTER COLUMN b TYPE bigint;\n")           # the correction is a new file
    assert fd.cmd_apply(fwd.args()) == 0


def test_one_transaction_per_file_and_rollback_on_error(fwd, capsys):
    fwd.write("V1__ok.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    fwd.write("V2__bad.sql", "ALTER TABLE app.customer ADD COLUMN b int;\nALTER TABLE app.nosuch ADD COLUMN x int;\n")
    fwd.write("V3__never.sql", "ALTER TABLE app.customer ADD COLUMN c int;\n")
    assert fd.cmd_apply(fwd.args()) == 1
    out = capsys.readouterr().out
    assert "stopped at V2__bad.sql" in out
    assert _cols(fwd.env, "silver") == "id,name,qty,modifieddate,a"                             # V1 committed; V2 rolled back completely; V3 not tried
    log = psql(fwd.env.dbs["silver"], "SELECT string_agg(file_name || ':' || status, ',' ORDER BY file_name) FROM sweep_ctl.fwd_ddl_log")
    assert log == "V1__ok.sql:APPLIED,V2__bad.sql:FAILED"
    assert psql(fwd.env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.fwd_ddl_changes WHERE file_name = 'V2__bad.sql'") == "0"
    fwd.write("V2__bad.sql", "ALTER TABLE app.customer ADD COLUMN b int;\n")                    # a FAILED file may be corrected and retried
    assert fd.cmd_apply(fwd.args()) == 0 and _cols(fwd.env, "silver").endswith("a,b,c")


def test_dry_run_rolls_back_and_prints_the_catalog_diff_and_warnings(fwd, capsys):
    fwd.write("V1__x.sql", "-- sweep:ignore-column app.customer.qty\nALTER TABLE app.customer ADD COLUMN a int;\nALTER TABLE app.customer DROP COLUMN qty;\n"
                           "CREATE TABLE app.fe_new (id int PRIMARY KEY);\nCREATE INDEX ix_cust_name ON app.customer (name);\n")
    assert fd.cmd_apply(fwd.args(dry_run=True)) == 0
    out = capsys.readouterr().out
    for needle in ("added    column     app.customer.a", "dropped  column     app.customer.qty", "added    table      app.fe_new",
                   "added    index      app.customer.ix_cust_name", "WARNING V1__x.sql:3: destructive: ALTER ... DROP column", "DRY_RUN"):
        assert needle in out, needle
    assert _cols(fwd.env, "silver") == "id,name,qty,modifieddate"                                # nothing changed
    assert psql(fwd.env.dbs["silver"], "SELECT to_regclass('app.fe_new') IS NULL") == "t"
    assert psql(fwd.env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.fwd_ddl_log") == "0"
    assert fd.cmd_apply(fwd.args()) == 0
    changes = psql(fwd.env.dbs["silver"], "SELECT string_agg(kind || ':' || action || ':' || object_name, ',' ORDER BY kind, object_name) "
                                          "FROM sweep_ctl.fwd_ddl_changes WHERE kind IN ('column', 'table')")
    assert changes == "column:added:customer.a,column:dropped:customer.qty,column:added:fe_new.id,table:added:fe_new"


def test_data_statements_are_refused_unless_allow_data(fwd, capsys):
    fwd.write("V1__data.sql", "ALTER TABLE app.customer ADD COLUMN a int;\nUPDATE app.customer SET a = 1;\n")
    assert fd.cmd_apply(fwd.args()) == 1
    assert "STRUCTURE ONLY" in capsys.readouterr().out and _cols(fwd.env, "silver") == "id,name,qty,modifieddate"
    assert fd.cmd_apply(fwd.args(allow_data=True)) == 0
    assert psql(fwd.env.dbs["silver"], "SELECT count(*) FROM app.customer WHERE a = 1") == "3"


def test_layer_pragma_scopes_a_file_and_gold_needs_allow_ddl(fwd, capsys):
    fwd.write("V1__vsam_only.sql", "-- sweep:layers silver\nALTER TABLE app.customer ADD COLUMN a int;\n")
    assert fd.cmd_apply(fwd.args("silver")) == 0 and _cols(fwd.env, "silver").endswith("a")
    assert fd.cmd_apply(fwd.args("gold_backup")) == 0 and _cols(fwd.env, "gold_backup") == "id,name,qty,modifieddate"      # not applicable there
    assert "NOT_APPLICABLE" in capsys.readouterr().out
    assert psql(fwd.env.dbs["gold_backup"], "SELECT status || ':' || message FROM sweep_ctl.fwd_ddl_log").startswith("APPLIED:not applicable")
    assert fd.cmd_apply(fwd.args("silver")) == 0 and "no pending" in capsys.readouterr().out
    with pytest.raises(SystemExit) as e:
        fd.cmd_apply(fwd.args("gold", allow_ddl=False))
    assert e.value.code == 2


def test_compatibility_check_blocks_and_rules_clear_the_blocks(fwd, capsys):
    # (a) a source column is neither in the table nor covered
    fwd.write("V1__drop.sql", "ALTER TABLE app.customer DROP COLUMN qty;\n")
    assert fd.cmd_apply(fwd.args()) == 1
    out = capsys.readouterr().out
    assert "BLOCKED" in out and "source column qty is neither in the table nor covered" in out and _cols(fwd.env, "silver") == "id,name,qty,modifieddate"
    assert fd.cmd_apply(fwd.args(dry_run=True)) == 1 and "compatibility BLOCK" in capsys.readouterr().out
    # ... covered by ignore-column
    fwd.write("V1__drop.sql", "-- sweep:ignore-column app.customer.qty\nALTER TABLE app.customer DROP COLUMN qty;\n")
    assert fd.cmd_apply(fwd.args()) == 0 and "compatibility OK" in capsys.readouterr().out
    assert psql(fwd.env.dbs["silver"], "SELECT rule || ':' || column_name FROM sweep_ctl.fwd_ddl_rules") == "ignore-column:qty"
    assert psql(fwd.env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.fwd_ddl_changes WHERE kind = 'rule'") == "1"
    # (b) a new NOT NULL column without default is absent from the source
    fwd.write("V2__nn.sql", "ALTER TABLE app.customer ADD COLUMN must text NOT NULL DEFAULT 'x';\nALTER TABLE app.customer ALTER COLUMN must DROP DEFAULT;\n")
    assert fd.cmd_apply(fwd.args()) == 1 and "NOT NULL column must without default is absent from the source" in capsys.readouterr().out
    fwd.write("V2__nn.sql", "-- sweep:default-column app.customer.must = 'x'\nALTER TABLE app.customer ADD COLUMN must text NOT NULL DEFAULT 'x';\n"
                            "ALTER TABLE app.customer ALTER COLUMN must DROP DEFAULT;\n")
    assert fd.cmd_apply(fwd.args()) == 0
    # (c) the table was dropped
    fwd.write("V3__dropt.sql", "DROP TABLE app.customer;\n")
    assert fd.cmd_apply(fwd.args()) == 1 and "table was dropped and no skip-table rule exists" in capsys.readouterr().out
    assert psql(fwd.env.dbs["silver"], "SELECT to_regclass('app.customer') IS NOT NULL") == "t"
    assert fd.cmd_apply(fwd.args(force=True)) == 0                                                       # --force overrides the block
    assert psql(fwd.env.dbs["silver"], "SELECT to_regclass('app.customer') IS NULL") == "t"


def test_rename_column_rule_covers_the_source_column(fwd, capsys):
    fwd.write("V1__rename.sql", "-- sweep:rename-column app.customer.name -> full_name\nALTER TABLE app.customer RENAME COLUMN name TO full_name;\n")
    assert fd.cmd_apply(fwd.args()) == 0
    fwd.write("V2__rename_missing.sql", "-- sweep:rename-column app.customer.qty -> quantity\nALTER TABLE app.customer RENAME COLUMN qty TO amount;\n")
    assert fd.cmd_apply(fwd.args()) == 1                                                                 # the rename target does not exist
    assert "source column qty" in capsys.readouterr().out


def test_skip_table_rule_and_new_tables_need_no_source(fwd):
    fwd.write("V1__new.sql", "CREATE TABLE app.fe_only (id int PRIMARY KEY, v text NOT NULL);\n")
    assert fd.cmd_apply(fwd.args()) == 0                                                                 # FE tables have no source columns
    fwd.write("V2__gone.sql", "-- sweep:skip-table app.customer\nDROP TABLE app.customer;\n")
    assert fd.cmd_apply(fwd.args()) == 0


def test_no_transaction_files_run_statement_by_statement(fwd, capsys):
    fwd.write("V1__idx.sql", "-- sweep:no-transaction\nCREATE INDEX CONCURRENTLY ix_cust_name ON app.customer (name);\n")
    assert fd.cmd_apply(fwd.args(dry_run=True)) == 0 and "not rehearsed" in capsys.readouterr().out
    assert fd.cmd_apply(fwd.args()) == 0
    assert psql(fwd.env.dbs["silver"], "SELECT count(*) FROM pg_indexes WHERE indexname = 'ix_cust_name'") == "1"
    assert psql(fwd.env.dbs["silver"], "SELECT count(*) FROM sweep_ctl.fwd_ddl_changes WHERE kind = 'index'") == "1"


def test_fwd_ddl_promote_goes_one_hop_in_order_and_gold_follows_gold_backup(fwd, capsys):
    fwd.write("V1__a.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    fwd.write("V2__b.sql", "ALTER TABLE app.customer ADD COLUMN b int;\n")
    assert fd.cmd_apply(fwd.args("silver")) == 0
    assert fd.cmd_promote(fwd.args("gold", from_layer="gold_backup", to_layer="gold")) == 0              # gold_backup applied nothing: nothing to take
    assert "nothing to promote" in capsys.readouterr().out and _cols(fwd.env, "gold") == "id,name,qty,modifieddate"
    assert fd.cmd_promote(fwd.args(from_layer="silver", to_layer="gold_backup")) == 0
    assert _cols(fwd.env, "gold_backup").endswith("a,b") and _cols(fwd.env, "gold") == "id,name,qty,modifieddate"
    assert fd.cmd_promote(fwd.args(from_layer="gold_backup", to_layer="gold")) == 0
    assert _cols(fwd.env, "gold").endswith("a,b")
    assert fd.cmd_promote(fwd.args(from_layer="gold_backup", to_layer="gold")) == 0 and "nothing to promote" in capsys.readouterr().out
    assert fd.cmd_promote(fwd.args(from_layer="silver", to_layer="gold")) == 1 and "ONE hop" in capsys.readouterr().out


def test_fe_files_never_run_in_bronze(fwd, capsys):
    fwd.write("V1__a.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    assert fd.cmd_apply(fwd.args("bronze")) == 2
    assert "FE files are never applied in bronze" in capsys.readouterr().out and _cols(fwd.env, "bronze") == "id,name,qty,modifieddate"
    assert psql(fwd.env.dbs["bronze"], "SELECT count(*) FROM sweep_ctl.fwd_ddl_log") == "0"
    assert fd.cmd_promote(fwd.args(from_layer="bronze", to_layer="silver")) == 2 and "never applied in bronze" in capsys.readouterr().out
    assert _cols(fwd.env, "silver") == "id,name,qty,modifieddate"
    assert fd.cmd_check(argparse.Namespace(layer="bronze", allow_ddl=True, allow_data=False, tables=None, schemas=None, exclude_tables=None,
                                           file=None, filename=None, dry_run=False)) == 2
    assert fd.cmd_status(argparse.Namespace(layer="bronze")) == 0 and "never applied in bronze" in capsys.readouterr().out
    assert fd.cmd_status(argparse.Namespace(layer=None)) == 0
    out = capsys.readouterr().out
    assert "== bronze" not in out and "== silver" in out and "layers behind: silver, gold_backup, gold" in out
    assert fd.FeIndex is not None and [l.name for l in fd.fe_layers()] == ["silver", "gold_backup", "gold"]


def test_status_without_layer_is_read_only_and_names_the_layers_that_are_behind(fwd, capsys):
    fwd.write("V1__a.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    assert fd.cmd_apply(fwd.args("silver")) == 0
    capsys.readouterr()
    assert fd.cmd_status(argparse.Namespace(layer=None)) == 0
    out = capsys.readouterr().out
    assert "== silver" in out and "applied 1, pending 0" in out and "== gold" in out and "layers behind: gold_backup, gold" in out
    assert fd.cmd_check(argparse.Namespace(layer=None, allow_ddl=False, allow_data=False, tables=None, schemas=None, exclude_tables=None,
                                           file=None, filename=None, dry_run=False)) == 0
    out = capsys.readouterr().out
    assert "== gold_backup: skipped (allow_ddl=false" in out and "== bronze" not in out
    assert _cols(fwd.env, "gold_backup") == "id,name,qty,modifieddate"                                    # check wrote nothing


def test_several_pending_files_are_rehearsed_in_one_transaction_in_order(fwd, capsys):
    fwd.write("V1__a.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    fwd.write("V2__b.sql", "ALTER TABLE app.customer RENAME COLUMN a TO a2;\n")                     # depends on V1
    assert fd.cmd_apply(fwd.args(dry_run=True)) == 0
    assert capsys.readouterr().out.count("DRY_RUN") == 2                                              # both files rehearsed, V2 on top of V1
    assert _cols(fwd.env, "silver") == "id,name,qty,modifieddate"
    assert fd.cmd_apply(fwd.args(file="V2__b.sql")) == 1 and "must be applied first" in capsys.readouterr().out


def test_data_promote_between_fe_layers_refuses_a_hop_while_the_target_has_pending_fe_files(fwd):
    import sweep_promote as sp
    fwd.env.bronze_load("id,name,qty\n2,b2,20\n")
    fwd.write("V1__a.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    assert fd.cmd_apply(fwd.args("silver")) == 0
    assert sp.promote("bronze", "silver", transport="program")[1] == 0                                    # bronze has no FE files: no gate there
    with pytest.raises(sl.LayerError, match="pending FE file.*V1__a.sql.*fwd-ddl promote --from silver --to gold_backup"):
        sp.promote("silver", "gold_backup", transport="program")
    assert psql(fwd.env.dbs["gold_backup"], "SELECT name FROM app.customer WHERE id = 2") == "b"          # nothing applied
    assert sp.promote("silver", "gold_backup", transport="program", ignore_pending_fwd=True)[1] == 0       # explicit override
    assert fd.cmd_promote(fwd.args(from_layer="silver", to_layer="gold_backup")) == 0
    assert sp.promote("silver", "gold_backup", transport="program")[1] == 0                               # gate open now


def test_promote_applies_the_fe_rules_of_silver_to_the_bronze_delta(fwd):
    import sweep_promote as sp
    fwd.write("V1__ignore.sql", "-- sweep:ignore-column app.customer.qty\nALTER TABLE app.customer DROP COLUMN qty;\n")
    fwd.write("V2__rename.sql", "-- sweep:rename-column app.customer.name -> full_name\nALTER TABLE app.customer RENAME COLUMN name TO full_name;\n")
    fwd.write("V3__default.sql", "-- sweep:default-column app.customer.must = 'dflt'\nALTER TABLE app.customer ADD COLUMN must text NOT NULL DEFAULT 'x';\n"
                                 "ALTER TABLE app.customer ALTER COLUMN must DROP DEFAULT;\n")
    assert fd.cmd_apply(fwd.args("silver")) == 0
    psql(fwd.env.dbs["silver"], "UPDATE app.customer SET must = 'old'")
    fwd.env.bronze_load("id,name,qty\n2,b2,20\n4,d,40\n")                                                # bronze keeps the raw source structure
    assert _cols(fwd.env, "bronze") == "id,name,qty,modifieddate"
    results, code = sp.promote("bronze", "silver", transport="program")
    assert code == 0 and results[0]["status"] == "SUCCESS", results
    assert psql(fwd.env.dbs["silver"], "SELECT string_agg(id || ':' || full_name || ':' || must, ',' ORDER BY id) FROM app.customer") == "1:a:old,2:b2:old,3:c:old,4:d:dflt"
    assert sp.promote("silver", "gold_backup", transport="program", ignore_pending_fwd=True)[1] == 1       # gold_backup lacks the FE files: drift is reported
    assert fd.cmd_promote(fwd.args(from_layer="silver", to_layer="gold_backup")) == 0
    assert sp.promote("silver", "gold_backup", transport="program")[1] == 0
    assert psql(fwd.env.dbs["gold_backup"], "SELECT string_agg(id || ':' || full_name || ':' || must, ',' ORDER BY id) FROM app.customer") == "1:a:x,2:b2:old,3:c:x,4:d:dflt"


def test_a_source_column_without_a_rule_is_still_schema_drift_and_skip_table_skips(fwd):
    import sweep_promote as sp
    fwd.write("V1__drop.sql", "ALTER TABLE app.customer DROP COLUMN qty;\n")
    assert fd.cmd_apply(fwd.args("silver", force=True)) == 0                                              # the compatibility check is a rule-less BLOCK: forced here
    fwd.env.bronze_load("id,name,qty\n2,b2,20\n")
    results, code = sp.promote("bronze", "silver", transport="program")
    assert code == 1 and "schema drift: column(s) qty" in results[0]["message"] and "ignore-column" in results[0]["message"]
    fwd.write("V2__skip.sql", "-- sweep:skip-table app.customer\nDROP TABLE app.customer;\n")
    assert fd.cmd_apply(fwd.args("silver", force=True)) == 0
    results, code = sp.promote("bronze", "silver", transport="program")
    assert code == 0 and results[0]["status"] == "SKIPPED" and "skip-table rule" in results[0]["message"]


def test_chain_loads_bronze_raw_promotes_to_silver_then_applies_fe_and_stops_at_silver(fwd, capsys):
    import sweep_promote as sp
    fwd.env.bronze_load("id,name,qty\n2,b2,20\n")
    fwd.write("V1__a.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    assert sp.run_chain(until="silver", skip_models=True, transport="program") == 0
    assert _cols(fwd.env, "bronze") == "id,name,qty,modifieddate"                                           # bronze never gets FE files
    assert _cols(fwd.env, "silver").endswith("a")
    assert psql(fwd.env.dbs["silver"], "SELECT name FROM app.customer WHERE id = 2") == "b2"
    assert _cols(fwd.env, "gold_backup") == "id,name,qty,modifieddate"                                      # stopped at silver
    # gold_backup needs --allow-ddl for the pending FE file; without it the chain stops and says so
    assert sp.run_chain(skip_models=True, transport="program") == 1
    assert _cols(fwd.env, "gold_backup") == "id,name,qty,modifieddate"
    assert sp.run_chain(skip_models=True, transport="program", allow_ddl=True) == 0
    assert _cols(fwd.env, "gold").endswith("a") and psql(fwd.env.dbs["gold"], "SELECT name FROM app.customer WHERE id = 2") == "b2"
    with pytest.raises(sl.LayerError, match="unknown --until"):
        sp.run_chain(until="nowhere")


def test_chain_applies_new_fe_files_in_silver_after_the_data_promote(fwd):
    import sweep_promote as sp
    fwd.write("V1__a.sql", "ALTER TABLE app.customer ADD COLUMN a int;\n")
    assert fd.cmd_apply(fwd.args("silver")) == 0
    fwd.write("V2__b.sql", "ALTER TABLE app.customer ADD COLUMN b int;\n")                              # a new file
    fwd.env.bronze_load("id,name,qty\n2,b2,20\n")
    assert sp.run_chain(until="gold_backup", skip_models=True, transport="program", allow_ddl=True) == 0
    assert _cols(fwd.env, "silver").endswith("a,b") and _cols(fwd.env, "gold_backup").endswith("a,b")     # silver first, then gold_backup
    assert _cols(fwd.env, "gold") == "id,name,qty,modifieddate" and _cols(fwd.env, "bronze") == "id,name,qty,modifieddate"


# ---------------------------------------------------------------- 6.3.0: audit post-step
def test_the_audit_post_step_adds_missing_columns_skips_existing_ones_and_creates_the_trigger(fwd, capsys):
    fwd.write("V1__new_table.sql", "CREATE TABLE app.fresh (id int PRIMARY KEY);\nALTER TABLE app.customer ADD COLUMN extra int;\n")
    assert fd.cmd_apply(fwd.args()) == 0
    db = fwd.env.dbs["silver"]
    six = {"createdby", "createddate", "modifiedby", "modifieddate", "createddate_m", "createdby_m"}
    cols = lambda t: set(psql(db, f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'app' AND table_name = '{t}'").split("\n"))
    assert six <= cols("fresh") and six <= cols("customer")                                           # created AND altered tables
    assert psql(db, "SELECT data_type FROM information_schema.columns WHERE table_schema = 'app' AND table_name = 'customer' AND column_name = 'modifieddate'") \
        == "timestamp without time zone"                                                              # an existing column is skipped, not redefined
    for t in ("fresh", "customer"):
        assert psql(db, f"SELECT count(*) FROM pg_trigger WHERE tgrelid = 'app.{t}'::regclass AND tgname = 'trg_sweep_audit'") == "1"
    # the audit columns are not listed as FE changes, and a table the file did not touch is left alone
    assert psql(db, "SELECT count(*) FROM sweep_ctl.fwd_ddl_changes WHERE object_name LIKE 'fresh.created%' OR object_name LIKE 'customer.created%'") == "0"
    # idempotent: the next file touching the table does not duplicate anything
    fwd.write("V2__more.sql", "ALTER TABLE app.fresh ADD COLUMN more int;\n")
    assert fd.cmd_apply(fwd.args()) == 0
    assert psql(db, "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'app.fresh'::regclass AND tgname = 'trg_sweep_audit'") == "1"
    assert psql(db, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'app' AND table_name = 'fresh' AND column_name = 'createdby'") == "1"
    # a row inserted by a third party (a plain session) gets the defaults, the _m columns stay NULL
    c.set_loader_tag("")
    psql(db, "INSERT INTO app.fresh (id) VALUES (1)")
    assert psql(db, "SELECT createdby = SESSION_USER AND createdby_m IS NULL AND createddate_m IS NULL FROM app.fresh") == "t"
