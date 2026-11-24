"""data_models/37_brokerglpair.py: the core logic is unchanged (golden comparison with the original script) and the apply step updates in place."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import sweep_common as c

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "data_models" / "37_brokerglpair.py"
ORIGINAL = ROOT / "tests" / "data" / "models" / "37_brokerglpair_original.py"

SRC_COLS = ["m921_kfund_kproduct", "m921_ktotal", "m921_gldrorgcd", "m921_gldrorg", "m921_gldracct", "m921_gldrcost", "m921_gldrkeycd",
            "m921_gldrcostc1", "m921_gldrtrancd", "m921_gldrconcd", "m921_gldrdesc", "m921_glcrorgcd", "m921_glcrorg", "m921_glcracct",
            "m921_glcrcost", "m921_glcrkeycd", "m921_glcrcostc1", "m921_glcrtrancd", "m921_glcrconcd", "m921_glcrdesc"]
BIZ = ["brokerid", "product_id", "hogan_product_code", "total_number", "debit_orgcode", "debit_org", "debit_acctnr", "debit_costctr", "debit_keycd",
       "debit_costcds", "debit_trancd", "debit_concd", "debit_descr", "credit_orgcode", "credit_org", "credit_acctnr", "credit_costctr",
       "credit_keycd", "credit_costcds", "credit_trancd", "credit_concd", "credit_descr", "is_active"]
TARGET_DDL = ("CREATE TABLE {t} (id serial PRIMARY KEY, brokerid int, product_id int, hogan_product_code varchar(10), total_number varchar(10), "
              "debit_orgcode text, debit_org text, debit_acctnr text, debit_costctr text, debit_keycd text, debit_costcds text, debit_trancd text, "
              "debit_concd text, debit_descr text, credit_orgcode text, credit_org text, credit_acctnr text, credit_costctr text, credit_keycd text, "
              "credit_costcds text, credit_trancd text, credit_concd text, credit_descr text, is_active boolean, createddate timestamp, "
              "createdby text, modifieddate timestamp, modifiedby text, createddate_m timestamp, createdby_m text)")


def q(db, sql):
    return c.run_psql(sql, dbname=db, tuples_only=True, unaligned=True)


def src_row(code, total, tag="x", **over):
    v = {"m921_kfund_kproduct": code, "m921_ktotal": total}
    for col in SRC_COLS[2:]:
        v[col] = f"{tag}_{col[5:]}"
    v.update(over)
    return v


def insert_src(db, rows, table="bss00098.stg_genledgeinterface_v"):
    for r in rows:
        cols = ", ".join(r)
        vals = ", ".join("'" + str(v).replace("'", "''") + "'" for v in r.values())
        q(db, f"INSERT INTO {table} ({cols}) VALUES ({vals})")


BASE_ROWS = [src_row("811", "0000100"), src_row("811", "0000101", "y"), src_row("812", "7", "z"), src_row("826", "00010", "w"),
             src_row("827", " 5 ", "v"), src_row("818", "42", "u"), src_row("819", "9", "t"),
             src_row("811", "0000100"),                                  # an exact duplicate (collapsed)
             src_row("999", "1", "no")]                                  # a product code that is not eligible


@pytest.fixture()
def bgl(pg_db, tmp_path):
    cols_ddl = ", ".join(f"{col} text" for col in SRC_COLS)
    q(pg_db, f"CREATE SCHEMA bss00098; CREATE SCHEMA bcs; CREATE SCHEMA gold_t; CREATE TABLE bss00098.stg_genledgeinterface_v ({cols_ddl}); "
             f"CREATE TABLE bcs.broker (brokerid int PRIMARY KEY, brokername text); "
             f"INSERT INTO bcs.broker VALUES (1, 'Edelman'), (2, 'Empower'), (3, 'LPL'), (4, 'Access Alternative Investments'), (5, 'Other');"
             + TARGET_DDL.format(t="bcs.brokerglpair") + ";" + TARGET_DDL.format(t="gold_t.brokerglpair"))
    insert_src(pg_db, BASE_ROWS)

    class B:
        db = pg_db
        work = tmp_path / "work"
        cfg = tmp_path / "config"

        @staticmethod
        def env(**extra):
            e = dict(os.environ, PGHOST=c.PGHOST, PGPORT=str(c.PGPORT), PGUSER=c.PGUSER, PGDATABASE=pg_db, SWEEP_MODEL_WORKDIR=str(B.work),
                     SWEEP_MODEL_CONFIG=str(B.cfg))
            e.pop("SWEEP_RUN_ID", None)
            e.pop("SWEEP_INPUT_DELTAS", None)
            e.pop("SWEEP_MODE", None)
            if c.PGPASSWORD:
                e["PGPASSWORD"] = c.PGPASSWORD
            e.update(extra)
            return e

        @staticmethod
        def run(*args, script=SCRIPT, env=None):
            r = subprocess.run([sys.executable, str(script), *args], capture_output=True, text=True, env=env or B.env(), cwd=str(tmp_path), timeout=120)
            line = next((ln for ln in r.stdout.splitlines() if ln.startswith("SWEEP_MODEL_RESULT")), None)
            r.result = json.loads(line.split(" ", 1)[1]) if line else None
            return r

        @staticmethod
        def rows(table="bcs.brokerglpair", cols=None):
            cols = cols or ", ".join(BIZ + ["createdby", "modifiedby", "createdby_m"])
            return q(pg_db, f"SELECT string_agg(concat_ws('|', {cols}), E'\\n' ORDER BY hogan_product_code, total_number, debit_descr) FROM {table}")

    B.work.mkdir()
    B.cfg.mkdir()
    return B


# ---------------------------------------------------------------- R1: the core logic did not change
def test_golden_the_adapted_script_produces_the_rows_of_the_original(bgl, tmp_path):
    old_env = bgl.env(VSAM_OUT=str(tmp_path / "old_out"))
    old = bgl.run("--execute", "--target", "gold_t.brokerglpair", "--host", c.PGHOST, "--port", str(c.PGPORT), "--user", c.PGUSER, "--db", bgl.db,
                  script=ORIGINAL, env=old_env)
    assert old.returncode == 0, old.stdout + old.stderr
    new = bgl.run("--execute")
    assert new.returncode == 0, new.stdout + new.stderr
    golden = bgl.rows("gold_t.brokerglpair")
    assert golden and golden.count("\n") == 6                                                    # 7 distinct rows, duplicate collapsed, 999 left out
    assert bgl.rows("bcs.brokerglpair") == golden                                               # every business and audit-name column equal
    assert q(bgl.db, "SELECT string_agg(DISTINCT createdby || modifiedby || createdby_m, ',') FROM bcs.brokerglpair") == "DATA_MIGRATIONDATA_MIGRATIONDATA_MIGRATION"
    assert "100" in golden.splitlines()[0] and "0000100" not in golden                         # clean_total strips the leading zeroes
    assert new.result["status"] == "SUCCESS" and new.result["inserted"] == 7 and new.result["updated"] == 0


def test_the_generated_files_go_to_the_workdir_and_the_map_to_the_config_folder(bgl, tmp_path):
    r = bgl.run()
    assert r.returncode == 0 and r.result["status"] == "DRY_RUN" and r.result["inserted"] == 7
    for name in ("source_rows.csv", "preview.csv", "broker_list.csv", "stage.csv", "load.sql"):
        assert (bgl.work / name).is_file(), name
    assert (bgl.cfg / "hogan_broker_map.csv").is_file() and not (bgl.work / "hogan_broker_map.csv").exists()
    assert not list(tmp_path.glob("*.csv")) and not list(tmp_path.glob("*.sql"))                  # nothing in the current directory
    assert q(bgl.db, "SELECT count(*) FROM bcs.brokerglpair") == "0"                              # dry run writes nothing
    (bgl.cfg / "hogan_broker_map.csv").write_text("hogan_product_code,broker_name\n811,Empower\n812,Edelman\n826,Empower\n827,Empower\n818,LPL\n819,LPL\n")
    assert bgl.run("--execute").returncode == 0                                                   # the edited map is read from the config folder
    assert q(bgl.db, "SELECT brokerid FROM bcs.brokerglpair WHERE hogan_product_code = '811' LIMIT 1") == "2"


# ---------------------------------------------------------------- A4: update in place
def test_rerunning_changes_nothing_and_a_changed_row_keeps_its_id(bgl):
    assert bgl.run("--execute").returncode == 0
    before = q(bgl.db, "SELECT string_agg(id || ':' || hogan_product_code || ':' || total_number || ':' || createddate || ':' || modifieddate, ',' "
                       "ORDER BY id) FROM bcs.brokerglpair")
    again = bgl.run("--execute")
    assert again.result["inserted"] == 0 and again.result["updated"] == 0 and again.result["unchanged"] == 7 and again.result["skipped_foreign"] == 0
    assert q(bgl.db, "SELECT string_agg(id || ':' || hogan_product_code || ':' || total_number || ':' || createddate || ':' || modifieddate, ',' "
                     "ORDER BY id) FROM bcs.brokerglpair") == before                              # untouched rows are not even re-stamped
    q(bgl.db, "UPDATE bss00098.stg_genledgeinterface_v SET m921_gldrdesc = 'changed desc' WHERE m921_kfund_kproduct = '812'")
    r = bgl.run("--execute")
    assert (r.result["inserted"], r.result["updated"], r.result["unchanged"]) == (0, 1, 6)
    row = q(bgl.db, "SELECT id || '|' || debit_descr || '|' || modifiedby || '|' || (modifieddate > createddate) || '|' || createdby FROM bcs.brokerglpair "
                    "WHERE hogan_product_code = '812'")
    assert row.split("|")[1:] == ["changed desc", "DATA_MIGRATION", "true", "DATA_MIGRATION"]
    assert row.split("|")[0] == before.split(",")[[x.split(":")[1] for x in before.split(",")].index("812")].split(":")[0]       # the id did not change
    assert q(bgl.db, "SELECT count(*) FROM bcs.brokerglpair") == "7"
    assert bgl.run("--execute").result["updated"] == 0                                           # the same change again: nothing


def test_a_row_not_created_by_the_migration_is_left_alone_and_counted(bgl):
    assert bgl.run("--execute").returncode == 0
    q(bgl.db, "UPDATE bcs.brokerglpair SET createdby = 'app_user', debit_descr = 'edited by hand' WHERE hogan_product_code = '812'")
    q(bgl.db, "UPDATE bss00098.stg_genledgeinterface_v SET m921_gldrdesc = 'source change' WHERE m921_kfund_kproduct = '812'")
    r = bgl.run("--execute")
    assert (r.result["inserted"], r.result["updated"], r.result["skipped_foreign"]) == (0, 0, 1)
    assert q(bgl.db, "SELECT debit_descr || '|' || createdby || '|' || modifiedby FROM bcs.brokerglpair WHERE hogan_product_code = '812'") == "edited by hand|app_user|DATA_MIGRATION"


def test_a_changed_key_creates_a_new_row_and_keeps_the_old_one(bgl):
    assert bgl.run("--execute").returncode == 0
    q(bgl.db, "UPDATE bss00098.stg_genledgeinterface_v SET m921_ktotal = '8' WHERE m921_kfund_kproduct = '812'")
    r = bgl.run("--execute")
    assert (r.result["inserted"], r.result["updated"]) == (1, 0)
    assert q(bgl.db, "SELECT string_agg(total_number, ',' ORDER BY total_number) FROM bcs.brokerglpair WHERE hogan_product_code = '812'") == "7,8"


def test_a_key_that_is_not_unique_in_the_source_stops_the_run_loudly_even_in_the_dry_run(bgl):
    insert_src(bgl.db, [src_row("811", "100", "other")])                                       # same key as a row of BASE_ROWS, different content
    for args in (("--execute",), ()):
        r = bgl.run(*args)
        assert r.returncode != 0 and "UPDATE KEY NOT UNIQUE" in r.stderr and "hogan_product_code" in r.stderr
        assert r.result["status"] == "FAILED" and "UPDATE KEY NOT UNIQUE" in r.result["error"]
    assert q(bgl.db, "SELECT count(*) FROM bcs.brokerglpair") == "0"


def test_a_key_matching_several_target_rows_rolls_everything_back(bgl):
    assert bgl.run("--execute").returncode == 0
    q(bgl.db, "INSERT INTO bcs.brokerglpair (hogan_product_code, total_number, createdby) VALUES ('812', '7', 'DATA_MIGRATION')")
    q(bgl.db, "UPDATE bss00098.stg_genledgeinterface_v SET m921_gldrdesc = 'new', m921_ktotal = '55' WHERE m921_kfund_kproduct = '826'")
    r = bgl.run("--execute")
    assert r.returncode != 0 and "matches more than one existing row" in (r.stderr + r.stdout)
    assert q(bgl.db, "SELECT count(*) FROM bcs.brokerglpair WHERE total_number = '55'") == "0"        # the whole transaction rolled back


def test_a_missing_unique_index_is_a_warning_and_a_unique_index_silences_it(bgl):
    assert "has no unique index on the update key" in bgl.run().stdout
    q(bgl.db, "CREATE UNIQUE INDEX ON bcs.brokerglpair (hogan_product_code, total_number)")
    assert "has no unique index" not in bgl.run().stdout


def test_replace_is_refused_under_the_runner_and_the_old_checks_still_stop_a_bad_run(bgl):
    r = bgl.run("--execute", "--replace", env=bgl.env(SWEEP_RUN_ID="r1"))
    assert r.returncode != 0 and "--replace deletes rows and is refused" in r.stderr
    assert q(bgl.db, "SELECT count(*) FROM bcs.brokerglpair") == "0"
    (bgl.cfg / "hogan_broker_map.csv").write_text("hogan_product_code,broker_name\n811,Nobody\n812,Edelman\n826,Empower\n827,Empower\n818,LPL\n819,LPL\n")
    r = bgl.run("--execute")
    assert r.returncode != 0 and "NOT EXECUTED" in r.stderr and "broker lookup incomplete" in r.stderr
    assert bgl.run("--execute", "--allow-null-broker").returncode == 0                           # the explicit override still works
    assert q(bgl.db, "SELECT count(*) FROM bcs.brokerglpair WHERE brokerid IS NULL") == "2"


# ---------------------------------------------------------------- delta mode
def _delta_table(db, rows, seqs, schema_table="bss00098__stg_genledgeinterface_v"):
    cols_ddl = ", ".join(f"{col} text" for col in SRC_COLS)
    q(db, f"CREATE SCHEMA IF NOT EXISTS sweep_delta; CREATE UNLOGGED TABLE IF NOT EXISTS sweep_delta.{schema_table} ({cols_ddl}, _seq bigint, _op text, _row_no bigint)")
    for r, sq in zip(rows, seqs):
        cols = ", ".join(list(r) + ["_seq", "_op"])
        vals = ", ".join("'" + str(v) + "'" for v in r.values()) + f", {sq}, 'U'"
        q(db, f"INSERT INTO sweep_delta.{schema_table} ({cols}) VALUES ({vals})")


def test_delta_mode_processes_only_the_rows_of_the_source_delta(bgl):
    assert bgl.run("--execute").returncode == 0
    q(bgl.db, "UPDATE bss00098.stg_genledgeinterface_v SET m921_gldrdesc = 'ch812' WHERE m921_kfund_kproduct = '812'")
    q(bgl.db, "UPDATE bss00098.stg_genledgeinterface_v SET m921_gldrdesc = 'ch818' WHERE m921_kfund_kproduct = '818'")
    _delta_table(bgl.db, [src_row("812", "7", "z", m921_gldrdesc="ch812")], [4])                  # only the 812 change is in the delta window
    _delta_table(bgl.db, [src_row("818", "42", "u", m921_gldrdesc="ch818")], [9])                 # a later delta that is outside the window
    spec = {"bss00098.stg_genledgeinterface_v": {"full": False, "delta_table": "sweep_delta.bss00098__stg_genledgeinterface_v", "seq_from": 3, "seq_to": 5},
            "bcs.broker": {"full": False, "delta_table": "sweep_delta.bcs__broker", "seq_from": 0, "seq_to": 0}}
    q(bgl.db, "CREATE TABLE sweep_delta.bcs__broker (brokerid int, brokername text, _seq bigint, _op text, _row_no bigint)")
    r = bgl.run("--execute", "--mode", "delta", env=bgl.env(SWEEP_INPUT_DELTAS=json.dumps(spec)))
    assert r.returncode == 0, r.stdout + r.stderr
    assert (r.result["mode"], r.result["updated"], r.result["inserted"], r.result["affected_source_rows"], r.result["rows_considered"]) == ("delta", 1, 0, 1, 1)
    assert q(bgl.db, "SELECT debit_descr FROM bcs.brokerglpair WHERE hogan_product_code = '812'") == "ch812"
    assert q(bgl.db, "SELECT debit_descr FROM bcs.brokerglpair WHERE hogan_product_code = '818'") == "u_gldrdesc"        # not in this window
    assert bgl.run("--execute", "--mode", "delta", env=bgl.env(SWEEP_INPUT_DELTAS=json.dumps(spec))).result["updated"] == 0     # idempotent
    # an input read in full, or a changed broker table, affects every row
    full = bgl.run("--execute", "--mode", "delta", env=bgl.env(SWEEP_INPUT_DELTAS=json.dumps({"bss00098.stg_genledgeinterface_v": {"full": True}})))
    assert full.result["updated"] == 1 and full.result["rows_considered"] == 7                    # 818 is picked up now
    q(bgl.db, "INSERT INTO sweep_delta.bcs__broker (brokerid, brokername, _seq, _op) VALUES (5, 'Other', 1, 'I')")
    spec["bss00098.stg_genledgeinterface_v"]["seq_from"] = 9
    spec["bcs.broker"]["seq_to"] = 1
    r = bgl.run("--mode", "delta", env=bgl.env(SWEEP_INPUT_DELTAS=json.dumps(spec)))
    assert r.result["rows_considered"] == 7 and r.result["status"] == "DRY_RUN"
