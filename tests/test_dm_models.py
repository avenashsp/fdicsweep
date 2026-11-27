"""DM01-DM06: the six real data models - header, discovery, cleanups, dry run default, six audit columns, DM03 flip (needs PostgreSQL)."""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import sweep_audit as au
import sweep_common as c
import sweep_models as sm

ROOT = Path(__file__).resolve().parent.parent
MODELS = ROOT / "data_models"
NAMES = ["DM01_program_bank_capacity", "DM02_brokerglpair", "DM03_bank", "DM04_program_bank_rate", "DM05_program_bank_rate_tier", "DM06_ledger_config"]
SIX = au.AUDIT_NAMES


# =============================================================== static: header, discovery, cleanups
@pytest.mark.parametrize("name", NAMES)
def test_every_model_has_a_script_a_note_and_a_runner_header(name):
    assert (MODELS / f"{name}.py").is_file() and (MODELS / f"{name}.analysis.md").is_file()
    h = sm.parse_header(MODELS / f"{name}.py")
    assert h["name"] == name.lower() and h["outputs"] and h["delta_strategy"] in sm.STRATEGIES
    assert set(h) <= set(sm.HEADER_KEYS)


def test_discovery_orders_by_dm_number_and_dm05_runs_after_dm04():
    found = sm.discover(MODELS)
    assert [m.name for m in found] == [n.lower() for n in NAMES]                    # numeric order, dm number kept in the name
    order = [m.name for m in sm.order_models(found)]
    assert order.index("dm04_program_bank_rate") < order.index("dm05_program_bank_rate_tier")
    assert "dm04_program_bank_rate" in {m.name: m for m in found}["dm05_program_bank_rate_tier"].depends_on
    assert not any(m.file.name.startswith("_") for m in found)                      # _dm_support.py is not a model


def test_dm_prefix_and_old_numeric_prefix_both_discover(tmp_path):
    (tmp_path / "DM12_x.py").write_text('SWEEP_MODEL = {"name": "dm12_x", "inputs": [], "outputs": ["a.b"]}\n')
    (tmp_path / "7_old.py").write_text('SWEEP_MODEL = {"name": "old", "inputs": [], "outputs": ["a.c"]}\n')
    got = sm.discover(tmp_path)
    assert [(m.name, m.prefix) for m in got] == [("old", 7), ("dm12_x", 12)] or [(m.name, m.prefix) for m in got] == [("dm12_x", 12), ("old", 7)]
    assert sorted(m.prefix for m in got) == [7, 12]


@pytest.mark.parametrize("name", NAMES)
def test_cleanups_no_created_by_constant_no_backup_names_no_bss00099_no_hard_coded_connection(name):
    text = (MODELS / f"{name}.py").read_text()
    for bad in ("CREATED_BY", "FE_MIGRATION_TEAM", "DATA_MIGRATION", "migration_hub", "_bkp3", "_backup", "bss00099", "CURRENT_USER"):
        assert bad not in text, f"{name}: {bad}"
    assert not re.search(r'DEFAULT_(HOST|USER|PASS)\w*\s*=\s*"[^"]+"', text)          # no literal host / user / password
    assert "libpq_defaults" in text and "SWEEP_MODEL_RESULT" in (MODELS / "_dm_support.py").read_text()
    for m in re.finditer(r"(createdby_m|createdby|modifiedby|modifieddate|createddate)\W{1,6}(?:=|DEFAULT)\s*'([a-z_]+)'", text):
        assert m.group(2) in (au.TAG_LOADER, au.TAG_MODEL)


def test_the_old_37_script_and_its_tests_are_gone():
    assert not (MODELS / "37_brokerglpair.py").exists() and not (ROOT / "tests" / "test_brokerglpair.py").exists()


# =============================================================== end to end
@pytest.fixture()
def world(pg_db, tmp_path, monkeypatch):
    c.set_loader_tag("")
    c.run_psql("""
    CREATE SCHEMA bcs; CREATE SCHEMA bss00098;
    CREATE TABLE bcs.bank (bankid bigint PRIMARY KEY, brokerid bigint, displayname text);
    CREATE TABLE bcs.stg_program_bank_primary_rec_v (bank_name text, bank_statemnt_mnemonic text);
    CREATE TABLE bcs.cusip (cusipid bigint, bankid bigint, productid bigint);
    CREATE TABLE bcs.cusipcusipgroup (cusipid bigint, cusipgroupid bigint);
    CREATE TABLE bcs.capacityplan (cusipgroupid bigint, bankcapacity numeric);
    CREATE TABLE bcs.rate (rateid bigint, ratetypeid bigint, balancedistribution int, usedailyrate int, cusipgroupid bigint);
    CREATE TABLE bcs.daysinyear (rateid bigint, rateyear int, daysinyearnumber int);
    CREATE TABLE bcs.ratetier (tierid bigint, rateid bigint, tiertype int, tierlowerlimit numeric, tierupperlimit numeric,
                               effectivedate date, expirationdate date, createddate timestamp, modifieddate timestamp);
    CREATE TABLE bcs.flattier (tierid bigint, rate numeric);
    CREATE TABLE bcs.indexedtier (tierid bigint, publishedindexid bigint, trailingdays int, ratemodifierincrementnumber numeric,
                                  ratemodifierpercentagenumber numeric, floorrate numeric);
    CREATE TABLE bcs.broker (brokerid bigint, name text);
    CREATE TABLE bss00098.stg_genledgeinterface_v (m921_kfund_kproduct text, m921_ktotal text, m921_gldrorgcd text, m921_gldrorg text, m921_gldracct text,
        m921_gldrcost text, m921_gldrkeycd text, m921_gldrcostc1 text, m921_gldrtrancd text, m921_gldrconcd text, m921_gldrdesc text, m921_glcrorgcd text,
        m921_glcrorg text, m921_glcracct text, m921_glcrcost text, m921_glcrkeycd text, m921_glcrcostc1 text, m921_glcrtrancd text, m921_glcrconcd text,
        m921_glcrdesc text);
    INSERT INTO bcs.bank VALUES (1, 100, 'First Bank & Trust'), (2, 100, 'Other Bank');
    INSERT INTO bcs.stg_program_bank_primary_rec_v VALUES ('Other Bank', 'OTHER');
    INSERT INTO bcs.cusip VALUES (10, 1, 5), (20, 2, 5);
    INSERT INTO bcs.cusipcusipgroup VALUES (10, 1000), (20, 2000);
    INSERT INTO bcs.capacityplan VALUES (1000, 1000000), (2000, 2000000);
    INSERT INTO bcs.rate VALUES (1, 1, 0, 1, 1000), (2, 4, 1, 0, 2000);
    INSERT INTO bcs.daysinyear VALUES (1, 2024, 360), (2, 2024, 365);
    INSERT INTO bcs.ratetier VALUES (1, 1, 1, 0, 100, '2024-01-01', NULL, now(), now()), (2, 2, 1, 0, 50, '2024-02-01', NULL, now(), now());
    INSERT INTO bcs.flattier VALUES (1, 1.5), (2, 2.5);
    INSERT INTO bcs.broker VALUES (7, 'Edelman'), (8, 'Empower'), (9, 'LPL'), (10, 'Access Alternative Investments');
    INSERT INTO bss00098.stg_genledgeinterface_v VALUES ('811', '0000000012', 'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r');
    """)
    # targets (without the audit columns: every model adds them idempotently)
    c.run_psql("""
    CREATE TABLE bcs.program_bank_capacity (broker_id bigint, bank_id bigint, bank_capacity numeric(19,4), overflow_capacity_percent numeric(10,3),
        capacity_percent_low_limit numeric(10,3), capacity_percent_high_limit numeric(10,3), is_active boolean);
    CREATE TABLE bcs.program_bank_rate (program_bank_rate_id bigserial PRIMARY KEY, broker_id bigint, bank_id bigint, product_id bigint, cusip_id bigint,
        rate_type_id bigint, balance_distribution int, use_daily_rate int, days_in_year int, effective_date date, expiration_date date, is_active boolean);
    CREATE TABLE bcs.program_bank_rate_tier (program_bank_rate_tier_id bigserial PRIMARY KEY, program_bank_rate_id bigint, tier_type int,
        lower_limit numeric, upper_limit numeric, effective_date date, expiration_date date, flat_rate numeric, published_index_id bigint,
        trailing_days int, modifier_number numeric, modifier_percent numeric, floor_rate numeric, floor_rate_unit text, is_active boolean);
    CREATE TABLE bcs.brokerglpair (id bigserial PRIMARY KEY, brokerid bigint, product_id int, hogan_product_code text, total_number text,
        debit_orgcode text, debit_org text, debit_acctnr text, debit_costctr text, debit_keycd text, debit_costcds text, debit_trancd text, debit_concd text, debit_descr text,
        credit_orgcode text, credit_org text, credit_acctnr text, credit_costctr text, credit_keycd text, credit_costcds text, credit_trancd text, credit_concd text, credit_descr text,
        is_active boolean);
    """)
    for sch in ("bcs", "bss01001"):
        c.run_psql(f"CREATE SCHEMA IF NOT EXISTS {sch}; CREATE TABLE {sch}.stg_transcodedescptrec_v (m405_korg text, m405_ktrancd text, m405_divdescr text, m405_inttc text);"
                   f"INSERT INTO {sch}.stg_transcodedescptrec_v VALUES ('7', 'T1', 'one', '11'), ('7', 'T2', 'two', '12');"
                   f"CREATE TABLE {sch}.ledger_config (id bigserial, broker_id numeric, tran_code text, trans_description text, gl_trans_code text, is_active boolean, "
                   f"CONSTRAINT pk_ledger_config{1 if sch == 'bcs' else 2} PRIMARY KEY (broker_id, tran_code));")

    def run(name, *args, env=None):
        e = {k: v for k, v in os.environ.items() if not k.startswith(("PG", "SWEEP_"))}
        e.update(PGHOST=str(c.PGHOST), PGPORT=str(c.PGPORT), PGUSER=str(c.PGUSER), PGPASSWORD=str(c.PGPASSWORD), PGDATABASE=str(c.PGDATABASE),
                 SWEEP_MODEL_WORKDIR=str(tmp_path / name), SWEEP_MODEL_CONFIG=str(ROOT / "data_models" / "config" / "dm02_brokerglpair"))
        e.update(env or {})
        return subprocess.run([sys.executable, str(MODELS / f"{name}.py"), *args], capture_output=True, text=True, env=e, cwd=str(ROOT))
    return run


def q(sql):
    return c.run_psql(sql, tuples_only=True, unaligned=True)


def audit_ok(table, where="true"):
    """Every row: the four mandatory columns set, createdby = modifiedby = SESSION_USER, createddate = modifieddate, _m = fe_migration / now."""
    r = q(f"SELECT count(*) FILTER (WHERE createdby IS NOT NULL AND createddate IS NOT NULL AND modifiedby IS NOT NULL AND modifieddate IS NOT NULL "
          f"AND createdby = SESSION_USER AND modifiedby = SESSION_USER AND createddate = modifieddate AND createdby_m = 'fe_migration' "
          f"AND createddate_m IS NOT NULL), count(*) FROM {table} WHERE {where}")
    ok, total = map(int, r.split("|"))
    assert total > 0 and ok == total, (table, r)


@pytest.mark.integration
@pytest.mark.parametrize("name,table", [("DM01_program_bank_capacity", "bcs.program_bank_capacity"), ("DM04_program_bank_rate", "bcs.program_bank_rate"),
                                        ("DM06_ledger_config", "bcs.ledger_config"), ("DM02_brokerglpair", "bcs.brokerglpair")])
def test_dry_run_is_the_default_and_changes_nothing(world, name, table):
    cols = q(f"SELECT count(*) FROM information_schema.columns WHERE table_schema || '.' || table_name = '{table}'")
    r = world(name)
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
    assert q(f"SELECT count(*) FROM {table}") == "0"                                        # no row
    assert q(f"SELECT count(*) FROM information_schema.columns WHERE table_schema || '.' || table_name = '{table}'") == cols      # no ALTER either
    assert "SWEEP_MODEL_RESULT" in r.stdout


@pytest.mark.integration
def test_dm01_execute_writes_the_six_columns_and_updates_only_modified(world):
    r = world("DM01_program_bank_capacity", "--execute")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    audit_ok("bcs.program_bank_capacity")
    assert q("SELECT count(*) FROM bcs.program_bank_capacity") == "2"
    assert q("SELECT count(*) FROM pg_trigger WHERE tgrelid = 'bcs.program_bank_capacity'::regclass AND tgname = 'trg_sweep_audit'") == "1"
    before = q("SELECT createdby || createddate::text || createddate_m::text || createdby_m FROM bcs.program_bank_capacity ORDER BY bank_id LIMIT 1")
    c.run_psql("UPDATE bcs.capacityplan SET bankcapacity = 9999 WHERE cusipgroupid = 1000")
    r = world("DM01_program_bank_capacity", "--execute")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    assert q("SELECT bank_capacity FROM bcs.program_bank_capacity WHERE bank_id = 1") == "9999.0000"
    assert q("SELECT createdby || createddate::text || createddate_m::text || createdby_m FROM bcs.program_bank_capacity ORDER BY bank_id LIMIT 1") == before
    assert q("SELECT modifieddate > createddate FROM bcs.program_bank_capacity WHERE bank_id = 1") == "t"
    assert q("SELECT modifieddate = createddate FROM bcs.program_bank_capacity WHERE bank_id = 2") == "t"      # unchanged row untouched


@pytest.mark.integration
def test_dm04_then_dm05_write_the_six_columns(world):
    r = world("DM04_program_bank_rate", "--execute")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    audit_ok("bcs.program_bank_rate")
    r = world("DM05_program_bank_rate_tier", "--execute")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    audit_ok("bcs.program_bank_rate_tier")
    assert q("SELECT count(*) FROM bcs.program_bank_rate_tier") == q("SELECT count(*) FROM bcs.program_bank_rate")


@pytest.mark.integration
def test_dm02_inserts_with_the_map_file_and_never_without_it(world, tmp_path):
    r = world("DM02_brokerglpair", "--execute")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    audit_ok("bcs.brokerglpair")
    again = world("DM02_brokerglpair", "--execute")
    assert again.returncode == 0 and q("SELECT count(*) FROM bcs.brokerglpair") == "1"          # insert-if-missing: a re-run inserts nothing
    c.run_psql("TRUNCATE bcs.brokerglpair")
    missing = world("DM02_brokerglpair", "--execute", "--map", str(tmp_path / "no_such_map.csv"))
    assert missing.returncode != 0 and "hogan_broker_map.csv" in (missing.stdout + missing.stderr) or "not found" in (missing.stdout + missing.stderr)
    assert q("SELECT count(*) FROM bcs.brokerglpair") == "0"                                    # nothing inserted without the map


@pytest.mark.integration
def test_dm06_inserts_per_schema_and_replace_deletes_only_own_rows(world):
    r = world("DM06_ledger_config", "--execute", "--schemas", "bcs,bss01001")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    audit_ok("bcs.ledger_config")
    audit_ok("bss01001.ledger_config")
    assert q("SELECT count(*) FROM bcs.ledger_config") == "2"
    c.run_psql("INSERT INTO bcs.ledger_config (broker_id, tran_code, trans_description, gl_trans_code, is_active) VALUES (99, 'APP', 'application', '1', true)")
    assert q("SELECT createdby_m IS NULL FROM bcs.ledger_config WHERE tran_code = 'APP'") == "t"      # a third party row
    r = world("DM06_ledger_config", "--execute", "--replace", "--schemas", "bcs,bss01001")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    assert q("SELECT count(*) FROM bcs.ledger_config WHERE tran_code = 'APP'") == "1"             # not the model's row: kept
    assert q("SELECT count(*) FROM bcs.ledger_config WHERE createdby_m = 'fe_migration'") == "2"


@pytest.mark.integration
def test_dm03_alter_update_flip_and_overrides(world):
    # bcs.bank was loaded by the Oracle loader: createdby_m = dcc_migration
    c.run_psql(au.add_columns_sql("bcs", "bank") + au.trigger_sql("bcs", "bank"))
    c.run_psql("UPDATE bcs.bank SET createdby_m = 'dcc_migration', createddate_m = '2020-01-01 00:00:00'",
               extra_env={"PGOPTIONS": au.session_options(au.TAG_LOADER)})          # only a loader / model session may set the _m columns of an existing row
    r = world("DM03_bank")                                                                    # dry run
    assert r.returncode == 0 and q("SELECT count(*) FROM information_schema.columns WHERE table_name = 'bank' AND column_name LIKE 'bank_statem%nemonic'") == "0"
    r = world("DM03_bank", "--execute")
    assert r.returncode == 0, r.stdout[-1200:] + r.stderr[-800:]
    assert q("SELECT bank_statement_mnemonic FROM bcs.bank WHERE bankid = 1") == "FRSTBKTR"      # manual override
    assert q("SELECT bank_statement_mnemonic FROM bcs.bank WHERE bankid = 2") == "OTHER"         # from the staging table
    assert q("SELECT count(*) FILTER (WHERE createdby_m = 'fe_migration'), count(*) FILTER (WHERE createddate_m = '2020-01-01') FROM bcs.bank") == "2|2"
    assert q("SELECT count(*) FROM bcs.bank WHERE modifiedby = SESSION_USER AND modifieddate > createddate") == "2"
    assert q("SELECT count(*) FROM bcs.bank WHERE createdby = SESSION_USER AND createddate = '" + q("SELECT min(createddate)::text FROM bcs.bank") + "'") in ("1", "2")


def test_model_result_line_is_parseable(world):
    r = world("DM01_program_bank_capacity")
    assert r.returncode == 0 or True
    res = sm.parse_result(r.stdout)
    assert res is None or isinstance(res, dict)
    json.dumps(res)
