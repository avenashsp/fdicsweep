"""6.3.0: the six canonical audit columns - DDL, trigger, loader session setting, upsert rules, fwd-ddl post-step, checks (needs PostgreSQL)."""
import re
from pathlib import Path

import pytest

import sweep_audit as au
import sweep_common as c
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_pipeline as pl

ROOT = Path(__file__).resolve().parent.parent
SIX = ("createdby", "createddate", "modifiedby", "modifieddate", "createddate_m", "createdby_m")


# =============================================================== DDL generators (no database needed)
def test_the_six_columns_have_the_exact_types_and_defaults():
    spec = {n: (t, nn, d) for n, t, nn, d in au.AUDIT_COLUMNS}
    assert tuple(spec) == SIX
    assert spec["createdby"] == ("character varying(40)", True, "SESSION_USER")
    assert spec["modifiedby"] == ("character varying(40)", True, "SESSION_USER")
    assert spec["createddate"] == ("timestamp without time zone", True, "now()")
    assert spec["modifieddate"] == ("timestamp without time zone", True, "now()")
    assert spec["createddate_m"][1] is False and spec["createdby_m"][1] is False             # only the two _m columns may be NULL
    assert "sweep.loader" in spec["createdby_m"][2] and "sweep.loader" in spec["createddate_m"][2]
    assert not any("CURRENT_USER" in d for _, _, _, d in au.AUDIT_COLUMNS)


def test_vsam_ddl_has_all_six_and_keeps_legacy_variants():
    from vsam.common_schema import build_table_ddl
    cols = [{"col_name": "ID", "sql_type": "integer"}, {"col_name": "created_at", "sql_type": "timestamp"},
            {"col_name": "created_by", "sql_type": "text"}]
    pre, _ = build_table_ddl("s", "t", cols, ["id"])
    for name in SIX:
        assert f'"{name}"' in pre
    assert '"created_at" timestamp' in pre and '"created_by" text' in pre                   # legacy variants are never touched
    assert "CURRENT_USER" not in pre
    assert "trg_sweep_audit" in pre


def test_oracle_generator_uses_the_audit_module():
    src = (ROOT / "oracle" / "oracle_04_generate_ddl.py").read_text()
    assert "sweep_audit.create_table_lines" in src and "sweep_audit.trigger_sql" in src
    assert "CURRENT_USER" not in src and "fn_update_audit_timestamp" not in src


def test_no_current_user_for_audit_and_no_foreign_literals_anywhere():
    """Grep-style rule: no CURRENT_USER for an audit column in the code, and no literal other than dcc_migration / fe_migration assigned to one."""
    files = [p for p in ROOT.rglob("*.py") if "tests" not in p.parts and "work" not in p.parts and ".git" not in p.parts]
    for p in files:
        text = p.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(r'["\']?(createdby|modifiedby|createdby_m)["\']?\s*(=|DEFAULT)\s*CURRENT_USER', text):
            pytest.fail(f"{p.relative_to(ROOT)}: CURRENT_USER assigned to {m.group(1)}")
        for m in re.finditer(r'["\']?(createdby_m|createdby|modifiedby)["\']?\s*(?:=|DEFAULT)\s*\'([A-Za-z_]+)\'', text):
            if m.group(2) not in (au.TAG_LOADER, au.TAG_MODEL):
                pytest.fail(f"{p.relative_to(ROOT)}: literal {m.group(2)!r} assigned to {m.group(1)}")


# =============================================================== trigger and defaults
pytestmark_db = pytest.mark.integration


@pytest.fixture()
def tbl(pg_db):
    c.set_loader_tag("")                                                                    # a plain (third-party) session
    c.run_psql("CREATE SCHEMA IF NOT EXISTS ac; DROP TABLE IF EXISTS ac.t; CREATE TABLE ac.t (id int PRIMARY KEY, v text);\n"
               + au.add_columns_sql("ac", "t") + au.trigger_sql("ac", "t"))
    return "ac.t"


def _q(sql, **kw):
    return c.run_psql(sql, tuples_only=True, unaligned=True, **kw)


@pytest.mark.integration
def test_trigger_insert_and_update_rules(tbl):
    c.run_psql("INSERT INTO ac.t (id, v) VALUES (1, 'a')")
    row = _q("SELECT createdby = SESSION_USER, modifiedby = SESSION_USER, createddate = modifieddate, createdby_m IS NULL, createddate_m IS NULL FROM ac.t")
    assert row == "t|t|t|t|t"                                                                  # a third-party insert: both _m columns NULL
    before = _q("SELECT createddate::text FROM ac.t")
    c.run_psql("SELECT pg_sleep(0.05); UPDATE ac.t SET v = 'b', createdby = 'hacker', createddate = '2000-01-01', createdby_m = 'x', createddate_m = now() WHERE id = 1")
    assert _q("SELECT createdby = SESSION_USER, createddate::text FROM ac.t") == f"t|{before}"      # created* never change on an update
    assert _q("SELECT createdby_m IS NULL AND createddate_m IS NULL FROM ac.t") == "t"             # plain session: the _m columns stay as they were
    assert _q("SELECT modifieddate > createddate FROM ac.t") == "t"


@pytest.mark.integration
def test_loader_session_setting_fills_the_m_columns_and_plain_session_does_not(tbl):
    c.run_psql("INSERT INTO ac.t (id) VALUES (1)", extra_env={"PGOPTIONS": au.session_options(au.TAG_LOADER)})
    c.run_psql("INSERT INTO ac.t (id) VALUES (2)")
    assert _q("SELECT createdby_m, createddate_m IS NOT NULL FROM ac.t WHERE id = 1") == "dcc_migration|t"
    assert _q("SELECT createdby_m IS NULL AND createddate_m IS NULL FROM ac.t WHERE id = 2") == "t"


@pytest.mark.integration
def test_model_session_tag_is_fe_migration(tbl):
    c.run_psql("INSERT INTO ac.t (id) VALUES (3)", extra_env={"PGOPTIONS": au.session_options(au.TAG_MODEL)})
    assert _q("SELECT createdby_m FROM ac.t") == "fe_migration"


@pytest.mark.integration
def test_add_columns_is_idempotent_and_keeps_existing_rows_valid(pg_db):
    c.set_loader_tag("")
    c.run_psql("CREATE SCHEMA IF NOT EXISTS ac; DROP TABLE IF EXISTS ac.old; CREATE TABLE ac.old (id int); INSERT INTO ac.old VALUES (1);\n"
               + au.add_columns_sql("ac", "old") + au.add_columns_sql("ac", "old") + au.trigger_sql("ac", "old") + au.trigger_sql("ac", "old"))
    assert _q("SELECT count(*) FROM ac.old WHERE createdby IS NOT NULL AND modifieddate IS NOT NULL AND createdby_m IS NULL") == "1"
    assert _q("SELECT count(*) FROM pg_trigger WHERE tgrelid = 'ac.old'::regclass AND tgname = 'trg_sweep_audit'") == "1"


# =============================================================== staged load / upsert
@pytest.fixture()
def loader(pg_db):
    o6.META_CACHE.clear()
    o6.PK_CACHE.clear()
    pl.ensure_reject_table()
    c.run_psql(f"TRUNCATE {pl.REJECT_TABLE}")
    return pg_db


def _t(schema, table, ddl):
    c.run_psql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"; DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE; CREATE TABLE "{schema}"."{table}" ({ddl});\n'
               + au.add_columns_sql(schema, table) + au.trigger_sql(schema, table))


def _load(gz_file, name, text, schema, table, **kw):
    o6.META_CACHE.clear()
    path = gz_file(name, text)
    job = {"uri": str(path), "target_schema": schema, "target_table": table, "size_bytes": path.stat().st_size, "size_mb": 0.01,
           "filename": path.name, "file_format": "gzip"}
    return o6.load_single_file(job, worker_id=1, **kw)


@pytest.mark.integration
def test_staged_load_marks_rows_dcc_migration_and_an_update_never_touches_created_or_m(loader, gz_file):
    _t("ad", "t", "id int PRIMARY KEY, v text")
    o = _load(gz_file, "a.csv.gz", "id,v\n1,a\n2,b\n", "ad", "t")
    assert (o.status, o.inserted) == ("SUCCESS", 2), o.error
    assert _q("SELECT count(*) FROM ad.t WHERE createdby_m = 'dcc_migration' AND createddate_m IS NOT NULL AND createdby = SESSION_USER") == "2"
    assert _q("SELECT count(*) FROM ad.t WHERE createddate = modifieddate AND modifiedby = SESSION_USER") == "2"
    created = _q("SELECT createddate::text || '|' || createddate_m::text FROM ad.t WHERE id = 1")
    o = _load(gz_file, "b.csv.gz", "id,v\n1,CHANGED\n2,b\n", "ad", "t")
    assert (o.updated, o.unchanged) == (1, 1)
    assert _q("SELECT createddate::text || '|' || createddate_m::text FROM ad.t WHERE id = 1") == created
    assert _q("SELECT createdby_m FROM ad.t WHERE id = 1") == "dcc_migration"
    assert _q("SELECT modifieddate > createddate FROM ad.t WHERE id = 1") == "t"            # only modifiedby / modifieddate move


@pytest.mark.integration
def test_a_source_column_named_like_an_audit_column_is_data_with_nulls_filled(loader, gz_file):
    _t("ae", "t", "id int PRIMARY KEY, v text")
    o = _load(gz_file, "a.csv.gz", "id,v,createdby\n1,a,SRC_USER\n2,b,\n", "ae", "t")
    assert (o.status, o.inserted) == ("SUCCESS", 2), o.error
    assert _q("SELECT createdby FROM ae.t WHERE id = 1") == "SRC_USER"
    assert _q("SELECT createdby = SESSION_USER FROM ae.t WHERE id = 2") == "t"              # the NULL of the file was filled with the default


@pytest.mark.integration
def test_audit_columns_never_count_as_a_change(loader, gz_file):
    _t("af", "t", "id int PRIMARY KEY, v text")
    _load(gz_file, "a.csv.gz", "id,v,modifiedby\n1,a,X\n", "af", "t")
    o = _load(gz_file, "b.csv.gz", "id,v,modifiedby\n1,a,Y\n", "af", "t")
    assert (o.updated, o.unchanged) == (0, 1)


@pytest.mark.integration
def test_fast_bulk_uses_the_session_setting_and_fails_loudly_without_it(loader, gz_file):
    _t("ag", "t", "id int, v text")
    o = _load(gz_file, "a.csv.gz", "id,v\n1,a\n2,b\n", "ag", "t", fast_bulk=True)
    assert (o.status, o.path, o.rows) == ("SUCCESS", "fast bulk", 2), o.error
    assert _q("SELECT count(*) FROM ag.t WHERE createdby_m = 'dcc_migration' AND createddate_m IS NOT NULL") == "2"
    c.set_loader_tag("")                                                                    # a session that did not set sweep.loader
    o = _load(gz_file, "b.csv.gz", "id,v\n3,c\n", "ag", "t", fast_bulk=True)
    assert o.status == "FAILED" and "sweep.loader is not set" in (o.error or "")
    assert _q("SELECT count(*) FROM ag.t") == "2"                                           # nothing was written


@pytest.mark.integration
def test_the_audit_trigger_does_not_block_fast_bulk(loader, gz_file):
    _t("ah", "t", "id int")                                                                   # carries trg_sweep_audit
    o = _load(gz_file, "a.csv.gz", "id\n1\n", "ah", "t", fast_bulk=True)
    assert (o.status, o.path) == ("SUCCESS", "fast bulk"), o.error


# =============================================================== reconcile / check
@pytest.mark.integration
def test_missing_audit_columns_are_reported(pg_db):
    from sweep_reconcile import missing_audit_columns
    c.run_psql("CREATE SCHEMA IF NOT EXISTS ai; DROP TABLE IF EXISTS ai.t; CREATE TABLE ai.t (id int, createdby varchar(40))")
    assert missing_audit_columns("ai", "t", c.PGDATABASE) == sorted(set(SIX) - {"createdby"})
    c.run_psql(au.add_columns_sql("ai", "t"))
    assert missing_audit_columns("ai", "t", c.PGDATABASE) == []
