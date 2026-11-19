"""Oracle Step 06 staged upsert pipeline: bad rows, alternate keys, triggers, reject limit (needs PostgreSQL)."""

import pytest

import sweep_common as c
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_pipeline as pl

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _fresh(pg_db):
    o6.META_CACHE.clear()
    o6.PK_CACHE.clear()
    pl.ensure_reject_table()
    c.run_psql(f"TRUNCATE {pl.REJECT_TABLE}")
    yield


def _t(schema, table, ddl):
    c.run_psql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"; DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE; '
               f'CREATE TABLE "{schema}"."{table}" ({ddl});')


def _job(path, schema, table):
    return {"uri": str(path), "target_schema": schema, "target_table": table, "size_bytes": path.stat().st_size,
            "size_mb": 0.01, "filename": path.name, "file_format": "gzip"}


def _load(gz_file, name, text, schema, table, **kw):
    o6.META_CACHE.clear()
    return o6.load_single_file(_job(gz_file(name, text), schema, table), worker_id=1, **kw)


def _q(sql):
    return c.run_psql(sql, tuples_only=True)


def test_upsert_counts_and_reconciliation(gz_file):
    _t("p1", "t", "id int primary key, v text")
    o = _load(gz_file, "a.csv.gz", "id,v\n1,a\n2,b\n", "p1", "t")
    assert (o.status, o.inserted, o.updated, o.rejected) == ("SUCCESS", 2, 0, 0)
    o = _load(gz_file, "b.csv.gz", "id,v\n1,a\n2,changed\n3,c\n2,last\n", "p1", "t")
    assert o.status == "SUCCESS"
    assert o.read == o.inserted + o.updated + o.unchanged + o.rejected + o.duplicates
    assert (o.inserted, o.duplicates) == (1, 1)
    assert _q("SELECT v FROM p1.t WHERE id=2") == "last"


def test_not_null_violation_is_rejected_not_fatal(gz_file):
    _t("p2", "t", "id int primary key, v text not null")
    o = _load(gz_file, "a.csv.gz", "id,v\n1,a\n2,\n3,c\n", "p2", "t")
    assert (o.status, o.inserted, o.rejected) == ("SUCCESS", 2, 1)
    assert _q(f"SELECT reason FROM {pl.REJECT_TABLE}") == "NOT_NULL"


def test_check_constraint_violation_is_rejected(gz_file):
    _t("p3", "t", "id int primary key, qty int check (qty >= 0)")
    o = _load(gz_file, "a.csv.gz", "id,qty\n1,5\n2,-1\n", "p3", "t")
    assert (o.inserted, o.rejected) == (1, 1)
    assert _q(f"SELECT reason FROM {pl.REJECT_TABLE}") == "CHECK"


def test_bad_value_is_rejected(gz_file):
    _t("p4", "t", "id int primary key, n int")
    o = _load(gz_file, "a.csv.gz", "id,n\n1,10\n2,abc\n", "p4", "t")
    assert (o.inserted, o.rejected) == (1, 1)
    assert _q(f"SELECT reason FROM {pl.REJECT_TABLE}") == "BAD_VALUE"


def test_alternate_key_conflict_is_rejected_not_updated(gz_file):
    _t("p5", "t", "id int primary key, code text unique, v text")
    _load(gz_file, "a.csv.gz", "id,code,v\n1,A,x\n", "p5", "t")
    o = _load(gz_file, "b.csv.gz", "id,code,v\n2,A,y\n3,B,z\n", "p5", "t")
    assert (o.status, o.inserted, o.rejected) == ("SUCCESS", 1, 1)
    assert _q("SELECT v FROM p5.t WHERE id=1") == "x"


def test_alternate_key_duplicate_inside_file(gz_file):
    _t("p6", "t", "id int primary key, code text unique")
    o = _load(gz_file, "a.csv.gz", "id,code\n1,A\n2,A\n", "p6", "t")
    assert o.status == "SUCCESS" and o.inserted + o.rejected + o.duplicates == 2
    assert _q("SELECT count(*) FROM p6.t") == "1"


def test_foreign_key_missing_is_rejected(gz_file):
    _t("p7", "parent", "id int primary key")
    c.run_psql("INSERT INTO p7.parent VALUES (1)")
    _t("p7", "child", "id int primary key, pid int references p7.parent(id)")
    o = _load(gz_file, "a.csv.gz", "id,pid\n1,1\n2,99\n", "p7", "child")
    assert (o.inserted, o.rejected) == (1, 1)
    assert _q(f"SELECT reason FROM {pl.REJECT_TABLE}") == "FK_MISSING"


@pytest.mark.parametrize("mode", ["replica", "disable"])
def test_triggers_are_skipped_and_restored(gz_file, mode):
    _t("p8", "t", "id int primary key, v text, hits int default 0")
    c.run_psql("CREATE OR REPLACE FUNCTION p8.bump() RETURNS trigger LANGUAGE plpgsql AS $$ "
               "BEGIN NEW.hits := coalesce(NEW.hits,0)+100; RETURN NEW; END $$; "
               "CREATE TRIGGER trg_bump BEFORE INSERT OR UPDATE ON p8.t FOR EACH ROW EXECUTE FUNCTION p8.bump();")
    o = _load(gz_file, "a.csv.gz", "id,v\n1,a\n", "p8", "t", trigger_mode=mode)
    assert o.status == "SUCCESS"
    assert _q("SELECT hits FROM p8.t WHERE id=1") == "0"                       # trigger did not fire
    assert _q("SELECT tgenabled FROM pg_trigger WHERE tgname='trg_bump'") == "O"   # and is enabled again
    c.run_psql("INSERT INTO p8.t(id) VALUES (2)")
    assert _q("SELECT hits FROM p8.t WHERE id=2") == "100"


def test_file_fails_at_reject_limit_and_rolls_back(gz_file):
    _t("p9", "t", "id int primary key, v text not null")
    body = "id,v\n" + "".join(f"{i},\n" for i in range(10)) + "100,ok\n"
    o = _load(gz_file, "a.csv.gz", body, "p9", "t", max_rejects=10)
    assert o.status == "FAILED" and "Too many rejected rows" in o.error
    assert _q("SELECT count(*) FROM p9.t") == "0"
    assert _q(f"SELECT count(*) FROM {c.LOAD_LOG_TABLE}") == "0"
    assert int(_q(f"SELECT count(*) FROM {pl.REJECT_TABLE}")) >= 10                 # kept for diagnosis
    # 9 rejects is below the limit -> file succeeds
    body = "id,v\n" + "".join(f"{i},\n" for i in range(9)) + "100,ok\n"
    o = _load(gz_file, "b.csv.gz", body, "p9", "t", max_rejects=10)
    assert (o.status, o.rejected, o.inserted) == ("SUCCESS", 9, 1)


def test_default_never_fails_a_file_for_rejected_rows(gz_file):
    assert o6.DEFAULT_MAX_REJECTS == 0
    _t("pa", "t", "id int primary key, v text not null")
    body = "id,v\n" + "".join(f"{i},\n" for i in range(20)) + "100,ok\n"
    o = _load(gz_file, "a.csv.gz", body, "pa", "t")
    assert (o.status, o.rejected, o.inserted) == ("SUCCESS", 20, 1)
    body = "id,v\n" + "".join(f"{i},\n" for i in range(20))
    o = _load(gz_file, "b.csv.gz", body, "pa", "t")
    assert (o.status, o.rejected) == ("SUCCESS", 20)


def test_fast_bulk_on_empty_constraint_free_table(gz_file):
    _t("pb", "t", "a int, b text")
    o = _load(gz_file, "a.csv.gz", "a,b\n1,x\n2,y\n", "pb", "t", fast_bulk=True)
    assert (o.status, o.rows) == ("SUCCESS", 2)
    assert _q("SELECT count(*) FROM pb.t") == "2"


def test_only_rows_that_differ_are_updated(gz_file):
    _t("pc", "t", "id int primary key, v text, n int, modifieddate timestamp, modifiedby text")
    c.run_psql("CREATE OR REPLACE FUNCTION pc.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.v := 'trigger'; RETURN NEW; END $$; "
               "CREATE TRIGGER trg_touch BEFORE UPDATE ON pc.t FOR EACH ROW EXECUTE FUNCTION pc.touch();")
    o = _load(gz_file, "a.csv.gz", "id,v,n\n1,a,1\n2,b,2\n3,c,3\n", "pc", "t")
    assert (o.inserted, o.updated, o.unchanged) == (3, 0, 0)
    first = _q("SELECT modifieddate FROM pc.t WHERE id=1")
    o = _load(gz_file, "b.csv.gz", "id,v,n\n1,a,1\n2,CHANGED,2\n3,c,3\n4,d,4\n", "pc", "t")
    assert (o.status, o.inserted, o.updated, o.unchanged) == ("SUCCESS", 1, 1, 2)
    assert o.read == o.inserted + o.updated + o.unchanged + o.rejected + o.duplicates
    assert _q("SELECT modifieddate FROM pc.t WHERE id=1") == first          # unchanged row not touched
    assert _q("SELECT modifiedby FROM pc.t WHERE id=2") == c.PGUSER          # tool-set audit value on the changed row
    assert _q("SELECT v FROM pc.t WHERE id=2") == "CHANGED"


def test_modifieddate_from_the_file_counts_as_a_change(gz_file):
    _t("pd", "t", "id int primary key, v text, modifieddate timestamp")
    _load(gz_file, "a.csv.gz", "id,v,modifieddate\n1,a,2020-01-01 00:00:00\n", "pd", "t")
    o = _load(gz_file, "b.csv.gz", "id,v,modifieddate\n1,a,2021-01-01 00:00:00\n", "pd", "t")
    assert (o.updated, o.unchanged) == (1, 0)
    assert _q("SELECT modifieddate FROM pd.t") == "2021-01-01 00:00:00"
    o = _load(gz_file, "c.csv.gz", "id,v,modifieddate\n1,a,2021-01-01 00:00:00\n", "pd", "t")
    assert (o.updated, o.unchanged) == (0, 1)


def test_counts_balance_on_a_partitioned_table(gz_file):
    c.run_psql('CREATE SCHEMA IF NOT EXISTS pe; CREATE TABLE pe.t (id int, d int, v text, PRIMARY KEY (id, d)) PARTITION BY RANGE (d); '
               'CREATE TABLE pe.t1 PARTITION OF pe.t FOR VALUES FROM (0) TO (100);')
    o = _load(gz_file, "a.csv.gz", "id,d,v\n1,5,a\n2,6,b\n", "pe", "t")
    assert (o.status, o.inserted) == ("SUCCESS", 2), o.error
    o = _load(gz_file, "b.csv.gz", "id,d,v\n1,5,a\n2,6,CH\n3,7,c\n", "pe", "t")
    assert (o.status, o.inserted, o.updated, o.unchanged) == ("SUCCESS", 1, 1, 1), o.error
