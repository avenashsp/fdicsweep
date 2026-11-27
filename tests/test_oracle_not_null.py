"""Inline NOT NULL columns (Oracle CREATE TABLE) become NOT_NULL constraint rows in Step 01 -> SET NOT NULL in Step 04; Step 08 compares the
combined nullability; the loaders reject NULLs in NOT NULL columns (6.2.2)."""
import csv
from collections import Counter

import pytest

import sweep_common as c
from oracle import oracle_01_extract_mtd as o1
from oracle import oracle_04_generate_ddl as o4
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_pipeline as pl
from oracle import oracle_08_audit as au

ACCOUNT = '''-- CONNECT BSS01001
CREATE TABLE "BSS01001"."ACCOUNT"
   (	"ACCOUNT_NO" VARCHAR2(16) NOT NULL ENABLE,
	"CUSTOMER_ID" NUMBER(12,0) NOT NULL ENABLE,
	"PRODUCT_CODE" VARCHAR2(10) NOT NULL ENABLE,
	"STATUS" CHAR(1),
	"BALANCE" NUMBER(15,2) DEFAULT 0 NOT NULL ENABLE,
	"OPENED_DATE" DATE,
	"CLOSED_DATE" DATE,
	"BRANCH_NO" NUMBER(5,0),
	 CONSTRAINT "PK_ACCOUNT" PRIMARY KEY ("ACCOUNT_NO")
   ) ;
ALTER TABLE "BSS01001"."ACCOUNT" MODIFY ("STATUS" DEFAULT 'A' NOT NULL ENABLE);
ALTER TABLE "BSS01001"."ACCOUNT" MODIFY ("OPENED_DATE" NOT NULL ENABLE);
ALTER TABLE "BSS01001"."ACCOUNT" MODIFY ("BALANCE" NOT NULL ENABLE);
CREATE TABLE "BSS01001"."EVENTS"
   (	"EVENT_ID" NUMBER GENERATED ALWAYS AS IDENTITY MINVALUE 1 MAXVALUE 9999999999 INCREMENT BY 1 START WITH 1 NOT NULL ENABLE,
	"PAYLOAD" VARCHAR2(40) NOT NULL ENABLE,
	"D" DATE NOT NULL ENABLE
   )  PARTITION BY RANGE ("D")
  (PARTITION "P1" VALUES LESS THAN (TO_DATE(' 2025-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')),
   PARTITION "P2" VALUES LESS THAN (TO_DATE(' 2026-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))) ;
'''


def _statements(text):
    out, buf = [], []
    for line in text.splitlines(True):
        if line.startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            out.append("".join(buf))
            buf = []
    return out


# ---------------------------------------------------------------- Step 01
def test_parser_inline_not_null_default_before_not_null_and_pk_identity_are_left_out():
    tbl = o1.parse_table_ddl(_statements(ACCOUNT)[0], default_owner="BSS01001")
    cols = {x["column_name"]: x for x in tbl["columns"]}
    assert [n for n, x in cols.items() if x["nullable"] == "N"] == ["ACCOUNT_NO", "CUSTOMER_ID", "PRODUCT_CODE", "BALANCE"]
    assert cols["BALANCE"]["default_expr"] == "0"                                           # DEFAULT 0 NOT NULL ENABLE: both are seen
    rows = o1.inline_not_null_rows(tbl)
    assert [r["columns"] for r in rows] == ["CUSTOMER_ID", "PRODUCT_CODE", "BALANCE"]       # ACCOUNT_NO is the primary key
    assert rows[0] == {"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": "NN_ACCOUNT_CUSTOMER_ID", "ctype": "NOT_NULL",
                       "columns": "CUSTOMER_ID", "ref_owner": "", "ref_table": "", "ref_columns": "", "check_condition": "", "source_file": ""}


def test_identity_columns_and_partitioned_tables():
    tbl = o1.parse_table_ddl(_statements(ACCOUNT)[4], default_owner="BSS01001")
    assert tbl["partitions"] and [r["columns"] for r in o1.inline_not_null_rows(tbl)] == ["PAYLOAD", "D"]       # identity EVENT_ID: none


def test_modify_rows_repeating_an_inline_not_null_are_deduplicated():
    stmts = _statements(ACCOUNT)
    tbl = o1.parse_table_ddl(stmts[0], default_owner="BSS01001")
    rows = tbl["constraints"] + o1.inline_not_null_rows(tbl)
    for st in stmts[1:4]:
        rows += [r for r in o1.parse_alter_ddl(st, source_file="x.sql") if r["ctype"] == "NOT_NULL"]
    nn = [r["columns"] for r in o1.dedupe_not_null(rows) if r["ctype"] == "NOT_NULL"]
    assert sorted(nn) == ["BALANCE", "CUSTOMER_ID", "OPENED_DATE", "PRODUCT_CODE", "STATUS"] and len(nn) == 5       # BALANCE once (inline + MODIFY)
    assert [r["ctype"] for r in o1.dedupe_not_null(rows)].count("P") == 1                                            # other rows untouched


@pytest.fixture()
def step1(tmp_path, monkeypatch):
    meta = tmp_path / "meta"
    meta.mkdir()
    for key, fname in {"TABLES": "tables", "CONSTRAINTS": "constraints", "INDEXES": "indexes", "PARTITIONS": "partitions",
                       "TABLE_STATS": "stats", "SEQUENCES": "sequences", "OBJECTS": "objects"}.items():
        monkeypatch.setattr(o1, f"META_{key}_CSV", meta / f"{fname}.csv")
    dump = tmp_path / "dump.sql"
    dump.write_text(ACCOUNT)
    assert o1.extract_metadata_from_master(str(dump)) is True
    return meta, tmp_path


def _rows(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def test_step_01_writes_one_not_null_row_for_every_not_null_column_inline_or_modify(step1):
    meta, _ = step1
    nn = [r for r in _rows(meta / "constraints.csv") if r["ctype"] == "NOT_NULL"]
    got = Counter((r["table_name"], r["columns"]) for r in nn)
    assert got == Counter({("ACCOUNT", k): 1 for k in ("CUSTOMER_ID", "PRODUCT_CODE", "BALANCE", "STATUS", "OPENED_DATE")}
                          | {("EVENTS", "PAYLOAD"): 1, ("EVENTS", "D"): 1})
    assert {r["constraint_name"] for r in nn if r["table_name"] == "ACCOUNT"} == {f"NN_ACCOUNT_{k}" for k in
                                                                                  ("CUSTOMER_ID", "PRODUCT_CODE", "BALANCE", "STATUS", "OPENED_DATE")}
    tables = {(r["table_name"], r["column_name"]): r["nullable"] for r in _rows(meta / "tables.csv")}
    assert tables[("ACCOUNT", "STATUS")] == "Y" and tables[("ACCOUNT", "BALANCE")] == "N"                # the table metadata is unchanged
    assert not [r for r in _rows(meta / "constraints.csv") if r["columns"] == "ACCOUNT_NO" and r["ctype"] == "NOT_NULL"]


def test_step_04_turns_every_not_null_row_into_set_not_null(step1, monkeypatch):
    meta, tmp = step1
    for key in ("TABLES", "CONSTRAINTS", "INDEXES", "PARTITIONS", "TABLE_STATS", "SEQUENCES", "OBJECTS"):
        if hasattr(o4, f"META_{key}_CSV"):
            monkeypatch.setattr(o4, f"META_{key}_CSV", getattr(o1, f"META_{key}_CSV"))
    for d in ("out", "pre", "post"):
        (tmp / d).mkdir()
    monkeypatch.setattr(o4, "ORACLE_PRE_LOAD_DIR", tmp / "pre")
    monkeypatch.setattr(o4, "ORACLE_POST_LOAD_DIR", tmp / "post")
    monkeypatch.setattr(o4, "ORACLE_OUTPUTS_DIR", tmp / "out")
    monkeypatch.setattr(o4, "DDL_FRAGMENT_ROOT", tmp / "out" / "ddl_fragments")
    monkeypatch.setattr(o4, "extract_views_and_triggers_from_dump", lambda: ({}, {}))
    assert o4.generate_ddl() is True
    sql = "".join(p.read_text() for p in (tmp / "out" / "ddl_fragments").rglob("*.sql")) + "".join(p.read_text() for p in (tmp / "post").rglob("*.sql"))
    for col in ("customer_id", "product_code", "balance", "status", "opened_date"):
        assert f'ALTER TABLE "bss01001"."account" ALTER COLUMN "{col}" SET NOT NULL;' in sql, col
    assert 'ALTER COLUMN "closed_date" SET NOT NULL' not in sql and 'ALTER COLUMN "branch_no" SET NOT NULL' not in sql


# ---------------------------------------------------------------- Step 08
def _meta(nullable_inline, nn_rows=()):
    cols = [{"owner": "BSS01001", "table_name": "ACCOUNT", "column_id": str(i), "column_name": n, "data_type": "VARCHAR2", "data_length": "10",
             "data_precision": "", "data_scale": "", "nullable": nullable_inline[n]} for i, n in enumerate(nullable_inline, 1)]
    cons = [{"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": f"NN_ACCOUNT_{n}", "ctype": "NOT_NULL", "columns": n, "ref_table": "",
             "ref_columns": ""} for n in nn_rows]
    return {"tables": cols, "constraints": cons}


def _pg(nullable):
    return {"columns": [{"table_schema": "bss01001", "table_name": "account", "column_name": n.lower(), "formatted_type": "character varying(10)",
                         "is_nullable": v, "column_id": i} for i, (n, v) in enumerate(nullable.items(), 1)],
            "constraints": []}


def test_step_08_compares_postgres_with_inline_or_constraint_not_null():
    inline = {"ACCOUNT_NO": "N", "STATUS": "Y", "OPENED_DATE": "Y", "CLOSED_DATE": "Y"}
    # Oracle: ACCOUNT_NO inline N; STATUS and OPENED_DATE N through ALTER MODIFY; CLOSED_DATE nullable
    meta = _meta(inline, nn_rows=("STATUS", "OPENED_DATE"))
    good = _pg({"ACCOUNT_NO": False, "STATUS": False, "OPENED_DATE": False, "CLOSED_DATE": True})
    disc = []
    rows, _ = au.compare_columns(True, disc, meta, good, None, None)
    assert {r["column_name"]: r["overall_status"] for r in rows} == {"account_no": "MATCH", "status": "MATCH", "opened_date": "MATCH", "closed_date": "MATCH"}
    assert not disc and {r["column_name"]: r["oracle_nullable"] for r in rows}["status"] == "N"
    # a column that is nullable in Postgres although a MODIFY made it NOT NULL (or an inline one) IS a mismatch
    bad = _pg({"ACCOUNT_NO": True, "STATUS": True, "OPENED_DATE": False, "CLOSED_DATE": False})
    disc = []
    rows, _ = au.compare_columns(True, disc, meta, bad, None, None)
    assert {r["column_name"]: r["overall_status"] for r in rows} == {"account_no": "NULLABILITY_MISMATCH", "status": "NULLABILITY_MISMATCH",
                                                                    "opened_date": "MATCH", "closed_date": "NULLABILITY_MISMATCH"}
    assert len(disc) == 3 and {d["Object_Name"] for d in disc} == {"account_no", "status", "closed_date"}


def test_step_08_constraint_comparison_ignores_column_rules_and_reports_unvalidated_checks_as_informational():
    meta = {"constraints": [
        {"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": "NN_ACCOUNT_STATUS", "ctype": "NOT_NULL", "columns": "STATUS", "ref_table": "", "ref_columns": ""},
        {"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": "DF_ACCOUNT_STATUS", "ctype": "DEFAULT", "columns": "STATUS", "ref_table": "", "ref_columns": ""},
        {"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": "ALTER_ACCOUNT", "ctype": "RAW_ALTER", "columns": "", "ref_table": "", "ref_columns": ""},
        {"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": "CK_BAL", "ctype": "C", "columns": "BALANCE", "ref_table": "", "ref_columns": ""},
        {"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": "FK_C", "ctype": "R", "columns": "CUSTOMER_ID", "ref_table": "CUSTOMER", "ref_columns": "ID"},
        {"owner": "BSS01001", "table_name": "ACCOUNT", "constraint_name": "UQ_X", "ctype": "U", "columns": "X", "ref_table": "", "ref_columns": ""}]}
    pg = {"constraints": [
        {"table_schema": "bss01001", "table_name": "account", "constraint_name": "ck_bal", "constraint_type": "c", "columns": "balance", "is_validated": False},
        {"table_schema": "bss01001", "table_name": "account", "constraint_name": "fk_c", "constraint_type": "f", "columns": "customer_id", "is_validated": False}]}
    disc = []
    rows, ora_map = au.compare_constraints(True, disc, meta, pg, None, None)
    status = {r["constraint_name"]: r["status"] for r in rows}
    assert status == {"ck_bal": "NOT_VALIDATED_BY_POLICY", "fk_c": "UNVALIDATED", "uq_x": "MISSING_IN_PG"}      # no NOT_NULL / DEFAULT / RAW_ALTER rows
    assert {d["Object_Name"]: d["Discrepancy_Type"] for d in disc} == {"fk_c": "CONSTRAINT_NOT_VALIDATED", "uq_x": "MISSING_CONSTRAINT_IN_PG"}
    assert set(ora_map[("bss01001", "account")]) == {"ck_bal", "fk_c", "uq_x"}


# ---------------------------------------------------------------- loads into a table that has NOT NULL columns
pytestmark_db = pytest.mark.integration


def _t(ddl):
    c.run_psql('CREATE SCHEMA IF NOT EXISTS nn; DROP TABLE IF EXISTS nn.account CASCADE; CREATE TABLE nn.account (' + ddl + ');')


@pytest.fixture()
def _db(pg_db):
    o6.META_CACHE.clear()
    o6.PK_CACHE.clear()
    pl.ensure_reject_table()
    c.run_psql(f"TRUNCATE {pl.REJECT_TABLE}")
    yield


def _load(gz_file, name, text, **kw):
    o6.META_CACHE.clear()
    p = gz_file(name, text)
    job = {"uri": str(p), "target_schema": "nn", "target_table": "account", "size_bytes": p.stat().st_size, "size_mb": 0.01, "filename": p.name, "file_format": "gzip"}
    return o6.load_single_file(job, worker_id=1, **kw)


DDL = "account_no varchar(16) NOT NULL PRIMARY KEY, product_code varchar(10) NOT NULL, status char(1) NOT NULL, balance numeric(15,2) NOT NULL, note text"
BODY = "account_no,product_code,status,balance,note\nA1,P1,A,10,ok\nA2,,A,20,null product\nA3,P3,,,null status and balance\nA4,P4,A,40,ok\nA5,P5,A,50,ok\n"


@pytest.mark.integration
def test_nulls_in_not_null_columns_are_rejected_not_fatal_and_a_row_counts_once(gz_file, _db):
    _t(DDL)
    o = _load(gz_file, "a.csv.gz", BODY)
    assert (o.status, o.read, o.inserted, o.rejected) == ("SUCCESS", 5, 3, 2), o.error
    assert c.run_psql(f"SELECT string_agg(row_no || ':' || reason || ':' || detail, ',' ORDER BY row_no, detail) FROM {pl.REJECT_TABLE}", tuples_only=True) \
        == "2:NOT_NULL:product_code,3:NOT_NULL:balance,3:NOT_NULL:status"
    assert c.run_psql(f"SELECT count(DISTINCT row_no) FROM {pl.REJECT_TABLE}", tuples_only=True) == "2"          # row 3 broke two columns: ONE rejected row


@pytest.mark.integration
def test_chunked_load_rejects_nulls_in_not_null_columns_the_same_way(gz_file, _db):
    _t(DDL)
    o = _load(gz_file, "a.csv.gz", BODY, chunk_rows=2, chunk_min_mb=0)
    assert (o.path, o.status, o.read, o.inserted, o.rejected) == ("staged, chunked", "SUCCESS", 5, 3, 2), o.error
    assert c.run_psql(f"SELECT count(*) || '/' || count(DISTINCT row_no) FROM {pl.REJECT_TABLE}", tuples_only=True) == "3/2"


@pytest.mark.integration
def test_fast_bulk_fails_the_file_on_a_null_in_a_not_null_column_by_design(gz_file, _db):
    c.run_psql("CREATE SCHEMA IF NOT EXISTS nn; DROP TABLE IF EXISTS nn.account; CREATE TABLE nn.account (account_no varchar(16) NOT NULL, product_code varchar(10) NOT NULL, note text)")
    o = _load(gz_file, "a.csv.gz", "account_no,product_code,note\nA1,P1,x\nA2,,y\n", fast_bulk=True)
    assert o.status == "FAILED" and "null value" in o.error.lower()
    assert c.run_psql("SELECT count(*) FROM nn.account", tuples_only=True) == "0"                                  # nothing loaded: one transaction
    o = _load(gz_file, "b.csv.gz", "account_no,product_code,note\nA1,P1,x\nA2,P2,y\n", fast_bulk=True)
    assert (o.status, o.path, o.rows) == ("SUCCESS", "fast bulk", 2), o.error                                    # clean data still takes the fast path
