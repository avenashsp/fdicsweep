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
    assert _q(f"SELECT count(*) FROM {c.ORACLE_LOAD_LOG_TABLE}") == "0"
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


# ================================================================ 6.2.1: fast bulk eligibility, chunked staged load, rejected rows counted once
from sweep_validate import ledger_check  # noqa: E402


def _chunked(gz_file, name, text, schema, table, rows=3, **kw):
    return _load(gz_file, name, text, schema, table, chunk_rows=rows, chunk_min_mb=0, **kw)


def _rules(monkeypatch, *rules):
    """FE loader rules for the Step 06 loader (bronze has none in 6.2; the plumbing is shared with promote and tested here)."""
    monkeypatch.setattr(o6, "fwd_rules", lambda: [tuple(r) for r in rules])


# ---------------------------------------------------------------- fast bulk eligibility
def test_fast_bulk_allows_not_null_and_identity_columns_and_registers_a_full_copy_and_the_ledger(gz_file):
    _t("pf", "t", "id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL, v text NOT NULL, n int")
    o = _load(gz_file, "a.csv.gz", "id,v,n\n1,a,1\n2,b,2\n", "pf", "t", fast_bulk=True)
    assert (o.status, o.path, o.rows) == ("SUCCESS", "fast bulk", 2), o.error
    assert _q("SELECT count(*) FROM pf.t") == "2"
    assert _q("SELECT full_copy || ':' || rows_inserted FROM sweep_ctl.delta_registry WHERE target_table = 't'") == "true:2"      # a full copy, once
    assert _q("SELECT rows_before || ':' || rows_inserted || ':' || source FROM sweep_ctl.load_history WHERE target_table = 't'") == "0:2:s3"
    assert [(r["ledger_status"], r["ledger_rows"]) for r in ledger_check()] == [("LEDGER_MATCH", 2)]
    # a NULL in a NOT NULL column fails the whole file (COPY enforces it): nothing is loaded and no registry / ledger row exists
    _t("pf", "t2", "id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL, v text NOT NULL, n int")
    o = _load(gz_file, "b.csv.gz", "id,v,n\n3,c,3\n4,,4\n", "pf", "t2", fast_bulk=True)
    assert o.status == "FAILED" and "null value" in o.error.lower()
    assert _q("SELECT count(*) FROM pf.t2") == "0"
    assert _q("SELECT count(*) FROM sweep_ctl.delta_registry WHERE target_table = 't2'") == "0"
    assert _q("SELECT status FROM sweep_ctl.load_history WHERE target_table = 't2'") == "FAILED"


@pytest.mark.parametrize("ddl,why", [
    ("id int primary key, v text", "primary / unique key"),
    ("id int, v text, CONSTRAINT ck CHECK (id > 0)", "CHECK"),
    ("id int primary key, v text, CONSTRAINT fk FOREIGN KEY (id) REFERENCES pg.parent(id)", "foreign key"),
])
def test_fast_bulk_skip_reason_is_logged_and_staged_load_used(gz_file, grab, ddl, why):
    _t("pg", "parent", "id int primary key")
    c.run_psql("INSERT INTO pg.parent VALUES (1)")
    _t("pg", "t", ddl)
    msgs = grab(o6.logger)
    o = _load(gz_file, "a.csv.gz", "id,v\n1,a\n", "pg", "t", fast_bulk=True)
    assert o.status == "SUCCESS" and o.path == "staged", o.error
    assert any("--fast-bulk skipped" in m and why in m for m in msgs)


def test_fast_bulk_is_used_for_a_bare_table_that_is_not_empty(gz_file, grab):
    _t("ph", "t", "id int, v text")
    c.run_psql("INSERT INTO ph.t VALUES (0, 'x')")
    msgs = grab(o6.logger)
    o = _load(gz_file, "a.csv.gz", "id,v\n1,a\n2,b\n", "ph", "t", fast_bulk=True)
    assert (o.status, o.path, o.rows) == ("SUCCESS", "fast bulk", 2), o.error                   # the rows of the FILE, not of the table
    assert _q("SELECT count(*) FROM ph.t") == "3" and not any("--fast-bulk skipped" in m for m in msgs)


def test_fast_bulk_is_skipped_for_fe_rules_and_generated_always_identity(gz_file, grab, monkeypatch):
    _t("pq", "t", "id int, v text")
    _rules(monkeypatch, ("ignore-column", "pq", "t", "old", "", ""))
    msgs = grab(o6.logger)
    o = _load(gz_file, "a.csv.gz", "id,v,old\n1,a,x\n", "pq", "t", fast_bulk=True)
    assert (o.status, o.path) == ("SUCCESS", "staged"), o.error
    assert any("FE column rules" in m for m in msgs)
    monkeypatch.setattr(o6, "fwd_rules", lambda: [])
    _t("pq", "t2", "id bigint GENERATED ALWAYS AS IDENTITY, v text")
    o = _load(gz_file, "b.csv.gz", "v\nx\n", "pq", "t2", fast_bulk=True)
    assert o.path == "fast bulk"                                           # the file does not carry the ALWAYS column: fine
    _t("pq", "t3", "id bigint GENERATED ALWAYS AS IDENTITY, v text")
    mt = pl.fast_bulk_blockers({"keys": [], "checks": [], "fks": [], "columns": {"id": {"identity": "a"}}}, ["id"])
    assert mt and "GENERATED ALWAYS" in mt[0]


# ---------------------------------------------------------------- chunked staged load
def test_chunked_load_matches_the_single_pass_result(gz_file):
    body = "id,v,n\n" + "".join(f"{i},v{i},{i}\n" for i in range(1, 11))
    _t("pi", "single", "id int primary key, v text, n int")
    _t("pi", "chunk", "id int primary key, v text, n int")
    a = _load(gz_file, "a.csv.gz", body, "pi", "single")
    b = _chunked(gz_file, "b.csv.gz", body, "pi", "chunk")
    assert (a.path, b.path) == ("staged", "staged, chunked")
    assert (a.inserted, a.updated, a.read) == (b.inserted, b.updated, b.read) == (10, 0, 10)
    assert _q("SELECT string_agg(id || v, ',' ORDER BY id) FROM pi.single") == _q("SELECT string_agg(id || v, ',' ORDER BY id) FROM pi.chunk")
    body2 = "id,v,n\n" + "".join(f"{i},{'new' if i % 4 == 0 else 'v' + str(i)},{i}\n" for i in range(1, 11))
    b = _chunked(gz_file, "c.csv.gz", body2, "pi", "chunk")
    assert (b.inserted, b.updated, b.unchanged, b.read) == (0, 2, 8, 10)
    assert b.read == b.inserted + b.updated + b.unchanged + b.rejected + b.duplicates


def test_chunked_load_handles_quoted_newlines_across_chunk_edges(gz_file):
    _t("pj", "t", "id int primary key, v text")
    rows = [f'{i},"line one {i}\nline two {i}"\n' for i in range(1, 8)]
    o = _chunked(gz_file, "a.csv.gz", "id,v\n" + "".join(rows), "pj", "t", rows=2)
    assert (o.status, o.inserted) == ("SUCCESS", 7), o.error
    assert _q("SELECT replace(v, chr(10), '|') FROM pj.t WHERE id = 5") == "line one 5|line two 5"


def test_chunked_load_rejects_and_reconciles_across_chunks(gz_file):
    _t("pk", "t", "id int primary key, v text not null, n int")
    body = "id,v,n\n1,a,1\n2,,2\n3,c,x\n4,d,4\n5,e,5\n6,,6\n7,g,7\n"
    o = _chunked(gz_file, "a.csv.gz", body, "pk", "t", rows=3)
    assert (o.status, o.inserted, o.rejected, o.read) == ("SUCCESS", 4, 3, 7), o.error
    assert _q(f"SELECT string_agg(row_no || ':' || reason, ',' ORDER BY row_no) FROM {pl.REJECT_TABLE}") == "2:NOT_NULL,3:BAD_VALUE,6:NOT_NULL"


def test_chunked_duplicate_key_across_chunks_last_row_wins(gz_file):
    _t("pl", "t", "id int primary key, v text")
    o = _chunked(gz_file, "a.csv.gz", "id,v\n1,first\n2,b\n3,c\n1,second\n", "pl", "t", rows=2)
    assert o.status == "SUCCESS" and o.read == o.inserted + o.updated + o.unchanged + o.rejected + o.duplicates
    assert _q("SELECT v FROM pl.t WHERE id = 1") == "second"                           # last row wins, as in the single pass
    assert (o.inserted, o.updated) == (3, 1)                                           # the first chunk inserted key 1, the last chunk updated it


def test_chunked_alternate_key_conflict_against_rows_of_an_earlier_chunk(gz_file):
    _t("pr", "t", "id int primary key, u text unique, v text")
    o = _chunked(gz_file, "a.csv.gz", "id,u,v\n1,x,a\n2,y,b\n3,x,c\n4,z,d\n", "pr", "t", rows=2)
    assert (o.status, o.inserted, o.rejected, o.read) == ("SUCCESS", 3, 1, 4), o.error
    assert _q(f"SELECT row_no || ':' || reason FROM {pl.REJECT_TABLE}") == "3:ALT_KEY_CONFLICT"
    # the same file as ONE pass: rows 1 and 3 share the alternate key inside the file (last row wins: row 1 is a duplicate), then row 3 meets
    # the table's row 1 (ALT_KEY_CONFLICT). Chunking changes where the duplicate is found, never the table: it ends the same.
    s = _load(gz_file, "b.csv.gz", "id,u,v\n1,x,a\n2,y,b\n3,x,c\n4,z,d\n", "pr", "t")
    assert (s.inserted, s.updated, s.unchanged, s.rejected, s.duplicates) == (0, 0, 2, 1, 1)
    assert _q("SELECT string_agg(id || u, ',' ORDER BY id) FROM pr.t") == "1x,2y,4z"


def test_chunked_reject_limit_counts_all_chunks_and_rolls_everything_back(gz_file):
    _t("pm", "t", "id int primary key, v text not null")
    body = "id,v\n" + "".join(f"{i},\n" for i in range(1, 7)) + "100,ok\n"
    o = _chunked(gz_file, "a.csv.gz", body, "pm", "t", rows=2, max_rejects=5)
    assert o.status == "FAILED" and "Too many rejected rows" in o.error
    assert _q("SELECT count(*) FROM pm.t") == "0"
    assert _q(f"SELECT count(*) FROM {c.ORACLE_LOAD_LOG_TABLE}") == "0"
    assert _q("SELECT count(*) FROM sweep_ctl.delta_registry WHERE target_table = 't'") == "0"


def test_chunked_load_of_a_truncated_stream_commits_nothing(pg_db, tmp_path):
    import gzip
    _t("pn", "t", "a int primary key, b int")
    good = gzip.compress(("a,b\n" + "".join(f"{i},{i}\n" for i in range(20000))).encode())
    bad = tmp_path / "bad.csv.gz"
    bad.write_bytes(good[: len(good) // 2])
    o = o6.load_single_file(_job(bad, "pn", "t"), worker_id=1, chunk_rows=1000, chunk_min_mb=0)
    assert o.status == "FAILED"
    assert _q("SELECT count(*) FROM pn.t") == "0" and _q(f"SELECT count(*) FROM {c.ORACLE_LOAD_LOG_TABLE}") == "0"
    assert _q("SELECT count(*) FROM sweep_ctl.delta_registry WHERE target_table = 't'") == "0"


def test_chunk_size_flags_decide_whether_a_file_is_chunked(gz_file):
    _t("ps", "t", "id int primary key, v text")
    body = "id,v\n1,a\n2,b\n3,c\n"
    assert _load(gz_file, "a.csv.gz", body, "ps", "t", chunk_rows=2, chunk_min_mb=0).path == "staged, chunked"
    assert _load(gz_file, "b.csv.gz", body, "ps", "t", chunk_rows=2, chunk_min_mb=500).path == "staged"      # too small to be chunked
    assert _load(gz_file, "c.csv.gz", body, "ps", "t", chunk_rows=0, chunk_min_mb=0).path == "staged"        # 0 = never chunk


# ---------------------------------------------------------------- chunked == single pass: table, delta, registry, ledger, rejects
def _snapshot(table):
    return {
        "rows": _q(f"SELECT string_agg(id || ':' || coalesce(v, '-'), ',' ORDER BY id) FROM ch.{table}"),
        "delta": _q(f"SELECT string_agg(id || ':' || _op, ',' ORDER BY id, _row_no) FROM sweep_delta.ch__{table}"),
        "registry": _q(f"SELECT count(*) || ':' || max(seq) || ':' || max(rows_inserted::text) || ':' || max(rows_updated::text) || ':' || bool_or(full_copy) "
                       f"FROM sweep_ctl.delta_registry WHERE target_table = '{table}'"),
        "history": _q(f"SELECT string_agg(rows_read || '/' || rows_inserted || '/' || rows_updated || '/' || rows_unchanged || '/' || rows_rejected || '/' || "
                      f"rows_duplicates || '/' || coalesce(rows_before::text, '-'), ',') FROM sweep_ctl.load_history WHERE target_table = '{table}'"),
        "rejects": _q(f"SELECT coalesce(string_agg(row_no || ':' || reason, ',' ORDER BY row_no, reason), '') FROM {pl.REJECT_TABLE} WHERE target_table = '{table}'"),
        "loadlog": _q(f"SELECT loaded_rows FROM {c.ORACLE_LOAD_LOG_TABLE} WHERE target_table = '{table}'"),
    }


def test_chunked_and_single_pass_give_the_same_table_delta_registry_ledger_and_rejects(gz_file):
    ddl = "id int primary key, v text not null, n int check (n >= 0)"
    for t in ("single", "chunk"):
        _t("ch", t, ddl)
        c.run_psql(f"INSERT INTO ch.{t} VALUES (1, 'old1', 1), (2, 'old2', 2), (3, 'old3', 3)")          # not empty: a delta is captured
    body = ("id,v,n\n1,old1,1\n2,changed,2\n4,new4,4\n5,,5\n6,new6,-6\n7,new7,7\n3,old3,3\n8,new8,8\n9,new9,x\n10,new10,10\n"
            "11,new11,11\n12,,-1\n")                                    # unchanged, changed, new, NOT_NULL, CHECK, BAD_VALUE, a row with two violations
    a = _load(gz_file, "a.csv.gz", body, "ch", "single")
    b = _chunked(gz_file, "b.csv.gz", body, "ch", "chunk", rows=5)
    assert (a.path, b.path) == ("staged", "staged, chunked")
    assert (a.read, a.inserted, a.updated, a.unchanged, a.rejected, a.duplicates) == (b.read, b.inserted, b.updated, b.unchanged, b.rejected, b.duplicates)
    assert a.read == a.inserted + a.updated + a.unchanged + a.rejected + a.duplicates
    sa, sb = _snapshot("single"), _snapshot("chunk")
    assert sa["rows"] == sb["rows"]
    assert sa["registry"].startswith("1:1:") and sb["registry"] == sa["registry"]                      # ONE registry entry, ONE sequence number per file
    assert sa["history"] == sb["history"] and sa["rejects"] == sb["rejects"] and sa["loadlog"] == sb["loadlog"]
    # the ledger sees the same totals and matches for both tables
    assert {(r["target_table"], r["ledger_status"]) for r in ledger_check()} == {("single", "LEDGER_MATCH"), ("chunk", "LEDGER_MATCH")}
    # the delta of the chunked load is the union of its chunks: every applied key is there with the same operation
    assert sa["delta"] == sb["delta"] and sa["delta"]                                                  # the delta of the chunks is the delta of the single pass
    # _row_no keeps increasing from chunk to chunk (promote reads the delta ORDER BY _row_no)
    nums = [int(x) for x in _q("SELECT string_agg(_row_no::text, ',' ORDER BY ctid) FROM sweep_delta.ch__chunk").split(",")]
    assert len(set(nums)) == len(nums) and len(nums) == len(sb["delta"].split(","))


def test_duplicate_keys_end_in_the_same_table_chunked_or_not_and_the_counts_still_balance(gz_file):
    for t in ("single", "chunk"):
        _t("ch", t, "id int primary key, v text")
        c.run_psql(f"INSERT INTO ch.{t} VALUES (0, 'seed')")
    body = "id,v\n1,a\n2,b\n1,c\n3,d\n2,e\n4,f\n1,g\n"
    a = _load(gz_file, "a.csv.gz", body, "ch", "single")
    b = _chunked(gz_file, "b.csv.gz", body, "ch", "chunk", rows=3)
    assert _q("SELECT string_agg(id || v, ',' ORDER BY id) FROM ch.single") == _q("SELECT string_agg(id || v, ',' ORDER BY id) FROM ch.chunk") == "0seed,1g,2e,3d,4f"
    for o in (a, b):
        assert o.read == 7 and o.read == o.inserted + o.updated + o.unchanged + o.rejected + o.duplicates
    assert (a.duplicates, a.inserted) == (3, 4)                                     # one pass: earlier duplicates of a key are dropped
    assert b.duplicates + b.updated + b.unchanged == 3 and b.inserted == 4          # chunked: a key met again in a LATER chunk is an update
    assert ledger_check() and all(r["ledger_status"] == "LEDGER_MATCH" for r in ledger_check())


def test_chunked_delta_orders_the_same_key_in_two_chunks_for_promote(gz_file):
    _t("ch", "k", "id int primary key, v text")
    c.run_psql("INSERT INTO ch.k VALUES (0, 'seed')")
    o = _chunked(gz_file, "a.csv.gz", "id,v\n1,first\n2,x\n3,y\n1,second\n", "ch", "k", rows=2)
    assert o.status == "SUCCESS", o.error
    # chunk 1 inserted key 1 ('I'), chunk 2 updated it ('U'): two delta rows, the later one has the higher _row_no
    assert _q("SELECT string_agg(_op || ':' || v, ',' ORDER BY _row_no) FROM sweep_delta.ch__k WHERE id = 1") == "I:first,U:second"


def test_chunked_empty_table_and_full_flag_register_one_full_copy(gz_file):
    _t("cf", "t", "id int primary key, v text")
    body = "id,v\n" + "".join(f"{i},v{i}\n" for i in range(1, 8))
    o = _chunked(gz_file, "a.csv.gz", body, "cf", "t", rows=3)
    assert o.status == "SUCCESS" and o.path == "staged, chunked", o.error
    assert _q("SELECT count(*) || ':' || max(seq) || ':' || bool_or(full_copy) || ':' || max(rows_inserted) FROM sweep_ctl.delta_registry WHERE target_table = 't'") == "1:1:true:7"
    assert _q("SELECT count(*) FROM sweep_delta.cf__t") == "0"                                        # a full copy stores no second copy of the data
    o6.FULL_LOAD = True
    try:
        o = _chunked(gz_file, "b.csv.gz", body.replace("v1", "w1"), "cf", "t", rows=3)
    finally:
        o6.FULL_LOAD = False
    assert o.status == "SUCCESS" and o.updated == 1, o.error
    assert _q("SELECT count(*) || ':' || max(seq) || ':' || bool_and(full_copy) FROM sweep_ctl.delta_registry WHERE target_table = 't'") == "2:2:true"


def test_fe_rules_work_in_a_chunked_load_and_give_the_single_pass_result(gz_file, monkeypatch):
    for t in ("a", "b"):
        _t("fr", t, "id int primary key, full_name text, must text not null")
        c.run_psql(f"INSERT INTO fr.{t} VALUES (1, 'x', 'kept')")
    body = "id,name,old\n1,n1,o\n2,n2,o\n3,n3,o\n4,n4,o\n5,n5,o\n"
    for t in ("a", "b"):
        _rules(monkeypatch, ("rename-column", "fr", t, "name", "full_name", ""), ("ignore-column", "fr", t, "old", "", ""),
               ("default-column", "fr", t, "must", "", "'dflt'"))
        if t == "a":
            x = _load(gz_file, "a.csv.gz", body, "fr", t)
        else:
            x = _chunked(gz_file, "b.csv.gz", body, "fr", t, rows=2)
        assert x.status == "SUCCESS", x.error
    q = lambda t: _q(f"SELECT string_agg(id || ':' || full_name || ':' || must, ',' ORDER BY id) FROM fr.{t}")
    assert q("a") == q("b") == "1:n1:kept,2:n2:dflt,3:n3:dflt,4:n4:dflt,5:n5:dflt"                       # existing row keeps its value
    assert _q("SELECT columns FROM sweep_ctl.delta_registry WHERE target_table = 'b'") == "id,full_name,must"      # the delta carries TABLE columns
    assert _q("SELECT count(*) FROM sweep_ctl.delta_registry WHERE target_table = 'b'") == "1"
    assert _q("SELECT count(*) FROM sweep_delta.fr__b") == _q("SELECT count(*) FROM sweep_delta.fr__a") == "5"


# ---------------------------------------------------------------- a row with several violations is ONE rejected row (5.4.2 behaviour)
def test_row_with_several_violations_is_counted_once(gz_file):
    _t("po", "t", "id int primary key, a text not null, b text not null, n int check (n > 0)")
    body = "id,a,b,n\n1,ok,ok,1\n2,,,-5\n3,x,,1\n4,ok,ok,2\n"        # row 2 breaks a, b and the CHECK; row 3 breaks b
    o = _load(gz_file, "a.csv.gz", body, "po", "t")
    assert (o.status, o.read, o.inserted, o.rejected) == ("SUCCESS", 4, 2, 2), o.error
    assert o.read == o.inserted + o.updated + o.unchanged + o.rejected + o.duplicates
    assert _q(f"SELECT count(*) FROM {pl.REJECT_TABLE} WHERE row_no = 2") == "3"             # every reason is still listed
    assert _q(f"SELECT count(DISTINCT row_no) FROM {pl.REJECT_TABLE}") == "2"


def test_row_with_several_violations_is_counted_once_when_chunked(gz_file):
    _t("pp", "t", "id int primary key, a text not null, b text not null")
    body = "id,a,b\n1,,\n2,x,y\n3,,\n4,x,y\n5,,\n"
    o = _chunked(gz_file, "a.csv.gz", body, "pp", "t", rows=2)
    assert (o.status, o.read, o.inserted, o.rejected) == ("SUCCESS", 5, 2, 3), o.error


# ---------------------------------------------------------------- promote keeps working on the single-pass script
def test_the_single_pass_script_keeps_unsuffixed_counters_and_promotes_signature():
    import inspect
    params = list(inspect.signature(pl.build_staged_script).parameters)
    assert params == ["schema", "table", "header_cols", "stream_cmd", "meta", "plan", "trigger_mode", "max_rejects", "uri", "log_sql", "track",
                      "copy_from", "keep_target", "capture", "pre_sql", "apply_pre_sql", "post_sql", "dry_run_sql", "raw_cols", "col_map", "defaults"]
    meta = {"columns": {"id": {"type": "integer", "notnull": False, "hasdef": False, "identity": "", "generated": ""}}, "keys": [], "checks": [],
            "fks": [], "triggers": []}
    plan = {"present": {"id"}, "arbiter": None, "alt_keys": [], "not_null": [], "checks": [], "skipped_checks": [], "fks": [], "identity": False}
    sql = pl.build_staged_script("s", "t", ["id"], "cat x", meta, plan, "replica", 0, "u", "SELECT 1;")
    assert "'inserted'" in sql and "'raw'" in sql and "'raw|" not in sql and "SELECT 'lo" not in sql and "'inserted|" not in sql         # promote reads _c with k = 'inserted'
    assert sql.count("BEGIN;") == 1 and sql.rstrip().endswith("COMMIT;")
