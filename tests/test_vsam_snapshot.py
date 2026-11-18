"""Snapshot replace for VSAM tables without a primary key (staging table + atomic swap)."""
import pytest

import sweep_common as c
import vsam.vsam_06_load_data as v6

NO_PK_WITH_DATA = {"has_pk": False, "has_data": True, "action": "TRUNCATE_AND_LOAD", "pk_cols": []}
NO_PK_FRESH = {"has_pk": False, "has_data": False, "action": "DIRECT_INSERT", "pk_cols": []}
WITH_PK = {"has_pk": True, "has_data": True, "action": "UPSERT", "pk_cols": ["id"]}


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    monkeypatch.setattr(v6, "_STAGED", {})
    monkeypatch.setattr(v6, "FORCE_TRUNCATE", False)
    monkeypatch.setattr(v6, "PARTIAL_RUN", False)


@pytest.fixture()
def sql(monkeypatch):
    calls = []
    monkeypatch.setattr(v6, "run_psql", lambda s, *a, **k: calls.append(s))
    return calls


def test_tables_with_a_pk_are_never_staged(sql):
    assert v6.claim_table("bcs", "t", WITH_PK) is None and sql == [] and v6._STAGED == {}


def test_tables_without_pk_share_one_staging_table_across_files(sql):
    first = v6.claim_table("bcs", "t", NO_PK_WITH_DATA)
    again = v6.claim_table("bcs", "t", NO_PK_WITH_DATA)
    assert first is again and first["stage"] == "t__sweep_stg"
    assert len(sql) == 1 and "CREATE UNLOGGED TABLE" in sql[0] and "DROP TABLE IF EXISTS" in sql[0]
    assert not any("TRUNCATE" in s for s in sql)            # the target is untouched until the swap


def test_fresh_tables_are_staged_too_so_a_failed_load_leaves_nothing_behind(sql):
    assert v6.claim_table("bcs", "t", NO_PK_FRESH) is not None


def test_partial_run_cannot_replace_a_populated_table_without_force(sql, monkeypatch):
    monkeypatch.setattr(v6, "PARTIAL_RUN", True)
    with pytest.raises(RuntimeError, match="partial run"):
        v6.claim_table("bcs", "t", NO_PK_WITH_DATA)
    assert sql == []
    assert v6.claim_table("bcs", "empty", NO_PK_FRESH) is not None      # nothing to wipe -> allowed
    monkeypatch.setattr(v6, "FORCE_TRUNCATE", True)
    assert v6.claim_table("bcs", "t", NO_PK_WITH_DATA) is not None


def test_schemas_are_staged_independently(sql):
    a = v6.claim_table("bss01104", "t", NO_PK_WITH_DATA)
    b = v6.claim_table("bss01112", "t", NO_PK_WITH_DATA)
    assert a is not b


def test_finalize_swaps_in_one_transaction_and_writes_logs_inside_it(sql):
    st = v6.claim_table("bcs", "t", NO_PK_WITH_DATA)
    st["files"].append("f1")
    st["logs"].append(("f1", "bcs", "t", 10, 5, 0.1, 1, "SUCCESS", "source=A|B;snapshot=2024-02-01"))
    results = [{"uri": "f1", "status": "SUCCESS", "rows_loaded": 5, "records_loaded": 5}]
    sql.clear()
    v6.finalize_replacements(results)
    script = sql[0]
    assert script.startswith("BEGIN;") and script.rstrip().endswith("COMMIT;")
    order = [script.index(x) for x in ("TRUNCATE TABLE", "INSERT INTO \"bcs\".\"t\" SELECT", "INSERT INTO sweep_ctl.load_log", "DROP TABLE")]
    assert order == sorted(order)
    assert results[0]["status"] == "SUCCESS"


def test_finalize_discards_staging_and_downgrades_siblings_when_a_file_failed(sql):
    st = v6.claim_table("bcs", "t", NO_PK_WITH_DATA)
    st["files"] += ["ok.csv", "bad.csv"]
    st["ok"] = False
    results = [{"uri": "ok.csv", "status": "SUCCESS", "rows_loaded": 5, "records_loaded": 5},
               {"uri": "bad.csv", "status": "FAILED", "rows_loaded": 0, "records_loaded": 0}]
    sql.clear()
    v6.finalize_replacements(results)
    assert not any("TRUNCATE" in s for s in sql)            # the old data stays
    assert results[0]["status"] == "FAILED" and "left unchanged" in results[0]["error"]


def test_failed_swap_is_reported(monkeypatch):
    calls = []

    def run(s, *a, **k):
        calls.append(s)
        if s.startswith("BEGIN"):
            raise RuntimeError("deadlock")
    monkeypatch.setattr(v6, "run_psql", run)
    monkeypatch.setattr(v6, "_STAGED", {("bcs", "t"): {"stage": "t__sweep_stg", "ok": True, "files": ["f"], "logs": []}})
    results = [{"uri": "f", "status": "SUCCESS", "rows_loaded": 3, "records_loaded": 3}]
    v6.finalize_replacements(results)
    assert results[0]["status"] == "FAILED" and "swap failed" in results[0]["error"]
    assert any("DROP TABLE IF EXISTS" in s for s in calls)


# ----------------------------------------------------------------------------- PostgreSQL integration
@pytest.mark.integration
def test_swap_replaces_the_table_atomically(pg_db):
    c.run_psql('CREATE SCHEMA s; CREATE TABLE s.t (a int, b text); INSERT INTO s.t VALUES (1, \'old\'), (2, \'old\');')
    strat = {"has_pk": False, "has_data": True, "action": "TRUNCATE_AND_LOAD", "pk_cols": []}
    st = v6.claim_table("s", "t", strat)
    c.run_psql(f'INSERT INTO s."{st["stage"]}" (a, b) VALUES (10, \'new\'), (11, \'new\'), (12, \'new\')')
    assert c.run_psql("SELECT count(*) FROM s.t", tuples_only=True) == "2"          # untouched during the run
    st["files"].append("f1")
    st["logs"].append(("f1", "s", "t", 1, 3, 0.1, 1, "SUCCESS", "source=X|Y;snapshot=2024-02-01"))
    results = [{"uri": "f1", "status": "SUCCESS", "rows_loaded": 3, "records_loaded": 3}]
    v6.finalize_replacements(results)
    assert c.run_psql("SELECT string_agg(a::text || b, ',' ORDER BY a) FROM s.t", tuples_only=True) == "10new,11new,12new"
    assert c.run_psql("SELECT count(*) FROM information_schema.tables WHERE table_name = 't__sweep_stg'", tuples_only=True) == "0"
    assert c.run_psql(f"SELECT count(*) FROM {c.LOAD_LOG_TABLE}", tuples_only=True) == "1"
    assert results[0]["status"] == "SUCCESS"


@pytest.mark.integration
def test_failed_file_leaves_target_and_log_untouched(pg_db):
    c.run_psql('CREATE SCHEMA s; CREATE TABLE s.t (a int); INSERT INTO s.t VALUES (1), (2);')
    st = v6.claim_table("s", "t", {"has_pk": False, "has_data": True, "action": "TRUNCATE_AND_LOAD", "pk_cols": []})
    c.run_psql(f'INSERT INTO s."{st["stage"]}" (a) VALUES (99)')
    st["files"].append("f1")
    st["ok"] = False
    v6.finalize_replacements([{"uri": "f1", "status": "SUCCESS", "rows_loaded": 1, "records_loaded": 1}])
    assert c.run_psql("SELECT string_agg(a::text, ',' ORDER BY a) FROM s.t", tuples_only=True) == "1,2"
    assert c.run_psql(f"SELECT count(*) FROM {c.LOAD_LOG_TABLE}", tuples_only=True) == "0"
    assert c.run_psql("SELECT count(*) FROM information_schema.tables WHERE table_name = 't__sweep_stg'", tuples_only=True) == "0"


@pytest.mark.integration
def test_older_snapshot_is_detected(pg_db):
    c.run_psql(v6.load_log_sql("f_new", "bcs", "t", 1, 1, 0.1, 1, "SUCCESS", "source=SWP|X;snapshot=2024-02-01"))
    assert v6.newer_snapshot_loaded("t", "SWP|X", "2024-01-01") is True
    assert v6.newer_snapshot_loaded("t", "SWP|X", "2024-02-01") is False
    assert v6.newer_snapshot_loaded("t", "SWP|X", "2024-03-01") is False
    assert v6.newer_snapshot_loaded("t", "OTHER|X", "2024-01-01") is False
    assert v6.newer_snapshot_loaded("t", "SWP|X", "") is False
