"""Integration tests against a real PostgreSQL (skipped automatically when none is reachable)."""

import pytest

import sweep_common as c
from oracle import oracle_06_load_data as o6

pytestmark = pytest.mark.integration

WEIRD = "logs it's; $(touch PWNED).csv.gz"


def test_run_psql_and_query_helpers(pg_db):
    assert c.run_psql("SELECT 1", tuples_only=True) == "1"
    assert c.query_db_rows("show standard_conforming_strings") == ["on"]
    # multi-line, trailing '--' comment, embedded newline: all preserved
    rows = c.query_db_dicts("SELECT 1 AS a, E'x\\ny' AS b -- trailing comment\n;")
    assert rows == [{"a": "1", "b": "x\ny"}]


def test_sessions_are_tagged_for_safe_kill_db(pg_db):
    assert c.run_psql("SHOW application_name", tuples_only=True) == c.PSQL_APP_NAME


def test_statement_errors_raise_and_do_not_retry(pg_db):
    with pytest.raises(RuntimeError, match="does_not_exist"):
        c.run_psql("SELECT * FROM does_not_exist")


def test_timeout_raises(pg_db):
    with pytest.raises(RuntimeError, match="timed out"):
        c.run_psql("SELECT pg_sleep(10)", timeout=1)


def test_unreachable_server_fails_fast_when_retries_disabled(monkeypatch):
    monkeypatch.setattr(c, "PGPORT", "1")
    monkeypatch.setattr(c, "CONNECT_RETRIES", 0)
    with pytest.raises(RuntimeError):
        c.run_subprocess_checked(c.psql_base_args("postgres") + ["-c", "select 1"], env=c.psql_env(),
                                 retries=0, timeout=20)


def test_run_psql_script_timeout_kills_the_pipeline(pg_db):
    with pytest.raises(RuntimeError, match="timed out"):
        c.run_psql_script("SELECT pg_sleep(10);", timeout=1)


def _make_table(schema, table, ddl_cols):
    c.run_psql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"; CREATE TABLE "{schema}"."{table}" ({ddl_cols});')


def _job(path, schema, table):
    return {"uri": str(path), "target_schema": schema, "target_table": table, "size_bytes": path.stat().st_size,
            "size_mb": 0.01, "filename": path.name, "file_format": "gzip"}


def test_oracle_load_handles_hostile_filenames_without_executing_them(pg_db, gz_file, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _make_table("s1", "logs", "log_id int, msg text")
    p = gz_file(WEIRD, "log_id,msg\n1,a\n2,b\n")
    out = o6.load_single_file(_job(p, "s1", "logs"), worker_id=1)
    assert (out.status, out.error) == ("SUCCESS", "") and out.rows == 2
    assert not (tmp_path / "PWNED").exists()
    assert c.run_psql("SELECT count(*) FROM s1.logs", tuples_only=True) == "2"
    assert c.run_psql(f"SELECT count(*) FROM {c.ORACLE_LOAD_LOG_TABLE}", tuples_only=True) == "1"


def test_oracle_load_is_atomic_when_stream_is_truncated(pg_db, tmp_path):
    _make_table("s1", "t", "a int, b int")
    bad = tmp_path / "bad.csv.gz"
    import gzip
    good = gzip.compress(("a,b\n" + "".join(f"{i},{i}\n" for i in range(5000))).encode())
    bad.write_bytes(good[: len(good) // 2])                    # truncated gzip -> pipeline fails mid-stream
    out = o6.load_single_file(_job(bad, "s1", "t"), worker_id=1)
    assert out.status == "FAILED" and out.error
    assert c.run_psql("SELECT count(*) FROM s1.t", tuples_only=True) == "0"            # nothing committed
    assert c.run_psql(f"SELECT count(*) FROM {c.ORACLE_LOAD_LOG_TABLE}", tuples_only=True) == "0"


def test_oracle_load_missing_column_fails_cleanly(pg_db, gz_file):
    _make_table("s1", "t2", "a int")
    p = gz_file("x.csv.gz", "a,nope\n1,2\n")
    out = o6.load_single_file(_job(p, "s1", "t2"), worker_id=1)
    assert out.status == "FAILED" and "nope" in out.error


def test_lookup_failures_are_not_mistaken_for_empty_or_pkless(pg_db, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("connection lost")
    monkeypatch.setattr(o6, "run_psql", boom)
    with pytest.raises(RuntimeError):
        o6.is_table_empty("s1", "t")
    o6.PK_CACHE.clear()
    with pytest.raises(RuntimeError):
        o6.get_table_pk_columns("s1", "t")
    assert not o6.PK_CACHE                                       # failure was not cached as "no PK"


def test_vsam_load_log_rows_are_keyed_by_uri_and_schema(pg_db):
    from vsam import vsam_06_load_data as v6
    v6.write_load_log("s3://b/f.csv", "bss01104", "t", 10, 5, 0.5, 1)
    v6.write_load_log("s3://b/f.csv", "bcs", "t", 10, 7, 0.5, 1)
    v6.write_load_log("s3://b/f.csv", "bcs", "t", 10, 9, 0.5, 1)          # upsert, not a duplicate
    rows = sorted(c.query_db_rows(f"SELECT uri || '|' || target_schema || '|' || loaded_rows FROM {c.VSAM_LOAD_LOG_TABLE}"))
    assert rows == ["s3://b/f.csv|bcs|9", "s3://b/f.csv|bss01104|5"]      # the real uri, no '#schema' suffix


def test_vsam_failed_log_write_raises(pg_db):
    from vsam import vsam_06_load_data as v6
    c.run_psql(f"DROP TABLE {c.VSAM_LOAD_LOG_TABLE}")
    with pytest.raises(RuntimeError):
        v6.write_load_log("s3://b/f.csv", "bcs", "t", 1, 1, 0.1, 1)


def test_kill_db_only_terminates_sweep_sessions(pg_db):
    import subprocess
    import sweep_jobs
    import os
    other = subprocess.Popen(
        [c.PSQL_BIN, "-X", "-w", "-h", c.PGHOST, "-p", str(c.PGPORT), "-U", c.PGUSER, "-d", pg_db, "-c",
         "SELECT pg_sleep(30)"],
        env=dict(c.psql_env(), PGAPPNAME="someone_elses_app"), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sweep_psql = subprocess.Popen(
        [c.PSQL_BIN, "-X", "-w", "-h", c.PGHOST, "-p", str(c.PGPORT), "-U", c.PGUSER, "-d", pg_db, "-c",
         "SELECT pg_sleep(30)"],
        env=c.psql_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        import time
        for _ in range(50):
            n = c.run_psql("SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(30)%' "
                           "AND pid <> pg_backend_pid()", tuples_only=True)
            if n == "2":
                break
            time.sleep(0.1)
        sweep_jobs.stop_jobs(kill_pg_backends=True)
        sweep_psql.wait(10)
        assert sweep_psql.returncode != 0               # SWEEP's own session was terminated
        time.sleep(0.5)
        assert other.poll() is None                     # a foreign application's session was left alone
    finally:
        for p in (other, sweep_psql):
            if p.poll() is None:
                p.kill()
                p.wait()
        del os
