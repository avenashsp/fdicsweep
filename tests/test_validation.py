"""load_history (I1), the ledger check (I2) and the FE overlay classes (I3)."""
import csv
import gzip

import pytest

import sweep_common as c
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_order as order
from sweep_fwd_ddl import FeIndex
from sweep_s3meta import head_object
from sweep_validate import FeLog, ledger_check

pytestmark = pytest.mark.integration


@pytest.fixture()
def load(pg_db, tmp_path, monkeypatch):
    (tmp_path / "reports").mkdir()
    monkeypatch.setattr(o6, "ORACLE_REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(order, "live_edges", lambda: {})
    monkeypatch.setattr(order, "extracted_edges", lambda *a: {})
    c.run_psql("CREATE SCHEMA oo;")

    def run(table, text, **kw):
        path = tmp_path / f"{table}_{abs(hash(text)) % 99999}.csv.gz"
        path.write_bytes(gzip.compress(text.encode()))
        m = head_object(str(path))
        out = tmp_path / "outputs"
        out.mkdir(exist_ok=True)
        cols = ["load_order_rank", "owner", "table_name", "target_schema", "target_table", "file_format", "size_bytes", "size_mb", "uri",
                "filename", "s3_last_modified", "s3_etag", "s3_size"]
        with open(out / "oracle_02_s3_file_map.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerow({"load_order_rank": 1, "owner": "OO", "table_name": table.upper(), "target_schema": "oo", "target_table": table,
                        "file_format": "gzip", "size_bytes": path.stat().st_size, "size_mb": 0.01, "uri": str(path), "filename": path.name,
                        "s3_last_modified": m["last_modified"], "s3_etag": m["etag"], "s3_size": m["size"]})
        monkeypatch.setattr(o6, "ORACLE_OUTPUTS_DIR", out)
        return o6.run_stream_load(workers=1, reload=True, **kw)

    return run


def q1(sql):
    return c.run_psql(sql, tuples_only=True, unaligned=True)



def _fe_row(kind, action, schema, obj, file="V3__x.sql"):
    c.run_psql(f"INSERT INTO sweep_ctl.fwd_ddl_changes (file_name, kind, action, schema_name, object_name) "
               f"VALUES ('{file}', '{kind}', '{action}', '{schema}', '{obj}')")


def test_every_load_attempt_is_a_history_row_with_the_baseline_on_the_first(load):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text); INSERT INTO oo.t VALUES (100, 'pre'), (101, 'pre');")
    assert load("t", "id,v\n1,a\n2,b\n100,changed\n") is True
    assert load("t", "id,v\n3,c\n") is True
    rows = q1("SELECT string_agg(status || ':' || source || ':' || rows_read || '/' || rows_inserted || '/' || rows_updated || '/' || "
              "coalesce(rows_before::text, '-'), ' ' ORDER BY id) FROM sweep_ctl.load_history WHERE target_table = 't'")
    assert rows == "SUCCESS:s3:3/2/1/2 SUCCESS:s3:1/1/0/-"          # baseline = the 2 rows the table already held
    assert q1("SELECT count(DISTINCT run_id) || ',' || min(track) || ',' || min(layer) FROM sweep_ctl.load_history") == "1,oracle,bronze"


def test_a_failed_load_leaves_a_failed_history_row_and_no_data(load):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    assert load("t", "id,v,unknown\n1,a,x\n") is False
    # rejected before any transaction (plan problem): no data, and the ledger is still consistent
    assert q1("SELECT count(*) FROM oo.t") == "0"
    assert ledger_check() == []


def test_ledger_matches_after_loads_and_flags_a_row_removed_outside_the_loaders(load):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    assert load("t", "id,v\n1,a\n2,b\n3,c\n") is True
    rows = ledger_check()
    assert [(r["target_table"], r["ledger_rows"], r["actual_rows"], r["ledger_status"]) for r in rows] == [("t", 3, 3, "LEDGER_MATCH")]
    c.run_psql("DELETE FROM oo.t WHERE id = 3")
    r = ledger_check()[0]
    assert r["ledger_status"] == "LEDGER_MISMATCH" and "-1 row(s)" in r["note"]


def test_ledger_reports_an_fe_dropped_table_instead_of_a_mismatch(load):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY)")
    assert load("t", "id\n1\n") is True
    c.run_psql("DROP TABLE oo.t")
    assert ledger_check()[0]["ledger_status"] == "LEDGER_MISMATCH"                        # nothing explains it yet
    from sweep_fwd_ddl import load_fe_changes
    _fe_row("table", "dropped", "oo", "t")
    r = ledger_check(fe=load_fe_changes())[0]
    assert r["ledger_status"] == "FE_DROPPED" and "V3__x.sql" in r["note"]


def test_a_keyless_vsam_table_ledger_is_the_latest_snapshot(pg_db):
    c.run_psql("CREATE SCHEMA bcs; CREATE TABLE bcs.k (a text)")
    ins = lambda run, n, before: c.run_psql(
        f"INSERT INTO bcs.k SELECT 'x' FROM generate_series(1, {n}); "
        f"INSERT INTO sweep_ctl.load_history (run_id, track, source, uri, target_schema, target_table, status, rows_inserted, rows_before) "
        f"VALUES ('{run}', 'vsam', 's3', 'u', 'bcs', 'k', 'SUCCESS', {n}, {before})")
    ins("r1", 5, 0)
    c.run_psql("TRUNCATE bcs.k")
    ins("r2", 3, 0)                                   # the second run replaced the snapshot
    r = ledger_check()[0]
    assert (r["ledger_rule"], r["ledger_rows"], r["actual_rows"], r["ledger_status"]) == ("latest snapshot", 3, 3, "LEDGER_MATCH")


def test_fe_overlay_classifies_only_what_an_applied_file_explains():
    fe = FeIndex(dropped_tables={("s", "gone"): "V2__a.sql"}, added_tables={("s", "new"): "V3__b.sql"},
                 dropped_columns={("s", "t"): {"old": "V4__c.sql"}}, added_columns={("s", "t"): {"extra": "V5__d.sql"}},
                 changed_columns={("s", "t"): {"wide": "V6__e.sql"}})
    log = FeLog(fe)
    assert log.table("s", "gone", "MISSING_IN_PG") == "FE_DROPPED"
    assert log.table("s", "new", "EXTRA_IN_PG") == "FE_ADDED"
    assert log.table("s", "other", "MISSING_IN_PG") == ""                                  # not explained: stays a difference
    assert log.column("s", "t", "old", "MISSING_IN_PG") == "FE_DROPPED"
    assert log.column("s", "gone", "anything", "MISSING_IN_PG") == "FE_DROPPED"            # its table was dropped
    assert log.column("s", "t", "extra", "EXTRA_IN_PG") == "FE_ADDED"
    assert log.column("s", "t", "wide", "TYPE_MISMATCH") == "FE_CHANGED"
    assert log.column("s", "t", "unknown", "MISSING_IN_PG") == ""
    assert log.column("s", "t", "wide", "MISSING_IN_PG") == ""                             # changed is not dropped
    assert log.counts() == {"FE_DROPPED": 3, "FE_ADDED": 2, "FE_CHANGED": 1}
    assert {r["fe_file"] for r in log.sheet()} == {"V2__a.sql", "V3__b.sql", "V4__c.sql", "V5__d.sql", "V6__e.sql"}
    assert FeLog(FeIndex()).sheet()[0]["fe_status"] == "NONE"
