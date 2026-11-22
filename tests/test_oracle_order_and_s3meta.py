"""Parent-first load order and S3-metadata change detection (Oracle Step 06 / Step 08)."""

import csv
import gzip
import os
import time

import pytest

import sweep_common as c
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_order as order
from sweep_s3meta import make_meta, meta_equal

K = lambda t: ("s", t)          # noqa: E731


def test_levels_parents_first_and_cycles_share_a_level():
    nodes = [K("grand"), K("child"), K("parent"), K("a"), K("b"), K("self"), K("solo")]
    edges = {K("child"): {K("parent")}, K("grand"): {K("child")},
             K("a"): {K("b")}, K("b"): {K("a"), K("parent")}, K("self"): {K("self")}}
    lv = order.compute_levels(nodes, edges)
    assert lv[K("parent")] == lv[K("solo")] == lv[K("self")] == 0
    assert lv[K("child")] == 1 and lv[K("grand")] == 2
    assert lv[K("a")] == lv[K("b")] == 1                      # cycle: one level, after their parent


def test_levels_ignore_parents_that_are_not_in_this_run():
    assert order.compute_levels([K("child")], {K("child"): {K("not_loaded")}}) == {K("child"): 0}


def test_plan_levels_reads_the_step01_constraints_csv(tmp_path, monkeypatch):
    p = tmp_path / "cons.csv"
    p.write_text("owner,table_name,constraint_name,ctype,columns,ref_owner,ref_table,ref_columns\n"
                 "BSS01,CHILD,FK1,R,PID,,PARENT,ID\nBSS01,PARENT,PK,P,ID,,,\n")
    real = order.extracted_edges
    monkeypatch.setattr(order, "extracted_edges", lambda: real(p))
    monkeypatch.setattr(order, "live_edges", lambda: {})
    jobs = [{"target_schema": "bss01", "target_table": "child", "size_bytes": 1, "filename": "c"},
            {"target_schema": "bss01", "target_table": "parent", "size_bytes": 999, "filename": "p"}]
    plan = order.plan_levels(jobs)
    assert [(lv, [j["target_table"] for j in files]) for lv, files in plan] == [(0, ["parent"]), (1, ["child"])]


def test_meta_equal_needs_all_three_values():
    a = make_meta("2026-11-19T10:00:00Z", '"abc"', 10)
    assert meta_equal(a, make_meta("2026-11-19 10:00:00+00", "abc", "10"))
    assert not meta_equal(a, make_meta("2026-11-19T10:00:01Z", "abc", 10))
    assert not meta_equal(a, make_meta("2026-11-19T10:00:00Z", "abd", 10))
    assert not meta_equal(a, make_meta("2026-11-19T10:00:00Z", "abc", 11))
    assert not meta_equal(a, make_meta("", "", ""))


def test_classify_job():
    job = {"uri": "u", "s3_last_modified": "2026-11-19T10:00:00Z", "s3_etag": "e1", "s3_size": "5", "size_bytes": "5"}
    same = make_meta("2026-11-19T10:00:00Z", "e1", 5)
    assert o6.classify_job(job, None, None)[0] == "load"
    assert o6.classify_job(job, same, None)[0] == "skip"
    assert o6.classify_job(job, same, make_meta("2026-11-19T10:00:00Z", "e2", 5))[0] == "reload"        # live HEAD wins
    assert o6.classify_job(job, make_meta("2026-11-18T10:00:00Z", "e1", 5), None)[0] == "reload"
    assert o6.classify_job(job, make_meta("", "", ""), None)[0] == "skip"                                  # legacy row


# ---------------------------------------------------------------- integration
pytestmark_db = pytest.mark.integration


def _write_gz(path, text):
    path.write_bytes(gzip.compress(text.encode()))


def _map(tmp_path, rows):
    out = tmp_path / "outputs"
    out.mkdir(exist_ok=True)
    cols = ["load_order_rank", "owner", "table_name", "target_schema", "target_table", "file_format", "size_bytes",
            "size_mb", "uri", "filename", "s3_last_modified", "s3_etag", "s3_size"]
    with open(out / "oracle_02_s3_file_map.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    return out


def _row(path, schema, table, rank):
    from sweep_s3meta import head_object
    m = head_object(str(path))
    return {"load_order_rank": rank, "owner": schema.upper(), "table_name": table.upper(), "target_schema": schema,
            "target_table": table, "file_format": "gzip", "size_bytes": path.stat().st_size, "size_mb": 0.01,
            "uri": str(path), "filename": path.name, "s3_last_modified": m["last_modified"], "s3_etag": m["etag"],
            "s3_size": m["size"]}


@pytest.mark.integration
def test_parent_loaded_before_child_and_changed_file_is_reloaded(pg_db, tmp_path, monkeypatch):
    c.run_psql("CREATE SCHEMA oo; CREATE TABLE oo.parent (id int primary key, v text); "
               "CREATE TABLE oo.child (id int primary key, pid int, v text);")
    cons = tmp_path / "cons.csv"
    cons.write_text("owner,table_name,constraint_name,ctype,columns,ref_owner,ref_table,ref_columns\n"
                    "OO,CHILD,FK1,R,PID,,PARENT,ID\n")
    monkeypatch.setattr(order, "META_CONSTRAINTS_CSV", cons)
    monkeypatch.setattr(order, "extracted_edges", (lambda f=order.extracted_edges: f(cons)))
    (tmp_path / "reports").mkdir()
    monkeypatch.setattr(o6, "ORACLE_REPORTS_DIR", tmp_path / "reports")
    parent, child = tmp_path / "parent.csv.gz", tmp_path / "child.csv.gz"
    _write_gz(parent, "id,v\n1,p\n")
    _write_gz(child, "id,pid,v\n1,1,c\n")
    # child is smaller-named/first in the map on purpose: ordering must come from the FK graph, not the map
    monkeypatch.setattr(o6, "ORACLE_OUTPUTS_DIR", _map(tmp_path, [_row(child, "oo", "child", 1), _row(parent, "oo", "parent", 2)]))
    assert o6.run_stream_load(workers=2) is True
    order_rows = c.run_psql("SELECT target_table FROM sweep_ctl.oracle_load_log ORDER BY loaded_at, uri", tuples_only=True).split()
    assert order_rows == ["parent", "child"]
    assert c.run_psql("SELECT count(*) FROM oo.child", tuples_only=True) == "1"      # no false FK_MISSING reject
    stored = c.run_psql("SELECT s3_etag IS NOT NULL AND s3_last_modified IS NOT NULL AND s3_size > 0 FROM sweep_ctl.oracle_load_log LIMIT 1", tuples_only=True)
    assert stored == "t"

    # second run: nothing changed -> skipped
    assert o6.run_stream_load(workers=2) is True
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.oracle_load_log WHERE loaded_rows > 0", tuples_only=True) == "2"

    # re-delivered file (same name and path, new content) -> loaded again
    time.sleep(1.1)
    _write_gz(parent, "id,v\n1,p-new\n2,q\n")
    os.utime(parent, None)
    monkeypatch.setattr(o6, "ORACLE_OUTPUTS_DIR", _map(tmp_path, [_row(child, "oo", "child", 1), _row(parent, "oo", "parent", 2)]))
    assert o6.run_stream_load(workers=2) is True
    assert c.run_psql("SELECT v FROM oo.parent WHERE id = 1", tuples_only=True) == "p-new"
    assert c.run_psql("SELECT count(*) FROM oo.parent", tuples_only=True) == "2"
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.oracle_load_log", tuples_only=True) == "2"      # still one row per URI
