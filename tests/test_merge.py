"""Merge-safe outputs (E3/E4): a targeted run changes only its own entries; writes are atomic, locked and backed up."""
import csv
import json
import multiprocessing as mp
from pathlib import Path

import pytest

from sweep_ddlfrag import SCHEMA_LEVEL, FragmentStore
from sweep_merge import atomic_write_csv, backup_path, merge_csv, merge_json, read_csv_rows

HEADER = ["owner", "table_name", "value"]


def _rows(*items):
    return [{"owner": o, "table_name": t, "value": v} for o, t, v in items]


def _scope(owner, table):
    return lambda r: r["owner"] == owner and r["table_name"] == table


def _seed(path):
    merge_csv(path, _rows(("S1", "A", "1"), ("S1", "B", "1"), ("S2", "C", "1")), full=True, fieldnames=HEADER)


def test_full_run_replaces_the_whole_file(tmp_path):
    p = tmp_path / "m.csv"
    _seed(p)
    merge_csv(p, _rows(("S9", "Z", "9")), full=True, fieldnames=HEADER)
    _, rows = read_csv_rows(p)
    assert [(r["owner"], r["table_name"]) for r in rows] == [("S9", "Z")]


def test_targeted_run_replaces_only_its_rows_and_keeps_the_others_byte_identical(tmp_path):
    p = tmp_path / "m.csv"
    _seed(p)
    before = p.read_text().splitlines()
    merge_csv(p, _rows(("S1", "A", "NEW")), scope=_scope("S1", "A"), fieldnames=HEADER)
    after = p.read_text().splitlines()
    assert before[0] == after[0]                                            # header unchanged
    others_before = [l for l in before[1:] if not l.startswith("S1,A,")]
    others_after = [l for l in after[1:] if not l.startswith("S1,A,")]
    assert others_before == others_after                                    # every other entry byte-identical
    _, rows = read_csv_rows(p)
    assert len(rows) == 3 and [r["value"] for r in rows if r["table_name"] == "A"] == ["NEW"]


def test_targeted_run_adds_new_entries_and_survives_repeated_merges(tmp_path):
    p = tmp_path / "m.csv"
    _seed(p)
    merge_csv(p, _rows(("S3", "D", "1")), scope=_scope("S3", "D"), fieldnames=HEADER)
    snapshot = p.read_text().splitlines()
    merge_csv(p, _rows(("S3", "D", "2")), scope=_scope("S3", "D"), fieldnames=HEADER)
    now = p.read_text().splitlines()
    assert [l for l in snapshot if not l.startswith("S3,D,")] == [l for l in now if not l.startswith("S3,D,")]
    assert sum(1 for l in now if l.startswith("S3,D,")) == 1


def test_provenance_columns_and_one_backup(tmp_path):
    p = tmp_path / "m.csv"
    _seed(p)
    fields, rows = read_csv_rows(p)
    assert "_run_id" in fields and "_updated_at" in fields and all(r["_run_id"] and r["_updated_at"] for r in rows)
    prev = p.read_text()
    merge_csv(p, _rows(("S1", "A", "2")), scope=_scope("S1", "A"), fieldnames=HEADER)
    assert backup_path(p).read_text() == prev                               # .bak = the version before the merge
    assert len(list(tmp_path.glob("m.csv.bak*"))) == 1


def test_log_line_says_merged_and_kept(tmp_path):
    import logging
    import sweep_merge

    records = []

    class Grab(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    h = Grab()
    sweep_merge.logger.addHandler(h)
    try:
        p = tmp_path / "m.csv"
        _seed(p)
        assert merge_csv(p, _rows(("S1", "A", "2")), scope=_scope("S1", "A"), fieldnames=HEADER) == (1, 2)
    finally:
        sweep_merge.logger.removeHandler(h)
    assert any("targeted run: merged 1 entries, kept 2" in m for m in records)


def test_post_hook_renumbers_merged_rows(tmp_path):
    p = tmp_path / "m.csv"
    merge_csv(p, [{"owner": "S", "table_name": "A", "value": "1"}], full=True, fieldnames=HEADER)
    def number(rows):
        for i, r in enumerate(rows, 1):
            r["value"] = str(i)
        return rows
    merge_csv(p, _rows(("S", "B", "x")), scope=_scope("S", "B"), fieldnames=HEADER, post=number)
    assert [r["value"] for r in read_csv_rows(p)[1]] == ["1", "2"]


def test_atomic_write_never_leaves_a_half_written_file(tmp_path, monkeypatch):
    p = tmp_path / "m.csv"
    _seed(p)
    good = p.read_text()
    import sweep_merge
    monkeypatch.setattr(sweep_merge.os, "replace", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        atomic_write_csv(p, _rows(("X", "Y", "1")), HEADER)
    assert p.read_text() == good and not list(tmp_path.glob("*.tmp"))       # old file intact, temp file cleaned up


def test_json_merge_keeps_other_keys_and_stamps_meta(tmp_path):
    p = tmp_path / "gate.json"
    merge_json(p, {"CO|A": {"status": "OK"}, "CO | A": {"status": "OK"}, "CO|B": {"status": "OK"}}, full=True)
    merge_json(p, {"CO|A": {"status": "BLOCKED"}}, full=False)
    doc = json.loads(p.read_text())
    assert doc["CO|A"]["status"] == "BLOCKED" and doc["CO|B"]["status"] == "OK" and doc["CO | A"]["status"] == "OK"
    assert set(doc["CO|A"]["_meta"]) == {"run_id", "timestamp"}
    assert backup_path(p).exists()
    merge_json(p, {"NEW|X": {"status": "OK"}}, full=True)                  # a full run replaces the document
    assert list(json.loads(p.read_text())) == ["NEW|X"]


def _worker(args):
    path, table, n = args
    for i in range(n):
        merge_csv(Path(path), _rows(("S", table, str(i))), scope=_scope("S", table), fieldnames=HEADER)
    return True


def test_concurrent_runs_do_not_corrupt_the_file(tmp_path):
    p = tmp_path / "m.csv"
    merge_csv(p, [], full=True, fieldnames=HEADER)
    tables = [f"T{i}" for i in range(6)]
    with mp.get_context("fork").Pool(6) as pool:
        assert all(pool.map(_worker, [(str(p), t, 15) for t in tables]))
    fields, rows = read_csv_rows(p)
    assert sorted(r["table_name"] for r in rows) == sorted(tables)           # every run's entry is there exactly once
    assert all(r["value"] == "14" for r in rows)                            # and holds that run's last write
    with open(p, newline="") as fh:
        assert all(len(r) == len(fields) for r in csv.reader(fh))           # no torn or ragged lines


# ------------------------------------------------------------------ DDL fragments
def test_fragment_store_targeted_run_rewrites_only_its_fragments_then_reassembles(tmp_path):
    store = FragmentStore(tmp_path)
    for t in ("a", "b"):
        store.write("s1", t, "pre", f"CREATE TABLE s1.{t} (id int);\n")
    store.write("s1", SCHEMA_LEVEL, "pre", "-- schema level\n")
    store.assemble("s1", "pre", "-- head\n", tmp_path / "s1.sql")
    untouched = (tmp_path / "fragments" / "s1" / "b.pre.sql").read_bytes()

    store.write("s1", "a", "pre", "CREATE TABLE s1.a (id int, x text);\n")       # targeted run: only table a
    store.assemble("s1", "pre", "-- head\n", tmp_path / "s1.sql")
    assert (tmp_path / "fragments" / "s1" / "b.pre.sql").read_bytes() == untouched
    text = (tmp_path / "s1.sql").read_text()
    assert text.startswith("-- head\n-- schema level\n") and "s1.a (id int, x text)" in text and "s1.b (id int)" in text
    assert text.index("s1.a") < text.index("s1.b")                                # tables sorted
    assert "-- head" in store.script_for("s1", "pre", ["a"]) and "s1.b" not in store.script_for("s1", "pre", ["a"])


def test_fragment_store_empty_text_removes_stale_fragment_and_full_run_prunes(tmp_path):
    store = FragmentStore(tmp_path)
    store.write("s1", "a", "pre", "x;\n")
    store.write("s1", "a", "pk", "y;\n")
    assert store.write("s1", "a", "pk", "") is False and store.read("s1", "a", "pk") == ""
    store.write("s1", "gone", "pre", "z;\n")
    assert store.prune({("s1", "a")}) == 1 and store.tables("s1") == ["a"]
    store.record({"s1.a": {"kinds": ["pre"]}}, full=True)
    store.record({"s1.b": {"kinds": ["pre"]}}, full=False)
    assert set(json.loads(store.manifest.read_text())) == {"s1.a", "s1.b"}
