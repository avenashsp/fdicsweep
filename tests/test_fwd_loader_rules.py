"""FE loader rules in Oracle Step 06 (ignore / rename / default / skip, unknown column still fails, dry-run header check)."""
import csv
import gzip

import pytest

import sweep_common as c
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_order as order
from sweep_s3meta import head_object

pytestmark = pytest.mark.integration


def _rule(rule, table, col=None, new=None, expr=None, schema="oo"):
    q = lambda v: "NULL" if v is None else "'" + v.replace("'", "''") + "'"
    c.run_psql(f"INSERT INTO sweep_ctl.fwd_ddl_rules (file_name, rule, schema_name, table_name, column_name, new_name, expression) VALUES "
               f"('V1__t.sql', {q(rule)}, {q(schema)}, {q(table)}, {q(col)}, {q(new)}, {q(expr)})")


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


def test_unknown_file_column_still_fails_and_names_the_column_and_the_pragmas(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    msgs = grab(o6.logger)
    assert load("t", "id,v,old\n1,a,x\n") is False
    text = " ".join(msgs)
    assert "not found in table: old" in text and "ignore-column" in text and "docs/fwd_ddl.md" in text
    assert q1("SELECT count(*) FROM oo.t") == "0"


def test_ignore_column_drops_it_while_loading(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    _rule("ignore-column", "t", "old")
    msgs = grab(o6.logger)
    assert load("t", "id,v,old\n1,a,x\n2,b,y\n") is True
    assert q1("SELECT string_agg(id || v, ',' ORDER BY id) FROM oo.t") == "1a,2b"
    assert any("old" in m and "ignored (FE ignore-column rule)" in m for m in msgs)


def test_rename_column_loads_the_file_column_into_the_new_table_column(load):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, full_name text, qty int)")
    _rule("rename-column", "t", "name", "full_name")
    assert load("t", "id,name,qty\n1,Ann,5\n2,Bob,not-a-number\n") is True
    assert q1("SELECT string_agg(id || full_name, ',' ORDER BY id) FROM oo.t") == "1Ann"                         # row 2 rejected on qty (BAD_VALUE)
    assert q1("SELECT reason || ':' || detail FROM sweep_ctl.load_rejects WHERE track = 'oracle'") == "BAD_VALUE:qty"
    c.run_psql("UPDATE oo.t SET full_name = 'old'")
    assert load("t", "id,name,qty\n1,Ann2,5\n") is True                                                            # update uses the renamed column too
    assert q1("SELECT full_name FROM oo.t WHERE id = 1") == "Ann2"


def test_rename_to_a_missing_column_fails(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    _rule("rename-column", "t", "name", "nowhere")
    msgs = grab(o6.logger)
    assert load("t", "id,v,name\n1,a,b\n") is False and any("rename-column target does not exist" in m for m in msgs)


def test_default_column_fills_a_new_required_column_but_never_overwrites_existing_rows(load):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text, must text NOT NULL)")
    c.run_psql("INSERT INTO oo.t VALUES (1, 'a', 'kept')")
    _rule("default-column", "t", "must", expr="'dflt'")
    assert load("t", "id,v\n1,a2\n2,b\n") is True
    assert q1("SELECT string_agg(id || ':' || v || ':' || must, ',' ORDER BY id) FROM oo.t") == "1:a2:kept,2:b:dflt"      # row 1 keeps its value
    assert q1("SELECT columns FROM sweep_ctl.delta_registry ORDER BY seq DESC LIMIT 1") == "id,v,must"                  # the delta carries TABLE columns


def test_missing_required_column_without_rule_fails(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, must text NOT NULL)")
    msgs = grab(o6.logger)
    assert load("t", "id\n1\n") is False and any("NOT NULL column(s) without a default are absent" in m for m in msgs)


def test_skip_table_reports_skipped_not_failed(load, grab):
    _rule("skip-table", "gone")
    msgs = grab(o6.logger)
    c.run_psql("CREATE TABLE oo.gone (id int)")                  # still in the file map, dropped by FE afterwards
    c.run_psql("DROP TABLE oo.gone")
    assert load("gone", "id\n1\n") is True
    assert any("SKIPPED" in m and "skip-table rule" in m for m in msgs)
    assert q1("SELECT count(*) FROM sweep_ctl.oracle_load_log") == "0"


def test_dry_run_reads_the_header_and_reports_problems_before_the_real_run(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    msgs = grab(o6.logger)
    assert load("t", "id,v,new_col\n1,a,x\n", dry_run=True) is False
    assert any("dry-run check" in m and "new_col" in m for m in msgs)
    assert q1("SELECT count(*) FROM oo.t") == "0" and q1("SELECT count(*) FROM sweep_ctl.oracle_load_log") == "0"
    _rule("ignore-column", "t", "new_col")
    assert load("t", "id,v,new_col\n1,a,x\n", dry_run=True) is True
    assert any("DRY-RUN OK" in m and "1 ignored" in m for m in msgs)
    assert q1("SELECT count(*) FROM oo.t") == "0"
