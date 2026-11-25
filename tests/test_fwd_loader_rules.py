"""Oracle Step 06 loads BRONZE, the raw mirror of the source: FE files are never applied there, so the loader has no FE rules (promote applies them in silver).
Unknown columns fail the file, the dry-run checks the header."""
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






def test_missing_required_column_without_rule_fails(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, must text NOT NULL)")
    msgs = grab(o6.logger)
    assert load("t", "id\n1\n") is False and any("NOT NULL column(s) without a default are absent" in m for m in msgs)



def test_dry_run_reads_the_header_and_reports_problems_before_the_real_run(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    msgs = grab(o6.logger)
    assert load("t", "id,v,new_col\n1,a,x\n", dry_run=True) is False
    assert any("dry-run check" in m and "new_col" in m for m in msgs)
    assert q1("SELECT count(*) FROM oo.t") == "0" and q1("SELECT count(*) FROM sweep_ctl.oracle_load_log") == "0"
    assert load("t", "id,v\n1,a\n", dry_run=True) is True
    assert any("DRY-RUN OK" in m for m in msgs)
    assert q1("SELECT count(*) FROM oo.t") == "0"


def test_the_bronze_loader_does_not_apply_fe_rules_even_when_a_rules_row_exists(load, grab):
    c.run_psql("CREATE TABLE oo.t (id int PRIMARY KEY, v text)")
    _rule("ignore-column", "t", "old")                            # FE files are applied in silver; this row would only be there by mistake
    msgs = grab(o6.logger)
    assert load("t", "id,v,old\n1,a,x\n") is False and any("not found in table: old" in m for m in msgs)
    assert q1("SELECT count(*) FROM oo.t") == "0"
