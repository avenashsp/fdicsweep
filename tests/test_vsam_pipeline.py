"""VSAM Step 06 SQL pipeline: one transaction, staged load, SQL ports of the Python rules (needs PostgreSQL for some)."""
import pytest

import sweep_common as c
import vsam.vsam_06_load_data as v6
from vsam.common_mapping import get_last_3_digits, get_active_schemas_map, match_schema_by_last3
from vsam.common_records import is_pk_val_invalid, is_sentinel_or_empty_val
from vsam.vsam_06_pipeline import HELPERS, build_unit_script, parse_counts


def _unit(arbiter, n_files=1):
    cols = {"id": {"type": "integer", "notnull": True, "hasdef": False, "identity": "", "generated": ""},
            "v": {"type": "text", "notnull": False, "hasdef": False, "identity": "", "generated": ""}}
    files = [{"i": i, "uri": f"s3://b/f{i}.csv", "name": f"f{i}.csv", "stream_cmd": "cat x", "raw_cols": ["id", "v"],
              "present": ["id", "v"], "route_col": None, "header": True, "delim_sql": "','", "quote_sql": "",
              "size_bytes": 1, "s3_meta": None, "snapshot_msg": "source=A|B;snapshot=1", "worker_id": 1}
             for i in range(1, n_files + 1)]
    return {"table": "t", "files": files, "dests": ["bcs"], "cols": cols, "arbiter": arbiter,
            "route": [("104", "bss01104")], "max_rejects": 0, "replica": False, "encoding": "UTF8"}


def test_a_keyed_file_is_one_transaction_without_insert_values_batches():
    sql = build_unit_script(_unit(["id"]))
    assert sql.count("BEGIN;") == 1 and sql.rstrip().endswith("COMMIT;")
    assert "\\copy _raw1" in sql and "ON CONFLICT (\"id\") DO UPDATE" in sql and "IS DISTINCT FROM" in sql
    assert "INSERT INTO \"bcs\".\"t\" AS t" in sql and " VALUES\n" not in sql.split("INSERT INTO _route")[-1]
    assert "PK_BLANK" in sql and "BAD_VALUE" in sql and "Row count check failed" in sql


def test_all_files_of_a_keyless_table_share_one_transaction_and_replace_it():
    sql = build_unit_script(_unit([], n_files=3))
    assert sql.count("BEGIN;") == 1 and sql.count("\\copy _raw") == 3
    assert sql.count("TRUNCATE TABLE") == 1 and "ON CONFLICT" not in sql and "PK_BLANK" not in sql
    assert sql.index("TRUNCATE TABLE") > sql.index("_raw3") and sql.index("TRUNCATE TABLE") < sql.index("INSERT INTO \"bcs\".\"t\"")


def test_reject_limit_is_checked_per_file_before_anything_is_applied():
    unit = _unit(["id"])
    unit["max_rejects"] = 5
    sql = build_unit_script(unit)
    assert "Too many rejected rows in %" in sql and sql.index("Too many rejected") < sql.index("ON CONFLICT")


def test_parse_counts_groups_by_file_and_destination():
    out = parse_counts({"raw|1": 10, "rejected|1": 2, "dup|1": 1, "inserted|1|bcs": 4, "updated|1|bcs": 1,
                        "unchanged|1|bss01104": 2, "staged|1|bcs": 5, "raw|2": 3})
    assert out[1]["by_dest"]["bcs"] == {"inserted": 4, "updated": 1, "unchanged": 0, "staged": 5}
    assert out[1]["by_dest"]["bss01104"]["unchanged"] == 2 and out[2]["raw"] == 3


@pytest.mark.integration
def test_sql_rules_match_the_python_rules(pg_db):
    c.run_psql(HELPERS.replace("pg_temp.", "public."))
    values = ["", " ", None, "nan", "None", "NULL", "9", "9999", "999999", "9999-99-99", "9999_99_99", "0", "00", "0000-00-00",
              "12", "0012", "9998", "9999-12-31", "abc", " 9999 "]
    for v in values:
        lit = "NULL" if v is None else c.lit(v)
        got = c.run_psql(f"SELECT public.sw_blank({lit}) OR public.sw_nines({lit}), public.sw_sent({lit})", tuples_only=True).replace(" ", "").split("|")
        assert (got[0] == "t") == is_pk_val_invalid(v), f"pk invalid mismatch for {v!r}"
        assert (got[1] == "t") == is_sentinel_or_empty_val(v), f"sentinel mismatch for {v!r}"


@pytest.mark.integration
def test_sql_routing_matches_match_schema_by_last3(pg_db):
    c.run_psql(HELPERS.replace("pg_temp.", "public."))
    route = get_active_schemas_map()
    for v in ["104", "0104", "112.0", "00110", "9999", "0106", "", " 105 ", "A110", "7", "x", "0001", "99"]:
        last3 = c.run_psql(f"SELECT coalesce(public.sw_last3({c.lit(v)}), '')", tuples_only=True)
        assert last3 == get_last_3_digits(v), f"last3 mismatch for {v!r}"
        assert route.get(last3, "bcs") == match_schema_by_last3(v), f"routing mismatch for {v!r}"


@pytest.mark.integration
def test_older_snapshot_is_detected(pg_db):
    c.run_psql(v6.load_log_sql("f_new", "bcs", "t", 1, 1, 0.1, 1, "SUCCESS", "source=SWP|X;snapshot=2024-02-01"))
    assert v6.newer_snapshot_loaded("t", "SWP|X", "2024-01-01") is True
    assert v6.newer_snapshot_loaded("t", "SWP|X", "2024-02-01") is False
    assert v6.newer_snapshot_loaded("t", "OTHER|X", "2024-01-01") is False
    assert v6.newer_snapshot_loaded("t", "SWP|X", "") is False


def _spec(path, i, name):
    return {"i": i, "uri": str(path), "name": name, "table": "kk", "task": {"target_schemas": ["bcs"]}, "source_key": f"A|{name}",
            "cb_display": "NO_COPYBOOK", "sc_display": "NO_KORG", "has_schema_col": False, "route_col": None,
            "columns_meta": [{"col_name": "a", "sql_type": "integer"}, {"col_name": "b", "sql_type": "text"}],
            "raw_cols": ["a", "b"], "header": True, "delim_sql": "','", "quote_sql": "", "size_bytes": path.stat().st_size,
            "size_mb": 0.01, "worker_id": 1, "snapshot_msg": f"source=A|{name};snapshot=1", "expected_key": [], "pk_error": "",
            "s3_meta": None}


@pytest.mark.integration
def test_keyless_table_files_replace_it_together_or_not_at_all(pg_db, tmp_path):
    c.run_psql("CREATE SCHEMA IF NOT EXISTS bcs; CREATE TABLE bcs.kk (a int, b text)")
    run = {"max_rejects": 0, "heads": {}, "replica": False}

    def files(*contents):
        out = []
        for n, text in enumerate(contents, 1):
            p = tmp_path / f"f{n}_{abs(hash(text)) % 9999}.csv"
            p.write_text(text)
            out.append(_spec(p, n, p.name))
        return out

    res = v6.run_unit("kk", files("a,b\n1,x\n2,x\n3,x\n", "a,b\n4,y\n5,y\n"), 1, run, {})
    assert [r["status"] for r in res] == ["SUCCESS", "SUCCESS"] and [r["rows_loaded"] for r in res] == [3, 2]
    assert c.run_psql("SELECT count(*) FROM bcs.kk", tuples_only=True) == "5"

    res = v6.run_unit("kk", files("a,b\n9,new\n", "a,b\n10,new\n"), 1, run, {})
    assert [r["status"] for r in res] == ["SUCCESS", "SUCCESS"]
    assert c.run_psql("SELECT string_agg(b, ',') FROM bcs.kk", tuples_only=True) == "new,new"       # replaced, not appended

    run_strict = dict(run, max_rejects=1)
    res = v6.run_unit("kk", files("a,b\n20,ok\n", "a,b\nnot-int,bad\n"), 1, run_strict, {})
    assert [r["status"] for r in res] == ["FAILED", "FAILED"] and "rolled back together" in res[0]["error"]
    assert c.run_psql("SELECT string_agg(b, ',') FROM bcs.kk", tuples_only=True) == "new,new"       # old snapshot kept
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.load_log WHERE status = 'SUCCESS' AND uri LIKE '%#bcs'", tuples_only=True) == "4"   # no row for the failed attempt
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.load_log WHERE status = 'FAILED'", tuples_only=True) == "2"


@pytest.mark.integration
def test_vsam_row_with_two_not_null_violations_is_one_rejected_row(pg_db, tmp_path):
    c.run_psql("CREATE SCHEMA IF NOT EXISTS bcs; CREATE TABLE bcs.kk (a int NOT NULL, b text NOT NULL)")
    p = tmp_path / "f.csv"
    p.write_text("a,b\n,\n2,x\n,y\n")
    res = v6.run_unit("kk", [_spec(p, 1, p.name)], 1, {"max_rejects": 0, "heads": {}, "replica": False}, {})
    assert res[0]["status"] == "SUCCESS", res[0]["error"]
    assert (res[0]["total_records"], res[0]["rows_loaded"], res[0]["rows_rejected"]) == (3, 1, 2)
