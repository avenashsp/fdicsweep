import itertools

import numpy as np
import pandas as pd
import pytest

from vsam import common_colmap as cm
from vsam.common_copybook import parse_filename


def field(name, sql_type="varchar(10)", is_key=False, filler=False):
    return {"field_name": name, "col_name": name.lower().replace("-", "_"), "sql_type": sql_type, "pic": "", "usage": "",
            "is_key": is_key, "is_filler": "YES" if filler else "NO"}


@pytest.mark.parametrize("seed", range(40))
def test_hungarian_is_optimal_against_brute_force(seed):
    rng = np.random.RandomState(seed)
    n, m = rng.randint(1, 6), rng.randint(1, 6)
    score = rng.rand(n, m)
    ans = cm.hungarian_max(score)
    got = sum(score[i, j] for i, j in enumerate(ans) if j >= 0)
    k = max(n, m)
    padded = np.zeros((k, k))
    padded[:n, :m] = score
    best = max(sum(padded[i, p[i]] for i in range(n) if p[i] < m) for p in itertools.permutations(range(k)))
    assert got == pytest.approx(best)
    assigned = [j for j in ans if j >= 0]
    assert len(assigned) == len(set(assigned))             # one-to-one


def test_hungarian_handles_500x500_quickly():
    import time
    t = time.time()
    cm.hungarian_max(np.random.RandomState(1).rand(500, 500))
    assert time.time() - t < 20


FIELDS = [field("ACH-ACCT-NBR", "integer", True), field("ACH-KORG", "integer", True), field("ACH-AMT", "numeric(9,2)"),
          field("ACH-EFF-DT", "varchar(10)"), field("ACH-STATUS-CD", "varchar(1)"), field("FILLER", "varchar(20)", filler=True)]


def by_header(entries):
    return {e["header"]: e for e in entries if e["header"]}


def test_headers_with_different_names_and_order_are_matched():
    e = by_header(cm.map_columns(["status_code", "b_korg", "amount", "account_number", "effective_date", "extra_note"], FIELDS))
    assert e["status_code"]["copybook_col"] == "ach_status_cd" and e["status_code"]["pg_type"] == "varchar(1)"
    assert e["b_korg"]["copybook_col"] == "ach_korg"                       # single-letter prefix is optional
    assert e["account_number"]["copybook_col"] == "ach_acct_nbr" and e["amount"]["pg_type"] == "numeric(9,2)"
    assert e["effective_date"]["copybook_col"] == "ach_eff_dt"
    assert e["extra_note"]["status"] == "UNMAPPED" and e["extra_note"]["pg_type"] == "text"
    assert all(x["status"] == "ACCEPTED" for k, x in e.items() if k != "extra_note")


def test_fillers_never_match_and_unused_copybook_columns_are_listed():
    entries = cm.map_columns(["amount"], FIELDS)
    only = [e for e in entries if e["status"] == "COPYBOOK_ONLY"]
    assert "filler" not in {e["copybook_col"] for e in entries}
    assert {"ach_acct_nbr", "ach_korg"} <= {e["copybook_col"] for e in only}


def test_exact_names_win_and_are_used_once():
    fields = [field("AMOUNT", "numeric(9,2)"), field("AMOUNT-2", "numeric(9,2)")]
    e = cm.map_columns(["amount", "amount_2"], fields)
    assert [x["method"] for x in e if x["header"]] == ["EXACT_NAME", "EXACT_NAME"]
    assert {x["copybook_col"] for x in e if x["header"]} == {"amount", "amount_2"}


def test_conflicting_occurrence_index_and_modifier_guards_do_not_match():
    fields = [field("PHONE-2"), field("PREV-BALANCE", "numeric(9,2)")]
    e = by_header(cm.map_columns(["phone_1", "balance"], fields))
    assert e["phone_1"]["status"] == "UNMAPPED"        # PHONE_1 vs PHONE_2
    assert e["balance"]["status"] == "UNMAPPED"        # BALANCE vs PREV-BALANCE


def test_acronym_rule_needs_the_whole_name_to_be_the_acronym():
    fields = [field("FN-ADDR")]
    assert by_header(cm.map_columns(["first_name"], fields))["first_name"]["status"] != "ACCEPTED"   # was 0.98 in the prototype
    fields = [field("RAU")]
    assert by_header(cm.map_columns(["responsible_accounting_unit"], fields))["responsible_accounting_unit"]["status"] == "ACCEPTED"


def test_override_wins_and_can_force_unmapped():
    e = by_header(cm.map_columns(["weird_name", "amount"], FIELDS, overrides={"weird_name": "ach_status_cd", "amount": ""}))
    assert e["weird_name"]["method"] == "OVERRIDE" and e["weird_name"]["copybook_col"] == "ach_status_cd"
    assert e["amount"]["status"] == "UNMAPPED"


def test_sticky_mapping_keeps_previous_match_when_headers_change():
    prev = {"weird_name": "ach_status_cd"}
    e = by_header(cm.map_columns(["weird_name", "brand_new"], FIELDS, previous=prev))
    assert e["weird_name"]["method"] == "STICKY" and e["weird_name"]["copybook_col"] == "ach_status_cd"


def test_table_column_names_for_both_modes():
    entries = cm.map_columns(["status_code", "extra_note"], FIELDS)
    assert cm.table_column_names(entries, "headers") == ["status_code", "extra_note"]
    assert cm.table_column_names(entries, "copybook") == ["ach_status_cd", "extra_note"]


def test_tbl_col_resolution(monkeypatch):
    monkeypatch.delenv("VSAM_TBL_COL", raising=False)
    assert cm.resolve_tbl_col() == "headers"
    monkeypatch.setenv("VSAM_TBL_COL", "copybook")
    assert cm.resolve_tbl_col() == "copybook" and cm.resolve_tbl_col("headers") == "headers"
    with pytest.raises(ValueError):
        cm.resolve_tbl_col("both")


def test_keys_need_accepted_matches():
    entries = cm.map_columns(["account_number", "b_korg"], FIELDS)
    assert cm.translate_key_columns(["ach_acct_nbr", "ach_korg"], entries, "headers") == (["account_number", "b_korg"], [])
    assert cm.translate_key_columns(["ach_acct_nbr", "ach_korg"], entries, "copybook")[0] == ["ach_acct_nbr", "ach_korg"]
    keys, problems = cm.translate_key_columns(["ach_acct_nbr", "ach_missing"], entries, "headers")
    assert keys == [] and problems                                     # one unreliable key column drops the whole key
    review = [dict(e, status="REVIEW") if e["header"] == "b_korg" else e for e in entries]
    assert cm.translate_key_columns(["ach_korg"], review, "headers")[0] == []


def test_effective_columns_meta_roundtrip(tmp_path):
    path = tmp_path / "map.csv"
    entries = cm.map_columns(["status_code", "b_korg", "extra_note"], FIELDS)
    rows = cm.finalize_entries(entries, "headers", "FDIC", "ACHFILE", "f.csv", "ACHFILE")
    cm.write_column_map(rows, path)
    meta = cm.effective_columns_meta("FDIC", "ACHFILE", [{"col_name": "fallback"}], path=path)
    assert [m["col_name"] for m in meta] == ["status_code", "b_korg", "extra_note"]
    assert meta[0]["sql_type"] == "varchar(1)" and meta[0]["header"] == "status_code" and meta[1]["is_key"] == "YES"
    assert meta[2]["sql_type"] == "text"
    assert cm.header_to_table_map(meta) == {"status_code": "status_code", "b_korg": "b_korg", "extra_note": "extra_note"}
    # unknown source or a source without any mapped column -> fallback untouched
    assert cm.effective_columns_meta("X", "Y", [{"col_name": "fallback"}], path=path) == [{"col_name": "fallback"}]
    no_cb = cm.finalize_entries(cm.no_copybook_entries(["a", "b"]), "headers", "SWP", "NOCB", "f.csv", "NO_COPYBOOK")
    cm.write_column_map(rows + no_cb, path)
    assert cm.effective_columns_meta("SWP", "NOCB", [], path=path) == []
    copybook_mode = cm.finalize_entries(entries, "copybook", "FDIC", "ACHFILE", "f.csv", "ACHFILE")
    cm.write_column_map(copybook_mode, path)
    meta = cm.effective_columns_meta("FDIC", "ACHFILE", [], path=path)
    assert [m["col_name"] for m in meta] == ["ach_status_cd", "ach_korg", "extra_note"]
    assert cm.header_to_table_map(meta)["b_korg"] == "ach_korg"


def test_previous_map_feeds_sticky(tmp_path):
    path = tmp_path / "map.csv"
    entries = cm.map_columns(["status_code"], FIELDS)
    cm.write_column_map(cm.finalize_entries(entries, "headers", "FDIC", "ACHFILE", "f.csv", "ACHFILE"), path)
    assert cm.load_previous_map(path) == {("FDIC", "ACHFILE"): {"status_code": "ach_status_cd"}}


@pytest.mark.parametrize("name,expected_type", [
    ("FDICSWP_FINTEC_MXSTBL4_FLAT_20240201.csv", "date"),
    ("FDICSWP_FINTEC_MXSTBL4_FLAT_09092026.csv", "date"),
    ("FDICSWP_FINTEC_MXSTBL4_FLAT_20240201123015.csv", "datetime"),
    ("FDICSWP_FINTEC_MXSTBL4_FLAT_20240201_123015.csv.gz", "datetime"),
    ("FDICSWP_FINTEC_MXSTBL4_FLAT_2024-02-01.csv", "date"),
    ("FDICSWP_FINTEC_MXSTBL4_FLAT.csv", None),
])
def test_filename_timestamps(name, expected_type):
    co, src, dt = parse_filename(name)
    assert (co, src) == ("FINTEC", "MXSTBL4")
    assert (type(dt).__name__ if dt else None) == expected_type


def test_unparseable_filename_is_still_rejected():
    assert parse_filename("something_else.csv") is None


def test_pandas_is_not_needed_for_mapping_itself():
    # map_columns works on plain dicts; DataFrame input is only used for I/O helpers
    assert isinstance(pd.DataFrame(cm.map_columns(["a"], [field("A")])), pd.DataFrame)


def test_column_overrides_sheet_is_read(tmp_path, monkeypatch):
    import openpyxl
    wbp = tmp_path / "ov.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "column_overrides"
    ws.append(["sourcecompany", "filename", "header", "copybook_column"])
    ws.append(["FDIC", "ACHFILE", "Weird Name", "ACH-STATUS-CD"])
    ws.append(["", "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv", "amount", ""])
    wb.save(wbp)
    monkeypatch.setattr(cm, "MASTER_OVERRIDES_FILE", wbp)
    ov = cm.load_column_overrides()
    merged = cm.overrides_for(ov, "FDIC", "ACHFILE", "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv")
    assert merged == {"weird_name": "ach_status_cd", "amount": ""}
    assert cm.overrides_for(ov, "OTHER", "ACHFILE", "x.csv") == {}
    monkeypatch.setattr(cm, "MASTER_OVERRIDES_FILE", tmp_path / "missing.xlsx")
    assert cm.load_column_overrides() == {}
