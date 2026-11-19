import pytest

import sweep_common as c
from vsam.vsam_06_load_data import build_load_summary, format_schema_counts


def test_short_messages_are_kept_exactly():
    msg = "psql: ERROR: duplicate key value violates unique constraint\nDETAIL: Key (id)=(1) already exists."
    assert c.clip_error(msg) == msg
    assert c.clip_error(None) == "" and c.clip_error("  x  ") == "x"


@pytest.mark.parametrize("n", [999, 1000])
def test_up_to_the_limit_is_not_touched(n):
    assert c.clip_error("a" * n) == "a" * n


@pytest.mark.parametrize("n", [1001, 2500, 50000])
def test_longer_messages_are_cut_to_exactly_the_limit_with_a_marker(n):
    out = c.clip_error("e" * n)
    assert len(out) == 1000 and out.endswith("chars]") and out.startswith("e" * 900)
    dropped = int(out.rsplit("[+", 1)[1].split(" ")[0])
    assert dropped == n - len(out.split(" ...[+")[0])


def test_the_old_300_character_cap_is_gone():
    msg = "x" * 700
    assert c.clip_error(msg) == msg


def test_schema_counts_format():
    assert format_schema_counts({"bss01112": 34, "bss01104": 20}) == "bss01104 = 20, bss01112 = 34"
    assert format_schema_counts({"bss01104": 0}) == "" and format_schema_counts({}) == ""


def test_load_summary_shows_schema_breakdowns_instead_of_totals():
    rows = build_load_summary([{
        "s3_filename": "f.csv", "table": "t", "status": "SUCCESS", "records_loaded_mapped": 54,
        "records_loaded_unmapped": 0, "records_loaded_mapped_detail": "bss01104 = 20, bss01112 = 34",
        "records_loaded_unmapped_detail": "", "records_loaded_bcs": 3, "full_path": "u"}])
    assert rows[0]["records loaded in mapped schema"] == "bss01104 = 20, bss01112 = 34"
    assert rows[0]["records loaded in unmapped schema"] == ""
    assert rows[0]["records loaded in bcs schema"] == 3
