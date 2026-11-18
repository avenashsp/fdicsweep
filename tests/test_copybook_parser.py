from pathlib import Path

import pytest

from vsam.common_cobol import clean_copybook_lines, parse_copybook_tree, split_statements, unwrap_pseudo_text
from vsam.common_copybook import parse_copybook
from vsam.vsam_01_extract_copybooks import parse_copybook_with_keys

DATA = Path(__file__).parent / "data" / "copybooks"


def fields(name):
    return parse_copybook_with_keys(DATA / name)


def test_fixed_format_with_sequence_numbers_and_change_tags():
    f = fields("MXSOPTO.txt")
    names = [x["field_name"] for x in f]
    assert names == [
        "MXSOPTO-AGILE-BANK", "MXSOPTO-BROKER-ACCOUNT", "MXSOPTO-FROM-PRODUCT-CUSIP", "MXSOPTO-TO-PRODUCT-CUSIP",
        "MXSOPTO-OPT-OUT-IND", "MXSOPTO-ORIGINAL-BRK-CUSIP", "MXSOPTO-REPORT-BRK-CUSIP", "FILLER",
    ]
    assert [x["col_name"] for x in f if x["is_key"]] == ["mxsopto_agile_bank", "mxsopto_broker_account"]
    assert f[-1]["is_filler"] and not f[-1]["is_key"]
    types = {x["field_name"]: x["sql_type"] for x in f}
    assert types["MXSOPTO-AGILE-BANK"] == "integer" and types["MXSOPTO-BROKER-ACCOUNT"] == "varchar(12)"


def test_pseudo_text_names_comp3_and_digit_leading_fields():
    f = fields("USA090.txt")
    by = {x["field_name"]: x for x in f}
    assert list(by)[:3] == ["USA090-KORG", "USA090-KAACCOUNT", "USA090-KRPT"]
    assert [x["field_name"] for x in f if x["is_key"]] == ["USA090-KORG", "USA090-KAACCOUNT"]
    assert by["USA090-KORG"]["usage"] == "COMP-3"
    assert by["USA090-06BALCUR"]["sql_type"] == "numeric(15,2)"
    assert by["USA090-06DIVENP"]["sql_type"] == "numeric(13,5)"
    assert by["USA090-KAACCOUNT"]["sql_type"] == "bigint"
    assert ":" not in "".join(by)


def test_multiline_value_clause_and_key_groups():
    f = fields("NEXTMODEL.txt")
    by = {x["field_name"]: x for x in f}
    assert by["NEXT-MODEL-TYPE"]["is_key"] and by["NEXT-MODEL-BANK"]["is_key"]
    assert by["NEXT-MODEL-READ-CUSIP"]["is_key"]                       # under NEXT-MODEL-READ-KEY
    assert not by["NEXT-MODEL-MAX-MODEL"]["is_key"]
    assert by["NEXT-MODEL-REST-OF-RECORD"]["sql_type"] == "varchar(2836)"
    assert sum(1 for x in f if x["is_filler"]) == 1


def test_redefines_is_skipped_and_numeric_sequence_columns_are_removed():
    f = fields("MXSDEBIT.txt")
    names = [x["field_name"] for x in f]
    assert names[0] == "MXS-KEY" and "MXS-COUNTER-ARRAY-R_1" not in names       # REDEFINES not duplicated
    assert [n for n in names if n.startswith("MXS-DEBIT-CNT")] == [f"MXS-DEBIT-CNT{i}" for i in range(6)]
    assert [x["field_name"] for x in f if x["is_key"]] == ["MXS-KEY"]
    assert names[-1] == "FILLER"


@pytest.mark.parametrize("name", ["MXSOPTO.txt", "USA090.txt", "NEXTMODEL.txt", "MXSDEBIT.txt"])
def test_legacy_parser_agrees_with_step01_parser(name):
    assert [x["name"] for x in parse_copybook(DATA / name)] == [x["field_name"] for x in fields(name)]


def test_glued_sequence_number_after_period_does_not_merge_statements():
    text = "       01  REC.                                                         00010001\n" \
           "           05  A    PIC X(2)   VALUE SPACE.00020001\n" \
           "           05  B    PIC 9(3)   VALUE ZERO. 00030001\n"
    tree = parse_copybook_tree(text)
    assert [c["name"] for c in tree["children"][0]["children"]] == ["A", "B"]


def test_periods_inside_quoted_literals_do_not_split_statements():
    stmts = split_statements(["05 A PIC X(5) VALUE 'A. B'.", "05 B PIC X."])
    assert len(stmts) == 2 and "A. B" in stmts[0]


def test_comments_banners_and_inline_comments_are_dropped():
    lines = clean_copybook_lines("=====\n154130***********  LAYOUT TO FILE\n      * a comment\n"
                                 "       01  R.   *> inline\n")
    assert lines == ["01  R."]


def test_unwrap_pseudo_text():
    assert unwrap_pseudo_text(":NEXT-MODEL:-KEY") == "NEXT-MODEL-KEY"
