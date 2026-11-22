"""Uniform targeting flags: matching, validation up front, exit code 2 on a bad selector."""
import argparse

import pytest

from sweep_targeting import (EXIT_BAD_SELECTOR, Selector, SelectorError, add_targeting_args, parse_table_ref, selector_or_exit,
                             validate_or_exit)


def _args(**kw):
    ns = argparse.Namespace(tables=None, schemas=None, owners=None, schema=None, exclude_tables=None, file=None, filename=None,
                            company=None, copybook=None, dry_run=False)
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


def test_parse_table_ref_is_case_insensitive_and_schema_optional():
    assert parse_table_ref("BSS01001.BrokerAccount") == ("bss01001", "brokeraccount")
    assert parse_table_ref(" t1 ") == (None, "t1")
    for bad in ("", "a.b.c", "a..b"):
        with pytest.raises(SelectorError):
            parse_table_ref(bad)


def test_matches_bare_and_qualified_names():
    sel = Selector.from_args(_args(tables="BSS01001.Customer, orders"))
    assert sel.matches("bss01001", "CUSTOMER")
    assert not sel.matches("bss01104", "customer")          # qualified name only matches that schema
    assert sel.matches("bss01104", "ORDERS")                # bare name matches every schema
    assert not sel.matches("bss01001", "other")


def test_schemas_aliases_and_exclude_always_wins():
    sel = Selector.from_args(_args(owners="BSS01001", exclude_tables="customer"))
    assert sel.matches("bss01001", "orders") and not sel.matches("bss01001", "customer") and not sel.matches("bss01104", "orders")
    assert Selector.from_args(_args(schema="x")).schemas == {"x"}


def test_no_selector_is_a_full_run_and_matches_everything():
    sel = Selector.from_args(_args())
    assert not sel.filtered and sel.matches("any", "thing") and sel.matches_file("s3://b/x") and sel.matches_company("ZZ")
    assert Selector.from_args(_args(company="a")).filtered and Selector.from_args(_args(dry_run=True)).filtered is False


def test_file_selector_accepts_uri_key_suffix_or_basename_case_insensitively():
    sel = Selector.from_args(_args(file="s3://b/p/A.csv.gz", filename="b.csv"))
    assert sel.matches_file("s3://b/p/a.csv.gz") and sel.matches_file("s3://b/q/B.CSV") and not sel.matches_file("s3://b/p/c.csv")
    assert Selector.from_args(_args(file="x.csv"), use_files=False).files == []        # a dump path is not an S3 file


def test_validation_reports_unknown_table_schema_file_and_company_with_suggestions():
    known = [("bss01001", "customer"), ("bss01001", "orders")]
    with pytest.raises(SelectorError, match="unknown table 'custmer'.*did you mean: customer"):
        Selector.from_args(_args(tables="custmer")).validate_tables(known)
    with pytest.raises(SelectorError, match="unknown schema 'bss0100'"):
        Selector.from_args(_args(schemas="bss0100")).validate_tables(known)
    with pytest.raises(SelectorError, match="no file matches 'nope.csv'"):
        Selector.from_args(_args(file="nope.csv")).validate_files(["s3://b/a.csv"])
    with pytest.raises(SelectorError, match="unknown company ZZ"):
        Selector.from_args(_args(company="zz")).validate_companies(["AA", "BB"])
    Selector.from_args(_args(tables="bss01001.orders")).validate_tables(known)             # fine
    with pytest.raises(SelectorError, match="match no table"):                            # excludes everything
        Selector.from_args(_args(schemas="bss01001", exclude_tables="customer,orders")).validate_tables(known)


def test_bad_selector_exits_with_code_2_and_a_message(capsys):
    with pytest.raises(SystemExit) as e:
        validate_or_exit(Selector.from_args(_args(tables="zzz")).validate_tables, [("s", "t")])
    assert e.value.code == EXIT_BAD_SELECTOR
    assert "Invalid selection: unknown table 'zzz'" in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:
        selector_or_exit(_args(tables="a.b.c"))
    assert e.value.code == EXIT_BAD_SELECTOR


def test_add_targeting_args_adds_each_flag_once_and_keeps_existing_ones():
    p = argparse.ArgumentParser()
    p.add_argument("--tables", help="custom help")
    add_targeting_args(p, schemas="--owners", file=True, company=True, copybook=True)
    ns = p.parse_args(["--tables", "a", "--owners", "S", "--exclude-tables", "x", "--file", "f", "--company", "c", "--copybook", "cb", "--dry-run"])
    sel = Selector.from_args(ns)
    assert sel.schemas == {"s"} and sel.files == ["f"] and sel.companies == {"C"} and sel.copybooks == {"cb"} and sel.dry_run
    assert p.parse_args(["--schemas", "s2"]).schemas == "s2"          # --owners / --schemas are aliases of one option


def test_matches_copybook_with_or_without_extension():
    sel = Selector.from_args(_args(copybook="BANK01.cbl"))
    assert sel.matches_copybook("bank01.CBL") and sel.matches_copybook("BANK01.txt") and not sel.matches_copybook("other.cbl")
    with pytest.raises(SelectorError, match="unknown copybook"):
        sel.validate_copybooks(["x.cbl"])
