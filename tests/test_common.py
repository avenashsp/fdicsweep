import os

import pytest

import sweep_common as c


def test_version_is_5_2():
    assert c.__version__ == "5.2"


@pytest.mark.parametrize("line,expected", [
    ("export A=1", ("A", "1")),
    ("A=1", ("A", "1")),
    ("A = 'x y' ", ("A", "x y")),
    ('A="x y"', ("A", "x y")),
    ("B=1=2", ("B", "1=2")),
    ("  # comment", None),
    ("", None),
    ("novalue", None),
    ("=x", None),
])
def test_parse_env_line(line, expected):
    assert c.parse_env_line(line) == expected


def test_env_file_does_not_override_real_environment(tmp_path, monkeypatch):
    f = tmp_path / "sweep.env"
    f.write_text("export SWEEP_TEST_VAR=from_file\nexport SWEEP_TEST_ONLY_FILE=1\n")
    monkeypatch.setenv("SWEEP_TEST_VAR", "from_shell")
    monkeypatch.delenv("SWEEP_TEST_ONLY_FILE", raising=False)
    c.load_env_file(f)
    assert os.environ["SWEEP_TEST_VAR"] == "from_shell"      # operator export wins
    assert os.environ["SWEEP_TEST_ONLY_FILE"] == "1"         # file fills gaps
    monkeypatch.delenv("SWEEP_TEST_ONLY_FILE", raising=False)


def test_has_db_auth(monkeypatch, tmp_path):
    monkeypatch.setattr(c, "PGPASSWORD", "")
    monkeypatch.delenv("SWEEP_ALLOW_NO_PASSWORD", raising=False)
    monkeypatch.delenv("PGSERVICE", raising=False)
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / "missing"))
    assert c.has_db_auth() is False
    with pytest.raises(RuntimeError):
        c.check_db_credentials()
    (tmp_path / "pgpass").write_text("*:*:*:*:pw\n")
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / "pgpass"))
    assert c.has_db_auth() is True
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / "missing"))
    monkeypatch.setenv("SWEEP_ALLOW_NO_PASSWORD", "1")
    assert c.has_db_auth() is True
    monkeypatch.delenv("SWEEP_ALLOW_NO_PASSWORD")
    monkeypatch.setattr(c, "PGPASSWORD", "secret")
    assert c.has_db_auth() is True


def test_psql_env_merges_options_and_tags_sessions(monkeypatch):
    monkeypatch.delenv("PGOPTIONS", raising=False)
    env = c.psql_env({"PGOPTIONS": "-c statement_timeout=0"}, options="-c synchronous_commit=off")
    assert "standard_conforming_strings=on" in env["PGOPTIONS"]
    assert "synchronous_commit=off" in env["PGOPTIONS"]
    assert "statement_timeout=0" in env["PGOPTIONS"]
    assert env["PGAPPNAME"] == c.PSQL_APP_NAME


def test_quoting_helpers():
    assert c.q('we"ird') == '"we""ird"'
    assert c.lit("it's") == "'it''s'"
    assert c.lit(None) == "NULL"
    assert c.lit("a\\b") == "'a\\b'"             # backslash stays literal (standard_conforming_strings=on)
    assert c.lit("a\x00b") == "'ab'"             # NUL cannot be stored


@pytest.mark.parametrize("args,expected", [
    (("VARCHAR2", 100, "", ""), "varchar(100)"),
    (("CHAR", 3, "", ""), "char(3)"),
    (("NUMBER", "", 4, 0), "smallint"),
    (("NUMBER", "", 9, 0), "integer"),
    (("NUMBER", "", 18, 0), "bigint"),
    (("NUMBER", "", 22, 0), "numeric(22,0)"),
    (("NUMBER", "", 12, 2), "numeric(12,2)"),
    (("NUMBER", "", "", ""), "numeric"),
    (("DATE", "", "", ""), "timestamp(0)"),
    (("TIMESTAMP(6) WITH TIME ZONE", "", "", ""), "timestamptz"),
    (("CLOB", "", "", ""), "text"),
    (("BLOB", "", "", ""), "bytea"),
    (("SOMETHING_ELSE", "", "", ""), "text"),
])
def test_oracle_to_pg(args, expected):
    assert c.oracle_to_pg(*args) == expected


def test_split_bounds_respects_parens_and_quotes():
    assert c.split_bounds("1, 'a,b', f(2,3), 4") == ["1", "'a,b'", "f(2,3)", "4"]


def test_bound_pg():
    assert c.bound_pg("MAXVALUE") == "MAXVALUE"
    assert c.bound_pg("TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')") == "'2024-01-01'"


def test_child_name_is_capped_at_63():
    assert len(c.child_name("t" * 50, "p" * 50)) == 63


def test_exception_groups_are_tuples_of_exception_types():
    for grp in (c.PARSE_ERRORS, c.DB_ERRORS, c.FILE_READ_ERRORS, c.AWS_ERRORS):
        assert all(issubclass(e, Exception) for e in grp)
    assert RuntimeError in c.DB_ERRORS and OSError in c.FILE_READ_ERRORS
