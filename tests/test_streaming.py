import subprocess

import pytest

from sweep_streaming import read_stream_head, stream_argv, stream_program_literal, validate_uri

WEIRD = "we ird it's; $(touch PWNED) `x` \"q\".csv.gz"


def test_validate_uri_rejects_control_chars_and_empty():
    for bad in ("", "   ", "a\nb", "a\x00b"):
        with pytest.raises(ValueError):
            validate_uri(bad)
    assert validate_uri("s3://b/k with space.gz") == "s3://b/k with space.gz"


def test_stream_argv_is_a_plain_list_not_a_shell_string():
    argv = stream_argv("s3://b/a;b.gz", "us-east-2")
    assert argv[0] == "bash" and argv[-2:] == ["s3://b/a;b.gz", "us-east-2"]


def test_read_stream_head_reads_weird_filename(gz_file, tmp_path):
    p = gz_file(WEIRD, "h1,h2\n1,2\n3,4\n")
    lines, err = read_stream_head(str(p), max_lines=2)
    assert lines == ["h1,h2", "1,2"] and err == ""
    assert not (tmp_path / "PWNED").exists()


def test_read_stream_head_reports_missing_file(tmp_path):
    lines, err = read_stream_head(str(tmp_path / "missing.csv"), max_lines=1)
    assert lines == [] and err            # error text surfaced, not "empty"


def test_read_stream_head_reports_corrupt_gzip(tmp_path):
    bad = tmp_path / "bad.csv.gz"
    bad.write_bytes(b"this is not gzip data")
    lines, err = read_stream_head(str(bad), max_lines=1)
    assert lines == [] and err


def test_program_literal_survives_psql_style_unquoting_and_shell(gz_file, tmp_path):
    """The literal goes inside SQL '...'; psql turns '' back into ' before handing it to the shell."""
    p = gz_file(WEIRD, "a,b\n1,2\n")
    literal = stream_program_literal(str(p), "us-east-2")
    shell_cmd = literal.replace("''", "'")
    out = subprocess.run(["bash", "-c", shell_cmd], capture_output=True, text=True, cwd=tmp_path)
    assert out.stdout == "a,b\n1,2\n"
    assert not (tmp_path / "PWNED").exists()
    assert "sweep_stream.sh" in literal   # the live dashboard recognises worker streams by this
