"""
End-to-end VSAM pipeline (Steps 01-07) on a throw-away COPY of the repository, so real inputs/outputs of a
checkout are never touched. Needs PostgreSQL (skipped otherwise).

Scenario: headers that differ from the copybook in name and order, a keyed table, a keyless table fed by
full snapshots (the second snapshot replaces the first), and `--tbl-col copybook`.
"""
import csv
import gzip
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import sweep_common as c

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parent.parent
COPYBOOK = Path(__file__).parent / "data" / "copybooks"
STEPS = ["vsam_01_extract_copybooks", "vsam_02_inventory", "vsam_03_type_gate", "vsam_04_generate_ddl",
         "vsam_05_apply_ddl", "vsam_06_load_data", "vsam_07_validate"]

ACH_COPYBOOK = """\
       01  ACH-REC.                                                     00010001
           05  ACH-KEY.                                                 00020001
               10  ACH-ACCT-NBR           PIC 9(9)    VALUE ZERO.       00030001
               10  ACH-KORG               PIC 9(4)    VALUE ZERO.       00040001
           05  ACH-DATA.                                                00050001
               10  ACH-AMT                PIC S9(7)V99 COMP-3.          00060001
               10  ACH-EFF-DT             PIC X(10)   VALUE SPACE.00070001
               10  ACH-STATUS-CD          PIC X(1)    VALUE SPACE.      00080001
               10  FILLER                 PIC X(20)                     00090001
                                          VALUE SPACE.                  00100001
"""


def write_csv(path, header, rows):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


@pytest.fixture()
def repo(tmp_path, pg_db):
    dst = tmp_path / "repo"
    shutil.copytree(ROOT, dst, ignore=shutil.ignore_patterns(
        ".git", "__pycache__", "outputs", "reports", "logs", ".pytest_cache", "tests", ".sweep_bg_jobs.json*"))
    (dst / "vsam" / "inputs" / "copybooks").mkdir(parents=True, exist_ok=True)
    (dst / "vsam" / "inputs" / "copybooks" / "ACHFILE.txt").write_text(ACH_COPYBOOK)
    data = tmp_path / "data" / "batch"
    data.mkdir(parents=True)
    env = dict(os.environ, PGDATABASE=pg_db, VSAM_BASE=str(tmp_path / "data"), PYTHONPATH=str(dst),
               SWEEP_CONNECT_RETRIES="0")
    env.pop("VSAM_COPYBOOK_DIR", None)
    return dst, data, env


def run_steps(repo_dir, env, names, *extra):
    for name in names:
        args = [sys.executable, "-m", f"vsam.{name}"] + (list(extra) if name == "vsam_02_inventory" else [])
        r = subprocess.run(args, cwd=repo_dir, env=env, capture_output=True, text=True, timeout=300)
        assert r.returncode == 0, f"{name} failed:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}"


def q(sql):
    return c.run_psql(sql, tuples_only=True, unaligned=True)


def write_snapshot(data, stamp, ach_status, ach_ids, xref_prefix, xref_n):
    hdr = ["status_code", "b_korg", "amount", "account_number", "effective_date", "extra_note"]
    write_csv(data / f"FDICSWP_FDIC_ACHFILE_FLAT_{stamp}.csv", hdr,
              [[ach_status, "104" if i % 2 else "112", i * 1.5, 1000 + i, "2024-01-01", "n"] for i in ach_ids])
    write_csv(data / f"FDICSWP_SWP_BCSXREF_FLAT_{stamp}.csv", ["ref_id", "ref_name", "ref_date"],
              [[i, f"{xref_prefix}{i}", "2024-01-01"] for i in range(1, xref_n + 1)])


def test_headers_map_to_copybook_types_and_snapshots_replace_keyless_tables(repo):
    repo_dir, data, env = repo
    write_snapshot(data, "20240101", "A", range(1, 41), "x", 20)
    run_steps(repo_dir, env, STEPS)

    cols = q("SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns "
             "WHERE table_schema = 'bss01104' AND table_name = 'stg_achfile_v' AND column_name NOT IN "
             "('createddate','createdby','modifieddate','modifiedby','createddate_m','createdby_m')")
    assert cols == ("status_code:character varying,b_korg:integer,amount:numeric,account_number:integer,"
                    "effective_date:date,extra_note:text")                  # header names, copybook / data types
    assert q("SELECT count(*) FROM bss01104.stg_achfile_v") == "20"
    import openpyxl
    report = sorted((repo_dir / "vsam" / "reports").glob("*vsam_06_load_data_report*.xlsx"))[-1]
    rows = list(openpyxl.load_workbook(report, read_only=True)["LoadSummary"].iter_rows(values_only=True))
    head = rows[0]
    ach = next(r for r in rows[1:] if r[head.index("tablename")] == "stg_achfile_v")
    assert ach[head.index("records loaded in mapped schema")] == "bss01104 = 20, bss01112 = 20"
    assert ach[head.index("records loaded in unmapped schema")] in (None, "")
    assert q("SELECT string_agg(a.attname, ',' ORDER BY a.attname) FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
             "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = 'bss01104.stg_achfile_v'::regclass AND i.indisprimary") == "account_number,b_korg"
    assert q("SELECT count(*) FROM pg_index WHERE indrelid = 'bcs.stg_bcsxref_rec_v'::regclass AND indisprimary") == "0"   # no key known
    assert q("SELECT count(*) FROM bcs.stg_bcsxref_rec_v") == "20"

    # newer full snapshots: keyed table upserts (nothing deleted), keyless table is replaced
    for f in data.glob("*20240101.csv"):
        f.unlink()
    write_snapshot(data, "20240201", "B", range(1, 31), "y", 25)
    run_steps(repo_dir, env, STEPS[1:])
    assert q("SELECT count(*) FROM bss01104.stg_achfile_v") == "20"
    assert q("SELECT string_agg(DISTINCT status_code, ',' ORDER BY status_code) FROM bss01104.stg_achfile_v") == "A,B"
    assert q("SELECT count(*), min(ref_name) FROM bcs.stg_bcsxref_rec_v") == "25|y1"
    assert q("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '%__sweep_stg'") == "0"


def test_sheet_and_map_outputs_exist(repo):
    repo_dir, data, env = repo
    write_snapshot(data, "20240101", "A", range(1, 11), "x", 5)
    run_steps(repo_dir, env, STEPS[:2])
    import openpyxl
    report = sorted((repo_dir / "vsam" / "reports").glob("*vsam_02_inventory_report*.xlsx"))[-1]
    wb = openpyxl.load_workbook(report, read_only=True)
    assert "ColumnMapping" in wb.sheetnames
    rows = list(wb["ColumnMapping"].iter_rows(values_only=True))
    assert rows[0][:8] == ("source_company", "source_filename", "latest_filename", "copybook_name", "header_position",
                           "header", "table_column", "copybook_field")
    mapped = {r[5]: r[7] for r in rows[1:] if r[1] == "ACHFILE" and r[5]}
    assert mapped["account_number"] == "ACH-ACCT-NBR" and mapped["status_code"] == "ACH-STATUS-CD"
    assert (repo_dir / "vsam" / "outputs" / "vsam_02_column_map.csv").exists()


def test_tbl_col_copybook_names_the_columns_after_the_copybook(repo):
    repo_dir, data, env = repo
    write_snapshot(data, "20240101", "A", range(1, 21), "x", 5)
    run_steps(repo_dir, env, STEPS, "--tbl-col", "copybook")
    cols = q("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'bss01104' "
             "AND table_name = 'stg_achfile_v' AND column_name NOT LIKE 'created%' AND column_name NOT LIKE 'modified%'")
    assert cols == "ach_status_cd,ach_korg,ach_amt,ach_acct_nbr,ach_eff_dt,extra_note"
    assert q("SELECT count(*) FROM bss01104.stg_achfile_v WHERE ach_amt IS NULL OR ach_eff_dt IS NULL") == "0"   # data followed the mapping


def test_wrong_copybook_type_falls_back_to_text_instead_of_nulling_data(repo):
    repo_dir, data, env = repo
    # 'amount' holds text although its copybook match is numeric -> must load as text, values preserved
    write_csv(data / "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv",
              ["status_code", "b_korg", "amount", "account_number", "effective_date"],
              [["A", "104", "n/a", 1, "2024-01-01"], ["A", "112", "12.50", 2, "2024-01-01"]])
    run_steps(repo_dir, env, STEPS)
    assert q("SELECT data_type FROM information_schema.columns WHERE table_schema = 'bss01104' AND table_name = 'stg_achfile_v' "
             "AND column_name = 'amount'") == "text"
    assert q("SELECT string_agg(amount, ',' ORDER BY amount) FROM (SELECT amount FROM bss01104.stg_achfile_v UNION ALL "
             "SELECT amount FROM bss01112.stg_achfile_v) x") == "12.50,n/a"


def test_guessed_keys_are_reported_but_not_applied(repo):
    repo_dir, data, env = repo
    write_snapshot(data, "20240101", "A", range(1, 6), "x", 5)
    run_steps(repo_dir, env, STEPS[:2])
    import pandas as pd
    inv = pd.read_csv(repo_dir / "vsam" / "outputs" / "vsam_02_inventory.csv", dtype=str).fillna("")
    row = inv[inv["source_filename"] == "BCSXREF"].iloc[0]
    assert row["key_columns"] in ("", "b_korg")          # never an auto-discovered key
    assert "ref_id" in row["candidate_key_columns"] and "KEY_CANDIDATE_NOT_APPLIED" in row["issues"]


def test_gzip_timestamped_file_names_are_accepted(repo):
    repo_dir, data, env = repo
    with gzip.open(data / "FDICSWP_SWP_BCSXREF_FLAT_20240201123015.csv.gz", "wt", newline="") as fh:
        csv.writer(fh).writerows([["ref_id", "ref_name"], [1, "a"], [2, "b"]])
    run_steps(repo_dir, env, STEPS[:3] + STEPS[3:6])
    assert q("SELECT count(*) FROM bcs.stg_bcsxref_rec_v") == "2"


# ---------------------------------------------------------------------------- staged load (v5.4.0)
def run_step6(repo_dir, env, *extra):
    return subprocess.run([sys.executable, "-m", "vsam.vsam_06_load_data", *extra], cwd=repo_dir, env=env,
                          capture_output=True, text=True, timeout=300)


def load_summary(repo_dir):
    import openpyxl
    report = sorted((repo_dir / "vsam" / "reports").glob("*vsam_06_load_data_report*.xlsx"))[-1]
    rows = list(openpyxl.load_workbook(report, read_only=True)["LoadSummary"].iter_rows(values_only=True))
    head = rows[0]
    return {r[head.index("tablename")]: dict(zip(head, r)) for r in rows[1:]}


HDR = ["status_code", "b_korg", "amount", "account_number", "effective_date", "extra_note"]


def test_bad_rows_are_rejected_valid_rows_load_and_9999_dates_are_kept(repo):
    repo_dir, data, env = repo
    write_csv(data / "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv", HDR, [
        ["A", "104", "1.5", "1001", "2024-01-01", "ok"], ["A", "112", "2.5", "1002", "2024-01-01", "ok"]])
    run_steps(repo_dir, env, STEPS)                                  # the first snapshot fixes the column types
    (data / "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv").unlink()
    write_csv(data / "FDICSWP_FDIC_ACHFILE_FLAT_20240201.csv", HDR, [
        ["A", "104", "1.5", "1001", "2024-01-01", "ok"],
        ["A", "112", "2.5", "1002", "9999-12-31", "far-future date is a real date"],
        ["A", "104", "3.5", "", "2024-01-01", "blank pk"],
        ["A", "104", "4.5", "999999999", "2024-01-01", "sentinel pk"],
        ["A", "104", "not-a-number", "1005", "2024-01-01", "bad amount"],
        ["", "", "", "", "", ""],
    ])
    run_steps(repo_dir, env, STEPS[1:6])
    assert q("SELECT count(*) FROM bss01104.stg_achfile_v") == "1"
    assert q("SELECT effective_date::text FROM bss01112.stg_achfile_v WHERE account_number = 1002") == "9999-12-31"
    reasons = q("SELECT string_agg(reason || '=' || n, ',' ORDER BY reason) FROM (SELECT reason, count(*) n FROM sweep_ctl.load_rejects "
                "WHERE target_table = 'stg_achfile_v' GROUP BY reason) x")
    assert reasons == "ALL_NULL=1,BAD_VALUE=1,PK_BLANK=1,PK_SENTINEL=1"
    assert q("SELECT detail FROM sweep_ctl.load_rejects WHERE reason = 'BAD_VALUE'").startswith("amount = 'not-a-number'")
    ach = load_summary(repo_dir)["stg_achfile_v"]
    assert (ach["total recs"], ach["loaded recs"], ach["skipped recs"], ach["duplicates dropped"]) == (6, 2, 4, 0)
    assert (ach["inserted"], ach["updated"], ach["unchanged"]) == (0, 1, 1)
    assert ach["rejected by reason"] == "ALL_NULL = 1, BAD_VALUE = 1, PK_BLANK = 1, PK_SENTINEL = 1"
    assert ach["status"] == "SUCCESS"
    import openpyxl
    report = sorted((repo_dir / "vsam" / "reports").glob("*vsam_06_load_data_report*.xlsx"))[-1]
    wb = openpyxl.load_workbook(report, read_only=True)
    assert {"Reject_Summary", "Rejected_Rows"} <= set(wb.sheetnames)
    assert len(list(wb["Rejected_Rows"].iter_rows(values_only=True))) == 1 + 4                  # header + 4 rejected rows


def test_unchanged_files_are_skipped_and_changed_ones_reload(repo):
    repo_dir, data, env = repo
    write_snapshot(data, "20240101", "A", range(1, 11), "x", 5)
    run_steps(repo_dir, env, STEPS[:6])
    first = q("SELECT string_agg(DISTINCT s3_etag, ',') FROM sweep_ctl.vsam_load_log WHERE s3_etag IS NOT NULL AND s3_last_modified IS NOT NULL AND s3_size > 0")
    assert first
    r = run_step6(repo_dir, env)
    assert r.returncode == 0 and "Nothing to do" in r.stdout + r.stderr          # nothing changed -> skipped
    # re-delivered file: same name, new content -> loaded again (keyed table: only that file's keys change)
    f = data / "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv"
    write_csv(f, HDR, [["B", "104", "9.5", "1001", "2024-01-01", "n"]])
    st = f.stat()
    os.utime(f, (st.st_atime + 5, st.st_mtime + 5))
    run_steps(repo_dir, env, ["vsam_02_inventory"])
    r = run_step6(repo_dir, env)
    assert r.returncode == 0, r.stdout[-1500:] + r.stderr[-1500:]
    assert q("SELECT status_code FROM bss01104.stg_achfile_v WHERE account_number = 1001") == "B"
    assert q("SELECT count(*) FROM bss01104.stg_achfile_v") == "5"                # nothing deleted
    assert q("SELECT count(*) FROM bcs.stg_bcsxref_rec_v") == "5"                 # the unchanged keyless table was not touched


def test_a_failing_keyless_load_rolls_back_and_keeps_the_old_snapshot(repo):
    repo_dir, data, env = repo
    write_snapshot(data, "20240101", "A", range(1, 6), "x", 5)
    run_steps(repo_dir, env, STEPS[:6])
    assert q("SELECT count(*) FROM bcs.stg_bcsxref_rec_v") == "5"
    for f in data.glob("*BCSXREF*"):
        f.unlink()
    write_csv(data / "FDICSWP_SWP_BCSXREF_FLAT_20240301.csv", ["ref_id", "ref_name", "ref_date"],
              [[1, "new", "2024-03-01"], [2, "new", "not-a-date"], [3, "new", "2024-03-01"]])
    run_steps(repo_dir, env, ["vsam_02_inventory"])
    r = run_step6(repo_dir, env, "--max-rejects", "1")
    assert r.returncode != 0
    assert q("SELECT count(*), min(ref_name) FROM bcs.stg_bcsxref_rec_v") == "5|x1"      # old snapshot untouched
    assert q("SELECT count(*) FROM sweep_ctl.vsam_load_log WHERE uri LIKE '%20240301%' AND status = 'SUCCESS'") == "0"
    assert q("SELECT count(*) FROM sweep_ctl.load_rejects WHERE file_status = 'FILE_FAILED'") == "1"
    # default (no reject limit): the valid rows load, the bad one is reported
    r = run_step6(repo_dir, env)
    assert r.returncode == 0, r.stdout[-1500:] + r.stderr[-1500:]
    assert q("SELECT count(*), min(ref_name) FROM bcs.stg_bcsxref_rec_v") == "2|new"
    assert q("SELECT count(*) FROM sweep_ctl.load_rejects WHERE reason = 'BAD_VALUE' AND file_status = 'LOADED'") == "1"


def test_a_copybook_key_that_cannot_be_applied_is_an_error(repo):
    repo_dir, data, env = repo
    write_csv(data / "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv", HDR, [
        ["A", "104", "1.5", "1001", "2024-01-01", "x"], ["A", "104", "2.5", "1001", "2024-01-01", "same key twice"]])
    run_steps(repo_dir, env, STEPS[:5])
    r = run_step6(repo_dir, env)
    assert r.returncode != 0
    assert q("SELECT count(*) FROM bss01104.stg_achfile_v") == "0"
    assert "cannot be applied" in r.stdout + r.stderr
