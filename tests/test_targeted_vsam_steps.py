"""VSAM Steps 01-07: targeted runs merge their outputs, selectors are validated up front (end to end, needs PostgreSQL)."""
import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import sweep_common as c

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parent.parent
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
"""


def _write_csv(path, header, rows):
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
    hdr = ["status_code", "b_korg", "amount", "account_number", "effective_date"]
    _write_csv(data / "FDICSWP_FDIC_ACHFILE_FLAT_20240101.csv", hdr,
               [["A", "104" if i % 2 else "112", i * 1.5, 1000 + i, "2024-01-01"] for i in range(1, 11)])
    _write_csv(data / "FDICSWP_SWP_BCSXREF_FLAT_20240101.csv", ["ref_id", "ref_name", "ref_date"],
               [[i, f"x{i}", "2024-01-01"] for i in range(1, 6)])
    env = dict(os.environ, PGDATABASE=pg_db, VSAM_BASE=str(tmp_path / "data"), PYTHONPATH=str(dst), SWEEP_CONNECT_RETRIES="0")
    env.pop("VSAM_COPYBOOK_DIR", None)
    return dst, data, env


def run(repo_dir, env, name, *extra, check=True):
    r = subprocess.run([sys.executable, "-m", f"vsam.{name}", *extra], cwd=repo_dir, env=env, capture_output=True, text=True, timeout=300)
    if check:
        assert r.returncode == 0, f"{name} {extra} failed:\n{r.stdout[-2500:]}\n{r.stderr[-2500:]}"
    return r


def rows(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def lines_with(path, needle):
    return [l for l in Path(path).read_text().splitlines()[1:] if needle in l]


def test_targeted_runs_keep_every_other_entry(repo):
    d, data, env = repo
    out = d / "vsam" / "outputs"
    for step in STEPS[:4]:
        run(d, env, step)
    lm = out / "vsam_02_load_matrix.csv"
    inv = out / "vsam_02_inventory.csv"
    cmap = out / "vsam_02_column_map.csv"
    assert {r["target_table"] for r in rows(lm)} == {"stg_achfile_v", "stg_bcsxref_rec_v"}
    bcs_lm = lines_with(lm, "stg_bcsxref_rec_v")
    bcs_inv = lines_with(inv, "BCSXREF")
    bcs_map = [l for l in cmap.read_text().splitlines()[1:] if "BCSXREF" in l.upper()]
    gate_before = json.loads((out / "vsam_03_type_gate.json").read_text())
    assert bcs_lm and bcs_inv and bcs_map and gate_before

    # ---- Step 02 targeted at one table: the other table's rows are untouched (also in the column map)
    run(d, env, "vsam_02_inventory", "--tables", "stg_achfile_v")
    assert lines_with(lm, "stg_bcsxref_rec_v") == bcs_lm
    assert lines_with(inv, "BCSXREF") == bcs_inv
    assert [l for l in cmap.read_text().splitlines()[1:] if "BCSXREF" in l.upper()] == bcs_map
    assert {r["target_table"] for r in rows(lm)} == {"stg_achfile_v", "stg_bcsxref_rec_v"}

    # ---- Step 03 targeted: only the "company|source" keys of the run change; the load matrix keeps all rows
    gate_keys = set(gate_before)
    run(d, env, "vsam_03_type_gate", "--tables", "stg_achfile_v")
    gate_after = json.loads((out / "vsam_03_type_gate.json").read_text())
    assert set(gate_after) == gate_keys
    other = [k for k in gate_keys if "BCSXREF" in k.upper()]
    assert other and all(gate_after[k] == gate_before[k] for k in other)                       # byte-identical entries (incl. _meta)
    ach = [k for k in gate_keys if "ACHFILE" in k.upper() and "|" in k][0]
    assert gate_after[ach]["_meta"]["run_id"]
    assert {r["target_table"] for r in rows(lm)} == {"stg_achfile_v", "stg_bcsxref_rec_v"}
    assert lines_with(lm, "stg_bcsxref_rec_v") == bcs_lm
    assert (out / "vsam_03_type_gate.json.bak").exists()

    # ---- Step 04 targeted: only the selected table's fragments are rewritten, assembled scripts hold everything
    frag = out / "ddl_fragments" / "fragments"
    bcs_frag = (frag / "bcs" / "stg_bcsxref_rec_v.pre.sql").read_bytes()
    run(d, env, "vsam_04_generate_ddl", "--tables", "stg_achfile_v")
    assert (frag / "bcs" / "stg_bcsxref_rec_v.pre.sql").read_bytes() == bcs_frag
    assert "stg_bcsxref_rec_v" in (out / "gen_ddl" / "bcs.sql").read_text()
    assert "stg_achfile_v" in (out / "gen_ddl" / "all_tables.sql").read_text() and "stg_bcsxref_rec_v" in (out / "gen_ddl" / "all_tables.sql").read_text()


def test_bad_selectors_fail_before_anything_runs_with_exit_code_2(repo):
    d, data, env = repo
    for step in STEPS[:3]:
        run(d, env, step)
    out = d / "vsam" / "outputs"
    snapshot = {p.name: p.read_bytes() for p in out.glob("*.csv")}
    cases = [("vsam_02_inventory", ["--tables", "no_such_table"]), ("vsam_02_inventory", ["--company", "NOPE"]),
             ("vsam_02_inventory", ["--file", "missing.csv"]), ("vsam_03_type_gate", ["--tables", "no_such_table"]),
             ("vsam_04_generate_ddl", ["--tables", "no_such_table"]), ("vsam_06_load_data", ["--file", "missing.csv"]),
             ("vsam_06_load_data", ["--exclude-tables", "no_such_table"]), ("vsam_07_validate", ["--tables", "no_such_table"]),
             ("vsam_01_extract_copybooks", ["--copybook", "nope"])]
    for step, extra in cases:
        r = run(d, env, step, *extra, check=False)
        assert r.returncode == 2, f"{step} {extra}: {r.returncode}\n{r.stdout[-800:]}\n{r.stderr[-800:]}"
        assert "Invalid selection" in (r.stdout + r.stderr)
    assert {p.name: p.read_bytes() for p in out.glob("*.csv")} == snapshot


def test_dry_run_writes_nothing_in_every_step(repo):
    d, data, env = repo
    for step in STEPS[:3]:
        run(d, env, step)
    out = d / "vsam" / "outputs"
    before = {str(p): p.read_bytes() for p in out.rglob("*") if p.is_file() and p.suffix in (".csv", ".json", ".sql")}
    for step in ("vsam_01_extract_copybooks", "vsam_02_inventory", "vsam_03_type_gate", "vsam_04_generate_ddl"):
        run(d, env, step, "--dry-run")
    after = {str(p): p.read_bytes() for p in out.rglob("*") if p.is_file() and p.suffix in (".csv", ".json", ".sql")}
    assert before == after
    run(d, env, "vsam_05_apply_ddl", "--dry-run")
    run(d, env, "vsam_06_load_data", "--dry-run")
    run(d, env, "vsam_07_validate", "--dry-run")
    assert c.run_psql("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'bcs'", tuples_only=True, unaligned=True) == "0"


def test_step06_file_selector_loads_only_that_file_and_copybook_selector_merges_step01(repo):
    d, data, env = repo
    out = d / "vsam" / "outputs"
    for step in STEPS[:5]:
        run(d, env, step)
    ext = out / "vsam_01_copybook_extracts.csv"
    all_lines = ext.read_text().splitlines()
    run(d, env, "vsam_01_extract_copybooks", "--copybook", "ACHFILE")
    assert [l for l in ext.read_text().splitlines()[1:] if not l.startswith("ACHFILE")] == [l for l in all_lines[1:] if not l.startswith("ACHFILE")]

    run(d, env, "vsam_06_load_data", "--file", "FDICSWP_SWP_BCSXREF_FLAT_20240101.csv", "--force-truncate")
    assert c.run_psql("SELECT count(*) FROM bcs.stg_bcsxref_rec_v", tuples_only=True, unaligned=True) == "5"
    assert c.run_psql("SELECT count(*) FROM bss01104.stg_achfile_v", tuples_only=True, unaligned=True) == "0"
    assert c.run_psql("SELECT string_agg(target_table, ',') FROM sweep_ctl.vsam_load_log", tuples_only=True, unaligned=True) == "stg_bcsxref_rec_v"


def test_step04_never_applies_a_key_taken_from_the_step03_profile(repo):
    d, data, env = repo
    out = d / "vsam" / "outputs"
    for step in STEPS[:3]:
        run(d, env, step)
    gate = json.loads((out / "vsam_03_type_gate.json").read_text())
    for key, entry in gate.items():           # pretend Step 03 had (wrongly) offered a key for the source without a copybook
        if "BCSXREF" in key.upper():
            entry.update({"pk_cols": ["ref_id"], "pk_valid": True})
    (out / "vsam_03_type_gate.json").write_text(json.dumps(gate))
    run(d, env, "vsam_04_generate_ddl")
    pk_sql = (out / "gen_ddl" / "bcs_pk.sql").read_text()
    assert "stg_bcsxref_rec_v" not in pk_sql                  # no copybook / override key -> no primary key, whatever Step 03 says
    assert "stg_achfile_v" in pk_sql and "PRIMARY KEY" in pk_sql   # the copybook key is still applied
