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
from conftest import layer_env

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
    env.update(layer_env(tmp_path, pg_db))
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
    frag = out / "ddl_fragments" / "bronze" / "fragments"
    bcs_frag = (frag / "bcs" / "stg_bcsxref_rec_v.pre.sql").read_bytes()
    run(d, env, "vsam_04_generate_ddl", "--tables", "stg_achfile_v")
    assert (frag / "bcs" / "stg_bcsxref_rec_v.pre.sql").read_bytes() == bcs_frag
    assert "stg_bcsxref_rec_v" in (out / "gen_ddl" / "bronze" / "bcs.sql").read_text()
    assert "stg_achfile_v" in (out / "gen_ddl" / "bronze" / "all_tables.sql").read_text() and "stg_bcsxref_rec_v" in (out / "gen_ddl" / "bronze" / "all_tables.sql").read_text()


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
    pk_sql = (out / "gen_ddl" / "bronze" / "bcs_pk.sql").read_text()
    assert "stg_bcsxref_rec_v" not in pk_sql                  # no copybook / override key -> no primary key, whatever Step 03 says
    assert "stg_achfile_v" in pk_sql and "PRIMARY KEY" in pk_sql   # the copybook key is still applied


def _q(sql):
    return c.run_psql(sql, tuples_only=True, unaligned=True)


def test_vsam_load_registers_full_copies_first_and_captures_deltas_afterwards(repo):
    d, data, env = repo
    for step in STEPS[:6]:
        run(d, env, step)
    reg = _q("SELECT string_agg(target_schema || '.' || target_table || '#' || seq || ':' || full_copy || ':' || rows_total, ',' ORDER BY target_schema, target_table) "
             "FROM sweep_ctl.delta_registry")
    # first load into EMPTY tables: registered as full copies (nothing captured), per destination schema; keyless table too
    assert "bss01104.stg_achfile_v#1:true:5" in reg and "bss01112.stg_achfile_v#1:true:5" in reg and "bcs.stg_bcsxref_rec_v#1:true:5" in reg
    assert _q("SELECT count(*) FROM sweep_delta.bss01104__stg_achfile_v") in ("0", "")

    # a newer snapshot with one changed and one new key: only those two rows are captured, per destination schema
    for f in data.glob("*20240101.csv"):
        f.unlink()
    hdr = ["status_code", "b_korg", "amount", "account_number", "effective_date"]
    rows_ = [["A", "104" if i % 2 else "112", i * 1.5, 1000 + i, "2024-01-01"] for i in range(1, 11)]
    rows_[0][0] = "Z"                                                   # account 1001 changed (korg 104)
    rows_.append(["A", "104", 99.5, 1999, "2024-01-01"])                # account 1999 is new (korg 104)
    _write_csv(data / "FDICSWP_FDIC_ACHFILE_FLAT_20240201.csv", hdr, rows_)
    _write_csv(data / "FDICSWP_SWP_BCSXREF_FLAT_20240201.csv", ["ref_id", "ref_name", "ref_date"], [[i, f"y{i}", "2024-01-01"] for i in range(1, 7)])
    for step in ("vsam_02_inventory", "vsam_03_type_gate", "vsam_06_load_data"):
        run(d, env, step)
    assert _q("SELECT seq || ':' || full_copy || ':' || rows_inserted || '/' || rows_updated FROM sweep_ctl.delta_registry "
              "WHERE target_schema = 'bss01104' AND target_table = 'stg_achfile_v' AND seq = 2") == "2:false:1/1"
    assert _q("SELECT string_agg(_op || account_number, ',' ORDER BY account_number) FROM sweep_delta.bss01104__stg_achfile_v WHERE _seq = 2") == "U1001,I1999"
    assert _q("SELECT count(*) FROM sweep_ctl.delta_registry WHERE target_schema = 'bss01112' AND seq = 2") == "0"     # nothing changed there
    # a keyless table is replaced by the snapshot: registered as a full copy again (the promote hop replaces it as a whole)
    assert _q("SELECT seq || ':' || full_copy || ':' || rows_total FROM sweep_ctl.delta_registry WHERE target_table = 'stg_bcsxref_rec_v' AND seq = 2") == "2:true:6"


def test_unchanged_vsam_files_are_reloaded_when_a_table_they_loaded_into_was_truncated(repo):
    d, data, env = repo
    for step in STEPS[:6]:
        run(d, env, step)
    assert _q("SELECT count(*) FROM bss01104.stg_achfile_v") == "5" and _q("SELECT count(*) FROM bcs.stg_bcsxref_rec_v") == "5"
    r = run(d, env, "vsam_06_load_data")                                       # unchanged files, nothing truncated: skipped
    assert "Pending: 0" in r.stdout
    _q("TRUNCATE bss01104.stg_achfile_v")                                      # a keyed table emptied by hand
    r = run(d, env, "vsam_06_load_data")
    assert "is empty" in r.stdout
    assert _q("SELECT count(*) FROM bss01104.stg_achfile_v") == "5" and _q("SELECT count(*) FROM bss01112.stg_achfile_v") == "5"
    _q("TRUNCATE bcs.stg_bcsxref_rec_v")                                       # a keyless table emptied by hand
    run(d, env, "vsam_06_load_data")
    assert _q("SELECT count(*) FROM bcs.stg_bcsxref_rec_v") == "5"


def test_step03_rows_flag_overrides_the_env_default(repo):
    d, data, env = repo
    for step in STEPS[:2]:
        run(d, env, step)
    out = d / "vsam" / "outputs"
    import json as _j

    def rows_checked(*extra, **envvars):
        run(d, {**env, **envvars}, "vsam_03_type_gate", *extra)
        gate = _j.loads((out / "vsam_03_type_gate.json").read_text())
        return next(v["rows_total"] for k, v in gate.items() if "ACHFILE" in k.upper() and "|" in k)

    assert rows_checked("--rows", "3") == 3
    assert rows_checked("--rows", "all", VSAM_ANALYZE_MAX_ROWS="2") == 10          # 'all' beats the env value
    assert rows_checked(VSAM_ANALYZE_MAX_ROWS="4") == 4                            # no flag: the env value is the default
    assert rows_checked() == 10                                                    # neither: all rows
    for bad in ("0", "-1", "abc"):
        r = run(d, env, "vsam_03_type_gate", "--rows", bad, check=False)
        assert r.returncode == 2 and "--rows must be" in (r.stdout + r.stderr)


def _fe_sql(sql):
    """FE-style change applied to the table in EVERY schema that has it (the loaders provision the table in all active schemas)."""
    _q("DO $$ DECLARE r record; BEGIN FOR r IN SELECT table_schema AS s FROM information_schema.tables WHERE table_name = 'stg_achfile_v' LOOP "
       "EXECUTE replace($sql$" + sql + "$sql$, '@S@', quote_ident(r.s)); END LOOP; END $$")


def _rule(rule, col=None, new=None, expr=None, table="stg_achfile_v"):
    f = lambda v: "NULL" if v is None else "'" + v.replace("'", "''") + "'"
    _q(f"INSERT INTO sweep_ctl.fwd_ddl_rules (file_name, rule, schema_name, table_name, column_name, new_name, expression) VALUES "
       f"('V1__t.sql', '{rule}', '*', '{table}', {f(col)}, {f(new)}, {f(expr)})")


def _newer_snapshot(d, data, status="Z"):
    for f in data.glob("*ACHFILE*.csv"):
        f.unlink()
    hdr = ["status_code", "b_korg", "amount", "account_number", "effective_date"]
    _write_csv(data / "FDICSWP_FDIC_ACHFILE_FLAT_20240301.csv", hdr,
               [[status if i == 1 else "A", "104" if i % 2 else "112", i * 1.5, 1000 + i, "2024-01-01"] for i in range(1, 11)])
    for step in ("vsam_02_inventory", "vsam_03_type_gate"):
        run(d, env_of(d), step)


def env_of(d):
    return _ENV[str(d)]


_ENV = {}


def test_vsam_loader_rules_ignore_rename_default_skip_and_unknown_column(repo):
    d, data, env = repo
    _ENV[str(d)] = env
    for step in STEPS[:6]:
        run(d, env, step)
    # FE dropped the column the source file still carries -> without a rule the file FAILS (it used to be dropped silently)
    _fe_sql("ALTER TABLE @S@.stg_achfile_v DROP COLUMN effective_date")
    _newer_snapshot(d, data)
    r = run(d, env, "vsam_06_load_data", check=False)
    assert "not found in table: effective_date" in (r.stdout + r.stderr) and "docs/fwd_ddl.md" in (r.stdout + r.stderr)
    assert _q("SELECT status_code FROM bss01104.stg_achfile_v WHERE account_number = 1001") == "A"           # nothing changed
    # dry-run reports the same problem before the real run
    r = run(d, env, "vsam_06_load_data", "--dry-run", check=False)
    assert r.returncode != 0 and "PROBLEM" in r.stdout and "effective_date" in r.stdout
    _rule("ignore-column", "effective_date")
    assert "OK" in run(d, env, "vsam_06_load_data", "--dry-run").stdout
    run(d, env, "vsam_06_load_data")
    assert _q("SELECT status_code FROM bss01104.stg_achfile_v WHERE account_number = 1001") == "Z"           # loaded, column ignored

    # rename: the table column is called status now
    _fe_sql("ALTER TABLE @S@.stg_achfile_v RENAME COLUMN status_code TO status")
    _newer_snapshot(d, data, status="Y")
    assert run(d, env, "vsam_06_load_data", check=False).returncode != 0
    _rule("rename-column", "status_code", "status")
    run(d, env, "vsam_06_load_data")
    assert _q("SELECT status FROM bss01104.stg_achfile_v WHERE account_number = 1001") == "Y"

    # default: a new NOT NULL column the file does not have
    _fe_sql("ALTER TABLE @S@.stg_achfile_v ADD COLUMN region text NOT NULL DEFAULT 'x'; ALTER TABLE @S@.stg_achfile_v ALTER COLUMN region DROP DEFAULT")
    _q("UPDATE bss01104.stg_achfile_v SET region = 'old'")
    _newer_snapshot(d, data, status="X")
    assert run(d, env, "vsam_06_load_data", check=False).returncode != 0
    _rule("default-column", "region", expr="'new'")
    run(d, env, "vsam_06_load_data")
    assert _q("SELECT status || ':' || region FROM bss01104.stg_achfile_v WHERE account_number = 1001") == "X:old"      # existing rows keep their value
    _fe_sql("ALTER TABLE @S@.stg_achfile_v ADD COLUMN extra_new int")                                   # nullable: adding columns needs no rule
    _newer_snapshot(d, data, status="W")
    run(d, env, "vsam_06_load_data")
    assert _q("SELECT status FROM bss01104.stg_achfile_v WHERE account_number = 1001") == "W"

    # skip-table: the table was dropped by FE
    _fe_sql("DROP TABLE @S@.stg_achfile_v")
    _rule("skip-table")
    _newer_snapshot(d, data, status="V")
    r = run(d, env, "vsam_06_load_data")
    assert "SKIPPED" in r.stdout and "skip-table rule" in r.stdout
