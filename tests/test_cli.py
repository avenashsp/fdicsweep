import subprocess
import sys

import sweep


def test_step_builds_module_command():
    cmd = sweep.step("oracle", "oracle_06_load_data", "--workers", 4)
    assert cmd == [sys.executable, "-m", "oracle.oracle_06_load_data", "--workers", "4"]


def test_pipeline_orders_cover_every_script():
    assert len(sweep.ORACLE_STEPS) == 8 and len(sweep.VSAM_STEPS) == 7
    for m in sweep.ORACLE_STEPS + sweep.VSAM_STEPS:
        track = m.split("_")[0]
        assert (sweep.BASE_DIR / track / f"{m}.py").exists()


def test_run_pipeline_stops_at_first_failure(tmp_path, capsys):
    marker = tmp_path / "ran3"
    ok = [sys.executable, "-c", "pass"]
    bad = [sys.executable, "-c", "import sys; sys.exit(7)"]
    never = [sys.executable, "-c", f"open(r'{marker}', 'w').write('x')"]
    assert sweep.run_pipeline("T", [ok, bad, never]) == 7
    assert not marker.exists()
    assert "Aborting pipeline" in capsys.readouterr().out
    assert sweep.run_pipeline("T", [ok, ok]) == 0


def test_child_env_puts_repo_root_first(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/somewhere")
    assert sweep.child_env()["PYTHONPATH"].split(":")[0] == str(sweep.BASE_DIR)


def test_cli_version_and_help_run():
    for args in (["--version"], ["oracle", "--help"], ["vsam", "load-data", "--help"], ["run-all", "--help"]):
        r = subprocess.run([sys.executable, "sweep.py", *args], cwd=sweep.BASE_DIR, capture_output=True, text=True)
        assert r.returncode == 0, (args, r.stderr)
    assert "5.2" in subprocess.run([sys.executable, "sweep.py", "--version"], cwd=sweep.BASE_DIR,
                                   capture_output=True, text=True).stdout


def test_vsam_load_flags_are_exposed():
    out = subprocess.run([sys.executable, "sweep.py", "vsam", "load-data", "--help"], cwd=sweep.BASE_DIR,
                         capture_output=True, text=True).stdout
    assert "--force-truncate" in out and "--dry-run" in out


def test_dead_patch_commands_are_gone():
    out = subprocess.run([sys.executable, "sweep.py", "oracle", "--help"], cwd=sweep.BASE_DIR,
                         capture_output=True, text=True).stdout
    assert "patch" not in out
