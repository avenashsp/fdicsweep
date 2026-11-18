import importlib
import subprocess
import sys

import pytest

import sweep

MODULES = [
    "sweep", "sweep_bg", "sweep_common", "sweep_dashboard", "sweep_format", "sweep_jobs", "sweep_metrics",
    "sweep_platform", "sweep_streaming",
    "oracle.oracle_07_parser", "oracle.oracle_08_audit", "oracle.oracle_08_catalog",
    "vsam.common_config", "vsam.common_mapping", "vsam.common_records", "vsam.common_copybook",
    "vsam.common_schema", "vsam.common_files", "vsam.vsam_common",
] + [f"oracle.{m}" for m in sweep.ORACLE_STEPS] + [f"vsam.{m}" for m in sweep.VSAM_STEPS]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name):
    importlib.import_module(name)


@pytest.mark.parametrize("name", sweep.ORACLE_STEPS + sweep.VSAM_STEPS)
def test_step_runs_as_module_and_shows_help(name):
    track = name.split("_")[0]
    r = subprocess.run([sys.executable, "-m", f"{track}.{name}", "--help"], cwd=sweep.BASE_DIR,
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_vsam_common_facade_still_exports_what_the_steps_import():
    import ast
    import glob
    import vsam.vsam_common as facade
    needed = set()
    for f in glob.glob(str(sweep.BASE_DIR / "**" / "*.py"), recursive=True):
        for node in ast.walk(ast.parse(open(f).read())):
            if isinstance(node, ast.ImportFrom) and node.module == "vsam.vsam_common":
                needed |= {a.name for a in node.names}
    assert needed and not [n for n in needed if not hasattr(facade, n)]


def test_sweep_bg_facade_exports():
    import sweep_bg
    for n in ("start_background_job", "list_jobs", "stop_jobs", "tail_logs", "print_status", "monitor_live",
              "render_dashboard", "is_pid_alive"):
        assert callable(getattr(sweep_bg, n))
