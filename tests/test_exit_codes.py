"""Every step must exit non-zero when it cannot do its job, so `run-all` stops instead of ploughing on."""
import os
import subprocess
import sys

import pytest

import sweep


def run(module, *args, extra_env=None):
    env = dict(os.environ, SWEEP_ALLOW_NO_PASSWORD="1", SWEEP_CONNECT_RETRIES="0", PGHOST="127.0.0.1",
               PGPORT="1", PGCONNECT_TIMEOUT="2", **(extra_env or {}))
    return subprocess.run([sys.executable, "-m", module, *args], cwd=sweep.BASE_DIR, env=env,
                          capture_output=True, text=True, timeout=120)


def test_oracle_apply_ddl_fails_without_scripts_or_db(tmp_path):
    assert run("oracle.oracle_05_apply_ddl", "--skip-meta").returncode != 0


def test_oracle_load_data_fails_when_file_map_missing(tmp_path):
    outputs = sweep.BASE_DIR / "oracle" / "outputs" / "oracle_02_s3_file_map.csv"
    if outputs.exists():
        pytest.skip("a real file map exists in this checkout")
    assert run("oracle.oracle_06_load_data").returncode != 0


def test_vsam_apply_ddl_fails_without_generated_ddl_or_db():
    assert run("vsam.vsam_05_apply_ddl", "--skip-meta").returncode != 0


def test_sweep_check_fails_when_database_unreachable():
    r = subprocess.run([sys.executable, "sweep.py", "check"], cwd=sweep.BASE_DIR, capture_output=True, text=True,
                       env=dict(os.environ, SWEEP_ALLOW_NO_PASSWORD="1", SWEEP_CONNECT_RETRIES="0",
                                PGHOST="127.0.0.1", PGPORT="1", PGCONNECT_TIMEOUT="2"), timeout=120)
    assert r.returncode != 0
    assert "Connection" in r.stdout
