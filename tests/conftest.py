"""Shared pytest fixtures. Integration tests need a reachable PostgreSQL (PG* env vars) and psql."""
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import sweep_common  # noqa: E402  (after sys.path setup)


def _pg_reachable():
    if not shutil.which(sweep_common.PSQL_BIN):
        return False
    if not sweep_common.has_db_auth():
        return False
    try:
        sweep_common.run_psql("SELECT 1", timeout=15)
        return True
    except RuntimeError:
        return False


@pytest.fixture(scope="session")
def pg_available():
    return _pg_reachable()


@pytest.fixture()
def pg_db(pg_available, monkeypatch):
    """A throw-away database with the SWEEP control schema applied; dropped afterwards."""
    if not pg_available:
        pytest.skip("PostgreSQL not reachable (set PGHOST/PGPORT/PGUSER/PGPASSWORD)")
    name = f"sweep_test_{uuid.uuid4().hex[:10]}"
    sweep_common.run_psql(f'CREATE DATABASE "{name}"', dbname="postgres")
    monkeypatch.setattr(sweep_common, "PGDATABASE", name)
    meta = ROOT / "oracle" / "inputs" / "sweep_meta.sql"
    sweep_common.run_psql(meta, is_file=True)
    try:
        yield name
    finally:
        monkeypatch.undo()
        subprocess.run(
            [sweep_common.PSQL_BIN, "-X", "-w", "-h", sweep_common.PGHOST, "-p", str(sweep_common.PGPORT),
             "-U", sweep_common.PGUSER, "-d", "postgres", "-c",
             f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'],
            env=sweep_common.psql_env(), capture_output=True, text=True,
        )


@pytest.fixture()
def gz_file(tmp_path):
    """Factory writing a gzip CSV and returning its path."""
    import gzip

    def make(name, text):
        p = tmp_path / name
        with gzip.open(p, "wt", newline="", encoding="utf-8") as fh:
            fh.write(text)
        return p
    return make
