"""Shared pytest fixtures. Integration tests need a reachable PostgreSQL (PG* env vars) and psql."""
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

pytest_plugins = ["chain_support"]     # the multi-database chain_env fixture

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
    import sweep_migrate
    sweep_migrate.migrate_current_database()       # records schema version 1 like `sweep.py migrate`
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


def write_layers_file(path, databases, **overrides):
    """A sweep_layers.json for tests: {layer: database name}; per-layer overrides {layer: {key: value}}."""
    import json
    names = ["bronze", "silver", "gold_backup", "gold"]
    layers = []
    for i, n in enumerate(names):
        entry = {"name": n, "database": databases.get(n, databases.get("bronze", "x")), "env_file": f"sweep.{n}.env.absent",
                 "upstream": names[i - 1] if i else None, "manifest": str(Path(path).parent / "layers" / f"{n}_tables.csv"),
                 "allow_ddl": n in ("bronze", "silver"), "trigger_mode": "auto", "lock_timeout": "5s", "statement_timeout": "0",
                 "conflict_policy": "source_wins"}
        entry.update(overrides.get(n, {}))
        layers.append(entry)
    Path(path).write_text(json.dumps({"layers": layers}))
    return Path(path)


def layer_env(tmp_path, db_name, layer="bronze"):
    """Environment variables that make a step subprocess run on `layer` of a one-database test setup."""
    f = write_layers_file(tmp_path / "sweep_layers.json", {"bronze": db_name, "silver": db_name, "gold_backup": db_name, "gold": db_name})
    return {"SWEEP_LAYERS_FILE": str(f), "SWEEP_LAYER": layer}


@pytest.fixture()
def grab():
    """grab(logger) -> list that collects that logger's messages (SWEEP loggers do not propagate to caplog)."""
    import logging
    attached = []

    def attach(logger):
        msgs = []

        class Handler(logging.Handler):
            def emit(self, record):
                msgs.append(record.getMessage())

        h = Handler()
        logger.addHandler(h)
        attached.append((logger, h))
        return msgs

    yield attach
    for lg, h in attached:
        lg.removeHandler(h)


@pytest.fixture(autouse=True)
def _no_real_data_models(tmp_path_factory, monkeypatch):
    """The data models of the repository need their own source tables: tests that do not test models see an empty models folder."""
    empty = tmp_path_factory.mktemp("no_data_models")
    monkeypatch.setenv("SWEEP_MODELS_DIR", str(empty))
