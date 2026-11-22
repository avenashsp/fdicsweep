"""Shared helpers of the multi-database (layer chain) tests."""
import csv
import gzip
import subprocess
import uuid

import pytest

import sweep_common as c
import sweep_layers as sl
import sweep_migrate
import sweep_promote as sp
from conftest import write_layers_file
from oracle import oracle_06_load_data as o6
from oracle import oracle_06_order as order

LAYERS = ["bronze", "silver", "gold_backup", "gold"]
MANIFEST_HEADER = ["track", "schema", "table", "promote_mode", "conflict_policy", "enabled"]


def psql(dbname, sql):
    return c.run_psql(sql, dbname=dbname, tuples_only=True, unaligned=True)


@pytest.fixture()
def chain_env(pg_available, tmp_path, monkeypatch):
    if not pg_available:
        pytest.skip("PostgreSQL not reachable")
    tag = uuid.uuid4().hex[:8]
    dbs = {l: f"sweep_t{tag}_{l}" for l in LAYERS}
    for name in dbs.values():
        c.run_psql(f'CREATE DATABASE "{name}"', dbname="postgres")
    meta = c.ORACLE_INPUTS_DIR / "sweep_meta.sql"
    f = write_layers_file(tmp_path / "sweep_layers.json", dbs, gold={"lock_timeout": "1s"}, gold_backup={"lock_timeout": "1s"})
    monkeypatch.setattr(sl, "LAYERS_FILE", f)
    monkeypatch.setattr(order, "live_edges", lambda: {})
    monkeypatch.setattr(order, "extracted_edges", lambda *a: {})
    for l in LAYERS:
        c.run_psql(meta, is_file=True, dbname=dbs[l])
        with sl.use_layer(l):
            sweep_migrate.migrate_current_database()
    (tmp_path / "layers").mkdir()
    (tmp_path / "reports").mkdir()
    monkeypatch.setattr(c, "ORACLE_REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(o6, "ORACLE_REPORTS_DIR", tmp_path / "reports")

    class Env:
        pass

    env = Env()
    env.dbs, env.tmp = dbs, tmp_path

    def manifest(layer, rows):
        path = tmp_path / "layers" / f"{layer}_tables.csv"
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=MANIFEST_HEADER)
            w.writeheader()
            for r in rows:
                w.writerow({"track": "oracle", "promote_mode": "upsert", "conflict_policy": "source_wins", "enabled": "true", **r})

    def everywhere(sql):
        for l in LAYERS:
            psql(dbs[l], sql)

    def bronze_load(text, table="customer", schema="app", name=None):
        """Loads a CSV text into bronze through the real Step 06 loader (so the delta is captured the real way)."""
        path = tmp_path / (name or f"{table}_{uuid.uuid4().hex[:6]}.csv.gz")
        path.write_bytes(gzip.compress(text.encode()))
        from sweep_s3meta import head_object
        m = head_object(str(path))
        out = tmp_path / "outputs"
        out.mkdir(exist_ok=True)
        cols = ["load_order_rank", "owner", "table_name", "target_schema", "target_table", "file_format", "size_bytes", "size_mb", "uri",
                "filename", "s3_last_modified", "s3_etag", "s3_size"]
        with open(out / "oracle_02_s3_file_map.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerow({"load_order_rank": 1, "owner": schema.upper(), "table_name": table.upper(), "target_schema": schema, "target_table": table,
                        "file_format": "gzip", "size_bytes": path.stat().st_size, "size_mb": 0.01, "uri": str(path), "filename": path.name,
                        "s3_last_modified": m["last_modified"], "s3_etag": m["etag"], "s3_size": m["size"]})
        monkeypatch.setattr(o6, "ORACLE_OUTPUTS_DIR", out)
        with sl.use_layer("bronze"):
            assert o6.run_stream_load(workers=1, reload=True) is True

    env.manifest, env.everywhere, env.bronze_load = manifest, everywhere, bronze_load
    env.promote = lambda a, b, **kw: sp.promote(a, b, **{"transport": "program", **kw})
    yield env
    for name in dbs.values():
        subprocess.run([c.PSQL_BIN, "-X", "-w", "-h", c.PGHOST, "-p", str(c.PGPORT), "-U", c.PGUSER, "-d", "postgres", "-c",
                        f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'], env=c.psql_env(), capture_output=True, text=True)


CUSTOMER = ("CREATE SCHEMA IF NOT EXISTS app; CREATE TABLE app.customer (id int PRIMARY KEY, name text, qty int, modifieddate timestamp);"
            "INSERT INTO app.customer VALUES (1,'a',10,'2020-01-01'),(2,'b',20,'2020-01-01'),(3,'c',30,'2020-01-01');")


def day1(env, extra=""):
    """Day-1: every database was loaded on its own, so all four hold the same rows (no registry rows anywhere)."""
    env.everywhere(CUSTOMER + extra)
    for l in LAYERS:
        env.manifest(l, [{"schema": "app", "table": "customer"}])


def rows(env, layer, table="app.customer", cols="id, name, qty"):
    return psql(env.dbs[layer], f"SELECT string_agg(concat_ws(':', {cols}), ',' ORDER BY id) FROM {table}")


DELTA1 = "id,name,qty\n1,a,10\n2,b2,20\n4,d,40\n"      # 1 unchanged, 2 changed, 4 new
