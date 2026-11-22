"""Migration 1: log tables, track columns, delta/promote tables; legacy rows are copied, nothing is dropped."""
import pytest

import sweep_common as c
import sweep_migrate as m

pytestmark = pytest.mark.integration

LEGACY_DDL = """
DROP TABLE IF EXISTS sweep_ctl.oracle_load_log, sweep_ctl.vsam_load_log, sweep_ctl.delta_registry, sweep_ctl.promote_log,
  sweep_ctl.schema_version CASCADE;
ALTER TABLE sweep_ctl.load_rejects DROP COLUMN IF EXISTS track;
ALTER TABLE sweep_ctl.load_audit DROP COLUMN IF EXISTS track;
CREATE TABLE sweep_ctl.load_log (
    uri text PRIMARY KEY, target_schema text NOT NULL, target_table text NOT NULL, size_bytes bigint DEFAULT 0,
    loaded_rows bigint DEFAULT 0, seconds numeric DEFAULT 0, file_format text DEFAULT 'gzip', worker_id int DEFAULT 1,
    status text DEFAULT 'SUCCESS', message text, loaded_at timestamptz DEFAULT now());
INSERT INTO sweep_ctl.load_log (uri, target_schema, target_table, loaded_rows) VALUES
  ('s3://b/oracle/a.csv.gz', 's1', 'a', 10),
  ('s3://b/vsam/x#y.csv#bcs', 'bcs', 'x', 3),
  ('s3://b/vsam/x#y.csv#bss01104', 'bss01104', 'x', 4);
"""


def _cols(table):
    return c.query_db_rows(
        "SELECT column_name || ':' || data_type FROM information_schema.columns "
        f"WHERE table_schema = 'sweep_ctl' AND table_name = '{table}' ORDER BY column_name")


def test_migration_copies_legacy_rows_and_renames(pg_db):
    c.run_psql(LEGACY_DDL)
    assert m.migrate_current_database() == [1]
    assert c.run_psql("SELECT uri || '|' || loaded_rows FROM sweep_ctl.oracle_load_log", tuples_only=True) == "s3://b/oracle/a.csv.gz|10"
    vs = c.query_db_rows("SELECT uri || '|' || target_schema || '|' || loaded_rows FROM sweep_ctl.vsam_load_log ORDER BY 1")
    assert vs == ["s3://b/vsam/x#y.csv|bcs|3", "s3://b/vsam/x#y.csv|bss01104|4"]      # split at the LAST '#'
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.load_log_legacy", tuples_only=True) == "3"   # kept, not dropped
    assert c.run_psql("SELECT to_regclass('sweep_ctl.load_log') IS NULL", tuples_only=True) == "t"
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.schema_version", tuples_only=True) == "1"
    for t in ("load_rejects", "load_audit"):
        assert any(x.startswith("track:") for x in _cols(t))


def test_migration_is_idempotent(pg_db):
    c.run_psql(LEGACY_DDL)
    assert m.migrate_current_database() == [1]
    assert m.migrate_current_database() == []
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.oracle_load_log", tuples_only=True) == "1"
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.schema_version", tuples_only=True) == "1"


def test_dry_run_changes_nothing(pg_db):
    c.run_psql(LEGACY_DDL)
    m.migrate_current_database(dry_run=True)
    assert c.run_psql("SELECT to_regclass('sweep_ctl.oracle_load_log') IS NULL", tuples_only=True) == "t"
    assert c.run_psql("SELECT to_regclass('sweep_ctl.load_log') IS NOT NULL", tuples_only=True) == "t"


def test_failed_migration_rolls_back(pg_db, monkeypatch):
    c.run_psql(LEGACY_DDL)
    monkeypatch.setattr(m, "MIGRATIONS", [(1, "broken", m.CTL_OBJECTS_SQL + "\nSELECT 1/0;\n")])
    with pytest.raises(RuntimeError, match="rolled back"):
        m.migrate_current_database()
    assert c.run_psql("SELECT to_regclass('sweep_ctl.oracle_load_log') IS NULL", tuples_only=True) == "t"
    assert c.run_psql("SELECT count(*) FROM sweep_ctl.schema_version", tuples_only=True) == "0"


def test_fresh_install_and_migrated_database_have_the_same_columns(pg_db):
    fresh = {t: _cols(t) for t in ("oracle_load_log", "vsam_load_log", "load_rejects", "load_audit",
                                    "delta_registry", "promote_log", "schema_version")}
    c.run_psql(LEGACY_DDL)
    m.migrate_current_database()
    assert fresh == {t: _cols(t) for t in fresh}
    assert m.migrate_current_database() == []          # nothing left to do


def test_meta_sql_files_are_identical():
    assert (c.ORACLE_INPUTS_DIR / "sweep_meta.sql").read_text() == (c.VSAM_INPUTS_DIR / "sweep_meta.sql").read_text()
