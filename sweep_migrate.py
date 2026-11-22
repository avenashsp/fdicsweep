#!/usr/bin/env python3
"""
sweep_migrate.py - versioned, idempotent, transactional migrations of the sweep_ctl control schema.

`python3 sweep.py migrate [--layer X | --all-layers] [--dry-run]` applies every migration that sweep_ctl.schema_version
does not list yet. Each migration runs in ONE transaction together with its schema_version row, so a failure leaves
the database exactly as it was. Re-running is a no-op.

Migration 1 (v6.0.0)
  * sweep_ctl.oracle_load_log (PK uri) and sweep_ctl.vsam_load_log (PK uri, target_schema) with the s3_* columns
  * rows of the old sweep_ctl.load_log are copied (uri LIKE '%#%' = VSAM, split at the LAST '#'; others = Oracle) and
    the old table is renamed load_log_legacy (never dropped)
  * `track` column on load_rejects and load_audit
  * sweep_ctl.delta_registry, promote_log, schema_version and the sweep_delta schema

CREATE TABLE IF NOT EXISTS does not add columns to an existing table, so every column that older versions of a table
may lack is also added with ALTER TABLE ... ADD COLUMN IF NOT EXISTS. oracle|vsam/inputs/sweep_meta.sql creates the
same objects for a fresh install; a test keeps both in step.
"""
from sweep_common import get_logger, lit, query_db_dicts, run_psql, run_psql_script

logger = get_logger("sweep_migrate", to_file=False)

# -----------------------------------------------------------------------------
# Objects of the 6.0.0 control schema (every statement is idempotent)
# -----------------------------------------------------------------------------
_LOG_COLUMNS = """
    target_schema   text NOT NULL,
    target_table    text NOT NULL,
    size_bytes      bigint DEFAULT 0,
    loaded_rows     bigint DEFAULT 0,
    seconds         numeric DEFAULT 0,
    rows_per_sec    numeric GENERATED ALWAYS AS (
        CASE WHEN seconds > 0 THEN ROUND(loaded_rows / seconds, 2) ELSE 0 END
    ) STORED,
    file_format     text DEFAULT 'gzip',
    worker_id       int DEFAULT 1,
    status          text DEFAULT 'SUCCESS',
    message         text,
    loaded_at       timestamptz DEFAULT now(),
    s3_last_modified text,
    s3_etag          text,
    s3_size          bigint"""

CTL_OBJECTS_SQL = f"""
CREATE SCHEMA IF NOT EXISTS sweep_ctl;
CREATE SCHEMA IF NOT EXISTS sweep_delta;

CREATE TABLE IF NOT EXISTS sweep_ctl.oracle_load_log (
    uri             text PRIMARY KEY,{_LOG_COLUMNS}
);
CREATE INDEX IF NOT EXISTS idx_sweep_oracle_load_log_target ON sweep_ctl.oracle_load_log (target_schema, target_table);
CREATE INDEX IF NOT EXISTS idx_sweep_oracle_load_log_status ON sweep_ctl.oracle_load_log (status);

CREATE TABLE IF NOT EXISTS sweep_ctl.vsam_load_log (
    uri             text NOT NULL,{_LOG_COLUMNS},
    PRIMARY KEY (uri, target_schema)
);
CREATE INDEX IF NOT EXISTS idx_sweep_vsam_load_log_target ON sweep_ctl.vsam_load_log (target_schema, target_table);
CREATE INDEX IF NOT EXISTS idx_sweep_vsam_load_log_status ON sweep_ctl.vsam_load_log (status);
ALTER TABLE sweep_ctl.oracle_load_log ADD COLUMN IF NOT EXISTS s3_last_modified text;
ALTER TABLE sweep_ctl.oracle_load_log ADD COLUMN IF NOT EXISTS s3_etag          text;
ALTER TABLE sweep_ctl.oracle_load_log ADD COLUMN IF NOT EXISTS s3_size          bigint;
ALTER TABLE sweep_ctl.vsam_load_log   ADD COLUMN IF NOT EXISTS s3_last_modified text;
ALTER TABLE sweep_ctl.vsam_load_log   ADD COLUMN IF NOT EXISTS s3_etag          text;
ALTER TABLE sweep_ctl.vsam_load_log   ADD COLUMN IF NOT EXISTS s3_size          bigint;

-- Rows rejected during Step 06 loads (bad rows are reported, not fatal); shared by both tracks
CREATE TABLE IF NOT EXISTS sweep_ctl.load_rejects (
    id            bigserial PRIMARY KEY,
    track         text,
    uri           text NOT NULL,
    target_schema text,
    target_table  text,
    row_no        bigint,
    reason        text NOT NULL,
    detail        text,
    row_data      jsonb,
    file_status   text DEFAULT 'LOADED',
    rejected_at   timestamptz DEFAULT now()
);
ALTER TABLE sweep_ctl.load_rejects ADD COLUMN IF NOT EXISTS track text;
CREATE INDEX IF NOT EXISTS idx_sweep_load_rejects_uri ON sweep_ctl.load_rejects (uri);

-- Run-level execution history and audit log; shared by both tracks
CREATE TABLE IF NOT EXISTS sweep_ctl.load_audit (
    id              bigserial PRIMARY KEY,
    track           text,
    run_id          text NOT NULL,
    step_name       text NOT NULL,
    status          text NOT NULL,
    total_files     int DEFAULT 0,
    total_rows      bigint DEFAULT 0,
    total_bytes     bigint DEFAULT 0,
    duration_sec    numeric DEFAULT 0,
    message         text,
    started_at      timestamptz DEFAULT now(),
    completed_at    timestamptz DEFAULT now()
);
ALTER TABLE sweep_ctl.load_audit ADD COLUMN IF NOT EXISTS track text;

-- Applied deltas of this database (one row per table and strictly increasing sequence number). The row exists in the
-- layer that captured the delta (bronze: while loading from S3) and in every layer that received it by `promote`.
CREATE TABLE IF NOT EXISTS sweep_ctl.delta_registry (
    delta_id        bigserial PRIMARY KEY,
    run_id          text NOT NULL,
    seq             bigint NOT NULL,
    target_schema   text NOT NULL,
    target_table    text NOT NULL,
    origin_layer    text,
    source_uris     text,
    rows_inserted   bigint DEFAULT 0,
    rows_updated    bigint DEFAULT 0,
    rows_total      bigint DEFAULT 0,
    full_copy       boolean DEFAULT false,
    delta_table     text,
    status          text NOT NULL DEFAULT 'SUCCESS',
    message         text,
    created_at      timestamptz DEFAULT now(),
    applied_at      timestamptz DEFAULT now(),
    UNIQUE (target_schema, target_table, seq)
);

CREATE TABLE IF NOT EXISTS sweep_ctl.promote_log (
    id              bigserial PRIMARY KEY,
    run_id          text NOT NULL,
    source_layer    text NOT NULL,
    target_layer    text NOT NULL,
    target_schema   text NOT NULL,
    target_table    text NOT NULL,
    track           text,
    seq             bigint,
    mode            text,
    inserted        bigint DEFAULT 0,
    updated         bigint DEFAULT 0,
    unchanged       bigint DEFAULT 0,
    rejected        bigint DEFAULT 0,
    status          text NOT NULL,
    message         text,
    started_at      timestamptz DEFAULT now(),
    finished_at     timestamptz
);
CREATE INDEX IF NOT EXISTS idx_sweep_promote_log_table ON sweep_ctl.promote_log (target_schema, target_table);

CREATE TABLE IF NOT EXISTS sweep_ctl.schema_version (
    version     int PRIMARY KEY,
    name        text NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    applied_by  text NOT NULL DEFAULT CURRENT_USER
);
"""

# Copies the rows of the old single load_log into the two new tables and renames it. Runs only while the old table
# exists and has not been renamed yet, so a second run (or a fresh install) does nothing.
LEGACY_COPY_SQL = """
DO $mig$
BEGIN
    IF to_regclass('sweep_ctl.load_log') IS NOT NULL AND to_regclass('sweep_ctl.load_log_legacy') IS NULL THEN
        ALTER TABLE sweep_ctl.load_log ADD COLUMN IF NOT EXISTS s3_last_modified text;
        ALTER TABLE sweep_ctl.load_log ADD COLUMN IF NOT EXISTS s3_etag          text;
        ALTER TABLE sweep_ctl.load_log ADD COLUMN IF NOT EXISTS s3_size          bigint;

        INSERT INTO sweep_ctl.oracle_load_log
               (uri, target_schema, target_table, size_bytes, loaded_rows, seconds, file_format, worker_id, status,
                message, loaded_at, s3_last_modified, s3_etag, s3_size)
        SELECT uri, target_schema, target_table, size_bytes, loaded_rows, seconds, file_format, worker_id, status,
               message, loaded_at, s3_last_modified, s3_etag, s3_size
          FROM sweep_ctl.load_log WHERE uri NOT LIKE '%#%'
        ON CONFLICT (uri) DO NOTHING;

        INSERT INTO sweep_ctl.vsam_load_log
               (uri, target_schema, target_table, size_bytes, loaded_rows, seconds, file_format, worker_id, status,
                message, loaded_at, s3_last_modified, s3_etag, s3_size)
        SELECT regexp_replace(uri, '#[^#]*$', ''), target_schema, target_table, size_bytes, loaded_rows, seconds,
               file_format, worker_id, status, message, loaded_at, s3_last_modified, s3_etag, s3_size
          FROM sweep_ctl.load_log WHERE uri LIKE '%#%'
        ON CONFLICT (uri, target_schema) DO NOTHING;

        ALTER TABLE sweep_ctl.load_log RENAME TO load_log_legacy;
    END IF;
END
$mig$;
"""

# (version, name, sql) - append new migrations here; never edit one that has been released.
MIGRATIONS = [
    (1, "log_tables_delta_registry_promote_log", CTL_OBJECTS_SQL + LEGACY_COPY_SQL),
]
LATEST_VERSION = MIGRATIONS[-1][0]

_VERSION_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS sweep_ctl;
CREATE TABLE IF NOT EXISTS sweep_ctl.schema_version (
    version     int PRIMARY KEY,
    name        text NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    applied_by  text NOT NULL DEFAULT CURRENT_USER
);
"""


def applied_versions():
    """Set of applied migration versions (empty when sweep_ctl.schema_version does not exist yet)."""
    try:
        return {int(r["version"]) for r in query_db_dicts("SELECT version FROM sweep_ctl.schema_version")}
    except RuntimeError:
        return set()


def pending_migrations():
    done = applied_versions()
    return [m for m in MIGRATIONS if m[0] not in done]


def apply_migration(version, name, sql):
    """Runs one migration and its schema_version row in a single transaction."""
    script = ("\\set ON_ERROR_STOP on\nBEGIN;\n" + sql +
              f"\nINSERT INTO sweep_ctl.schema_version (version, name) VALUES ({int(version)}, {lit(name)});\nCOMMIT;\n")
    res = run_psql_script(script, timeout=600)
    if res.returncode != 0:
        raise RuntimeError(f"migration {version} ({name}) failed and was rolled back: {(res.stderr or res.stdout).strip()}")


def migrate_current_database(dry_run=False):
    """Applies the pending migrations to the database the process is connected to. Returns the versions applied."""
    if not dry_run:
        run_psql(_VERSION_TABLE_SQL)  # the version table exists before anything is recorded in it
    todo = pending_migrations()
    if not todo:
        logger.info(f"sweep_ctl is up to date (schema version {LATEST_VERSION})")
        return []
    for version, name, sql in todo:
        if dry_run:
            logger.info(f"[dry-run] would apply migration {version}: {name}")
            continue
        apply_migration(version, name, sql)
        logger.info(f"applied migration {version}: {name}")
    return [m[0] for m in todo]

