-- =============================================================================
-- sweep_meta.sql : Migration control and audit schemas for SWEEP Data Migration
-- Safe to re-run; creates tracking tables, indexes, and audit catalog.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Control & Runtime Tracking Schema (sweep_ctl)
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS sweep_ctl;
CREATE SCHEMA IF NOT EXISTS sweep_delta;

CREATE TABLE IF NOT EXISTS sweep_ctl.oracle_load_log (
    uri             text PRIMARY KEY,
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
    s3_size          bigint
);
CREATE INDEX IF NOT EXISTS idx_sweep_oracle_load_log_target ON sweep_ctl.oracle_load_log (target_schema, target_table);
CREATE INDEX IF NOT EXISTS idx_sweep_oracle_load_log_status ON sweep_ctl.oracle_load_log (status);

CREATE TABLE IF NOT EXISTS sweep_ctl.vsam_load_log (
    uri             text NOT NULL,
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
    s3_size          bigint,
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
    columns         text,
    purged_at       timestamptz,
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

-- Forward-engineering (FE) SQL files applied to this database (`sweep.py fwd-ddl`)
CREATE TABLE IF NOT EXISTS sweep_ctl.fwd_ddl_log (
    file_name     text PRIMARY KEY,
    order_no      bigint NOT NULL,
    checksum      text NOT NULL,
    status        text NOT NULL,                 -- APPLIED / FAILED
    applied_by    text NOT NULL DEFAULT CURRENT_USER,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    duration_sec  numeric,
    message       text
);
-- Structured before / after catalog diff of every applied file
CREATE TABLE IF NOT EXISTS sweep_ctl.fwd_ddl_changes (
    id            bigserial PRIMARY KEY,
    file_name     text NOT NULL,
    kind          text NOT NULL,                 -- table / column / constraint / index / view / trigger / sequence / rule
    action        text NOT NULL,                 -- added / dropped / changed
    schema_name   text,
    object_name   text,
    detail        jsonb,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sweep_fwd_ddl_changes_file ON sweep_ctl.fwd_ddl_changes (file_name);
-- Loader rules registered from the pragmas of FE files
CREATE TABLE IF NOT EXISTS sweep_ctl.fwd_ddl_rules (
    id            bigserial PRIMARY KEY,
    file_name     text NOT NULL,
    rule          text NOT NULL,                 -- ignore-column / rename-column / default-column / skip-table
    schema_name   text NOT NULL,                 -- '*' = every schema
    table_name    text NOT NULL,
    column_name   text,
    new_name      text,
    expression    text,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sweep_fwd_ddl_rules_table ON sweep_ctl.fwd_ddl_rules (table_name);
-- Append-only load history: one row per load attempt (the load log keeps ONE row per file and is overwritten on a reload)
CREATE TABLE IF NOT EXISTS sweep_ctl.load_history (
    id               bigserial PRIMARY KEY,
    run_id           text,
    track            text NOT NULL,              -- oracle / vsam
    layer            text,
    source           text NOT NULL DEFAULT 's3', -- s3 / promote
    uri              text,
    target_schema    text NOT NULL,
    target_table     text NOT NULL,
    status           text NOT NULL,              -- SUCCESS / FAILED
    rows_read        bigint DEFAULT 0,
    rows_inserted    bigint DEFAULT 0,
    rows_updated     bigint DEFAULT 0,
    rows_unchanged   bigint DEFAULT 0,
    rows_rejected    bigint DEFAULT 0,
    rows_duplicates  bigint DEFAULT 0,
    rows_before      bigint,                     -- rows in the table before the first recorded load (ledger baseline)
    s3_last_modified text,
    s3_etag          text,
    s3_size          bigint,
    message          text,
    started_at       timestamptz,
    finished_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sweep_load_history_table ON sweep_ctl.load_history (target_schema, target_table);
-- One row per data-model run (used in silver)
CREATE TABLE IF NOT EXISTS sweep_ctl.model_run_log (
    id            bigserial PRIMARY KEY,
    run_id        text NOT NULL,
    model         text NOT NULL,
    status        text NOT NULL,                 -- SUCCESS / FAILED / SKIPPED
    mode          text,                          -- full / delta
    started_at    timestamptz,
    ended_at      timestamptz DEFAULT now(),
    exit_code     int,
    log_file      text,
    input_seqs    jsonb,                         -- delta sequences processed per input table
    result        jsonb,                         -- the SWEEP_MODEL_RESULT line
    message       text
);
CREATE INDEX IF NOT EXISTS idx_sweep_model_run_log_model ON sweep_ctl.model_run_log (model, id);

-- -----------------------------------------------------------------------------
-- Immutable Native DDL Audit Trail & Event Triggers
-- Automatically records every CREATE, ALTER, DROP, TRUNCATE, and schema change
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sweep_ctl.ddl_history (
    id              BIGSERIAL PRIMARY KEY,
    username       TEXT DEFAULT CURRENT_USER,
    sessionuser    TEXT DEFAULT SESSION_USER,
    client_ip       INET DEFAULT inet_client_addr(),
    action_time     TIMESTAMPTZ DEFAULT clock_timestamp(),
    command_tag     TEXT,            -- e.g. 'ALTER TABLE', 'CREATE INDEX'
    object_type     TEXT,            -- e.g. 'table', 'index', 'view'
    object_identity TEXT,            -- e.g. 'bss01105.brokeraccountdetail'
    query_text      TEXT             -- The exact SQL statement executed
);

CREATE INDEX IF NOT EXISTS idx_sweep_ddl_history_action_time
    ON sweep_ctl.ddl_history (action_time);

CREATE INDEX IF NOT EXISTS idx_sweep_ddl_history_tag
    ON sweep_ctl.ddl_history (command_tag);

CREATE INDEX IF NOT EXISTS idx_sweep_ddl_history_object
    ON sweep_ctl.ddl_history (object_identity);

-- Event Trigger Function: Records executed DDL commands (CREATE, ALTER, etc.)
CREATE OR REPLACE FUNCTION sweep_ctl.fn_record_ddl_history()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    r RECORD;
    found_cmd BOOLEAN := false;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        found_cmd := true;
        INSERT INTO sweep_ctl.ddl_history (
            username,
            sessionuser,
            client_ip,
            action_time,
            command_tag,
            object_type,
            object_identity,
            query_text
        ) VALUES (
            CURRENT_USER,
            SESSION_USER,
            inet_client_addr(),
            clock_timestamp(),
            r.command_tag,
            r.object_type,
            r.object_identity,
            current_query()
        );
    END LOOP;

    IF NOT found_cmd THEN
        INSERT INTO sweep_ctl.ddl_history (
            username,
            sessionuser,
            client_ip,
            action_time,
            command_tag,
            object_type,
            object_identity,
            query_text
        ) VALUES (
            CURRENT_USER,
            SESSION_USER,
            inet_client_addr(),
            clock_timestamp(),
            TG_TAG,
            'UNKNOWN',
            NULL,
            current_query()
        );
    END IF;
END;
$$;

-- Event Trigger Function: Records dropped objects (DROP TABLE, DROP INDEX, etc.)
CREATE OR REPLACE FUNCTION sweep_ctl.fn_record_ddl_drop_history()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF r.original THEN
            INSERT INTO sweep_ctl.ddl_history (
                username,
                sessionuser,
                client_ip,
                action_time,
                command_tag,
                object_type,
                object_identity,
                query_text
            ) VALUES (
                CURRENT_USER,
                SESSION_USER,
                inet_client_addr(),
                clock_timestamp(),
                TG_TAG,
                r.object_type,
                r.object_identity,
                current_query()
            );
        END IF;
    END LOOP;
END;
$$;

-- Register Event Triggers (Safe recreation)
DROP EVENT TRIGGER IF EXISTS trg_sweep_ddl_command_history;
CREATE EVENT TRIGGER trg_sweep_ddl_command_history
    ON ddl_command_end
    EXECUTE FUNCTION sweep_ctl.fn_record_ddl_history();

DROP EVENT TRIGGER IF EXISTS trg_sweep_ddl_drop_history;
CREATE EVENT TRIGGER trg_sweep_ddl_drop_history
    ON sql_drop
    EXECUTE FUNCTION sweep_ctl.fn_record_ddl_drop_history();

-- Protection Trigger Function: Ensures sweep_ctl.ddl_history is an immutable audit log
CREATE OR REPLACE FUNCTION sweep_ctl.fn_protect_ddl_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'sweep_ctl.ddl_history is an immutable audit trail and cannot be modified, deleted, or truncated';
END;
$$;

DROP TRIGGER IF EXISTS trg_protect_ddl_history_dml ON sweep_ctl.ddl_history;
CREATE TRIGGER trg_protect_ddl_history_dml
    BEFORE UPDATE OR DELETE ON sweep_ctl.ddl_history
    FOR EACH ROW
    EXECUTE FUNCTION sweep_ctl.fn_protect_ddl_history();

DROP TRIGGER IF EXISTS trg_protect_ddl_history_truncate ON sweep_ctl.ddl_history;
CREATE TRIGGER trg_protect_ddl_history_truncate
    BEFORE TRUNCATE ON sweep_ctl.ddl_history
    FOR EACH STATEMENT
    EXECUTE FUNCTION sweep_ctl.fn_protect_ddl_history();

-- -----------------------------------------------------------------------------
-- 2. Oracle Compatibility Helpers
-- -----------------------------------------------------------------------------
-- Provides DUAL view for seamless execution of Oracle views and scripts using 'FROM DUAL'
CREATE OR REPLACE VIEW public.dual AS SELECT 'X'::varchar AS dummy;
GRANT SELECT ON public.dual TO PUBLIC;

-- Oracle Compatibility Function: ADD_MONTHS
CREATE OR REPLACE FUNCTION public.add_months(d date, n integer)
RETURNS date AS $$
    SELECT (d + (n * INTERVAL '1 month'))::date;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION public.add_months(ts timestamp without time zone, n integer)
RETURNS timestamp without time zone AS $$
    SELECT (ts + (n * INTERVAL '1 month'));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION public.add_months(tstz timestamp with time zone, n integer)
RETURNS timestamp with time zone AS $$
    SELECT (tstz + (n * INTERVAL '1 month'));
$$ LANGUAGE sql IMMUTABLE;

-- Oracle Compatibility Function: LAST_DAY
CREATE OR REPLACE FUNCTION public.last_day(d date)
RETURNS date AS $$
    SELECT (date_trunc('month', d) + interval '1 month - 1 day')::date;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION public.last_day(ts timestamp without time zone)
RETURNS date AS $$
    SELECT (date_trunc('month', ts) + interval '1 month - 1 day')::date;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION public.last_day(tstz timestamp with time zone)
RETURNS date AS $$
    SELECT (date_trunc('month', tstz) + interval '1 month - 1 day')::date;
$$ LANGUAGE sql IMMUTABLE;

-- Oracle Timestamp +/- Integer Operator Emulation
CREATE OR REPLACE FUNCTION public.timestamptz_sub_int(ts timestamptz, days integer)
RETURNS timestamptz AS $$
    SELECT ts - (days || ' days')::interval;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION public.timestamp_sub_int(ts timestamp without time zone, days integer)
RETURNS timestamp without time zone AS $$
    SELECT ts - (days || ' days')::interval;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION public.timestamptz_add_int(ts timestamptz, days integer)
RETURNS timestamptz AS $$
    SELECT ts + (days || ' days')::interval;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION public.timestamp_add_int(ts timestamp without time zone, days integer)
RETURNS timestamp without time zone AS $$
    SELECT ts + (days || ' days')::interval;
$$ LANGUAGE sql IMMUTABLE;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_operator WHERE oprname = '-' AND oprleft = 'timestamptz'::regtype AND oprright = 'integer'::regtype) THEN
        CREATE OPERATOR public.- (FUNCTION = public.timestamptz_sub_int, LEFTARG = timestamptz, RIGHTARG = integer);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_operator WHERE oprname = '-' AND oprleft = 'timestamp'::regtype AND oprright = 'integer'::regtype) THEN
        CREATE OPERATOR public.- (FUNCTION = public.timestamp_sub_int, LEFTARG = timestamp without time zone, RIGHTARG = integer);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_operator WHERE oprname = '+' AND oprleft = 'timestamptz'::regtype AND oprright = 'integer'::regtype) THEN
        CREATE OPERATOR public.+ (FUNCTION = public.timestamptz_add_int, LEFTARG = timestamptz, RIGHTARG = integer);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_operator WHERE oprname = '+' AND oprleft = 'timestamp'::regtype AND oprright = 'integer'::regtype) THEN
        CREATE OPERATOR public.+ (FUNCTION = public.timestamp_add_int, LEFTARG = timestamp without time zone, RIGHTARG = integer);
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 6.3.0: canonical audit trigger function, DDL capture of data models (migration 3)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sweep_ctl.fn_audit_cols() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE loader text := nullif(current_setting('sweep.loader', true), '');
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.createdby := coalesce(NEW.createdby, SESSION_USER);
        NEW.modifiedby := coalesce(NEW.modifiedby, SESSION_USER);
        NEW.createddate := coalesce(NEW.createddate, now());
        NEW.modifieddate := coalesce(NEW.modifieddate, NEW.createddate);
    ELSE
        NEW.createdby := OLD.createdby;
        NEW.createddate := OLD.createddate;
        IF loader IS NULL THEN                       -- only a loader / model session may set the _m columns of an existing row
            NEW.createdby_m := OLD.createdby_m;
            NEW.createddate_m := OLD.createddate_m;
        END IF;
        NEW.modifiedby := SESSION_USER;
        NEW.modifieddate := now();
    END IF;
    RETURN NEW;
END
$fn$;

-- DDL a data model ran in silver (captured by the event trigger below, replayed by promote into GB / gold)
CREATE TABLE IF NOT EXISTS sweep_ctl.model_ddl_history (
    id            bigserial PRIMARY KEY,
    capture_tag   text NOT NULL,                 -- '<run_id>:<model>'
    command_tag   text,
    object_type   text,
    schema_name   text,
    object_identity text,
    captured_by   text NOT NULL DEFAULT SESSION_USER,
    captured_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sweep_model_ddl_history_tag ON sweep_ctl.model_ddl_history (capture_tag);
ALTER TABLE sweep_ctl.model_run_log ADD COLUMN IF NOT EXISTS ddl_capture text;
CREATE OR REPLACE FUNCTION sweep_ctl.fn_capture_ddl() RETURNS event_trigger AS $fn$
DECLARE r record; tag text := nullif(current_setting('sweep.capture', true), '');
BEGIN
    IF tag IS NULL THEN RETURN; END IF;
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        CONTINUE WHEN r.schema_name IN ('sweep_ctl', 'sweep_delta', 'pg_temp') OR r.in_extension;
        INSERT INTO sweep_ctl.model_ddl_history (capture_tag, command_tag, object_type, schema_name, object_identity)
        VALUES (tag, r.command_tag, r.object_type, r.schema_name, r.object_identity);
    END LOOP;
END
$fn$ LANGUAGE plpgsql;
DO $ev$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'sweep_capture_ddl') THEN
        BEGIN
            CREATE EVENT TRIGGER sweep_capture_ddl ON ddl_command_end EXECUTE FUNCTION sweep_ctl.fn_capture_ddl();
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'sweep: event trigger not created (needs rds_superuser); data-model DDL capture is unavailable';
        END;
    END IF;
END
$ev$;
