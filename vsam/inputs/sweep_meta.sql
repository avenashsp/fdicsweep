-- =============================================================================
-- sweep_meta.sql : Migration control and audit schemas for SWEEP Data Migration
-- Safe to re-run; creates tracking tables, indexes, and audit catalog.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Control & Runtime Tracking Schema (sweep_ctl)
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS sweep_ctl;

-- Atomic file load log: tracks every S3 file loaded into PostgreSQL
CREATE TABLE IF NOT EXISTS sweep_ctl.load_log (
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
    loaded_at       timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sweep_load_log_target 
    ON sweep_ctl.load_log (target_schema, target_table);

CREATE INDEX IF NOT EXISTS idx_sweep_load_log_status 
    ON sweep_ctl.load_log (status);

-- S3 object metadata of the file as loaded (a file counts as loaded only while all three still match S3).
-- CREATE TABLE IF NOT EXISTS does not add columns to an existing table, hence the ALTERs.
ALTER TABLE sweep_ctl.load_log ADD COLUMN IF NOT EXISTS s3_last_modified text;
ALTER TABLE sweep_ctl.load_log ADD COLUMN IF NOT EXISTS s3_etag          text;
ALTER TABLE sweep_ctl.load_log ADD COLUMN IF NOT EXISTS s3_size          bigint;

-- Run-level execution history and audit log
CREATE TABLE IF NOT EXISTS sweep_ctl.load_audit (
    id              bigserial PRIMARY KEY,
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

-- Rows rejected during Oracle Step 06 loads (bad rows are reported, not fatal)
CREATE TABLE IF NOT EXISTS sweep_ctl.load_rejects (
    id            bigserial PRIMARY KEY,
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
CREATE INDEX IF NOT EXISTS idx_sweep_load_rejects_uri ON sweep_ctl.load_rejects (uri);
