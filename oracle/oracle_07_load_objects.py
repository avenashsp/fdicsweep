#!/usr/bin/env python3
"""
07_loadObjects.py - Post-Ingestion Keys, Constraints, Indexes, Views, and Triggers Enforcement
Enforces intelligent, idempotent object creation:
  1. Loads objects for ALL tables defined in post-load scripts (regardless of whether tables currently contain data).
  2. If the objects already exist in the PostgreSQL catalog, LEAVES THEM ALONE (skips execution).
  3. Stages:
     - Stage 1: Primary Keys & Unique Constraints (AK) (unique_and_pk.sql)
     - Stage 2: Secondary Indexes (CONCURRENTLY) (indexes.sql)
     - Stage 3: Foreign Keys (NOT VALID - rapid attach without table scans) (fk_not_valid.sql)
     - Stage 4: Foreign Key Validation (Safe off-peak constraint validation) (fk_validate.sql)
     - Stage 5: Views and Materialized Views (views.sql)
     - Stage 6: Triggers and Trigger Functions (triggers.sql)

Generates: reports/07_loadObjects_report_<timestamp>.xlsx
"""
import re
import sys
import time
import argparse
from pathlib import Path
from sweep_common import (
    log_summary,
    ORACLE_POST_LOAD_DIR,
    ORACLE_REPORTS_DIR,
    clip_error,
    get_logger,
    get_timestamp,
    get_db_name,
    generate_excel_report,
    run_psql,
    query_db_rows,
    query_db_dicts,
    PGHOST,
    PGPORT,
    PGDATABASE,
    PGUSER,
    has_db_auth,
    AUTH_HELP,
    CTL_SCHEMA,
    ORACLE_LOAD_LOG_TABLE,
    DB_ERRORS,
)

from sweep_targeting import Selector, add_targeting_args, selector_or_exit, validate_or_exit
from sweep_layers import add_layer_arg, ensure_layer, manifest_tables
from oracle.oracle_07_parser import parse_sql_file_blocks, simplify_case_to_coalesce

logger = get_logger("oracle_07_load_objects", "oracle_07_load_objects")

STAGES = [
    ("pk", "unique_and_pk.sql", "Primary Keys and Unique Constraints (AK)"),
    ("alter", "alter_statements.sql", "Check Constraints & Alter Statements"),
    ("index", "indexes.sql", "Secondary Indexes (Concurrent)"),
    ("fk", "fk_not_valid.sql", "Foreign Keys (NOT VALID)"),
    ("validate", "fk_validate.sql", "Foreign Key Validations"),
    ("views", "views.sql", "Views and Materialized Views"),
    ("triggers", "triggers.sql", "Triggers and Trigger Functions")
]


def get_loaded_tables():
    """
    Returns a set of lowercase 'schema.table' identifiers that have confirmed loaded data.
    Checks:
      1. sweep_ctl.load_log records with status = 'SUCCESS'
      2. pg_stat_user_tables with n_live_tup > 0
      3. pg_inherits to include parent and child partitions of loaded tables
    """
    loaded = set()
    sql_check = (
        "DO $$ BEGIN "
        f"IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{CTL_SCHEMA}' AND table_name = 'oracle_load_log') THEN "
        f"  PERFORM 1; "
        "END IF; END $$;"
    )
    try:
        run_psql(sql_check)
    except RuntimeError as exc:
        logger.debug(f"Existence check failed (continuing): {exc}")

    sql_loaded = (
        "WITH raw_loaded AS ( "
        f"    SELECT DISTINCT LOWER(target_schema) || '.' || LOWER(target_table) AS tbl "
        f"    FROM {ORACLE_LOAD_LOG_TABLE} "
        f"    WHERE status = 'SUCCESS' "
        "    UNION "
        "    SELECT LOWER(schemaname) || '.' || LOWER(relname) AS tbl "
        "    FROM pg_stat_user_tables "
        "    WHERE n_live_tup > 0 "
        ") "
        "SELECT tbl FROM raw_loaded "
        "UNION "
        "SELECT DISTINCT LOWER(pn.nspname) || '.' || LOWER(p.relname) AS tbl "
        "FROM pg_inherits i "
        "JOIN pg_class c ON i.inhrelid = c.oid "
        "JOIN pg_namespace cn ON c.relnamespace = cn.oid "
        "JOIN pg_class p ON i.inhparent = p.oid "
        "JOIN pg_namespace pn ON p.relnamespace = pn.oid "
        "WHERE LOWER(cn.nspname) || '.' || LOWER(c.relname) IN (SELECT tbl FROM raw_loaded) "
        "UNION "
        "SELECT DISTINCT LOWER(cn.nspname) || '.' || LOWER(c.relname) AS tbl "
        "FROM pg_inherits i "
        "JOIN pg_class c ON i.inhrelid = c.oid "
        "JOIN pg_namespace cn ON c.relnamespace = cn.oid "
        "JOIN pg_class p ON i.inhparent = p.oid "
        "JOIN pg_namespace pn ON p.relnamespace = pn.oid "
        "WHERE LOWER(pn.nspname) || '.' || LOWER(p.relname) IN (SELECT tbl FROM raw_loaded);"
    )
    try:
        rows = query_db_rows(sql_loaded)
        for r in rows:
            loaded.add(r.strip().lower())
    except DB_ERRORS:
        # Fallback to pg_stat_user_tables if load_log is not yet present
        try:
            fallback_sql = "SELECT LOWER(schemaname) || '.' || LOWER(relname) FROM pg_stat_user_tables WHERE n_live_tup > 0;"
            for r in query_db_rows(fallback_sql):
                loaded.add(r.strip().lower())
        except DB_ERRORS as e2:
            logger.warning(f"Could not query loaded tables from PostgreSQL: {e2}")

    return loaded


def get_existing_catalog_objects():
    """
    Queries PostgreSQL system catalogs to discover all currently existing:
      - Constraints (PK, Unique, FK) and their validation status
      - Indexes
      - Views
      - Triggers
    """
    catalog = {
        "constraints": set(),           # (schema, conname) and (schema, table, conname)
        "validated_constraints": set(), # (schema, conname)
        "indexes": set(),               # (schema, relname) and (schema, table, relname)
        "views": set(),                 # (schema, relname)
        "triggers": set()               # (schema, table, tgname) and (schema, tgname)
    }

    try:
        # 1. Existing Constraints
        sql_cons = (
            "SELECT LOWER(n.nspname) AS s, LOWER(cl.relname) AS t, LOWER(c.conname) AS cn, c.convalidated "
            "FROM pg_constraint c "
            "JOIN pg_namespace n ON n.oid = c.connamespace "
            "JOIN pg_class cl ON cl.oid = c.conrelid "
            "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta')"
        )
        for r in query_db_dicts(sql_cons):
            s = r["s"].lower()
            t = r["t"].lower()
            cn = r["cn"].lower()
            catalog["constraints"].add((s, cn))
            catalog["constraints"].add((s, t, cn))
            if str(r.get("convalidated", "")).lower() in ("true", "t", "1"):
                catalog["validated_constraints"].add((s, cn))
                catalog["validated_constraints"].add((s, t, cn))

        # 2. Existing Indexes
        sql_idx = (
            "SELECT LOWER(n.nspname) AS s, LOWER(cl.relname) AS t, LOWER(i.relname) AS iname "
            "FROM pg_index x "
            "JOIN pg_class i ON i.oid = x.indexrelid "
            "JOIN pg_class cl ON cl.oid = x.indrelid "
            "JOIN pg_namespace n ON n.oid = cl.relnamespace "
            "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta')"
        )
        for r in query_db_dicts(sql_idx):
            s = r["s"].lower()
            t = r["t"].lower()
            iname = r["iname"].lower()
            catalog["indexes"].add((s, iname))
            catalog["indexes"].add((s, t, iname))

        # 3. Existing Views
        sql_views = (
            "SELECT LOWER(table_schema) AS s, LOWER(table_name) AS v "
            "FROM information_schema.views "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta')"
        )
        for r in query_db_dicts(sql_views):
            s = r["s"].lower()
            v = r["v"].lower()
            catalog["views"].add((s, v))

        # 4. Existing Triggers
        sql_trg = (
            "SELECT LOWER(trigger_schema) AS s, LOWER(event_object_table) AS t, LOWER(trigger_name) AS trg "
            "FROM information_schema.triggers "
            "WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta')"
        )
        for r in query_db_dicts(sql_trg):
            s = r["s"].lower()
            t = r["t"].lower()
            trg = r["trg"].lower()
            catalog["triggers"].add((s, t, trg))
            catalog["triggers"].add((s, trg))

    except DB_ERRORS as e:
        logger.warning(f"Could not query existing PostgreSQL catalog objects: {e}")

    return catalog


def sync_identity_sequences():
    """
    Synchronizes PostgreSQL identity column sequences with current table data
    so subsequent database inserts will not generate duplicate primary key errors.
    """
    logger.info("Synchronizing PostgreSQL identity column sequences with current table data...")
    sql_sync = """
    DO $$
    DECLARE
        r RECORD;
        v_max BIGINT;
        v_seq TEXT;
        v_cnt INT := 0;
    BEGIN
        FOR r IN (
            SELECT table_schema, table_name, column_name
            FROM information_schema.columns
            WHERE is_identity = 'YES'
              AND table_schema NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta')
        ) LOOP
            v_seq := pg_get_serial_sequence(quote_ident(r.table_schema) || '.' || quote_ident(r.table_name), r.column_name);
            IF v_seq IS NOT NULL THEN
                EXECUTE format('SELECT COALESCE(MAX(%I), 0) FROM %I.%I', r.column_name, r.table_schema, r.table_name) INTO v_max;
                IF v_max > 0 THEN
                    EXECUTE format('SELECT setval(%L, %s, true)', v_seq, v_max);
                    v_cnt := v_cnt + 1;
                END IF;
            END IF;
        END LOOP;
        RAISE NOTICE 'Synchronized % identity sequences.', v_cnt;
    END $$;
    """
    try:
        run_psql(sql_sync)
        logger.info("Identity sequences synchronized successfully.")
    except DB_ERRORS as e:
        logger.warning(f"Could not synchronize identity sequences: {e}")

def apply_post_load(stages_to_run=None, schemas_filter=None, only_loaded=False, selector=None, layer=None):
    """
    Executes post-load scripts selectively:
      - By default, creates objects for ALL tables.
      - Skips objects that already exist in the PostgreSQL catalog (safe idempotency).
      - If only_loaded=True, restricts creation to tables where data has been loaded.
    """
    ts = get_timestamp()
    if not has_db_auth():
        logger.error(AUTH_HELP)
        return False
    sel = selector or Selector(schemas={s.lower() for s in (schemas_filter or ())})
    if sel.table_selectors:
        try:
            cat = query_db_dicts("SELECT table_schema AS s, table_name AS t FROM information_schema.tables "
                                 "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta', 'sweep_delta')")
        except RuntimeError as e:
            logger.error(f"Cannot validate the selectors: {e}")
            return False
        validate_or_exit(sel.validate_tables, [(r["s"], r["t"]) for r in cat], what="the database", logger=logger)
    all_script_results = []
    all_object_actions = []
    listed = manifest_tables(layer) if (layer is not None and layer.manifest_path.exists()) else None   # None = no restriction

    logger.info("Connecting to target database to evaluate tables and existing objects...")
    logger.info(f"Target Database: {PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE}")

    # 1. Discover tables with loaded data (if restricted mode enabled)
    if only_loaded:
        loaded_tables = get_loaded_tables()
        logger.info(f"Restricted mode enabled: Detected {len(loaded_tables)} table(s) with loaded data in database.")
    else:
        loaded_tables = set()
        logger.info("Mode: Applying post-load objects to ALL tables across target schema(s).")

    # 2. Discover existing catalog objects
    catalog_objects = get_existing_catalog_objects()
    total_existing = (len(catalog_objects["constraints"]) // 2 +
                      len(catalog_objects["indexes"]) // 2 +
                      len(catalog_objects["views"]) +
                      len(catalog_objects["triggers"]) // 2)
    logger.info(f"Discovered {total_existing} existing object(s) already in PostgreSQL catalog.")

    for stage_code, file_suffix, description in STAGES:
        if stages_to_run and stage_code not in stages_to_run:
            continue

        stage_files = list(ORACLE_POST_LOAD_DIR.glob(f"*_{file_suffix}"))
        if stage_code == "views":
            # Process _code views first so base views (e.g. daewithoverrides) are created before downstream views
            stage_files = sorted(stage_files, key=lambda f: (0 if "_code" in f.name.lower() else 1, f.name.lower()))
        else:
            stage_files = sorted(stage_files)

        logger.info("\n===============================================================================")
        logger.info(f"--- Stage: {description} ({len(stage_files)} script(s)) ---")
        logger.info("===============================================================================")

        for sf in stage_files:
            schema_name = sf.name[:-len(f"_{file_suffix}")]
            if not sel.matches_schema(schema_name):
                continue

            blocks = parse_sql_file_blocks(sf, stage_code)
            applied_count = 0
            skipped_exists_count = 0
            skipped_no_data_count = 0
            failed_count = 0

            stmts_to_execute = []
            session_headers = []

            for b in blocks:
                obj_type = b["object_type"]
                s_name = b["schema"]
                t_name = b["table"]
                o_name = b["object_name"]

                # Session settings are retained
                if obj_type == "SESSION":
                    session_headers.append(b["sql"])
                    continue

                target_table_id = f"{s_name}.{t_name}" if t_name else ""

                # --tables / --exclude-tables: only the selected tables' objects; schema-level objects (views) are
                # left alone by a run that names tables
                # a layer with a manifest only ever gets objects on the tables of that manifest
                if listed is not None and obj_type != "SESSION" and (s_name.lower(), t_name.lower()) not in listed:
                    continue
                if sel.table_selectors:
                    if t_name and not sel.matches(s_name, t_name):
                        continue
                    if not t_name and sel.tables:
                        continue

                # -------------------------------------------------------------
                # RULE 0: Do not validate CHECK constraints (skipped by policy)
                # -------------------------------------------------------------
                if obj_type == "CHECK_VALIDATE":
                    all_object_actions.append({
                        "stage": stage_code,
                        "schema": s_name,
                        "table": t_name,
                        "object_type": obj_type,
                        "object_name": o_name,
                        "status": "SKIPPED_POLICY",
                        "reason": "Check constraint validation is skipped by policy.",
                        "duration_sec": 0.0
                    })
                    continue

                # -------------------------------------------------------------
                # RULE 1: Optional filter for only loaded tables (default: load for all tables)
                # -------------------------------------------------------------
                if only_loaded and target_table_id and target_table_id not in loaded_tables:
                    skipped_no_data_count += 1
                    all_object_actions.append({
                        "stage": stage_code,
                        "schema": s_name,
                        "table": t_name,
                        "object_type": obj_type,
                        "object_name": o_name,
                        "status": "SKIPPED_NO_DATA",
                        "reason": f"Table {target_table_id} has no loaded data yet.",
                        "duration_sec": 0.0
                    })
                    continue

                # -------------------------------------------------------------
                # RULE 2: If the object is already there, leave it untouched
                # -------------------------------------------------------------
                already_exists = False
                exists_reason = ""

                if obj_type in ("PK", "UNIQUE", "FK", "CHECK", "CONSTRAINT"):
                    if (s_name, o_name) in catalog_objects["constraints"] or (s_name, t_name, o_name) in catalog_objects["constraints"]:
                        already_exists = True
                        exists_reason = f"Constraint {s_name}.{o_name} already exists."
                elif obj_type in ("FK_VALIDATE", "CHECK_VALIDATE"):
                    if (s_name, o_name) in catalog_objects["validated_constraints"] or (s_name, t_name, o_name) in catalog_objects["validated_constraints"]:
                        already_exists = True
                        exists_reason = f"Constraint {s_name}.{o_name} is already validated."
                elif obj_type == "INDEX":
                    if (s_name, o_name) in catalog_objects["indexes"] or (s_name, t_name, o_name) in catalog_objects["indexes"]:
                        already_exists = True
                        exists_reason = f"Index {s_name}.{o_name} already exists."
                elif obj_type == "INDEX_ATTACH":
                    if (s_name, o_name) in catalog_objects["indexes"]:
                        already_exists = True
                        exists_reason = f"Child index {s_name}.{o_name} already exists/attached."
                elif obj_type == "VIEW":
                    if (s_name, o_name) in catalog_objects["views"]:
                        already_exists = True
                        exists_reason = f"View {s_name}.{o_name} already exists."
                elif obj_type == "TRIGGER":
                    if (s_name, t_name, o_name) in catalog_objects["triggers"] or (s_name, o_name) in catalog_objects["triggers"]:
                        already_exists = True
                        exists_reason = f"Trigger {s_name}.{o_name} already exists on {s_name}.{t_name}."

                if already_exists:
                    skipped_exists_count += 1
                    all_object_actions.append({
                        "stage": stage_code,
                        "schema": s_name,
                        "table": t_name,
                        "object_type": obj_type,
                        "object_name": o_name,
                        "status": "SKIPPED_ALREADY_EXISTS",
                        "reason": exists_reason,
                        "duration_sec": 0.0
                    })
                    continue

                # Statement passed all checks: Queue for execution
                stmts_to_execute.append(b)

            # -----------------------------------------------------------------
            # Execute Filtered Statements
            # -----------------------------------------------------------------
            script_t0 = time.time()
            if sel.dry_run:
                logger.info(f"[dry-run] {sf.name}: {len(stmts_to_execute)} statement(s) would be executed "
                            f"({skipped_exists_count} already exist); nothing executed")
                all_script_results.append({"stage": stage_code, "description": description, "schema": schema_name, "file_name": sf.name,
                                           "status": "DRY_RUN", "applied": 0, "skipped_already_exists": skipped_exists_count,
                                           "skipped_no_data": skipped_no_data_count, "duration_seconds": 0.0,
                                           "message": f"{len(stmts_to_execute)} statement(s) pending"})
                continue
            if not stmts_to_execute:
                logger.info(f"Skipping execution of {sf.name}: All objects already exist ({skipped_exists_count}) or tables have no loaded data ({skipped_no_data_count}).")
                all_script_results.append({
                    "stage": stage_code,
                    "description": description,
                    "schema": schema_name,
                    "file_name": sf.name,
                    "status": "UP_TO_DATE",
                    "applied": 0,
                    "skipped_already_exists": skipped_exists_count,
                    "skipped_no_data": skipped_no_data_count,
                    "duration_seconds": 0.0,
                    "message": "All objects already exist or tables not loaded."
                })
                continue

            logger.info(f"Executing {len(stmts_to_execute)} actionable statement(s) for {sf.name}...")
            max_passes = 3 if stage_code == "views" else 1
            current_queue = list(stmts_to_execute)

            for pass_idx in range(1, max_passes + 1):
                deferred_queue = []
                for b in current_queue:
                    obj_t0 = time.time()
                    stmt_sql = b["sql"]
                    if b.get("object_type") == "VIEW" and b.get("schema"):
                        v_schema = b["schema"].lower()
                        paths = [f'"{v_schema}"']
                        if v_schema.endswith("_code"):
                            paths.append(f'"{v_schema[:-5]}"')
                        else:
                            paths.append(f'"{v_schema}_code"')
                        paths.append("public")
                        search_cmd = f"SET search_path TO {', '.join(paths)};\n"
                        stmt_sql = search_cmd + stmt_sql
                        # Dynamically convert any CASE WHEN ... IS NULL to COALESCE(...)
                        stmt_sql = simplify_case_to_coalesce(stmt_sql)
                        # Translate Oracle ROWNUM pseudo-column to ANSI ROW_NUMBER() OVER ()
                        stmt_sql = re.sub(r'(?:"ROWNUM"|\bROWNUM\b)', 'ROW_NUMBER() OVER ()', stmt_sql, flags=re.I)
                        # Translate ADD_MONTHS(date_expr, n) -> (date_expr + (n * INTERVAL '1 month'))
                        pat_add_months = re.compile(r'\bADD_MONTHS\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', re.I)
                        stmt_sql = pat_add_months.sub(lambda m: f"({m.group(1).strip()} + ({m.group(2).strip()} * INTERVAL '1 month'))", stmt_sql)
                        # Translate LAST_DAY(date_expr) -> (DATE_TRUNC('month', date_expr) + INTERVAL '1 month - 1 day')::date
                        pat_last_day = re.compile(r'\bLAST_DAY\s*\(\s*([^)]+?)\s*\)', re.I)
                        stmt_sql = pat_last_day.sub(lambda m: f"(DATE_TRUNC('month', {m.group(1).strip()}) + INTERVAL '1 month - 1 day')::date", stmt_sql)
                        # Translate CURRENT_TIMESTAMP / CURRENT_DATE / date col +/- integer
                        pat_ts_math = re.compile(
                            r'(\b(?:CURRENT_TIMESTAMP|CURRENT_DATE|SYSDATE|SYSTIMESTAMP|\w+\.(?:dateid|processdate|date|lastupdate))\b)\s*([+-])\s*(\d+)\b',
                            re.I
                        )
                        stmt_sql = pat_ts_math.sub(r"\1 \2 INTERVAL '\3 days'", stmt_sql)
                        # Translate Oracle date addition: (col.dateid + 1) -> (col.dateid + '1 day'::interval)
                        stmt_sql = re.sub(r'(\b\w+\.(?:dateid|processdate|date)\s*\+\s*)(\d+)\b', r"\1'\2 day'::interval", stmt_sql, flags=re.I)
                    if session_headers:
                        # Strip any backslash commands to guarantee 100% valid pure SQL
                        clean_headers = [h for h in session_headers if not h.strip().startswith("\\")]
                        if clean_headers:
                            stmt_sql = "\n".join(clean_headers) + "\n" + stmt_sql

                    try:
                        # Configure connection-level timeouts and index building parallelism
                        pg_opts = "-c statement_timeout=0"
                        if stage_code == "indexes":
                            pg_opts += " -c maintenance_work_mem=2GB -c max_parallel_maintenance_workers=4"

                        run_psql(stmt_sql, extra_env={"PGOPTIONS": pg_opts})
                        obj_sec = round(time.time() - obj_t0, 2)
                        applied_count += 1

                        # Update local catalog so subsequent statements know it exists
                        s_name = b["schema"]
                        t_name = b["table"]
                        o_name = b["object_name"]
                        obj_type = b["object_type"]

                        if obj_type in ("PK", "UNIQUE", "FK", "CHECK", "CONSTRAINT"):
                            catalog_objects["constraints"].add((s_name, o_name))
                            if t_name:
                                catalog_objects["constraints"].add((s_name, t_name, o_name))
                        elif obj_type in ("FK_VALIDATE", "CHECK_VALIDATE"):
                            catalog_objects["validated_constraints"].add((s_name, o_name))
                            if t_name:
                                catalog_objects["validated_constraints"].add((s_name, t_name, o_name))
                        elif obj_type == "INDEX":
                            catalog_objects["indexes"].add((s_name, o_name))
                            if t_name:
                                catalog_objects["indexes"].add((s_name, t_name, o_name))
                        elif obj_type == "VIEW":
                            catalog_objects["views"].add((s_name, o_name))
                        elif obj_type == "TRIGGER":
                            catalog_objects["triggers"].add((s_name, t_name, o_name))
                            catalog_objects["triggers"].add((s_name, o_name))

                        all_object_actions.append({
                            "stage": stage_code,
                            "schema": s_name,
                            "table": t_name,
                            "object_type": obj_type,
                            "object_name": o_name,
                            "status": "APPLIED",
                            "reason": "Successfully created in PostgreSQL.",
                            "duration_sec": obj_sec
                        })
                        logger.info(f"  [+] APPLIED: {b['raw']} ({obj_sec}s)")
                    except RuntimeError as e:
                        obj_sec = round(time.time() - obj_t0, 2)
                        err_msg = str(e).strip()
                        err_lower = err_msg.lower()

                        # If constraint or relation/object already exists in PostgreSQL, do NOT mark as FAILED
                        if "already exists" in err_lower:
                            skipped_exists_count += 1
                            all_object_actions.append({
                                "stage": stage_code,
                                "schema": b["schema"],
                                "table": b["table"],
                                "object_type": b["object_type"],
                                "object_name": b["object_name"],
                                "status": "SKIPPED_ALREADY_EXISTS",
                                "reason": clip_error(f"Already exists in database: {err_msg}"),
                                "duration_sec": obj_sec
                            })
                            # Cache in catalog so subsequent checks know it exists
                            s_name = b["schema"]
                            t_name = b["table"]
                            o_name = b["object_name"]
                            if b["object_type"] in ("PK", "UNIQUE", "FK", "CHECK", "CONSTRAINT") or "constraint" in err_lower:
                                catalog_objects["constraints"].add((s_name, o_name))
                                if t_name:
                                    catalog_objects["constraints"].add((s_name, t_name, o_name))
                            elif b["object_type"] == "INDEX" or "index" in err_lower or "relation" in err_lower:
                                catalog_objects["indexes"].add((s_name, o_name))
                                if t_name:
                                    catalog_objects["indexes"].add((s_name, t_name, o_name))
                            elif b["object_type"] == "VIEW" or "view" in err_lower:
                                catalog_objects["views"].add((s_name, o_name))
                            elif b["object_type"] == "TRIGGER" or "trigger" in err_lower:
                                catalog_objects["triggers"].add((s_name, t_name, o_name))
                                catalog_objects["triggers"].add((s_name, o_name))
                            logger.info(f"  [-] ALREADY EXISTS: {b['raw']} (marked as SKIPPED_ALREADY_EXISTS)")
                        elif pass_idx < max_passes and ("does not exist" in err_lower or "undefined" in err_lower):
                            deferred_queue.append(b)
                            logger.info(f"  [~] DEFERRED (pass {pass_idx}): {b['raw']} (waiting for dependent relation, will retry in pass {pass_idx + 1})...")
                        else:
                            failed_count += 1
                            all_object_actions.append({
                                "stage": stage_code,
                                "schema": b["schema"],
                                "table": b["table"],
                                "object_type": b["object_type"],
                                "object_name": b["object_name"],
                                "status": "FAILED",
                                "reason": clip_error(err_msg),
                                "duration_sec": obj_sec
                            })
                            logger.warning(f"  [!] FAILED: {b['raw']} -> {clip_error(err_msg)}")
                if not deferred_queue:
                    break
                current_queue = deferred_queue

            script_duration = round(time.time() - script_t0, 2)
            all_script_results.append({
                "stage": stage_code,
                "description": description,
                "schema": schema_name,
                "file_name": sf.name,
                "status": "SUCCESS" if failed_count == 0 else "PARTIAL_FAIL",
                "applied": applied_count,
                "skipped_already_exists": skipped_exists_count,
                "skipped_no_data": skipped_no_data_count,
                "duration_seconds": script_duration,
                "message": f"Applied: {applied_count}, Skipped Exists: {skipped_exists_count}, Skipped No Data: {skipped_no_data_count}, Failed: {failed_count}"
            })

    # 4. Query live object counts from database catalog
    catalog_summary = []
    try:
        sql_counts = (
            "SELECT n.nspname AS schema_name, "
            "COUNT(DISTINCT c.conname) FILTER (WHERE c.contype = 'p') AS pk_count, "
            "COUNT(DISTINCT c.conname) FILTER (WHERE c.contype = 'u') AS unique_count, "
            "COUNT(DISTINCT c.conname) FILTER (WHERE c.contype = 'f') AS fk_count, "
            "COUNT(DISTINCT i.relname) AS index_count "
            "FROM pg_namespace n "
            "LEFT JOIN pg_class cl ON cl.relnamespace = n.oid AND cl.relkind = 'r' "
            "LEFT JOIN pg_constraint c ON c.conrelid = cl.oid "
            "LEFT JOIN pg_index x ON x.indrelid = cl.oid "
            "LEFT JOIN pg_class i ON i.oid = x.indexrelid "
            "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta') "
            "GROUP BY n.nspname ORDER BY 1"
        )
        catalog_summary = query_db_dicts(sql_counts)
    except DB_ERRORS as e:
        logger.warning(f"Could not query catalog constraint counts: {e}")

    # 5. Generate Excel Report
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_07_load_objects_report_{ts}.xlsx"
    total_applied = sum(1 for a in all_object_actions if a["status"] == "APPLIED")
    total_skipped_exists = sum(1 for a in all_object_actions if a["status"] == "SKIPPED_ALREADY_EXISTS")
    total_skipped_no_data = sum(1 for a in all_object_actions if a["status"] == "SKIPPED_NO_DATA")
    total_failed = sum(1 for a in all_object_actions if a["status"] == "FAILED")

    summary_sheet = [
        {"Metric": "Post-Load Run Timestamp", "Value": ts},
        {"Metric": "Total Objects Evaluated", "Value": len(all_object_actions)},
        {"Metric": "Objects Applied (Newly Created)", "Value": total_applied},
        {"Metric": "Objects Skipped (Already Exist in DB)", "Value": total_skipped_exists},
        {"Metric": "Objects Skipped (No Data Loaded Yet)", "Value": total_skipped_no_data},
        {"Metric": "Objects Failed", "Value": total_failed},
        {"Metric": "Execution Stages", "Value": ", ".join(stages_to_run if stages_to_run else [s[0] for s in STAGES])}
    ]

    sheets = {
        "Summary": summary_sheet,
        "Object_Actions": all_object_actions if all_object_actions else [{"Status": "No objects evaluated"}],
        "Script_Summaries": all_script_results if all_script_results else [{"Status": "No scripts run"}],
        "PostgreSQL_Object_Counts": catalog_summary if catalog_summary else [{"Status": "Not queried"}]
    }
    generate_excel_report(report_file, sheets)
    sync_identity_sequences()
    logger.info(f"Excel report generated: {report_file}")
    logger.info(f"Summary: Applied: {total_applied} | Skipped (Already Exist): {total_skipped_exists} | Skipped (No Data): {total_skipped_no_data} | Failed: {total_failed}")
    
    log_summary(logger, Path(__file__).name, [
        ('total objects evaluated', len(all_object_actions)),
        ('objects applied (created)', total_applied),
        ('  - primary keys & unique', sum(1 for a in all_object_actions if a['status'] == 'APPLIED' and a['object_type'] in ('PK', 'UNIQUE'))),
        ('  - check & alter stmts', sum(1 for a in all_object_actions if a['status'] == 'APPLIED' and a['object_type'] in ('CHECK', 'CHECK_VALIDATE', 'ALTER', 'ALTER_COLUMN', 'CONSTRAINT'))),
        ('  - secondary indexes', sum(1 for a in all_object_actions if a['status'] == 'APPLIED' and a['object_type'] in ('INDEX', 'INDEX_ATTACH'))),
        ('  - foreign keys (valid)', sum(1 for a in all_object_actions if a['status'] == 'APPLIED' and a['object_type'] in ('FK', 'FK_VALIDATE'))),
        ('  - views & triggers', sum(1 for a in all_object_actions if a['status'] == 'APPLIED' and a['object_type'] in ('VIEW', 'TRIGGER'))),
        ('objects skipped (exists)', total_skipped_exists),
        ('objects skipped (no data)', total_skipped_no_data),
        ('objects failed', total_failed),
    ])
        
    return total_failed == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="07_loadObjects.py - Staged Post-Load Object Creator")
    parser.add_argument("--stage", choices=["all", "pk", "alter", "index", "fk", "validate", "views", "triggers"], default="all",
                        help="Stage: pk (PK/Unique), alter (Check & Alter constraints), index (Concurrent indexes), fk (Foreign keys), validate (FK validate), views (Views), triggers (Triggers), all (default)")
    parser.add_argument("--only-loaded", action="store_true", help="Only load objects for tables where data has been loaded (default: load for all tables)")
    add_targeting_args(parser)
    add_layer_arg(parser)
    args = parser.parse_args()
    layer = ensure_layer(args, logger, ddl_what="Step 07 (keys, indexes, views, triggers)")

    stages = [args.stage] if args.stage != "all" else None

    ok = apply_post_load(stages_to_run=stages, only_loaded=args.only_loaded, selector=selector_or_exit(args, logger), layer=layer)
    sys.exit(0 if ok else 1)
