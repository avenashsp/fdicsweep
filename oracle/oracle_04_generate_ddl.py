#!/usr/bin/env python3
"""
04_generateDDL.py - Target DDL Generator (Bare Tables & Partitions) + View/Trigger Translation
Enforces migration rules:
  1. Base tables created BARE (NO PK, NO FK, NO AK/Unique, NO indexes, NO triggers).
  2. If partitioned in Oracle, declarative PostgreSQL partitions are generated.
  3. Bare tables and partition definitions saved in outputs/pre_load/<schema>_tables.sql.
  4. Post-load constraints and indexes written into outputs/post_load/.
  5. Parses master Oracle SQL dump and translates VIEW and TRIGGER definitions to PostgreSQL,
     saving them into outputs/post_load/<schema>_views.sql and _triggers.sql.

Generates:
  - outputs/pre_load/<schema>_tables.sql
  - outputs/post_load/<schema>_unique_and_pk.sql
  - outputs/post_load/<schema>_indexes.sql
  - outputs/post_load/<schema>_fk_not_valid.sql
  - outputs/post_load/<schema>_fk_validate.sql
  - outputs/post_load/<schema>_views.sql
  - outputs/post_load/<schema>_triggers.sql
  - reports/04_generateDDL_report_<timestamp>.xlsx
"""
import re
import sys
import csv
import argparse
from pathlib import Path
from itertools import groupby
from collections import defaultdict
from sweep_common import (
    ORACLE_INPUTS_DIR, ORACLE_DUMP_DIR, ORACLE_PRE_LOAD_DIR, ORACLE_POST_LOAD_DIR, ORACLE_REPORTS_DIR,
    META_TABLES_CSV, META_CONSTRAINTS_CSV, META_INDEXES_CSV,
    META_PARTITIONS_CSV, META_SEQUENCES_CSV,
    get_logger, get_timestamp, get_db_name, generate_excel_report,
    norm_col, q, oracle_to_pg, bound_pg, split_bounds, child_name
)

logger = get_logger("oracle_04_generate_ddl", "oracle_04_generate_ddl")


def load_partitions():
    """Reads oracle_metadata_partitions.csv into grouped dict: {(owner, table): [partition_dicts]}."""
    p_file = META_PARTITIONS_CSV
    parts = defaultdict(list)
    if p_file.exists():
        with open(p_file, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                o = (r.get("owner") or "").upper().strip()
                t = (r.get("table_name") or "").upper().strip()
                if o and t:
                    parts[(o, t)].append(r)
    return parts


def load_constraints():
    """Reads oracle_metadata_constraints.csv into grouped dict: {(owner, table): [constraint_dicts]}."""
    c_file = META_CONSTRAINTS_CSV
    cons = defaultdict(list)
    if c_file.exists():
        with open(c_file, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                o = (r.get("owner") or "").upper().strip()
                t = (r.get("table_name") or "").upper().strip()
                if o and t:
                    cons[(o, t)].append(r)
    return cons


def load_indexes():
    """Reads oracle_metadata_indexes.csv into grouped dict: {(owner, table): [index_dicts]}."""
    i_file = META_INDEXES_CSV
    idxs = defaultdict(list)
    if i_file.exists():
        with open(i_file, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                o = (r.get("table_owner") or r.get("owner") or "").upper().strip()
                t = (r.get("table_name") or "").upper().strip()
                if o and t:
                    idxs[(o, t)].append(r)
    return idxs


def load_sequences():
    """Reads oracle_metadata_sequences.csv into grouped dict: {owner: [sequence_dicts]}."""
    s_file = META_SEQUENCES_CSV
    seqs = defaultdict(list)
    if s_file.exists():
        with open(s_file, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                o = (r.get("owner") or "").upper().strip()
                if o:
                    seqs[o].append(r)
    return seqs


def generate_partition_ddl(schema, table, part_rows):
    """
    Generates (PARTITION BY clause, list of child partition CREATE statements, child names).
    """
    if not part_rows:
        return "", [], []

    first = part_rows[0]
    method = (first.get("method") or "RANGE").upper().strip()
    raw_key_cols = first.get("key_columns") or ""
    key_cols = [norm_col(c) for c in raw_key_cols.split(";") if c.strip()]
    if not key_cols:
        key_cols = [norm_col(raw_key_cols)] if raw_key_cols else ["processdate"]

    clause = f"PARTITION BY {method} ({', '.join(q(k) for k in key_cols)})"
    parent_q = f"{q(schema)}.{q(table)}"

    child_stmts = []
    child_names = []
    prev_bound = None
    has_max = False

    for r in part_rows:
        pname = r.get("partition_name", "").strip()
        bound_raw = r.get("bound", "").strip()
        cname = child_name(table, pname)
        child_names.append(cname)
        child_q = f"{q(schema)}.{q(cname)}"

        if method == "RANGE":
            hi_bounds = split_bounds(bound_raw)
            hi_pg = [bound_pg(x) for x in hi_bounds]
            if all(x.upper() == "MAXVALUE" for x in hi_pg):
                has_max = True
            lo_pg = prev_bound if prev_bound else ["MINVALUE"] * len(hi_pg)

            stmt = (f"CREATE TABLE IF NOT EXISTS {child_q} PARTITION OF {parent_q} "
                    f"FOR VALUES FROM ({', '.join(lo_pg)}) TO ({', '.join(hi_pg)});")
            child_stmts.append(stmt)
            prev_bound = hi_pg
        elif method == "LIST":
            if bound_raw.upper() == "DEFAULT":
                stmt = f"CREATE TABLE IF NOT EXISTS {child_q} PARTITION OF {parent_q} DEFAULT;"
            else:
                vals = [bound_pg(v) for v in split_bounds(bound_raw)]
                stmt = f"CREATE TABLE IF NOT EXISTS {child_q} PARTITION OF {parent_q} FOR VALUES IN ({', '.join(vals)});"
            child_stmts.append(stmt)
        elif method == "HASH":
            n = len(part_rows)
            pos = int(r.get("partition_position") or 1) - 1
            stmt = f"CREATE TABLE IF NOT EXISTS {child_q} PARTITION OF {parent_q} FOR VALUES WITH (MODULUS {n}, REMAINDER {pos});"
            child_stmts.append(stmt)

    # Add DEFAULT partition for RANGE if MAXVALUE wasn't explicitly present
    if method == "RANGE" and not has_max:
        def_cname = child_name(table, "pdefault")
        child_names.append(def_cname)
        def_q = f"{q(schema)}.{q(def_cname)}"
        child_stmts.append(f"CREATE TABLE IF NOT EXISTS {def_q} PARTITION OF {parent_q} DEFAULT;")

    return clause, child_stmts, child_names


def extract_views_and_triggers_from_dump(dump_file=None):
    """
    Scans the master Oracle SQL dump file in inputs/oracle_dump/ (or inputs/) to extract full VIEW and TRIGGER DDL blocks.
    Returns: (dict_of_views_by_owner, dict_of_triggers_by_owner)
    """
    if dump_file and Path(dump_file).exists():
        target_sql = Path(dump_file)
    else:
        candidates = []
        if ORACLE_DUMP_DIR.exists():
            candidates = [f for f in (list(ORACLE_DUMP_DIR.glob("*.sql")) +
                                     list(ORACLE_DUMP_DIR.glob("*.ddl")) +
                                     list(ORACLE_DUMP_DIR.glob("*.txt")))
                          if f.is_file() and f.name.lower() != "sweep_meta.sql"]
        if not candidates and ORACLE_INPUTS_DIR.exists():
            candidates = [f for f in (list(ORACLE_INPUTS_DIR.glob("*.sql")) +
                                     list(ORACLE_INPUTS_DIR.glob("*.ddl")) +
                                     list(ORACLE_INPUTS_DIR.glob("*.txt")))
                          if f.is_file() and f.name.lower() != "sweep_meta.sql"]
        if not candidates:
            return {}, {}
        target_sql = candidates[0]

    views_by_owner = defaultdict(list)
    triggers_by_owner = defaultdict(list)

    curr_stmt = []
    in_plsql = False
    in_view = False
    curr_owner = "PUBLIC"

    connect_re = re.compile(r'^\s*--\s*CONNECT\s+"?([\w$#]+)"?\s*$', re.I)
    view_re = re.compile(r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NO)?FORCE\s+)?(?:(?:NON)?EDITIONABLE\s+)?VIEW\s+(?:\"?([\w$#]+)\"?\s*\.\s*)?\"?([\w$#]+)\"?', re.I)
    trg_re = re.compile(r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NON)?EDITIONABLE\s+)?TRIGGER\s+(?:\"?([\w$#]+)\"?\s*\.\s*)?\"?([\w$#]+)\"?', re.I)

    logger.info(f"Scanning {target_sql.name} for view and trigger DDL blocks...")
    try:
        with open(target_sql, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                s_strip = line.strip()
                if not s_strip:
                    continue

                m_conn = connect_re.match(s_strip)
                if m_conn:
                    curr_owner = m_conn.group(1).upper()
                    continue

                if not curr_stmt:
                    # Check start of view
                    m_v = view_re.match(s_strip)
                    if m_v:
                        in_view = True
                        owner = (m_v.group(1) or curr_owner).upper()
                        name = m_v.group(2).upper()
                        curr_stmt = [line]
                        if s_strip.endswith(";"):
                            raw_ddl = "".join(curr_stmt)
                            views_by_owner[owner].append({"name": name, "ddl": raw_ddl})
                            curr_stmt = []
                            in_view = False
                        continue

                    # Check start of trigger
                    m_t = trg_re.match(s_strip)
                    if m_t:
                        in_plsql = True
                        owner = (m_t.group(1) or curr_owner).upper()
                        name = m_t.group(2).upper()
                        curr_stmt = [line]
                        continue
                else:
                    if in_view:
                        curr_stmt.append(line)
                        if s_strip.endswith(";") or s_strip == "/":
                            raw_ddl = "".join(curr_stmt)
                            views_by_owner[owner].append({"name": name, "ddl": raw_ddl})
                            curr_stmt = []
                            in_view = False
                    elif in_plsql:
                        if s_strip == "/":
                            raw_ddl = "".join(curr_stmt)
                            triggers_by_owner[owner].append({"name": name, "ddl": raw_ddl})
                            curr_stmt = []
                            in_plsql = False
                            continue

                        # When next statement starts without an explicit '/'
                        if re.match(r'^\s*(?:CREATE\s+|--\s*CONNECT\b|ALTER\s+TRIGGER\b)', s_strip, re.I):
                            raw_ddl = "".join(curr_stmt)
                            triggers_by_owner[owner].append({"name": name, "ddl": raw_ddl})
                            curr_stmt = []
                            in_plsql = False

                            m_conn = connect_re.match(s_strip)
                            if m_conn:
                                curr_owner = m_conn.group(1).upper()
                                continue

                            m_v = view_re.match(s_strip)
                            if m_v:
                                in_view = True
                                owner = (m_v.group(1) or curr_owner).upper()
                                name = m_v.group(2).upper()
                                curr_stmt = [line]
                                if s_strip.endswith(";"):
                                    raw_ddl = "".join(curr_stmt)
                                    views_by_owner[owner].append({"name": name, "ddl": raw_ddl})
                                    curr_stmt = []
                                    in_view = False
                                continue

                            m_t = trg_re.match(s_strip)
                            if m_t:
                                in_plsql = True
                                owner = (m_t.group(1) or curr_owner).upper()
                                name = m_t.group(2).upper()
                                curr_stmt = [line]
                                continue
                            continue

                        curr_stmt.append(line)

            if in_plsql and curr_stmt:
                raw_ddl = "".join(curr_stmt)
                triggers_by_owner[owner].append({"name": name, "ddl": raw_ddl})
            elif in_view and curr_stmt:
                raw_ddl = "".join(curr_stmt)
                views_by_owner[owner].append({"name": name, "ddl": raw_ddl})
    except Exception as e:
        logger.warning(f"Could not extract views/triggers from dump: {e}")

    logger.info(f"Extracted {sum(len(v) for v in views_by_owner.values())} view(s) and {sum(len(t) for t in triggers_by_owner.values())} trigger(s) from dump.")
    return views_by_owner, triggers_by_owner


def simplify_case_to_coalesce(sql):
    """
    Simplifies CASE WHEN e1 IS NULL THEN e2 ELSE e3 END to COALESCE(e1, e2)
    whenever e1 == e3 (case-insensitive).
    Also simplifies CASE WHEN e1 IS NOT NULL THEN e3 ELSE e2 END.
    """
    if not sql:
        return ""

    pat_null = re.compile(
        r'CASE\s+WHEN\s+((?:(?!CASE|WHEN|THEN|ELSE|END).)+?)\s+IS\s+NULL\s+THEN\s+((?:(?!CASE|WHEN|THEN|ELSE|END).)+?)\s+ELSE\s+((?:(?!CASE|WHEN|THEN|ELSE|END).)+?)\s+END',
        re.I | re.S
    )
    def repl_null(m):
        e1, e2, e3 = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        if e1.lower() == e3.lower():
            return f"COALESCE({e1}, {e2})"
        return m.group(0)

    sql = pat_null.sub(repl_null, sql)

    pat_not_null = re.compile(
        r'CASE\s+WHEN\s+((?:(?!CASE|WHEN|THEN|ELSE|END).)+?)\s+IS\s+NOT\s+NULL\s+THEN\s+((?:(?!CASE|WHEN|THEN|ELSE|END).)+?)\s+ELSE\s+((?:(?!CASE|WHEN|THEN|ELSE|END).)+?)\s+END',
        re.I | re.S
    )
    def repl_not_null(m):
        e1, e3, e2 = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        if e1.lower() == e3.lower():
            return f"COALESCE({e1}, {e2})"
        return m.group(0)

    sql = pat_not_null.sub(repl_not_null, sql)
    return sql


def translate_view_ddl(owner, view_name, raw_ddl, known_tables=None):
    """Translates Oracle VIEW DDL statement to PostgreSQL syntax."""
    v_clean = norm_col(view_name)
    s_clean = owner.lower()
    base_schema = s_clean[:-5] if s_clean.endswith("_code") else s_clean

    # Clean Oracle-specific keywords
    ddl = raw_ddl.strip().rstrip("/").rstrip(";").strip()
    ddl = re.sub(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NO)?FORCE\s+)?(?:(?:NON)?EDITIONABLE\s+)?VIEW\s+',
                 'CREATE OR REPLACE VIEW ', ddl, flags=re.I)

    # Standardize header: CREATE OR REPLACE VIEW schema.view_name
    ddl = re.sub(
        r'CREATE\s+OR\s+REPLACE\s+VIEW\s+(?:\"?[\w$#]+\"?\s*\.\s*)?\"?[\w$#]+\"?',
        f'CREATE OR REPLACE VIEW {q(s_clean)}.{q(v_clean)}',
        ddl, count=1, flags=re.I
    )

    # Function and syntax translation
    ddl = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', ddl, flags=re.I)
    ddl = re.sub(r'\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP', ddl, flags=re.I)
    ddl = re.sub(r'\bNVL\s*\(', 'COALESCE(', ddl, flags=re.I)
    ddl = re.sub(r'\bVARCHAR2\b', 'VARCHAR', ddl, flags=re.I)
    ddl = re.sub(r'\bNUMBER\b', 'NUMERIC', ddl, flags=re.I)
    ddl = re.sub(r'\bINSTR\s*\(', 'STRPOS(', ddl, flags=re.I)
    ddl = re.sub(r'\bSUBSTR\s*\(', 'SUBSTRING(', ddl, flags=re.I)
    ddl = re.sub(r'\bTRUNC\s*\(\s*CURRENT_TIMESTAMP\s*\)', "DATE_TRUNC('day', CURRENT_TIMESTAMP)", ddl, flags=re.I)
    ddl = re.sub(r'\bTRUNC\s*\(\s*SYSDATE\s*\)', "DATE_TRUNC('day', CURRENT_TIMESTAMP)", ddl, flags=re.I)
    ddl = re.sub(r'\bFROM\s+DUAL\b', 'FROM public.dual', ddl, flags=re.I)
    ddl = re.sub(r'(?:"ROWNUM"|\bROWNUM\b)', 'ROW_NUMBER() OVER ()', ddl, flags=re.I)

    # Translate CASE WHEN ... IS NULL THEN ... ELSE ... END to COALESCE(...)
    ddl = simplify_case_to_coalesce(ddl)

    # Translate ADD_MONTHS(date_expr, n) -> (date_expr + (n * INTERVAL '1 month'))
    pat_add_months = re.compile(r'\bADD_MONTHS\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', re.I)
    def repl_add_months(m):
        e1, e2 = m.group(1).strip(), m.group(2).strip()
        return f"({e1} + ({e2} * INTERVAL '1 month'))"
    ddl = pat_add_months.sub(repl_add_months, ddl)

    # Translate LAST_DAY(date_expr) -> (DATE_TRUNC('month', date_expr) + INTERVAL '1 month - 1 day')::date
    pat_last_day = re.compile(r'\bLAST_DAY\s*\(\s*([^)]+?)\s*\)', re.I)
    def repl_last_day(m):
        e1 = m.group(1).strip()
        return f"(DATE_TRUNC('month', {e1}) + INTERVAL '1 month - 1 day')::date"
    ddl = pat_last_day.sub(repl_last_day, ddl)

    # Translate CURRENT_TIMESTAMP / CURRENT_DATE / date column +/- integer to interval arithmetic
    pat_ts_math = re.compile(
        r'(\b(?:CURRENT_TIMESTAMP|CURRENT_DATE|SYSDATE|SYSTIMESTAMP|\w+\.(?:dateid|processdate|date|lastupdate))\b)\s*([+-])\s*(\d+)\b',
        re.I
    )
    ddl = pat_ts_math.sub(r"\1 \2 INTERVAL '\3 days'", ddl)

    # Translate Oracle date addition in conditions: (col.dateid + 1) -> (col.dateid + '1 day'::interval)
    ddl = re.sub(r'(\b\w+\.(?:dateid|processdate|date)\s*\+\s*)(\d+)\b', r"\1'\2 day'::interval", ddl, flags=re.I)

    # Qualify unqualified base tables in FROM / JOIN clauses if known_tables provided
    if known_tables and base_schema != s_clean:
        for tbl in sorted(known_tables, key=len, reverse=True):
            pat_from = re.compile(rf'(\bFROM\s+)(?!\"?\w+\"?\.)\"?({re.escape(tbl)})\"?\b', re.I)
            ddl = pat_from.sub(rf'\1{q(base_schema)}.\2', ddl)
            pat_join = re.compile(rf'(\bJOIN\s+)(?!\"?\w+\"?\.)\"?({re.escape(tbl)})\"?\b', re.I)
            ddl = pat_join.sub(rf'\1{q(base_schema)}.\2', ddl)
            pat_comma = re.compile(rf'(,\s*)(?!\"?\w+\"?\.)\"?({re.escape(tbl)})\"?\b', re.I)
            ddl = pat_comma.sub(rf'\1{q(base_schema)}.\2', ddl)

    # Lowercase quoted schema references: "OWNER"."TABLE" -> "owner"."table"
    ddl = re.sub(r'"([A-Za-z0-9_$#]+)"', lambda m: f'"{m.group(1).lower()}"', ddl)

    # Search path resolution: current view schema, sibling code/data schema, public
    search_paths = [q(s_clean)]
    if s_clean.endswith("_code"):
        search_paths.append(q(s_clean[:-5]))
    else:
        search_paths.append(q(f"{s_clean}_code"))
    search_paths.append("public")
    search_path_cmd = f"SET search_path TO {', '.join(search_paths)};"

    return f"-- View: {s_clean}.{v_clean} (Translated from Oracle)\n{search_path_cmd}\n{ddl.strip()};\n\n"


def translate_oracle_expr(expr):
    """Translates Oracle SQL functions, keywords, and identifiers to PostgreSQL equivalents."""
    if not expr:
        return ""
    e = expr.strip()
    e = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', e, flags=re.I)
    e = re.sub(r'\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP', e, flags=re.I)
    e = re.sub(r'\bNVL\s*\(', 'COALESCE(', e, flags=re.I)
    e = re.sub(r'\bVARCHAR2\b', 'VARCHAR', e, flags=re.I)
    e = re.sub(r'\bNUMBER\b', 'NUMERIC', e, flags=re.I)
    e = re.sub(r'\bINSTR\s*\(', 'STRPOS(', e, flags=re.I)
    e = re.sub(r'\bSUBSTR\s*\(', 'SUBSTRING(', e, flags=re.I)
    e = re.sub(r'\bFROM\s+DUAL\b', '', e, flags=re.I)
    e = re.sub(r'(?:"ROWNUM"|\bROWNUM\b)', 'ROW_NUMBER() OVER ()', e, flags=re.I)
    e = re.sub(r'"([A-Za-z0-9_$#]+)"', lambda m: f'"{m.group(1).lower()}"', e)
    return e


def translate_trigger_ddl(owner, trg_name, raw_ddl):
    """Translates an Oracle TRIGGER DDL statement into a PostgreSQL function + trigger."""
    t_clean = norm_col(trg_name)
    s_clean = owner.lower()

    ddl = raw_ddl.strip().rstrip("/").strip()

    # Match timing & event: e.g. BEFORE INSERT OR UPDATE ON "BSS01001"."ACCOUNT"
    # Allow optional OF <columns> clause (e.g. BEFORE INSERT OR UPDATE OF LASTUPDATE ON "BCS"."PLAN")
    hdr_m = re.search(
        r'\b(BEFORE|AFTER|INSTEAD\s+OF)\s+'
        r'((?:(?:INSERT|UPDATE|DELETE)(?:\s+OF\s+[\w$#",\s]+?)?)(?:\s+OR\s+(?:INSERT|UPDATE|DELETE)(?:\s+OF\s+[\w$#",\s]+?)?)*)\s+'
        r'ON\s+(?:\"?([\w$#]+)\"?\s*\.\s*)?\"?([\w$#]+)\"?',
        ddl, re.I
    )
    if not hdr_m:
        return f"-- Could not parse trigger header for {owner}.{trg_name}\n/*\n{ddl}\n*/\n\n"

    timing = hdr_m.group(1).upper()
    events_raw = re.sub(r'\s+', ' ', hdr_m.group(2).strip())
    # Format OF <columns> clause if present, normalizing each column name
    def repl_of(m_of):
        cols = [c.strip().strip('"') for c in m_of.group(1).split(',') if c.strip()]
        return 'OF ' + ', '.join(q(norm_col(c)) for c in cols)
    events = re.sub(r'\bOF\s+([A-Za-z0-9_$#",\s]+?)(?=\s+\bOR\b|$)', repl_of, events_raw, flags=re.I)

    raw_tbl_owner = hdr_m.group(3)
    if raw_tbl_owner:
        tbl_owner = raw_tbl_owner.lower()
    else:
        tbl_owner = s_clean[:-5] if s_clean.endswith("_code") else s_clean
    tbl_name = norm_col(hdr_m.group(4))

    # CHANGE 2: Create trigger function in the same schema as the table/trigger
    fn_schema = tbl_owner

    # Extract body: DECLARE ... BEGIN ... END or BEGIN ... END
    body_m = re.search(r'\b((?:DECLARE\b|BEGIN\b).*)\s*$', ddl, re.I | re.S)
    body = body_m.group(1).strip() if body_m else "BEGIN\n    RETURN NEW;\nEND;"

    # Translate body
    body = re.sub(r'\bVARCHAR2\b', 'VARCHAR', body, flags=re.I)
    body = re.sub(r'\bNUMBER\b', 'NUMERIC', body, flags=re.I)
    body = re.sub(r':NEW\.', 'NEW.', body, flags=re.I)
    body = re.sub(r':OLD\.', 'OLD.', body, flags=re.I)
    body = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', body, flags=re.I)
    body = re.sub(r'\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP', body, flags=re.I)
    body = re.sub(r'\bNVL\s*\(', 'COALESCE(', body, flags=re.I)
    body = re.sub(r'\bFROM\s+DUAL\b', 'FROM public.dual', body, flags=re.I)
    body = re.sub(r'\bINSERTING\b', "TG_OP = 'INSERT'", body, flags=re.I)
    body = re.sub(r'\bUPDATING\b', "TG_OP = 'UPDATE'", body, flags=re.I)
    body = re.sub(r'\bDELETING\b', "TG_OP = 'DELETE'", body, flags=re.I)
    body = re.sub(r'\bSUBSTR\s*\(\s*([^,]+?)\s*,\s*-\s*(\d+)\s*\)', r'RIGHT(\1, \2)', body, flags=re.I)

    # Ensure RETURN statement before final END if missing
    if not re.search(r'\bRETURN\b', body, re.I):
        if timing == "AFTER":
            ret_stmt = "    RETURN NULL;"
        elif "DELETE" in events and ("INSERT" in events or "UPDATE" in events):
            ret_stmt = "    IF TG_OP = 'DELETE' THEN\n        RETURN OLD;\n    ELSE\n        RETURN NEW;\n    END IF;"
        elif "DELETE" in events:
            ret_stmt = "    RETURN OLD;"
        else:
            ret_stmt = "    RETURN NEW;"
        body = re.sub(r'(\bEND(?:\s+[\w$#]+)?\s*;?\s*)$', f'{ret_stmt}\n\\1', body, flags=re.I)

    fn_name = f"{t_clean}_fn"
    pg_ddl = (
        f"-- Trigger Function: {fn_schema}.{fn_name}()\n"
        f"CREATE SCHEMA IF NOT EXISTS {q(fn_schema)};\n"
        f"CREATE OR REPLACE FUNCTION {q(fn_schema)}.{q(fn_name)}()\n"
        f"RETURNS TRIGGER AS $$\n"
        f"{body}\n"
        f"$$ LANGUAGE plpgsql;\n\n"
        f"-- Trigger: {t_clean} on {tbl_owner}.{tbl_name}\n"
        f"DROP TRIGGER IF EXISTS {q(t_clean)} ON {q(tbl_owner)}.{q(tbl_name)};\n"
        f"CREATE TRIGGER {q(t_clean)}\n"
        f"    {timing} {events} ON {q(tbl_owner)}.{q(tbl_name)}\n"
        f"    FOR EACH ROW\n"
        f"    EXECUTE FUNCTION {q(fn_schema)}.{q(fn_name)}();\n\n"
    )
    return pg_ddl


def generate_ddl(owners_filter=None, tables_filter=None):
    ts = get_timestamp()
    dd_path = META_TABLES_CSV
    if not dd_path.exists():
        logger.error(f"Oracle metadata tables not found at {dd_path}. Run 01_extractMetadata.py first.")
        return False

    partitions = load_partitions()
    constraints = load_constraints()
    indexes = load_indexes()
    sequences = load_sequences()

    # Extract Views and Triggers from Master Dump
    views_by_owner, triggers_by_owner = extract_views_and_triggers_from_dump()

    # Read column definitions grouped by table
    with open(dd_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    # Map tables by schema for cross-schema view qualification
    tables_by_schema = defaultdict(set)
    for r in reader:
        o = (r.get("owner") or "").lower().strip()
        t = norm_col(r.get("table_name", ""))
        if o and t:
            tables_by_schema[o].add(t)

    # Identify parent table columns defined as GENERATED ... AS IDENTITY with BIGINT datatype
    identity_bigint_cols = set()
    for r in reader:
        if r.get("is_identity") == "Y" or r.get("generated") == "IDENTITY":
            identity_bigint_cols.add((r["owner"].upper().strip(), r["table_name"].upper().strip(), norm_col(r["column_name"])))

    # Cascade BIGINT to all child columns referencing identity columns via foreign keys
    fk_bigint_cols = set()
    changed = True
    while changed:
        changed = False
        for (c_owner, c_table), cons_list in constraints.items():
            for cn in cons_list:
                if cn.get("ctype", "").upper() == "R":
                    p_owner = (cn.get("ref_owner") or c_owner).upper().strip()
                    p_table = (cn.get("ref_table") or "").upper().strip()
                    c_cols = [norm_col(c) for c in (cn.get("columns") or "").split(";") if c.strip()]
                    p_cols = [norm_col(c) for c in (cn.get("ref_columns") or "").split(";") if c.strip()]
                    for cc, pc in zip(c_cols, p_cols):
                        if (p_owner, p_table, pc) in identity_bigint_cols or (p_owner, p_table, pc) in fk_bigint_cols:
                            if (c_owner, c_table, cc) not in fk_bigint_cols:
                                fk_bigint_cols.add((c_owner, c_table, cc))
                                changed = True
    if fk_bigint_cols:
        logger.info(f"Identified {len(fk_bigint_cols)} child column(s) referencing parent identity columns to align as BIGINT.")

    grouped_tables = groupby(reader, key=lambda r: (r["owner"].upper().strip(), r["table_name"].upper().strip()))

    file_handles = {}

    def get_handles(schema_name, owner_name):
        if schema_name not in file_handles:
            # Table DDL placed into ORACLE_PRE_LOAD_DIR (outputs/pre_load)
            s_tables = open(ORACLE_PRE_LOAD_DIR / f"{schema_name}_tables.sql", "w", encoding="utf-8")

            # Post-load scripts placed into ORACLE_POST_LOAD_DIR (outputs/post_load)
            s_pk = open(ORACLE_POST_LOAD_DIR / f"{schema_name}_unique_and_pk.sql", "w", encoding="utf-8")
            s_alter = open(ORACLE_POST_LOAD_DIR / f"{schema_name}_alter_statements.sql", "w", encoding="utf-8")
            s_idx = open(ORACLE_POST_LOAD_DIR / f"{schema_name}_indexes.sql", "w", encoding="utf-8")
            s_fk = open(ORACLE_POST_LOAD_DIR / f"{schema_name}_fk_not_valid.sql", "w", encoding="utf-8")
            s_val = open(ORACLE_POST_LOAD_DIR / f"{schema_name}_fk_validate.sql", "w", encoding="utf-8")
            s_views = open(ORACLE_POST_LOAD_DIR / f"{schema_name}_views.sql", "w", encoding="utf-8")
            s_trg = open(ORACLE_POST_LOAD_DIR / f"{schema_name}_triggers.sql", "w", encoding="utf-8")

            # Header configurations
            s_tables.write(f"-- Target DDL for Schema: {schema_name}\n")
            s_tables.write(f"CREATE SCHEMA IF NOT EXISTS {q(schema_name)};\n\n")

            # Sequences for this schema
            schema_seqs = sequences.get(owner_name, sequences.get(schema_name.upper(), sequences.get(schema_name.lower(), [])))
            if schema_seqs:
                s_tables.write(f"-- Sequences ({len(schema_seqs)}) for {schema_name}\n")
                for sq in schema_seqs:
                    s_name = norm_col(sq["sequence_name"])
                    inc = sq.get("increment_by") or 1
                    min_v = sq.get("min_value") or 1
                    start_v = sq.get("start_with") or 1
                    cache_v = sq.get("cache_size") or 20
                    cycle_v = "CYCLE" if sq.get("cycle") == "Y" else "NO CYCLE"
                    s_tables.write(f"CREATE SEQUENCE IF NOT EXISTS {q(schema_name)}.{q(s_name)} "
                                   f"INCREMENT BY {inc} MINVALUE {min_v} START WITH {start_v} CACHE {cache_v} {cycle_v};\n")
                s_tables.write("\n")

            for h in (s_pk, s_alter, s_idx, s_fk, s_val, s_views, s_trg):
                h.write("\\set ON_ERROR_STOP off\n\n")
                h.write("SET statement_timeout = 0;\n")

            # In s_alter, ensure audit function exists
            s_alter.write("""CREATE SCHEMA IF NOT EXISTS sweep_ctl;
CREATE OR REPLACE FUNCTION sweep_ctl.fn_update_audit_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modifieddate = CURRENT_TIMESTAMP;
    NEW.modifiedby = CURRENT_USER;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

""")

            s_idx.write("SET maintenance_work_mem = '2GB';\nSET max_parallel_maintenance_workers = 4;\n")
            s_views.write("""-- Oracle Compatibility Functions for Views
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

""")
            file_handles[schema_name] = (s_tables, s_pk, s_alter, s_idx, s_fk, s_val, s_views, s_trg)
        return file_handles[schema_name]

    generated_tables_summary = []
    generated_partitions_summary = []
    synth_cons_counts = defaultdict(int)

    for (owner, table), cols_iter in grouped_tables:
        if owners_filter and owner not in owners_filter:
            continue
        if tables_filter and f"{owner}.{table}" not in tables_filter and table not in tables_filter:
            continue

        schema = owner.lower()
        t_clean = norm_col(table)
        cols = list(cols_iter)

        h_tables, h_pk, h_alter, h_idx, h_fk, h_val, h_views, h_trg = get_handles(schema, owner)

        # Check existing columns
        existing_cols = {norm_col(c_col["column_name"]) for c_col in cols}

        # 1. Base Columns DDL (with Identity & Foreign Key Identity Alignment support)
        col_lines = []
        for c_col in cols:
            cname = norm_col(c_col["column_name"])
            pg_type = oracle_to_pg(c_col["data_type"], c_col.get("data_length"), c_col.get("data_precision"), c_col.get("data_scale"))
            is_id = c_col.get("is_identity") == "Y" or c_col.get("generated") == "IDENTITY"
            id_opts = c_col.get("identity_options", "")
            if is_id:
                # Force BIGINT NOT NULL for auto-increment identity columns
                pg_type = "BIGINT NOT NULL"
                if id_opts:
                    col_lines.append(f"    {q(cname)} {pg_type} GENERATED BY DEFAULT AS IDENTITY {id_opts}")
                else:
                    col_lines.append(f"    {q(cname)} {pg_type} GENERATED BY DEFAULT AS IDENTITY")
            elif (owner, table, cname) in fk_bigint_cols:
                # Align child FK column datatype to BIGINT matching parent identity column
                nullable = (c_col.get("nullable") or "Y").upper().strip()
                col_type_str = "BIGINT NOT NULL" if nullable == "N" else "BIGINT"
                col_lines.append(f"    {q(cname)} {col_type_str}")
                logger.info(f"Aligned child FK column {owner}.{table}.{cname} to {col_type_str} (matches parent identity)")
            else:
                col_lines.append(f"    {q(cname)} {pg_type}")

        # Inject the 4 required audit columns if not already present
        audit_cols_to_add = [
            ("createddate_m", "TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP"),
            ("createdby_m", "VARCHAR(50) DEFAULT CURRENT_USER"),
            ("modifieddate", "TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP"),
            ("modifiedby", "VARCHAR(50) DEFAULT CURRENT_USER")
        ]
        for a_col, a_def in audit_cols_to_add:
            if a_col not in existing_cols:
                col_lines.append(f"    {q(a_col)} {a_def}")

        # Check if table is partitioned
        part_info = partitions.get((owner, table), [])
        is_partitioned = len(part_info) > 0
        part_clause, child_stmts, child_names = generate_partition_ddl(schema, t_clean, part_info)

        # Write CREATE TABLE
        h_tables.write(f"-- Table: {schema}.{t_clean} (Source: {owner}.{table})\n")
        h_tables.write(f"CREATE TABLE IF NOT EXISTS {q(schema)}.{q(t_clean)} (\n")
        h_tables.write(",\n".join(col_lines))
        h_tables.write("\n)")
        if is_partitioned and part_clause:
            h_tables.write(f" {part_clause}")
        h_tables.write(";\n\n")

        # Write Child Partitions
        if is_partitioned and child_stmts:
            h_tables.write(f"-- Child Partitions ({len(child_stmts)}) for {schema}.{t_clean}\n")
            for c_stmt in child_stmts:
                h_tables.write(c_stmt + "\n")
            h_tables.write("\n")

            generated_partitions_summary.append({
                "schema": schema,
                "parent_table": t_clean,
                "partition_count": len(child_stmts),
                "method": part_info[0].get("method", "RANGE"),
                "sample_child": child_names[0] if child_names else ""
            })

        # 2. Post-Load Scripts: Primary Keys & Unique Constraints
        table_cons = constraints.get((owner, table), [])
        for cn in table_cons:
            ctype = cn.get("ctype", "").upper()
            c_name = norm_col(cn.get("constraint_name", ""))
            raw_cols = cn.get("columns", "")
            target_cols = ", ".join(q(norm_col(x)) for x in raw_cols.split(";") if x.strip())

            # Fallback generated synthetic name whenever c_name is empty
            if not c_name:
                synth_cons_counts[(schema, t_clean, ctype)] += 1
                idx = synth_cons_counts[(schema, t_clean, ctype)]
                pfx = {"P": "pk", "U": "uk", "R": "fk", "C": "ck"}.get(ctype, "cn")
                col_part = "_".join(norm_col(x) for x in raw_cols.split(";") if x.strip())[:25]
                if not col_part:
                    col_part = f"col_{idx}"
                base = f"{pfx}_{t_clean}_{col_part}".strip("_")[:50]
                c_name = norm_col(f"{base}_{idx}")

            if ctype == "P" and target_cols:
                h_pk.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} ADD CONSTRAINT {q(c_name)} PRIMARY KEY ({target_cols});\n")
            elif ctype == "U" and target_cols:
                h_pk.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} ADD CONSTRAINT {q(c_name)} UNIQUE ({target_cols});\n")
            elif ctype == "R" and target_cols:
                ref_s = (cn.get("ref_owner") or owner).lower()
                ref_t = norm_col(cn.get("ref_table", ""))
                ref_cols = ", ".join(q(norm_col(x)) for x in (cn.get("ref_columns") or "").split(";") if x.strip())
                if ref_t and ref_cols:
                    if is_partitioned and child_names:
                        # PostgreSQL does NOT support NOT VALID on partitioned parent tables.
                        # Recommended low-lock pattern for large partitioned tables:
                        # 1. Add FK to each child partition with NOT VALID (instant, no long locks)
                        # 2. Validate each child partition individually (only locks one partition at a time)
                        # 3. Add FK to parent partitioned table without NOT VALID (instant scan, matches validated children)
                        for cname in child_names:
                            child_fk_name = norm_col(f"{cname}_{c_name}")[:63]
                            h_fk.write(f"ALTER TABLE {q(schema)}.{q(cname)} ADD CONSTRAINT {q(child_fk_name)} "
                                       f"FOREIGN KEY ({target_cols}) REFERENCES {q(ref_s)}.{q(ref_t)} ({ref_cols}) NOT VALID;\n")
                        for cname in child_names:
                            child_fk_name = norm_col(f"{cname}_{c_name}")[:63]
                            h_val.write(f"ALTER TABLE {q(schema)}.{q(cname)} VALIDATE CONSTRAINT {q(child_fk_name)};\n")
                        h_val.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} ADD CONSTRAINT {q(c_name)} "
                                    f"FOREIGN KEY ({target_cols}) REFERENCES {q(ref_s)}.{q(ref_t)} ({ref_cols});\n")
                    else:
                        h_fk.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} ADD CONSTRAINT {q(c_name)} "
                                   f"FOREIGN KEY ({target_cols}) REFERENCES {q(ref_s)}.{q(ref_t)} ({ref_cols}) NOT VALID;\n")
                        h_val.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} VALIDATE CONSTRAINT {q(c_name)};\n")
            elif ctype == "C":
                ck_cond = translate_oracle_expr(cn.get("check_condition") or "")
                if ck_cond:
                    # CHECK constraints added with NOT VALID; validation is skipped per policy
                    h_alter.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} ADD CONSTRAINT {q(c_name)} CHECK ({ck_cond}) NOT VALID;\n")
            elif ctype == "NOT_NULL":
                raw_c = norm_col(cn.get("columns", ""))
                if raw_c:
                    h_alter.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} ALTER COLUMN {q(raw_c)} SET NOT NULL;\n")
            elif ctype == "DEFAULT":
                raw_c = norm_col(cn.get("columns", ""))
                def_expr = translate_oracle_expr(cn.get("check_condition") or "")
                # Guard against identity clauses being emitted as SET DEFAULT
                if def_expr and any(tok in def_expr.upper() for tok in ("AS IDENTITY", "GENERATED", "IDENTITY")):
                    continue
                if raw_c and def_expr:
                    h_alter.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} ALTER COLUMN {q(raw_c)} SET DEFAULT {def_expr};\n")
            elif ctype == "RAW_ALTER":
                raw_stmt = translate_oracle_expr(cn.get("check_condition") or "")
                if raw_stmt and any(tok in raw_stmt.upper() for tok in ("AS IDENTITY", "GENERATED BY DEFAULT AS IDENTITY")):
                    continue
                if raw_stmt:
                    h_alter.write(f"ALTER TABLE {q(schema)}.{q(t_clean)} {raw_stmt};\n")

        # Attach audit trigger to automatically update modifieddate and modifiedby
        h_alter.write(f"DROP TRIGGER IF EXISTS trg_audit_modified ON {q(schema)}.{q(t_clean)};\n")
        h_alter.write(f"CREATE TRIGGER trg_audit_modified BEFORE UPDATE ON {q(schema)}.{q(t_clean)} FOR EACH ROW EXECUTE FUNCTION sweep_ctl.fn_update_audit_timestamp();\n\n")

        # 3. Post-Load Scripts: Indexes (CONCURRENTLY)
        table_idxs = indexes.get((owner, table), [])
        pk_uk_names = {norm_col(cn.get("constraint_name", "")) for cn in table_cons if cn.get("ctype") in ("P", "U")}
        for ix in table_idxs:
            i_name = norm_col(ix.get("index_name", ""))
            if i_name in pk_uk_names:
                continue
            unq = "UNIQUE " if ix.get("uniqueness") == "UNIQUE" else ""
            raw_cols = ix.get("columns", "")
            target_cols = ", ".join(q(norm_col(x)) for x in raw_cols.split(";") if x.strip())

            if not target_cols:
                continue

            if is_partitioned:
                h_idx.write(f"CREATE {unq}INDEX IF NOT EXISTS {q(i_name)} ON ONLY {q(schema)}.{q(t_clean)} ({target_cols});\n")
                for cname in child_names:
                    part_idx_name = norm_col(f"{cname}_{i_name}")[:63]
                    h_idx.write(f"CREATE {unq}INDEX CONCURRENTLY IF NOT EXISTS {q(part_idx_name)} ON {q(schema)}.{q(cname)} ({target_cols});\n")
                    h_idx.write(f"ALTER INDEX {q(schema)}.{q(i_name)} ATTACH PARTITION {q(schema)}.{q(part_idx_name)};\n")
            else:
                h_idx.write(f"CREATE {unq}INDEX CONCURRENTLY IF NOT EXISTS {q(i_name)} ON {q(schema)}.{q(t_clean)} ({target_cols});\n")

        generated_tables_summary.append({
            "schema": schema,
            "table_name": t_clean,
            "source_owner": owner,
            "source_table": table,
            "column_count": len(cols),
            "is_partitioned": is_partitioned,
            "partitions": len(child_stmts) if is_partitioned else 0
        })

    # Translate and write Views and Triggers per schema
    translated_views_summary = []
    translated_triggers_summary = []

    all_owners = set(views_by_owner.keys()) | set(triggers_by_owner.keys()) | set(sequences.keys())
    for s_owner in sorted(all_owners):
        if owners_filter and s_owner not in owners_filter:
            continue
        s_clean = s_owner.lower()
        h_tables, h_pk, h_alter, h_idx, h_fk, h_val, h_views, h_trg = get_handles(s_clean, s_owner)

        # Views for this schema
        base_s = s_clean[:-5] if s_clean.endswith("_code") else s_clean
        schema_known_tables = tables_by_schema.get(base_s, set())
        s_views_list = views_by_owner.get(s_owner, [])
        for v in s_views_list:
            pg_view_sql = translate_view_ddl(s_owner, v["name"], v["ddl"], known_tables=schema_known_tables)
            h_views.write(pg_view_sql)
            translated_views_summary.append({
                "schema": s_clean,
                "view_name": norm_col(v["name"]),
                "source_owner": s_owner,
                "status": "TRANSLATED"
            })

        # Triggers for this schema
        s_trg_list = triggers_by_owner.get(s_owner, [])
        for tr in s_trg_list:
            pg_trg_sql = translate_trigger_ddl(s_owner, tr["name"], tr["ddl"])
            h_trg.write(pg_trg_sql)
            translated_triggers_summary.append({
                "schema": s_clean,
                "trigger_name": norm_col(tr["name"]),
                "source_owner": s_owner,
                "status": "TRANSLATED"
            })

    # Close all files
    for handles in file_handles.values():
        for h in handles:
            h.close()

    logger.info(f"Generated DDL for {len(generated_tables_summary)} table(s) in outputs/pre_load/ and outputs/post_load/")

    # Generate Excel Report
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_04_generate_ddl_report_{ts}.xlsx"
    summary_sheet = [
        {"Metric": "DDL Generation Timestamp", "Value": ts},
        {"Metric": "Total Tables Processed", "Value": len(generated_tables_summary)},
        {"Metric": "Partitioned Tables", "Value": len(generated_partitions_summary)},
        {"Metric": "Total Child Partitions", "Value": sum(r["partition_count"] for r in generated_partitions_summary)},
        {"Metric": "Views Translated", "Value": len(translated_views_summary)},
        {"Metric": "Triggers Translated", "Value": len(translated_triggers_summary)},
        {"Metric": "Pre-Load Table DDL Path", "Value": "oracle/outputs/pre_load/<schema>_tables.sql"},
        {"Metric": "Post-Load Scripts Generated", "Value": "PK, AK, Alter Statements & Audit Triggers, Indexes (Concurrent), FK, Views (50), Triggers (60)"}
    ]

    sheets = {
        "Summary": summary_sheet,
        "Tables_Generated": generated_tables_summary,
        "Partitions_Generated": generated_partitions_summary if generated_partitions_summary else [{"Status": "No partitioned tables"}],
        "Views_Translated": translated_views_summary if translated_views_summary else [{"Status": "No views found in dump"}],
        "Triggers_Translated": translated_triggers_summary if translated_triggers_summary else [{"Status": "No triggers found in dump"}]
    }
    generate_excel_report(report_file, sheets)
    logger.info(f"Excel report generated: {report_file}")
    
    summary_lines = [
        "=" * 65,
        f" EXECUTION SUMMARY: {Path(__file__).name}",
        "=" * 65,
        f"  schemas processed          : {len(file_handles):>10,}",
        f"  tables generated           : {len(generated_tables_summary):>10,}",
        f"  partitioned tables         : {len(generated_partitions_summary):>10,}",
        f"  child partitions generated : {sum(r['partition_count'] for r in generated_partitions_summary):>10,}",
        f"  views translated           : {len(translated_views_summary):>10,}",
        f"  triggers translated        : {len(translated_triggers_summary):>10,}",
        "=" * 65
    ]
    for line in summary_lines:
        logger.info(line)
        
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="04_generateDDL.py - Target DDL & Partition Generator + View/Trigger Translation")
    parser.add_argument("--owners", help="Comma-separated owners to generate (e.g. BSS01001,BSS01104)")
    parser.add_argument("--tables", help="Comma-separated tables to generate (e.g. BSS01001.BROKERACCOUNTDETAIL)")
    args = parser.parse_args()

    owners = {o.strip().upper() for o in args.owners.split(",")} if args.owners else None
    tables = {t.strip().upper() for t in args.tables.split(",")} if args.tables else None

    ok = generate_ddl(owners_filter=owners, tables_filter=tables)
    sys.exit(0 if ok else 1)
