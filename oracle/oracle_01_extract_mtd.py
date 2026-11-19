#!/usr/bin/env python3
"""
01_extractMetadata.py - Stream Extract Metadata from Master Oracle SQL Dump
Parses an Oracle SQL dump file (Data Pump SQLFILE or DBMS_METADATA export) placed in:
  inputs/

Extracts and saves the 7 required metadata CSV files into outputs/oracle_metadata/:
  1. oracle_metadata_tables.csv
  2. oracle_metadata_constraints.csv
  3. oracle_metadata_indexes.csv
  4. oracle_metadata_partitions.csv
  5. oracle_metadata_table_stats.csv
  6. oracle_metadata_sequences.csv
  7. oracle_metadata_objects.csv

Generates: reports/01_extractMetadata_report_<timestamp>.xlsx
"""
import re
import sys
import csv
import argparse
from pathlib import Path
from sweep_common import (
    log_summary,
    ORACLE_INPUTS_DIR, ORACLE_DUMP_DIR, ORACLE_REPORTS_DIR,
    META_TABLES_CSV, META_CONSTRAINTS_CSV, META_INDEXES_CSV, META_PARTITIONS_CSV,
    META_TABLE_STATS_CSV, META_SEQUENCES_CSV, META_OBJECTS_CSV,
    get_logger, get_timestamp, get_db_name, generate_excel_report
)

logger = get_logger("oracle_01_extract_mtd", "oracle_01_extract_mtd")

# Regex token patterns for Oracle DDL syntax
NAME = r'"?([\w$#]+)"?'
OBJ_RE = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NO)?FORCE\s+)?(?:(?:NON)?EDITIONABLE\s+)?'
    r'(GLOBAL\s+TEMPORARY\s+TABLE|TABLE|MATERIALIZED\s+VIEW|VIEW|PROCEDURE|FUNCTION|PACKAGE\s+BODY|PACKAGE|'
    r'TRIGGER|TYPE\s+BODY|TYPE|PUBLIC\s+SYNONYM|SYNONYM|SEQUENCE|(?:(?:UNIQUE|BITMAP|REVERSE)\s+)*INDEX)\s+'
    + r'(?:' + NAME + r'\s*\.\s*)?' + NAME, re.I
)
CONNECT_RE = re.compile(r'^\s*--\s*CONNECT\s+"?([\w$#]+)"?\s*$', re.I)
ALTER_RE = re.compile(r'^\s*ALTER\s+TABLE\s+(?:' + NAME + r'\s*\.\s*)?' + NAME + r'\s+(ADD|MODIFY|ENABLE|DISABLE)\b', re.I)
PLSQL_TYPES = {"PROCEDURE", "FUNCTION", "PACKAGE BODY", "PACKAGE", "TRIGGER", "TYPE BODY", "TYPE"}


def match_paren(s, start):
    """Finds matching closing parenthesis taking nested parens and quotes into account."""
    depth, i, q1, q2 = 0, start, False, False
    while i < len(s):
        ch = s[i]
        if ch == "'" and not q2:
            q1 = not q1
        elif ch == '"' and not q1:
            q2 = not q2
        elif not q1 and not q2:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return -1


def split_top(s):
    """Splits top-level commas outside parentheses and quotes."""
    out, depth, cur, q1, q2 = [], 0, [], False, False
    for ch in s:
        if ch == "'" and not q2:
            q1 = not q1
        elif ch == '"' and not q1:
            q2 = not q2
        if not q1 and not q2:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                item = "".join(cur).strip()
                if item:
                    out.append(item)
                cur = []
                continue
        cur.append(ch)
    final_item = "".join(cur).strip()
    if final_item:
        out.append(final_item)
    return out


def clean_idents(s):
    """Parses comma-delimited column list into clean upper-case column names."""
    names = []
    has_expr = False
    for part in split_top(s):
        part = re.sub(r"\s+(ASC|DESC)\s*$", "", part.strip(), flags=re.I)
        m = re.fullmatch(r'"?([\w$#]+)"?', part)
        if m:
            names.append(m.group(1).upper())
        else:
            names.append(part)
            has_expr = True
    return names, has_expr


def is_statement_end(line):
    """Checks whether the line terminates an Oracle SQL DDL statement (ends with ; or is /)."""
    s = line.strip()
    return s.endswith(";") or s == "/"


def parse_column_def(item):
    """Parses single column definition string into structured column dict."""
    m = re.match(r'"?([\w$#]+)"?\s+(.*)$', item.strip(), re.S)
    if not m:
        return None
    name, rest = m.group(1).upper(), m.group(2)
    t = re.match(r'([A-Za-z][\w$#]*)\s*(?:\(([^)]*)\))?(.*)$', rest, re.S)
    if not t:
        return None
    dtype, args, tail = t.group(1).upper(), t.group(2), t.group(3)

    if dtype == "TIMESTAMP" and re.search(r"WITH(\s+LOCAL)?\s+TIME\s+ZONE", tail, re.I):
        dtype = "TIMESTAMP WITH TIME ZONE"
    elif dtype == "DOUBLE":
        dtype = "FLOAT"
    elif dtype == "LONG" and re.search(r"RAW", tail, re.I):
        dtype = "LONG RAW"

    length = prec = scale = ""
    if args:
        nums = [x.strip() for x in args.split(",")]
        first = re.match(r"(\d+|\*)", nums[0])
        if dtype in ("NUMBER", "FLOAT", "DECIMAL", "NUMERIC", "INTEGER", "INT"):
            prec = first.group(1) if first and first.group(1) != "*" else ""
            if len(nums) > 1 and re.fullmatch(r"-?\d+", nums[1]):
                scale = nums[1]
        elif first and first.group(1) != "*":
            length = first.group(1)

    if dtype in ("INTEGER", "INT", "SMALLINT"):
        dtype, prec, scale = "NUMBER", prec or "38", "0"

    up = tail.upper()
    dm = re.search(r"(?<!BY\s)DEFAULT\s+(?!ON\s+NULL)(.+?)(?=\s+(?:NOT\s+NULL|NULL|ENABLE|NOVALIDATE|CONSTRAINT|GENERATED)\b|\s*$)", tail, re.I | re.S)
    
    # Parse Oracle Identity Column: GENERATED [ALWAYS | BY DEFAULT [ON NULL]] AS IDENTITY [( options )]
    id_m = re.search(r"GENERATED\s+(ALWAYS|BY\s+DEFAULT(?:\s+ON\s+NULL)?)\s+AS\s+IDENTITY(?:\s*\((.*?)\)|\s+([^,\)]+))?", tail, re.I)
    is_identity = "Y" if id_m else "N"
    id_options = ""
    if id_m:
        gen = "IDENTITY"
        opts_raw = (id_m.group(2) or id_m.group(3) or "").upper()
        opts = []
        sm = re.search(r"START\s+WITH\s+(\d+)", opts_raw)
        im = re.search(r"INCREMENT\s+BY\s+(\d+)", opts_raw)
        min_m = re.search(r"MINVALUE\s+(\d+)", opts_raw)
        max_m = re.search(r"MAXVALUE\s+(\d+)", opts_raw)
        cache_m = re.search(r"CACHE\s+(\d+)", opts_raw)
        if sm: opts.append(f"START WITH {sm.group(1)}")
        if im: opts.append(f"INCREMENT BY {im.group(1)}")
        if min_m: opts.append(f"MINVALUE {min_m.group(1)}")
        if max_m:
            val = int(max_m.group(1))
            opts.append(f"MAXVALUE {min(val, 9223372036854775807)}")
        else:
            opts.append("MAXVALUE 9223372036854775807")
        if cache_m: opts.append(f"CACHE {cache_m.group(1)}")
        if "CYCLE" in opts_raw and "NOCYCLE" not in opts_raw: opts.append("CYCLE")
        elif "NOCYCLE" in opts_raw: opts.append("NO CYCLE")
        id_options = f"({' '.join(opts)})" if opts else ""
    else:
        gen = "VIRTUAL" if ("VIRTUAL" in up or "GENERATED ALWAYS AS" in up) else ""

    return {
        "column_name": name,
        "data_type": dtype,
        "data_length": length,
        "data_precision": prec,
        "data_scale": scale,
        "nullable": "N" if "NOT NULL" in up else "Y",
        "default_expr": dm.group(1).strip() if dm else "",
        "generated": gen,
        "is_identity": is_identity,
        "identity_options": id_options
    }


def parse_constraint_clause(item):
    """Parses constraint clause string into constraint dict."""
    m = re.match(r'(?:CONSTRAINT\s+"?([\w$#]+)"?\s+)?(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK)\s*(\(.*)', item.strip(), re.I | re.S)
    if not m:
        return None
    name, kind, rest = m.group(1) or "", re.sub(r"\s+", " ", m.group(2).upper()), m.group(3)
    end = match_paren(rest, 0)
    cols = rest[1:end] if end > 0 else ""

    ctype_map = {"PRIMARY KEY": "P", "UNIQUE": "U", "FOREIGN KEY": "R", "CHECK": "C"}
    out = {
        "constraint_name": name.upper(),
        "ctype": ctype_map.get(kind, "C"),
        "columns": "",
        "ref_owner": "",
        "ref_table": "",
        "ref_columns": ""
    }

    if kind == "CHECK":
        out["columns"] = ""
        out["check_condition"] = cols.strip()
        return out
    out["check_condition"] = ""

    col_names, _ = clean_idents(cols)
    out["columns"] = ";".join(col_names)

    if kind == "FOREIGN KEY":
        r = re.search(r'REFERENCES\s+(?:"?([\w$#]+)"?\s*\.\s*)?"?([\w$#]+)"?\s*(\([^)]*\))?', rest, re.I)
        if r:
            out["ref_owner"] = (r.group(1) or "").upper()
            out["ref_table"] = r.group(2).upper()
            ref_cols, _ = clean_idents(r.group(3)[1:-1]) if r.group(3) else ([], False)
            out["ref_columns"] = ";".join(ref_cols)

    return out


def parse_partitions_clause(tail):
    """Parses PARTITION BY RANGE/LIST/HASH clause of CREATE TABLE."""
    m = re.search(r"PARTITION\s+BY\s+(RANGE|LIST|HASH|REFERENCE)\s*\(", tail, re.I)
    if not m:
        return None
    method = m.group(1).upper()
    p0 = m.end() - 1
    p1 = match_paren(tail, p0)
    if p1 < 0:
        return None

    keys, _ = clean_idents(tail[p0 + 1:p1])
    rest = tail[p1 + 1:]

    interval = ""
    im = re.search(r"\bINTERVAL\s*\(", rest, re.I)
    if im:
        e = match_paren(rest, im.end() - 1)
        interval = rest[im.end():e].strip()
        rest = rest[e + 1:]

    sub = bool(re.search(r"\bSUBPARTITION\s+BY\b", rest, re.I))

    parts = []
    lm = re.search(r"\(\s*PARTITION\b", rest, re.I)
    if lm:
        e = match_paren(rest, lm.start())
        part_content = rest[lm.start() + 1:e if e > 0 else len(rest)]
        for item in split_top(part_content):
            n = re.match(r'PARTITION\s+"?([\w$#]+)"?\s*(.*)$', item, re.I | re.S)
            if not n:
                continue
            p_name = n.group(1).upper()
            p_rest = n.group(2)
            bound = ""
            b_range = re.search(r"VALUES\s+LESS\s+THAN\s*\(", p_rest, re.I)
            if b_range:
                b_end = match_paren(p_rest, b_range.end() - 1)
                bound = p_rest[b_range.end():b_end]
            else:
                b_list = re.search(r"VALUES\s*\(", p_rest, re.I)
                if b_list:
                    b_end = match_paren(p_rest, b_list.end() - 1)
                    bound = p_rest[b_list.end():b_end]
            parts.append((p_name, bound.strip()))

    return {
        "method": method,
        "keys": ";".join(keys),
        "interval": interval,
        "subpart": sub,
        "parts": parts
    }


def parse_index_ddl(text, default_owner="PUBLIC", source_file=""):
    """Parses complete (single or multi-line) CREATE INDEX statement into structured dict."""
    m_obj = OBJ_RE.match(text)
    if not m_obj:
        return None
    kind = re.sub(r"\s+", " ", m_obj.group(1).upper())
    owner = (m_obj.group(2) or default_owner or "PUBLIC").upper()
    name = m_obj.group(3).upper()

    unq = "UNIQUE" if "UNIQUE" in kind else ("BITMAP" if "BITMAP" in kind else "NONUNIQUE")
    on_m = re.search(r'\bON\s+(?:"?([\w$#]+)"?\s*\.\s*)?"?([\w$#]+)"?\s*\(', text, re.I)
    if not on_m:
        return None

    t_owner = (on_m.group(1) or owner).upper()
    t_name = on_m.group(2).upper()
    p0 = on_m.end() - 1
    p1 = match_paren(text, p0)
    col_content = text[p0 + 1:p1] if p1 > 0 else ""
    i_cols, expr = clean_idents(col_content)
    is_part = "True" if re.search(r'\b(LOCAL|PARTITION\s+BY)\b', text, re.I) else "False"

    return {
        "owner": owner,
        "index_name": name,
        "table_owner": t_owner,
        "table_name": t_name,
        "uniqueness": unq,
        "columns": ";".join(i_cols),
        "has_expression": "True" if expr else "False",
        "partitioned": is_part,
        "source_file": source_file
    }


def parse_sequence_ddl(text, default_owner="PUBLIC"):
    """Parses complete (single or multi-line) CREATE SEQUENCE statement into structured dict."""
    m_obj = OBJ_RE.match(text)
    if not m_obj:
        return None
    owner = (m_obj.group(2) or default_owner or "PUBLIC").upper()
    name = m_obj.group(3).upper()

    min_v = re.search(r"\bMINVALUE\s+(-?\d+)", text, re.I)
    max_v = re.search(r"\bMAXVALUE\s+(-?\d+)", text, re.I)
    inc_v = re.search(r"\bINCREMENT\s+BY\s+(-?\d+)", text, re.I)
    strt_v = re.search(r"\bSTART\s+WITH\s+(-?\d+)", text, re.I)
    cache_v = re.search(r"\bCACHE\s+(\d+)", text, re.I)

    has_cycle = bool(re.search(r"\bCYCLE\b", text, re.I) and not re.search(r"\bNOCYCLE\b", text, re.I))
    cache_val = cache_v.group(1) if cache_v else ("1" if re.search(r"\bNOCACHE\b", text, re.I) else "20")

    return {
        "owner": owner,
        "sequence_name": name,
        "min_value": min_v.group(1) if min_v else "1",
        "max_value": max_v.group(1) if max_v else "",
        "increment_by": inc_v.group(1) if inc_v else "1",
        "start_with": strt_v.group(1) if strt_v else "1",
        "cache_size": cache_val,
        "cycle": "Y" if has_cycle else "N"
    }


def parse_alter_ddl(text, default_owner="", source_file=""):
    """Parses complete (single or multi-line) ALTER TABLE statement into structured dicts."""
    alt_m = ALTER_RE.match(text)
    if not alt_m:
        return []
    o = (alt_m.group(1) or default_owner or "").upper()
    t = alt_m.group(2).upper()
    action = (alt_m.group(3) or "ADD").upper()
    c_body = text[alt_m.end():].strip().rstrip(";").rstrip("/").strip()

    # Check for multiple constraints/items enclosed in parentheses: ADD ( ... , ... )
    if c_body.startswith("(") and match_paren(c_body, 0) == len(c_body) - 1:
        inner = c_body[1:-1]
        items = split_top(inner)
    else:
        items = [c_body]

    results = []
    for item in items:
        item_clean = item.strip()
        if not item_clean:
            continue

        if action == "MODIFY":
            # e.g. "STATUS" NOT NULL ENABLE or "CREATEDDATE" DEFAULT SYSDATE
            col_m = re.match(r'"?([\w$#]+)"?\s*(.*)$', item_clean)
            if col_m:
                col_name = col_m.group(1).upper()
                rest = col_m.group(2).strip()
                if re.search(r'\bNOT\s+NULL\b', rest, re.I):
                    results.append({
                        "owner": o, "table_name": t, "constraint_name": f"NN_{t}_{col_name}",
                        "ctype": "NOT_NULL", "columns": col_name, "ref_owner": "", "ref_table": "",
                        "ref_columns": "", "check_condition": "", "source_file": source_file
                    })
                def_m = re.search(r'\bDEFAULT\s+(.+?)(?=\s+(?:NOT\s+NULL|ENABLE|DISABLE|NULL)\b|$)', rest, re.I)
                if def_m:
                    def_expr = def_m.group(1).strip()
                    results.append({
                        "owner": o, "table_name": t, "constraint_name": f"DF_{t}_{col_name}",
                        "ctype": "DEFAULT", "columns": col_name, "ref_owner": "", "ref_table": "",
                        "ref_columns": "", "check_condition": def_expr, "source_file": source_file
                    })
            continue

        # Standard ADD / ENABLE / DISABLE constraint clauses
        c_parsed = parse_constraint_clause(item_clean)
        if c_parsed:
            results.append({
                "owner": o,
                "table_name": t,
                "constraint_name": c_parsed["constraint_name"],
                "ctype": c_parsed["ctype"],
                "columns": c_parsed["columns"],
                "ref_owner": c_parsed["ref_owner"],
                "ref_table": c_parsed["ref_table"],
                "ref_columns": c_parsed["ref_columns"],
                "check_condition": c_parsed.get("check_condition", ""),
                "source_file": source_file
            })
        else:
            # Fallback for other supported ALTER statements
            results.append({
                "owner": o,
                "table_name": t,
                "constraint_name": f"ALTER_{t}",
                "ctype": "RAW_ALTER",
                "columns": "",
                "ref_owner": "",
                "ref_table": "",
                "ref_columns": "",
                "check_condition": item_clean,
                "source_file": source_file
            })
    return results


def parse_table_ddl(text, default_owner="", source_file=""):
    """Parses complete (single or multi-line) CREATE TABLE statement into columns, constraints, and partitions."""
    m_tbl = OBJ_RE.match(text)
    if not m_tbl or "TABLE" not in m_tbl.group(1).upper():
        return None

    owner = (m_tbl.group(2) or default_owner or "").upper()
    table = m_tbl.group(3).upper()

    start = text.find("(", m_tbl.end())
    if start < 0:
        return None
    end = match_paren(text, start)
    if end < 0:
        return None

    body = text[start + 1:end]
    tail = text[end + 1:]

    inline_cols = []
    inline_cons = []

    for item in split_top(body):
        c = parse_constraint_clause(item)
        if c:
            inline_cons.append(c)
            continue
        col = parse_column_def(item)
        if col:
            inline_cols.append(col)

    pinfo = parse_partitions_clause(tail)
    is_part = "Y" if pinfo else "N"
    is_temp = "Y" if "TEMPORARY" in m_tbl.group(1).upper() else "N"

    pk_cols = {col for cn in inline_cons if cn["ctype"] == "P" for col in cn["columns"].split(";") if col}

    cols_list = []
    for idx, c in enumerate(inline_cols, 1):
        cols_list.append({
            "owner": owner,
            "table_name": table,
            "column_id": idx,
            "column_name": c["column_name"],
            "data_type": c["data_type"],
            "data_length": c["data_length"],
            "data_precision": c["data_precision"],
            "data_scale": c["data_scale"],
            "nullable": c["nullable"],
            "is_pk": "Y" if c["column_name"] in pk_cols else "N",
            "is_identity": c.get("is_identity", "N"),
            "identity_options": c.get("identity_options", ""),
            "default_expr": c["default_expr"],
            "partitioned": is_part,
            "temporary": is_temp
        })

    cons_list = []
    for cn in inline_cons:
        cons_list.append({
            "owner": owner,
            "table_name": table,
            "constraint_name": cn["constraint_name"],
            "ctype": cn["ctype"],
            "columns": cn["columns"],
            "ref_owner": cn["ref_owner"],
            "ref_table": cn["ref_table"],
            "ref_columns": cn["ref_columns"],
            "source_file": source_file
        })

    parts_list = []
    if pinfo:
        for pos, (pname, bound) in enumerate(pinfo["parts"], 1):
            parts_list.append({
                "owner": owner,
                "table_name": table,
                "partition_name": pname,
                "partition_position": pos,
                "bound": bound,
                "method": pinfo["method"],
                "key_columns": pinfo["keys"],
                "interval_expr": pinfo["interval"],
                "subpartitioned": "Y" if pinfo["subpart"] else "N"
            })

    return {
        "owner": owner,
        "table": table,
        "columns": cols_list,
        "constraints": cons_list,
        "partitions": parts_list
    }


def extract_metadata_from_master(sql_file_path=None, owners_filter=None):
    ts = get_timestamp()

    # Locate master SQL file: prioritize inputs/oracle_dump/, fallback to inputs/
    master_files = []
    if sql_file_path:
        p = Path(sql_file_path)
        if p.exists():
            master_files = [p]
    else:
        # Check inputs/oracle_dump/ first
        if ORACLE_DUMP_DIR.exists():
            master_files = [f for f in (list(ORACLE_DUMP_DIR.glob("*.sql")) +
                                       list(ORACLE_DUMP_DIR.glob("*.ddl")) +
                                       list(ORACLE_DUMP_DIR.glob("*.txt")))
                            if f.is_file() and f.name.lower() != "sweep_meta.sql"]
        # Fallback to inputs/ excluding sweep_meta.sql
        if not master_files and ORACLE_INPUTS_DIR.exists():
            master_files = [f for f in (list(ORACLE_INPUTS_DIR.glob("*.sql")) +
                                       list(ORACLE_INPUTS_DIR.glob("*.ddl")) +
                                       list(ORACLE_INPUTS_DIR.glob("*.txt")))
                            if f.is_file() and f.name.lower() != "sweep_meta.sql"]

    if not master_files:
        logger.error(f"No Oracle SQL dump file found in {ORACLE_DUMP_DIR} or {ORACLE_INPUTS_DIR}!")
        logger.info(f"Please place your Oracle SQL dump file in: {ORACLE_DUMP_DIR}")
        return False

    target_sql = master_files[0]
    file_size_mb = round(target_sql.stat().st_size / (1024**2), 1)
    logger.info(f"Extracting metadata from master SQL dump: {target_sql.name} ({file_size_mb} MB)...")

    if target_sql.stat().st_size == 0:
        logger.warning(f"File {target_sql.name} is empty (0 bytes). Please ensure your SQL dump contains DDL content.")

    # CSV outputs in outputs/oracle_metadata/
    tables_list = []
    constraints_list = []
    indexes_list = []
    partitions_list = []
    sequences_list = []
    objects_list = []
    table_stats_list = []

    seen_tables = set()
    cur_schema = ""

    # Stream file line-by-line with dedicated state tracking
    mode = None  # None, "TABLE", "INDEX", "SEQUENCE", "ALTER", "SKIP_PLSQL", "SKIP_OBJ"
    buf = []
    statement_count = 0

    with open(target_sql, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if mode is None:
                # Check for Data Pump CONNECT header
                cm = CONNECT_RE.match(line)
                if cm:
                    cur_schema = cm.group(1).upper()
                    continue

                m_obj = OBJ_RE.match(line)
                m_alt = None if m_obj else ALTER_RE.match(line)

                if not m_obj and not m_alt:
                    continue

                if m_obj:
                    kind = re.sub(r"\s+", " ", m_obj.group(1).upper())
                    owner = (m_obj.group(2) or cur_schema or "PUBLIC").upper()
                    name = m_obj.group(3).upper()

                    if owners_filter and owner not in owners_filter and not kind.startswith("PUBLIC"):
                        continue

                    # Views / PLSQL objects
                    if kind in ("VIEW", "MATERIALIZED VIEW", "PUBLIC SYNONYM", "SYNONYM") or kind in PLSQL_TYPES:
                        objects_list.append({
                            "owner": owner,
                            "object_type": kind,
                            "object_name": name,
                            "source_file": target_sql.name
                        })
                        if kind in PLSQL_TYPES:
                            if line.strip() == "/":
                                mode = None
                            else:
                                mode = "SKIP_PLSQL"
                        else:
                            if is_statement_end(line):
                                mode = None
                            else:
                                mode = "SKIP_OBJ"
                        continue

                    # Sequences
                    if kind == "SEQUENCE":
                        if is_statement_end(line):
                            seq = parse_sequence_ddl(line, default_owner=cur_schema)
                            if seq and (not owners_filter or seq["owner"] in owners_filter):
                                sequences_list.append(seq)
                            mode = None
                        else:
                            mode = "SEQUENCE"
                            buf = [line]
                        continue

                    # Indexes
                    if "INDEX" in kind:
                        if is_statement_end(line):
                            idx = parse_index_ddl(line, default_owner=cur_schema, source_file=target_sql.name)
                            if idx and (not owners_filter or idx["owner"] in owners_filter):
                                indexes_list.append(idx)
                            mode = None
                        else:
                            mode = "INDEX"
                            buf = [line]
                        continue

                    # Tables
                    if "TABLE" in kind:
                        if is_statement_end(line):
                            tbl = parse_table_ddl(line, default_owner=cur_schema, source_file=target_sql.name)
                            if tbl and (not owners_filter or tbl["owner"] in owners_filter):
                                seen_tables.add((tbl["owner"], tbl["table"]))
                                tables_list.extend(tbl["columns"])
                                constraints_list.extend(tbl["constraints"])
                                partitions_list.extend(tbl["partitions"])
                                table_stats_list.append({
                                    "owner": tbl["owner"],
                                    "table_name": tbl["table"],
                                    "num_rows": "",
                                    "size_mb": ""
                                })
                            mode = None
                        else:
                            mode = "TABLE"
                            buf = [line]
                        continue

                    # Fallback for unrecognized CREATE object
                    if is_statement_end(line):
                        mode = None
                    else:
                        mode = "SKIP_OBJ"
                    continue

                elif m_alt:
                    # ALTER TABLE ... ADD CONSTRAINT
                    owner = (m_alt.group(1) or cur_schema or "").upper()
                    if owners_filter and owner not in owners_filter:
                        continue
                    if is_statement_end(line):
                        cons = parse_alter_ddl(line, default_owner=cur_schema, source_file=target_sql.name)
                        for cn in cons:
                            if not owners_filter or cn["owner"] in owners_filter:
                                constraints_list.append(cn)
                        mode = None
                    else:
                        mode = "ALTER"
                        buf = [line]
                    continue

            # Multi-line statement continuation
            elif mode == "SKIP_PLSQL":
                if line.strip() == "/":
                    mode = None
                continue

            elif mode == "SKIP_OBJ":
                if is_statement_end(line):
                    mode = None
                continue

            elif mode in ("TABLE", "INDEX", "SEQUENCE", "ALTER"):
                buf.append(line)
                if not is_statement_end(line):
                    continue

                text = "".join(buf)
                statement_count += 1

                if mode == "TABLE":
                    tbl = parse_table_ddl(text, default_owner=cur_schema, source_file=target_sql.name)
                    if tbl and (not owners_filter or tbl["owner"] in owners_filter):
                        seen_tables.add((tbl["owner"], tbl["table"]))
                        tables_list.extend(tbl["columns"])
                        constraints_list.extend(tbl["constraints"])
                        partitions_list.extend(tbl["partitions"])
                        table_stats_list.append({
                            "owner": tbl["owner"],
                            "table_name": tbl["table"],
                            "num_rows": "",
                            "size_mb": ""
                        })

                elif mode == "INDEX":
                    idx = parse_index_ddl(text, default_owner=cur_schema, source_file=target_sql.name)
                    if idx and (not owners_filter or idx["owner"] in owners_filter):
                        indexes_list.append(idx)

                elif mode == "SEQUENCE":
                    seq = parse_sequence_ddl(text, default_owner=cur_schema)
                    if seq and (not owners_filter or seq["owner"] in owners_filter):
                        sequences_list.append(seq)

                elif mode == "ALTER":
                    cons = parse_alter_ddl(text, default_owner=cur_schema, source_file=target_sql.name)
                    for cn in cons:
                        if not owners_filter or cn["owner"] in owners_filter:
                            constraints_list.append(cn)

                mode = None
                buf = []

    # -------------------------------------------------------------------------
    # Write the 7 required metadata CSV files in outputs/oracle_metadata/
    # -------------------------------------------------------------------------
    def write_csv(filepath, data, default_header):
        with open(filepath, "w", newline="", encoding="utf-8") as out_f:
            if data:
                writer = csv.DictWriter(out_f, fieldnames=list(data[0].keys()))
                writer.writeheader()
                writer.writerows(data)
            else:
                writer = csv.writer(out_f)
                writer.writerow(default_header)
        logger.info(f"Saved: {filepath.name} ({len(data)} rows)")

    write_csv(META_TABLES_CSV, tables_list,
              ["owner", "table_name", "column_id", "column_name", "data_type", "data_length", "data_precision", "data_scale", "nullable", "is_pk", "is_identity", "identity_options", "default_expr", "partitioned", "temporary"])

    write_csv(META_CONSTRAINTS_CSV, constraints_list,
              ["owner", "table_name", "constraint_name", "ctype", "columns", "ref_owner", "ref_table", "ref_columns", "check_condition", "source_file"])

    write_csv(META_INDEXES_CSV, indexes_list,
              ["owner", "index_name", "table_owner", "table_name", "uniqueness", "columns", "has_expression", "partitioned", "source_file"])

    write_csv(META_PARTITIONS_CSV, partitions_list,
              ["owner", "table_name", "partition_name", "partition_position", "bound", "method", "key_columns", "interval_expr", "subpartitioned"])

    write_csv(META_TABLE_STATS_CSV, table_stats_list,
              ["owner", "table_name", "num_rows", "size_mb"])

    write_csv(META_SEQUENCES_CSV, sequences_list,
              ["owner", "sequence_name", "min_value", "max_value", "increment_by", "start_with", "cache_size", "cycle"])

    write_csv(META_OBJECTS_CSV, objects_list,
              ["owner", "object_type", "object_name", "source_file"])

    # Generate Excel Report
    report_file = ORACLE_REPORTS_DIR / f"{get_db_name()}_oracle_01_extract_mtd_report_{ts}.xlsx"
    summary_data = [
        {"Metric": "Extraction Timestamp", "Value": ts},
        {"Metric": "Master SQL Source File", "Value": target_sql.name},
        {"Metric": "Source File Size", "Value": f"{file_size_mb} MB"},
        {"Metric": "Tables Extracted", "Value": len(seen_tables)},
        {"Metric": "Columns Extracted", "Value": len(tables_list)},
        {"Metric": "Constraints Extracted (PK/UK/FK)", "Value": len(constraints_list)},
        {"Metric": "Indexes Extracted", "Value": len(indexes_list)},
        {"Metric": "Partitions Extracted", "Value": len(partitions_list)},
        {"Metric": "Sequences Extracted", "Value": len(sequences_list)},
        {"Metric": "Views / PLSQL Objects Extracted", "Value": len(objects_list)}
    ]

    sheets = {
        "Summary": summary_data,
        "Tables_Summary": [{"owner": o, "table_name": t} for o, t in sorted(seen_tables)],
        "Columns": tables_list[:3000],
        "Partitions": partitions_list[:3000],
        "Constraints": constraints_list[:2000],
        "Indexes": indexes_list[:2000],
        "Sequences": sequences_list,
        "Objects_Views": objects_list[:2000]
    }
    generate_excel_report(report_file, sheets)
    logger.info(f"Excel metadata report generated: {report_file}")
    
    views_count = sum(1 for o in objects_list if o.get("object_type") == "VIEW")
    triggers_count = sum(1 for o in objects_list if o.get("object_type") == "TRIGGER")
    
    log_summary(logger, Path(__file__).name, [
        ('tables extracted', len(seen_tables)),
        ('partitions extracted', len(partitions_list)),
        ('columns extracted', len(tables_list)),
        ('constraints extracted', len(constraints_list)),
        ('triggers extracted', triggers_count),
        ('views extracted', views_count),
        ('indexes extracted', len(indexes_list)),
        ('sequences extracted', len(sequences_list)),
    ])
        
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="01_extractMetadata.py - Stream Extract Metadata from Master SQL Dump")
    parser.add_argument("--file", help="Specific master SQL dump file path (defaults to inputs/*.sql)")
    parser.add_argument("--owners", help="Comma-separated owners to filter (e.g. BSS01001,BSS01104)")
    args = parser.parse_args()

    owners = {o.strip().upper() for o in args.owners.split(",")} if args.owners else None
    ok = extract_metadata_from_master(sql_file_path=args.file, owners_filter=owners)
    sys.exit(0 if ok else 1)
