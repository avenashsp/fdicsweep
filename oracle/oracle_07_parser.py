#!/usr/bin/env python3
"""
oracle_07_parser.py - Splits the generated post-load SQL scripts (PK / FK / index / view / trigger
files from Step 04) into individually executable object blocks for Step 07.
"""
import re


# Regex patterns for matching SQL statements and extracting schema/table/object names
PK_RE = re.compile(
    r'ALTER\s+TABLE\s+(?:ONLY\s+)?\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?\s+ADD\s+CONSTRAINT\s+\"?([\w$#]+)\"?\s+(PRIMARY\s+KEY|UNIQUE)',
    re.I
)


FK_RE = re.compile(
    r'ALTER\s+TABLE\s+(?:ONLY\s+)?\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?\s+ADD\s+CONSTRAINT\s+\"?([\w$#]+)\"?\s+FOREIGN\s+KEY',
    re.I
)


VAL_RE = re.compile(
    r'ALTER\s+TABLE\s+(?:ONLY\s+)?\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?\s+VALIDATE\s+CONSTRAINT\s+\"?([\w$#]+)\"?',
    re.I
)


IDX_RE = re.compile(
    r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?\"?([\w$#]+)\"?\s+ON\s+(?:ONLY\s+)?\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?',
    re.I
)


ATTACH_RE = re.compile(
    r'ALTER\s+INDEX\s+\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?\s+ATTACH\s+PARTITION\s+\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?',
    re.I
)


VIEW_RE = re.compile(
    r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?',
    re.I
)


TRG_RE = re.compile(
    r'CREATE\s+TRIGGER\s+\"?([\w$#]+)\"?\s+.*?\s+ON\s+\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?',
    re.I | re.S
)


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


def parse_sql_file_blocks(file_path, stage_code):
    """
    Parses a post-load SQL file into actionable object blocks.
    Returns: list of dicts:
      {
        'sql': full_statement_sql,
        'object_type': 'PK'/'UNIQUE'/'INDEX'/'INDEX_ATTACH'/'FK'/'FK_VALIDATE'/'VIEW'/'TRIGGER'/'SESSION',
        'schema': schema_name,
        'table': table_name or None,
        'object_name': object_identifier,
        'raw': raw_snippet
      }
    """
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    blocks = []

    # 0. Alter Statements File: Function, Check constraints, Alter column, Audit triggers
    if stage_code == "alter":
        # First split out any function definitions with $$ ... $$
        func_chunks = re.findall(r'(CREATE\s+OR\s+REPLACE\s+FUNCTION\s+sweep_ctl\.fn_update_audit_timestamp[\s\S]*?\$\$\s*LANGUAGE\s+plpgsql;)', content, re.I)
        for fc in func_chunks:
            blocks.append({
                "sql": fc.strip(),
                "object_type": "SESSION",
                "schema": "sweep_ctl",
                "table": "",
                "object_name": "fn_update_audit_timestamp",
                "raw": "CREATE FUNCTION sweep_ctl.fn_update_audit_timestamp()"
            })
            content = content.replace(fc, "")

        # Split remaining statements by semicolon
        lines = content.splitlines()
        curr_stmt = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("--") or stripped.startswith("\\"):
                continue
            curr_stmt.append(line)
            if stripped.endswith(";"):
                full_stmt = "\n".join(curr_stmt).strip()
                curr_stmt = []
                if full_stmt.lower().startswith("set "):
                    blocks.append({
                        "sql": full_stmt, "object_type": "SESSION", "schema": "", "table": "",
                        "object_name": "session", "raw": full_stmt
                    })
                    continue
                if "CREATE SCHEMA" in full_stmt.upper():
                    blocks.append({
                        "sql": full_stmt, "object_type": "SESSION", "schema": "sweep_ctl", "table": "",
                        "object_name": "schema_sweep_ctl", "raw": full_stmt
                    })
                    continue

                m_cn = re.search(r'ALTER\s+TABLE\s+(?:ONLY\s+)?\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?\s+ADD\s+CONSTRAINT\s+\"?([\w$#]+)\"?', full_stmt, re.I)
                if m_cn:
                    s_name, t_name, c_name = m_cn.group(1).lower(), m_cn.group(2).lower(), m_cn.group(3).lower()
                    upper_stmt = full_stmt.upper()
                    if "CHECK" in upper_stmt:
                        ctype = "CHECK"
                    elif "PRIMARY KEY" in upper_stmt:
                        ctype = "PK"
                    elif "UNIQUE" in upper_stmt:
                        ctype = "UNIQUE"
                    elif "FOREIGN KEY" in upper_stmt:
                        ctype = "FK"
                    else:
                        ctype = "CONSTRAINT"
                    blocks.append({
                        "sql": full_stmt, "object_type": ctype, "schema": s_name, "table": t_name,
                        "object_name": c_name, "raw": f"{ctype} {s_name}.{c_name} on {s_name}.{t_name}"
                    })
                    continue

                m_ck_val = re.search(r'ALTER\s+TABLE\s+\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?\s+VALIDATE\s+CONSTRAINT\s+\"?([\w$#]+)\"?', full_stmt, re.I)
                if m_ck_val:
                    s_name, t_name, c_name = m_ck_val.group(1).lower(), m_ck_val.group(2).lower(), m_ck_val.group(3).lower()
                    blocks.append({
                        "sql": full_stmt, "object_type": "CHECK_VALIDATE", "schema": s_name, "table": t_name,
                        "object_name": c_name, "raw": f"VALIDATE CHECK {s_name}.{c_name} on {s_name}.{t_name}"
                    })
                    continue

                m_trg = TRG_RE.search(full_stmt)
                if m_trg:
                    trg_name, s_name, t_name = m_trg.group(1).lower(), m_trg.group(2).lower(), m_trg.group(3).lower()
                    blocks.append({
                        "sql": full_stmt, "object_type": "TRIGGER", "schema": s_name, "table": t_name,
                        "object_name": trg_name, "raw": f"TRIGGER {s_name}.{trg_name} on {s_name}.{t_name}"
                    })
                    continue

                # General ALTER TABLE
                m_alt = re.search(r'ALTER\s+TABLE\s+\"?([\w$#]+)\"?\s*\.\s*\"?([\w$#]+)\"?', full_stmt, re.I)
                if m_alt:
                    s_name, t_name = m_alt.group(1).lower(), m_alt.group(2).lower()
                    blocks.append({
                        "sql": full_stmt, "object_type": "ALTER", "schema": s_name, "table": t_name,
                        "object_name": "alter_stmt", "raw": full_stmt[:60]
                    })
                    continue

                blocks.append({
                    "sql": full_stmt, "object_type": "OTHER", "schema": "", "table": "",
                    "object_name": "other", "raw": full_stmt[:60]
                })
        return blocks

    # 1. Triggers File: Trigger functions + triggers
    if stage_code == "triggers":
        # Split by trigger blocks (each trigger definition begins with -- Trigger Function: or CREATE OR REPLACE FUNCTION)
        trg_chunks = re.split(r'(?=--\s*Trigger\s+Function:|\bCREATE\s+OR\s+REPLACE\s+FUNCTION\b)', content)
        for chunk in trg_chunks:
            chunk = chunk.strip()
            if not chunk or chunk.startswith("\\set") or chunk.startswith("SET "):
                continue

            # Find target table and trigger name in CREATE TRIGGER
            m = TRG_RE.search(chunk)
            if m:
                trg_name = m.group(1).lower()
                s_name = m.group(2).lower()
                t_name = m.group(3).lower()
                blocks.append({
                    "sql": chunk if chunk.endswith(";") else chunk + ";",
                    "object_type": "TRIGGER",
                    "schema": s_name,
                    "table": t_name,
                    "object_name": trg_name,
                    "raw": f"TRIGGER {s_name}.{trg_name} ON {s_name}.{t_name}"
                })
            else:
                # Standalone function or other statement
                blocks.append({
                    "sql": chunk if chunk.endswith(";") else chunk + ";",
                    "object_type": "SESSION",
                    "schema": "",
                    "table": "",
                    "object_name": "function",
                    "raw": chunk[:60]
                })
        return blocks

    # 2. Views File: CREATE OR REPLACE VIEW
    if stage_code == "views":
        # Each view ends with ;\n\n or ;
        view_chunks = re.split(r';\s*\n', content)
        for chunk in view_chunks:
            chunk = chunk.strip()
            if not chunk or chunk.startswith("\\set") or (chunk.startswith("SET ") and not VIEW_RE.search(chunk)):
                continue

            m = VIEW_RE.search(chunk)
            if m:
                s_name = m.group(1).lower()
                v_name = m.group(2).lower()
                blocks.append({
                    "sql": chunk + ";",
                    "object_type": "VIEW",
                    "schema": s_name,
                    "table": "",
                    "object_name": v_name,
                    "raw": f"VIEW {s_name}.{v_name}"
                })
        return blocks

    # 3. Standard statement-by-statement parsing for PK, Indexes, FK, Validate
    lines = content.splitlines()
    curr_stmt = []

    for line in lines:
        stripped = line.strip()
        # Skip empty lines, SQL comments, and psql meta-commands (like \set, \copy)
        if not stripped or stripped.startswith("--") or stripped.startswith("\\"):
            continue

        curr_stmt.append(line)
        if stripped.endswith(";"):
            full_stmt = "\n".join(curr_stmt).strip()
            curr_stmt = []

            # Session commands (only pure SQL SET commands, never psql \set)
            if full_stmt.lower().startswith("set "):
                blocks.append({
                    "sql": full_stmt,
                    "object_type": "SESSION",
                    "schema": "",
                    "table": "",
                    "object_name": "session",
                    "raw": full_stmt
                })
                continue

            # Stage 1: PK / Unique
            m_pk = PK_RE.search(full_stmt)
            if m_pk:
                s_name = m_pk.group(1).lower()
                t_name = m_pk.group(2).lower()
                c_name = m_pk.group(3).lower()
                ctype = "PK" if "primary" in m_pk.group(4).lower() else "UNIQUE"
                blocks.append({
                    "sql": full_stmt,
                    "object_type": ctype,
                    "schema": s_name,
                    "table": t_name,
                    "object_name": c_name,
                    "raw": f"{ctype} {s_name}.{c_name} on {s_name}.{t_name}"
                })
                continue

            # Stage 2: Indexes
            m_idx = IDX_RE.search(full_stmt)
            if m_idx:
                i_name = m_idx.group(1).lower()
                s_name = m_idx.group(2).lower()
                t_name = m_idx.group(3).lower()
                blocks.append({
                    "sql": full_stmt,
                    "object_type": "INDEX",
                    "schema": s_name,
                    "table": t_name,
                    "object_name": i_name,
                    "raw": f"INDEX {s_name}.{i_name} on {s_name}.{t_name}"
                })
                continue

            m_att = ATTACH_RE.search(full_stmt)
            if m_att:
                s_parent = m_att.group(1).lower()
                parent_i = m_att.group(2).lower()
                s_child = m_att.group(3).lower()
                child_i = m_att.group(4).lower()
                blocks.append({
                    "sql": full_stmt,
                    "object_type": "INDEX_ATTACH",
                    "schema": s_child,
                    "table": "",
                    "object_name": child_i,
                    "raw": f"ATTACH INDEX {s_parent}.{parent_i} <- {s_child}.{child_i}"
                })
                continue

            # Stage 3: Foreign Keys
            m_fk = FK_RE.search(full_stmt)
            if m_fk:
                s_name = m_fk.group(1).lower()
                t_name = m_fk.group(2).lower()
                c_name = m_fk.group(3).lower()
                blocks.append({
                    "sql": full_stmt,
                    "object_type": "FK",
                    "schema": s_name,
                    "table": t_name,
                    "object_name": c_name,
                    "raw": f"FK {s_name}.{c_name} on {s_name}.{t_name}"
                })
                continue

            # Stage 4: Validate Foreign Keys
            m_val = VAL_RE.search(full_stmt)
            if m_val:
                s_name = m_val.group(1).lower()
                t_name = m_val.group(2).lower()
                c_name = m_val.group(3).lower()
                blocks.append({
                    "sql": full_stmt,
                    "object_type": "FK_VALIDATE",
                    "schema": s_name,
                    "table": t_name,
                    "object_name": c_name,
                    "raw": f"VALIDATE FK {s_name}.{c_name} on {s_name}.{t_name}"
                })
                continue

            # Fallback
            blocks.append({
                "sql": full_stmt,
                "object_type": "OTHER",
                "schema": "",
                "table": "",
                "object_name": "other",
                "raw": full_stmt[:60]
            })

    return blocks
