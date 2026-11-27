#!/usr/bin/env python3
"""
sweep_fwd_ddl.py - `sweep.py fwd-ddl`: the forward-engineering (FE) SQL files, applied by the Database team in SILVER and then moved on to gold_backup and gold. They never run in bronze,
which stays the raw mirror of the source; `promote` bridges the difference (the rules below are applied while bronze deltas enter silver).

FE asks for tables to be created and Oracle / VSAM tables to be altered (add / drop / rename columns, retype ...). The changes are
SQL files in `fwd_ddl/` (STRUCTURE ONLY) named `V<number>__<description>.sql`, applied in numeric order, once per database:

  * a file that was applied is IMMUTABLE (SHA-256 in sweep_ctl.fwd_ddl_log); a changed file is refused - a correction is a new file;
  * ONE transaction per file (`-- sweep:no-transaction` for statements that cannot run in one); the catalog of the database is
    snapshotted before and after the file inside the same transaction and the difference is stored in sweep_ctl.fwd_ddl_changes;
  * data statements (INSERT / UPDATE / DELETE / COPY / TRUNCATE / MERGE, CREATE TABLE AS with data) are refused without --allow-data;
  * pragmas (`-- sweep:layers`, `ignore-column`, `rename-column`, `default-column`, `skip-table`, `no-transaction`) register the
    rules `promote` reads from sweep_ctl.fwd_ddl_rules of the target layer, so a bronze delta keeps loading into silver after the change;
  * the compatibility check compares, for every table the file touches, the SOURCE columns (Oracle: the Step 01 metadata; VSAM: the
    Step 02 column map) with the table after the file and BLOCKs the apply when the next delta load would fail;
  * `fwd-ddl promote` applies to the next layer the files the previous layer applied (the layers are separate databases).
"""
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import sweep_audit
import sweep_common as c
from sweep_common import CTL_SCHEMA, clip_error, get_logger, lit, norm_col, query_db_dicts, run_psql, run_psql_script
from sweep_layers import LayerError, chain, ensure_layer, get_layer, use_layer, verify_database
from sweep_targeting import Selector

logger = get_logger("sweep_fwd_ddl", "sweep_fwd_ddl")

FWD_DIR = Path(os.environ.get("SWEEP_FWD_DDL_DIR") or (c.BASE_DIR / "fwd_ddl"))
FILE_RE = re.compile(r"^V(\d+)__(.+)\.sql$")
PRAGMA_RE = re.compile(r"^\s*--\s*sweep:([\w-]+)\s*(.*?)\s*$")
DATA_WORDS = {"INSERT", "UPDATE", "DELETE", "COPY", "TRUNCATE", "MERGE"}
TX_WORDS = {"BEGIN", "COMMIT", "ROLLBACK", "END", "START", "SAVEPOINT", "RELEASE", "ABORT"}
SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "sweep_ctl", "sweep_delta", "sweep_meta")
RULE_KINDS = ("ignore-column", "rename-column", "default-column", "skip-table")
LOCK_RETRIES = 5


class FwdError(RuntimeError):
    """A refused or failed FE operation (the message says why)."""


# -----------------------------------------------------------------------------
# parsing a file
# -----------------------------------------------------------------------------
def split_statements(text):
    """[(statement text, first line number)] - splits on ';' outside comments, quotes and dollar quotes."""
    out, i, n, start, line, start_line = [], 0, len(text), 0, 1, 1
    while i < n:
        ch = text[i]
        if ch == "\n":
            line += 1
        if text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if text.startswith("/*", i):
            depth, i = 1, i + 2
            while i < n and depth:
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    line += text[i] == "\n"
                    i += 1
            continue
        if ch in "'\"":
            i += 1
            while i < n:
                if text[i] == "\n":
                    line += 1
                if text[i] == ch:
                    if i + 1 < n and text[i + 1] == ch:
                        i += 2
                        continue
                    break
                i += 1
            i += 1
            continue
        if ch == "$":
            m = re.match(r"\$(\w*)\$", text[i:])
            if m:
                tag = m.group(0)
                j = text.find(tag, i + len(tag))
                j = n if j < 0 else j + len(tag)
                line += text.count("\n", i, j)
                i = j
                continue
        if ch == ";":
            stmt = text[start:i + 1]
            if strip_comments(stmt).strip(" \n\t\r;"):
                out.append((stmt.strip(), start_line + _code_offset(stmt)))
            start, start_line = i + 1, line
        i += 1
    tail = text[start:]
    if strip_comments(tail).strip(" \n\t\r;"):
        out.append((tail.strip(), start_line + _code_offset(tail)))
    return out


def _code_offset(chunk):
    """Number of lines before the first line of real code in a chunk (comments and blank lines in front do not count)."""
    masked = re.sub(r"--[^\n]*", lambda m: " " * len(m.group(0)), chunk)
    masked = re.sub(r"/\*.*?\*/", lambda m: re.sub(r"[^\n]", " ", m.group(0)), masked, flags=re.S)
    lead = len(masked) - len(masked.lstrip())
    return masked[:lead].count("\n")


def strip_comments(text):
    """The statement without comments (string contents are kept)."""
    out, i, n = [], 0, len(text)
    while i < n:
        if text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif text[i] in "'\"":
            ch, j = text[i], i + 1
            while j < n:
                if text[j] == ch:
                    if j + 1 < n and text[j + 1] == ch:
                        j += 2
                        continue
                    break
                j += 1
            out.append(text[i:j + 1])
            i = j + 1
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


def blank_strings(text):
    """Comments and the CONTENT of quoted strings / dollar quotes removed: what is left is code only."""
    t = strip_comments(text)
    t = re.sub(r"\$(\w*)\$.*?\$\1\$", "''", t, flags=re.S)
    t = re.sub(r"'(?:[^']|'')*'", "''", t)
    return re.sub(r'"(?:[^"]|"")*"', '""', t)


def classify_statement(stmt):
    """('data' | 'transaction' | 'ddl', warnings). Data statements need --allow-data; destructive DDL only produces a warning."""
    code = blank_strings(stmt).strip()
    words = re.findall(r"[A-Za-z_]+", code.upper())
    first = words[0] if words else ""
    warns = []
    if first in TX_WORDS:
        return "transaction", warns
    if first in DATA_WORDS:
        return "data", warns
    if first == "WITH" and re.search(r"\b(INSERT|UPDATE|DELETE|MERGE)\b", code, re.I):
        return "data", warns
    if first == "SELECT" and re.search(r"\bINTO\s+(?!STRICT\b)[\w\"]", code, re.I):
        return "data", warns                                   # SELECT ... INTO creates a table with data
    if first == "CREATE" and re.search(r"\bAS\s+(SELECT|WITH|VALUES|TABLE)\b", code, re.I) and not re.search(r"WITH\s+NO\s+DATA", code, re.I) \
            and re.search(r"\bCREATE\s+(\w+\s+)*TABLE\b", code, re.I):
        return "data", warns
    if first == "DO" and re.search(r"\b(INSERT\s+INTO|UPDATE\s+\S+\s+SET|DELETE\s+FROM|TRUNCATE|COPY)\b", strip_comments(stmt), re.I):
        return "data", warns
    if first == "DROP":
        warns.append("destructive: DROP " + " ".join(words[1:3]).lower())
    elif first == "ALTER" and re.search(r"\bDROP\s+(COLUMN|CONSTRAINT)\b", code, re.I):
        warns.append("destructive: ALTER ... DROP " + re.search(r"\bDROP\s+(COLUMN|CONSTRAINT)\b", code, re.I).group(1).lower())
    if first == "ALTER" and re.search(r"\bALTER\s+(COLUMN\s+)?\S+\s+(SET\s+DATA\s+)?TYPE\b", code, re.I):
        warns.append("type change: existing values must convert (a USING clause may be needed)")
    if first == "ALTER" and re.search(r"\bRENAME\b", code, re.I):
        warns.append("rename: loaders need a rename-column / skip-table rule for source files")
    return "ddl", warns


def _name_parts(text, expect):
    parts = [p.strip().strip('"').lower() for p in text.strip().split(".")]
    if len(parts) != expect or any(not p for p in parts):
        raise FwdError(f"expected {expect} dotted names (schema.table{'.column' if expect == 3 else ''}), got {text!r}")
    return parts


@dataclass
class FwdFile:
    name: str
    order: int
    path: Path
    text: str
    checksum: str
    layers: tuple = ("silver", "gold_backup", "gold")
    rules: list = field(default_factory=list)          # (rule, schema, table, column, new_name, expression)
    no_transaction: bool = False
    statements: list = field(default_factory=list)     # (text, line)


def parse_file(path):
    path = Path(path)
    m = FILE_RE.match(path.name)
    if not m:
        raise FwdError(f"{path.name}: file names must be V<number>__<description>.sql")
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    f = FwdFile(path.name, int(m.group(1)), path, text, hashlib.sha256(raw).hexdigest())
    for ln, line in enumerate(text.splitlines(), 1):
        pm = PRAGMA_RE.match(line)
        if not pm:
            if re.match(r"^\s*\\", line):
                raise FwdError(f"{path.name}:{ln}: psql meta-commands are not allowed in FE files")
            continue
        kind, arg = pm.group(1), pm.group(2)
        try:
            if kind == "layers":
                names = tuple(x.strip().lower() for x in arg.split(",") if x.strip())
                known = [l.name for l in chain()]
                bad = [x for x in names if x not in known]
                if not names or bad:
                    raise FwdError(f"unknown layer(s) {', '.join(bad) or arg!r}: choose from {', '.join(known)}")
                f.layers = names
            elif kind == "ignore-column":
                s, t, col = _name_parts(arg, 3)
                f.rules.append(("ignore-column", s, t, col, None, None))
            elif kind == "rename-column":
                left, sep, new = arg.partition("->")
                if not sep or not new.strip():
                    raise FwdError("use: -- sweep:rename-column schema.table.old_name -> new_name")
                s, t, col = _name_parts(left, 3)
                f.rules.append(("rename-column", s, t, col, new.strip().strip('"').lower(), None))
            elif kind == "default-column":
                left, sep, expr = arg.partition("=")
                if not sep or not expr.strip():
                    raise FwdError("use: -- sweep:default-column schema.table.column = <SQL expression>")
                s, t, col = _name_parts(left, 3)
                f.rules.append(("default-column", s, t, col, None, expr.strip()))
            elif kind == "skip-table":
                s, t = _name_parts(arg, 2)
                f.rules.append(("skip-table", s, t, None, None, None))
            elif kind == "no-transaction":
                f.no_transaction = True
            else:
                raise FwdError(f"unknown pragma sweep:{kind}")
        except FwdError as e:
            raise FwdError(f"{path.name}:{ln}: {e}")
    f.statements = split_statements(text)
    if not f.no_transaction:
        for stmt, ln in f.statements:
            if classify_statement(stmt)[0] == "transaction":
                raise FwdError(f"{path.name}:{ln}: transaction control (BEGIN / COMMIT ...) is not allowed: the tool wraps every file in one "
                               f"transaction (use -- sweep:no-transaction for statements that cannot run in one)")
    return f


def list_files(directory=None):
    """Parsed FE files in numeric order. Duplicate numbers are an error."""
    d = Path(directory or FWD_DIR)
    if not d.is_dir():
        return []
    files = [parse_file(p) for p in sorted(d.glob("*.sql")) if FILE_RE.match(p.name)]
    files.sort(key=lambda f: (f.order, f.name))
    seen = {}
    for f in files:
        if f.order in seen:
            raise FwdError(f"{f.name} and {seen[f.order]} have the same number V{f.order}")
        seen[f.order] = f.name
    return files


# -----------------------------------------------------------------------------
# state of a layer
# -----------------------------------------------------------------------------
def layer_log(layer):
    """{file_name: row} of sweep_ctl.fwd_ddl_log of the layer's database; None when the database is not migrated to version 2."""
    try:
        rows = query_db_dicts(f"SELECT file_name, order_no, checksum, status, applied_by, applied_at, message FROM {CTL_SCHEMA}.fwd_ddl_log",
                              dbname=layer.database)
    except RuntimeError:
        return None
    return {r["file_name"]: r for r in rows}


def layer_state(layer, files):
    """[(file, state)] state: APPLIED / PENDING / FAILED / CHANGED (applied, but the file on disk differs now). None = not migrated."""
    log = layer_log(layer)
    if log is None:
        return None
    out = []
    for f in files:
        row = log.get(f.name)
        if row is None:
            out.append((f, "PENDING", None))
        elif row["status"] != "APPLIED":
            out.append((f, "FAILED", row))
        elif row["checksum"] != f.checksum:
            out.append((f, "CHANGED", row))
        else:
            out.append((f, "APPLIED", row))
    return out


def check_immutable(layer, files):
    """Refuses (FwdError) when a file that was applied in this layer changed on disk."""
    st = layer_state(layer, files) or []
    changed = [f.name for f, s, _ in st if s == "CHANGED"]
    if changed:
        raise FwdError(f"layer {layer.name}: {', '.join(changed)} was applied and has changed since (an applied FE file is immutable: "
                       f"put the correction in a NEW file V<next number>__...sql)")


def pending_files(layer, files=None):
    files = list_files() if files is None else files
    st = layer_state(layer, files)
    if st is None:
        raise FwdError(f"layer {layer.name} ({layer.database}) is not migrated to schema version 2: run `python3 sweep.py migrate --layer {layer.name}`")
    return [f for f, s, _ in st if s in ("PENDING", "FAILED")]


def pending_count(layer_name):
    """Number of pending FE files of a layer (None when the layer cannot be read) - used by promote, chain and the monitor."""
    try:
        return len(pending_files(get_layer(layer_name)))
    except (FwdError, LayerError):
        return None


# -----------------------------------------------------------------------------
# source columns (Oracle: Step 01 metadata, VSAM: Step 02 column map) for the compatibility check
# -----------------------------------------------------------------------------
def candidate_tables(f):
    """(schema, table) names a file mentions (qualified names; unqualified ones are not resolved)."""
    code = blank_strings("\n".join(s for s, _ in f.statements))
    found = {(a.lower(), b.lower()) for a, b in re.findall(r'"?([A-Za-z_][\w$]*)"?\s*\.\s*"?([A-Za-z_][\w$]*)"?', code)}
    found |= {(r[1], r[2]) for r in f.rules}
    return found


def source_columns(candidates):
    """{(schema, table): {column}} for the candidate tables that are loaded from source files."""
    import csv
    out = {}
    try:
        with open(c.META_TABLES_CSV, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                key = ((r.get("owner") or "").strip().lower(), norm_col(r.get("table_name", "")))
                if key in candidates:
                    out.setdefault(key, set()).add(norm_col(r.get("column_name", "")))
    except OSError:
        pass
    try:
        from vsam.common_colmap import COLMAP_FILE
        lm_path = c.VSAM_OUTPUTS_DIR / "vsam_02_load_matrix.csv"
        by_source, tables = {}, set()
        with open(lm_path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                by_source.setdefault(((r.get("source_company") or "").upper(), (r.get("source_filename") or "").upper()), set()).add(
                    (r.get("target_table") or "").strip().lower())
                tables.add((r.get("target_table") or "").strip().lower())
        cols_of = {}
        with open(COLMAP_FILE, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                col = (r.get("table_column") or "").strip().lower()
                if col:
                    for t in by_source.get(((r.get("source_company") or "").upper(), (r.get("source_filename") or "").upper()), ()):
                        cols_of.setdefault(t, set()).add(col)
        for (s, t) in candidates:
            if t in cols_of:
                out.setdefault((s, t), set()).update(cols_of[t])
    except OSError:
        pass
    return out


# -----------------------------------------------------------------------------
# the script that applies / rehearses a file
# -----------------------------------------------------------------------------
_SNAP = """
CREATE TEMP TABLE {name} AS
SELECT 'table' AS kind, n.nspname AS schema, c.relname AS object, c.relname AS tbl, jsonb_build_object('relkind', c.relkind::text) AS detail
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition AND {sf}
UNION ALL
SELECT 'column', n.nspname, c.relname || '.' || a.attname, c.relname,
       jsonb_build_object('type', format_type(a.atttypid, a.atttypmod), 'notnull', a.attnotnull, 'default', pg_get_expr(d.adbin, d.adrelid),
                          'identity', a.attidentity::text, 'generated', a.attgenerated::text)
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
 WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p') AND NOT c.relispartition AND {sf}
UNION ALL
SELECT 'constraint', n.nspname, c.relname || '.' || k.conname, c.relname,
       jsonb_build_object('type', k.contype::text, 'definition', pg_get_constraintdef(k.oid))
  FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE NOT c.relispartition AND {sf}
UNION ALL
SELECT 'index', n.nspname, c.relname || '.' || ic.relname, c.relname, jsonb_build_object('definition', pg_get_indexdef(i.indexrelid))
  FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE NOT c.relispartition AND {sf}
UNION ALL
SELECT 'view', n.nspname, c.relname, NULL, jsonb_build_object('definition_md5', md5(pg_get_viewdef(c.oid)))
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('v', 'm') AND {sf}
UNION ALL
SELECT 'trigger', n.nspname, c.relname || '.' || t.tgname, c.relname, jsonb_build_object('definition', pg_get_triggerdef(t.oid))
  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE NOT t.tgisinternal AND {sf}
UNION ALL
SELECT 'sequence', n.nspname, c.relname, NULL, '{{}}'::jsonb
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'S' AND {sf};
"""
_SF = ("n.nspname NOT IN (" + ", ".join(f"'{s}'" for s in SYSTEM_SCHEMAS) + ") AND n.nspname NOT LIKE 'pg\\_%'")


def snapshot_sql(name):
    return _SNAP.format(name=name, sf=_SF)


def _values(rows, width):
    return ", ".join("(" + ", ".join(lit(v) for v in r) + ")" for r in rows)


def compat_sql(n, src_cols, rules):
    """
    Statements that run AFTER the file inside its transaction and emit one @@FWDCHK@@ line per touched table that is loaded from
    source files: OK, or BLOCK with the reason (a) a source column is neither in the table nor covered by ignore-column / rename-column,
    (b) a NOT NULL column without default is absent from the source and has no default-column rule, (c) the table was dropped and no
    skip-table rule exists.
    """
    src_rows = [(s, t, col) for (s, t), cols in sorted(src_cols.items()) for col in sorted(cols)]
    rule_rows = [(r[0], r[1], r[2], r[3] or "", r[4] or "", r[5] or "") for r in rules]
    out = [f"CREATE TEMP TABLE _src{n} (s text, t text, col text);\nCREATE TEMP TABLE _rl{n} (rule text, s text, t text, col text, newname text, expr text);\n"]
    if src_rows:
        out.append(f"INSERT INTO _src{n} VALUES {_values(src_rows, 3)};\n")
    if rule_rows:
        out.append(f"INSERT INTO _rl{n} VALUES {_values(rule_rows, 6)};\n")
    out.append(f"""
CREATE TEMP TABLE _tg{n} AS
SELECT n.nspname AS s, c.relname AS t, a.attname AS col, a.attnotnull AS nn, a.atthasdef AS hd, a.attidentity::text AS idn, a.attgenerated::text AS gen
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p') AND (n.nspname, c.relname) IN (SELECT s, t FROM _src{n});
CREATE TEMP TABLE _touched{n} AS
SELECT DISTINCT s.s, s.t FROM (SELECT DISTINCT s, t FROM _src{n}) s
  JOIN _fd{n} d ON d.schema = s.s AND d.tbl = s.t;
CREATE TEMP TABLE _chk{n} AS
SELECT t.s, t.t, 'BLOCK'::text AS status, 'table was dropped and no skip-table rule exists'::text AS reason
  FROM _touched{n} t WHERE NOT EXISTS (SELECT 1 FROM _tg{n} g WHERE g.s = t.s AND g.t = t.t)
    AND NOT EXISTS (SELECT 1 FROM _rl{n} r WHERE r.rule = 'skip-table' AND r.t = t.t AND r.s IN (t.s, '*'))
UNION ALL
SELECT t.s, t.t, 'BLOCK', 'source column ' || sc.col || ' is neither in the table nor covered by an ignore-column / rename-column rule'
  FROM _touched{n} t JOIN _src{n} sc ON sc.s = t.s AND sc.t = t.t
 WHERE EXISTS (SELECT 1 FROM _tg{n} g WHERE g.s = t.s AND g.t = t.t)
   AND NOT EXISTS (SELECT 1 FROM _tg{n} g WHERE g.s = t.s AND g.t = t.t AND g.col = sc.col)
   AND NOT EXISTS (SELECT 1 FROM _rl{n} r WHERE r.rule = 'ignore-column' AND r.t = t.t AND r.col = sc.col AND r.s IN (t.s, '*'))
   AND NOT EXISTS (SELECT 1 FROM _rl{n} r JOIN _tg{n} g ON g.s = t.s AND g.t = t.t AND g.col = r.newname
                    WHERE r.rule = 'rename-column' AND r.t = t.t AND r.col = sc.col AND r.s IN (t.s, '*'))
UNION ALL
SELECT t.s, t.t, 'BLOCK', 'NOT NULL column ' || g.col || ' without default is absent from the source and has no default-column rule'
  FROM _touched{n} t JOIN _tg{n} g ON g.s = t.s AND g.t = t.t
 WHERE g.nn AND NOT g.hd AND g.idn = '' AND g.gen = ''
   AND NOT EXISTS (SELECT 1 FROM _src{n} sc WHERE sc.s = t.s AND sc.t = t.t AND sc.col = g.col)
   AND NOT EXISTS (SELECT 1 FROM _rl{n} r JOIN _src{n} sc ON sc.s = t.s AND sc.t = t.t AND sc.col = r.col
                    WHERE r.rule = 'rename-column' AND r.t = t.t AND r.newname = g.col AND r.s IN (t.s, '*'))
   AND NOT EXISTS (SELECT 1 FROM _rl{n} r WHERE r.rule = 'default-column' AND r.t = t.t AND r.col = g.col AND r.s IN (t.s, '*'));
INSERT INTO _chk{n} SELECT t.s, t.t, 'OK', '' FROM _touched{n} t
 WHERE NOT EXISTS (SELECT 1 FROM _chk{n} k WHERE k.s = t.s AND k.t = t.t);
SELECT '@@FWDCHK@@' || json_build_object('schema', s, 't', t, 'status', status, 'reason', reason) FROM _chk{n} ORDER BY s, t, status;
""")
    return "".join(out)


def build_section(f, layer, n, *, src_cols, rules, apply_, force, allow_data, selector=None):
    """The SQL of one file inside an open transaction: snapshot, statements, snapshot, diff, compatibility check, optional bookkeeping."""
    s = [f"-- ===== {f.name}\nCREATE TEMP TABLE _t0_{n} AS SELECT clock_timestamp() AS t;\n", snapshot_sql(f"_fb{n}")]
    for stmt, ln in f.statements:
        s.append(f"{stmt}\n")
    s.append(snapshot_sql(f"_fa{n}"))
    s.append(f"""CREATE TEMP TABLE _fd{n} AS
SELECT coalesce(a.kind, b.kind) AS kind, coalesce(a.schema, b.schema) AS schema, coalesce(a.object, b.object) AS object,
       coalesce(a.tbl, b.tbl) AS tbl,
       CASE WHEN b.kind IS NULL THEN 'added' WHEN a.kind IS NULL THEN 'dropped' ELSE 'changed' END AS action,
       jsonb_build_object('before', b.detail, 'after', a.detail) AS detail
  FROM _fa{n} a FULL JOIN _fb{n} b ON a.kind = b.kind AND a.schema = b.schema AND a.object = b.object
 WHERE b.kind IS NULL OR a.kind IS NULL OR a.detail IS DISTINCT FROM b.detail;
SELECT '@@FWDCHG@@' || json_build_object('kind', kind, 'action', action, 'schema', schema, 'object', object, 'detail', detail) FROM _fd{n} ORDER BY kind, schema, object;
""")
    # audit post-step (6.3.0): every table the file created or altered carries the six canonical audit columns and the audit trigger
    # (the FE file itself stays untouched; it runs in silver only). It runs after the diff, so the columns are not listed as FE changes.
    s.append(sweep_audit.ensure_for_query_sql(f"SELECT schema AS s, tbl AS t FROM _fd{n} WHERE kind IN ('table', 'column') AND action <> 'dropped' AND tbl IS NOT NULL"))
    s.append(compat_sql(n, src_cols, rules))
    if not force and apply_:
        s.append(f"DO $blk$ BEGIN IF EXISTS (SELECT 1 FROM _chk{n} WHERE status = 'BLOCK') THEN RAISE EXCEPTION 'FWD_BLOCK: compatibility check failed for {f.name}'; END IF; END $blk$;\n")
    if apply_:
        s.append(f"""INSERT INTO {CTL_SCHEMA}.fwd_ddl_changes (file_name, kind, action, schema_name, object_name, detail)
SELECT {lit(f.name)}, kind, action, schema, object, detail FROM _fd{n};
""")
        for r in f.rules:
            s.append(f"INSERT INTO {CTL_SCHEMA}.fwd_ddl_rules (file_name, rule, schema_name, table_name, column_name, new_name, expression) "
                     f"VALUES ({lit(f.name)}, {lit(r[0])}, {lit(r[1])}, {lit(r[2])}, {lit(r[3])}, {lit(r[4])}, {lit(r[5])});\n")
            s.append(f"INSERT INTO {CTL_SCHEMA}.fwd_ddl_changes (file_name, kind, action, schema_name, object_name, detail) VALUES "
                     f"({lit(f.name)}, 'rule', 'added', {lit(r[1])}, {lit(r[2] + ('.' + r[3] if r[3] else ''))}, "
                     f"{lit(json.dumps({'rule': r[0], 'new_name': r[4], 'expression': r[5]}))}::jsonb);\n")
        s.append(f"INSERT INTO {CTL_SCHEMA}.fwd_ddl_log (file_name, order_no, checksum, status, message, duration_sec) VALUES "
                 f"({lit(f.name)}, {f.order}, {lit(f.checksum)}, 'APPLIED', NULL, "
                 f"extract(epoch from clock_timestamp() - (SELECT t FROM _t0_{n}))) "
                 f"ON CONFLICT (file_name) DO UPDATE SET order_no = EXCLUDED.order_no, checksum = EXCLUDED.checksum, status = 'APPLIED', "
                 f"message = NULL, applied_by = CURRENT_USER, applied_at = now(), duration_sec = EXCLUDED.duration_sec;\n")
    return "".join(s)


def timeout_options(layer):
    sql = ""
    if layer.lock_timeout and str(layer.lock_timeout) != "0":
        sql += f"SET LOCAL lock_timeout = {lit(layer.lock_timeout)};\n"
    if layer.statement_timeout and str(layer.statement_timeout) != "0":
        sql += f"SET LOCAL statement_timeout = {lit(layer.statement_timeout)};\n"
    return sql


def registered_rules():
    """Rules already registered in the connected database (used together with the pragmas of the file being applied)."""
    try:
        rows = query_db_dicts(f"SELECT rule, schema_name, table_name, column_name, new_name, expression FROM {CTL_SCHEMA}.fwd_ddl_rules")
    except RuntimeError:
        return []
    return [(r["rule"], r["schema_name"], r["table_name"], r["column_name"] or None, r["new_name"] or None, r["expression"] or None) for r in rows]


def parse_markers(stdout):
    changes, checks = [], []
    for line in (stdout or "").splitlines():
        if line.startswith("@@FWDCHG@@"):
            changes.append(json.loads(line[10:]))
        elif line.startswith("@@FWDCHK@@"):
            checks.append(json.loads(line[10:]))
    return changes, checks


def refuse_data(f, allow_data):
    """Structure-only: data statements are refused unless --allow-data."""
    if allow_data:
        return
    for stmt, ln in f.statements:
        if classify_statement(stmt)[0] == "data":
            first = " ".join(strip_comments(stmt).split())[:70]
            raise FwdError(f"{f.name}:{ln}: FE files are STRUCTURE ONLY - this statement inserts or changes data: {first!r} "
                           f"(--allow-data overrides; data belongs to the application in gold, not to FE files)")


def warnings_of(f):
    out = []
    for stmt, ln in f.statements:
        for w in classify_statement(stmt)[1]:
            out.append(f"{f.name}:{ln}: {w}")
    return out


def _run(script, layer, retries=LOCK_RETRIES):
    """Runs a script on the connected database; retries with backoff when a lock timeout hit (short lock_timeout on gold)."""
    res = None
    for attempt in range(retries):
        res = run_psql_script(script, options="-c statement_timeout=0", timeout=3600)
        text = (res.stderr or "") + (res.stdout or "")
        if res.returncode == 0 or "lock timeout" not in text.lower():
            return res
        wait = min(2 ** attempt, 30)
        logger.warning(f"lock timeout on {layer.name}; retry {attempt + 1}/{retries - 1} in {wait}s")
        time.sleep(wait)
    return res


def apply_file(f, layer, *, dry_run, force=False, allow_data=False, selector=None):
    """
    Applies one file to the connected database (use_layer(layer) must be active). Returns a result dict:
    {file, status: APPLIED|DRY_RUN|BLOCKED|FAILED|NOT_APPLICABLE, changes, checks, warnings, message}.
    """
    res = {"file": f.name, "status": "APPLIED", "changes": [], "checks": [], "warnings": warnings_of(f), "message": ""}
    if layer.name not in f.layers:
        res["status"] = "NOT_APPLICABLE"
        res["message"] = f"layers pragma ({', '.join(f.layers)}) does not include {layer.name}"
        if not dry_run:
            run_psql(f"INSERT INTO {CTL_SCHEMA}.fwd_ddl_log (file_name, order_no, checksum, status, message, duration_sec) VALUES "
                     f"({lit(f.name)}, {f.order}, {lit(f.checksum)}, 'APPLIED', {lit('not applicable: ' + res['message'])}, 0) "
                     f"ON CONFLICT (file_name) DO UPDATE SET checksum = EXCLUDED.checksum, status = 'APPLIED', message = EXCLUDED.message, applied_at = now()")
        return res
    refuse_data(f, allow_data)
    rules = list({*registered_rules(), *f.rules})
    src_cols = source_columns(candidate_tables(f))
    if f.no_transaction:
        return _apply_no_transaction(f, layer, res, rules, src_cols, dry_run=dry_run, force=force)
    body = build_section(f, layer, 0, src_cols=src_cols, rules=rules, apply_=not dry_run, force=force, allow_data=allow_data)
    script = ("\\set ON_ERROR_STOP on\n\\pset tuples_only on\n\\pset format unaligned\nBEGIN;\n" + timeout_options(layer) + body +
              ("ROLLBACK;\n" if dry_run else "COMMIT;\n"))
    out = _run(script, layer)
    res["changes"], res["checks"] = parse_markers(out.stdout)
    blocks = [k for k in res["checks"] if k["status"] == "BLOCK"]
    if out.returncode != 0:
        err = clip_error(out.stderr or out.stdout or "psql failed")
        if "FWD_BLOCK" in err:
            res.update(status="BLOCKED", message="compatibility check: " + "; ".join(f"{k['schema']}.{k['t']}: {k['reason']}" for k in blocks))
        else:
            res.update(status="FAILED", message=err)
            if not dry_run:
                _record_failure(f, err)
        return res
    if dry_run:
        res["status"] = "DRY_RUN"
        if blocks:
            res["status"] = "BLOCKED" if not force else "DRY_RUN"
            res["message"] = "compatibility check: " + "; ".join(f"{k['schema']}.{k['t']}: {k['reason']}" for k in blocks)
    return res


def _apply_no_transaction(f, layer, res, rules, src_cols, *, dry_run, force):
    if dry_run:
        res.update(status="DRY_RUN", message="no-transaction file: its statements cannot be rehearsed (they run outside a transaction)")
        return res
    pre = ("\\set ON_ERROR_STOP on\n\\pset tuples_only on\n\\pset format unaligned\n" +
           (f"SET lock_timeout = {lit(layer.lock_timeout)};\n" if layer.lock_timeout and str(layer.lock_timeout) != "0" else "") +
           snapshot_sql("_fb0").replace("CREATE TEMP TABLE", "CREATE TEMP TABLE"))
    script = pre + "".join(f"{stmt}\n" for stmt, _ in f.statements)
    body = build_section(f, layer, 0, src_cols=src_cols, rules=rules, apply_=True, force=True, allow_data=True)
    # the statements already ran: take the diff / compatibility / bookkeeping part of the section only
    tail = body[body.index(snapshot_sql("_fa0")):]
    script += "CREATE TEMP TABLE _t0_0 AS SELECT clock_timestamp() AS t;\n" + tail
    out = _run(script, layer)
    res["changes"], res["checks"] = parse_markers(out.stdout)
    if out.returncode != 0:
        err = clip_error(out.stderr or out.stdout or "psql failed")
        res.update(status="FAILED", message=err)
        _record_failure(f, err)
        return res
    blocks = [k for k in res["checks"] if k["status"] == "BLOCK"]
    if blocks:
        res.update(status="BLOCKED", message="applied (no-transaction file) but the compatibility check found: " +
                   "; ".join(f"{k['schema']}.{k['t']}: {k['reason']}" for k in blocks))
    return res


def _record_failure(f, message):
    try:
        run_psql(f"INSERT INTO {CTL_SCHEMA}.fwd_ddl_log (file_name, order_no, checksum, status, message) VALUES "
                 f"({lit(f.name)}, {f.order}, {lit(f.checksum)}, 'FAILED', {lit(message)}) "
                 f"ON CONFLICT (file_name) DO UPDATE SET checksum = EXCLUDED.checksum, status = 'FAILED', message = EXCLUDED.message, applied_at = now()")
    except RuntimeError as e:
        logger.error(f"could not record the failure of {f.name}: {e}")


def dry_run_files(files, layer, *, force=False, allow_data=False):
    """Rehearses several pending files in ONE transaction in order (a later file may depend on an earlier one), then rolls back."""
    results, sections = [], []
    rules0 = registered_rules()
    for n, f in enumerate(files):
        res = {"file": f.name, "status": "DRY_RUN", "changes": [], "checks": [], "warnings": warnings_of(f), "message": ""}
        results.append(res)
        if layer.name not in f.layers:
            res.update(status="NOT_APPLICABLE", message=f"layers pragma ({', '.join(f.layers)}) does not include {layer.name}")
            continue
        refuse_data(f, allow_data)
        if f.no_transaction:
            res["message"] = "no-transaction file: not rehearsed"
            continue
        rules0 = list({*rules0, *f.rules})
        sections.append((n, build_section(f, layer, n, src_cols=source_columns(candidate_tables(f)), rules=rules0, apply_=False, force=True,
                                           allow_data=allow_data)))
    if not sections:
        return results
    # one transaction; every section prints its own markers after a separator line so they can be told apart
    script = "\\set ON_ERROR_STOP on\n\\pset tuples_only on\n\\pset format unaligned\nBEGIN;\n" + timeout_options(layer)
    for n, body in sections:
        script += f"SELECT '@@FWDFILE@@{n}';\n" + body
    script += "ROLLBACK;\n"
    out = _run(script, layer)
    current, buckets = None, {}
    for line in (out.stdout or "").splitlines():
        if line.startswith("@@FWDFILE@@"):
            current = int(line[11:])
            buckets[current] = []
        elif current is not None:
            buckets[current].append(line)
    for n, lines in buckets.items():
        results[n]["changes"], results[n]["checks"] = parse_markers("\n".join(lines))
        blocks = [k for k in results[n]["checks"] if k["status"] == "BLOCK"]
        if blocks:
            results[n].update(status="BLOCKED", message="compatibility check: " + "; ".join(f"{k['schema']}.{k['t']}: {k['reason']}" for k in blocks))
    if out.returncode != 0:
        err = clip_error(out.stderr or out.stdout or "psql failed")
        failing = max(buckets) if buckets else sections[0][0]
        results[failing].update(status="FAILED", message=err)
        for later in results[failing + 1:]:
            if later["status"] == "DRY_RUN":
                later["message"] = "not rehearsed: an earlier file failed"
    return results


# -----------------------------------------------------------------------------
# commands
# -----------------------------------------------------------------------------
def _filter_report(res, sel):
    """--tables / --schemas / --exclude-tables narrow the printed diff and the compatibility lines."""
    if sel is None or not sel.table_selectors:
        return res
    res = dict(res)
    res["changes"] = [ch for ch in res["changes"] if sel.matches(ch.get("schema", ""), (ch.get("object") or "").split(".")[0])]
    res["checks"] = [k for k in res["checks"] if sel.matches(k["schema"], k["t"])]
    return res


def print_results(layer, results, sel=None):
    for r in results:
        r = _filter_report(r, sel)
        print(f"[{layer.name}] {r['status']:<15} {r['file']}" + (f"  - {r['message']}" if r["message"] else ""))
        for w in r["warnings"]:
            print(f"    WARNING {w}")
        for ch in r["changes"]:
            print(f"    {ch['action']:<8} {ch['kind']:<10} {ch['schema']}.{ch['object']}")
        for k in r["checks"]:
            print(f"    compatibility {k['status']:<5} {k['schema']}.{k['t']}" + (f": {k['reason']}" if k["reason"] else ""))


NOT_IN_BRONZE = ("FE files are never applied in {name}: bronze is the raw mirror of the source. Apply them in silver "
                 "(`python3 sweep.py fwd-ddl apply --layer silver`); `fwd-ddl promote` moves them silver -> gold_backup -> gold")


def fe_layers():
    """The layers that get FE files: every layer except the first (bronze)."""
    return [l for l in chain() if l.upstream is not None]


def cmd_status(args):
    """Read-only. Without --layer: every FE layer (silver, gold_backup, gold), and which layers are behind."""
    if getattr(args, "layer", None) and get_layer(args.layer).upstream is None:
        print(NOT_IN_BRONZE.format(name=get_layer(args.layer).name))
        return 0
    layers = [get_layer(args.layer)] if getattr(args, "layer", None) else fe_layers()
    try:
        files = list_files()
    except FwdError as e:
        print(f"Error: {e}")
        return 2
    print(f"{len(files)} FE file(s) in {FWD_DIR}")
    behind, changed_total, unreachable = [], 0, 0
    for lay in layers:
        st = layer_state(lay, files)
        if st is None:
            print(f"== {lay.name} ({lay.database}): not reachable or not migrated to version 2")
            unreachable += 1
            continue
        counts = {k: sum(1 for _, s, _r in st if s == k) for k in ("APPLIED", "PENDING", "FAILED", "CHANGED")}
        print(f"== {lay.name} ({lay.database}): applied {counts['APPLIED']}, pending {counts['PENDING']}, failed {counts['FAILED']}, "
              f"changed-after-apply {counts['CHANGED']}")
        for f, s, row in st:
            if s != "APPLIED":
                print(f"     {s:<8} {f.name}" + (f"  - {row['message']}" if row and row.get("message") else ""))
        if counts["PENDING"] or counts["FAILED"]:
            behind.append(lay.name)
        changed_total += counts["CHANGED"]
    print("layers behind: " + (", ".join(behind) if behind else "none"))
    if changed_total:
        print(f"ERROR: {changed_total} applied file(s) changed on disk (applied FE files are immutable)")
    return 1 if changed_total else 0


def _selected_files(args, files):
    sel = Selector.from_args(args)
    if sel.files:
        wanted = [f for f in files if sel.matches_file(f.name)]
        if not wanted:
            raise FwdError(f"no FE file matches {', '.join(sel.files)}")
        return wanted, sel
    return files, sel


def cmd_apply(args):
    layer = ensure_layer(args, logger, required=True, db=False, ddl_what="fwd-ddl apply")
    if layer.upstream is None:
        print(f"Error: {NOT_IN_BRONZE.format(name=layer.name)}")
        return 2
    dry = bool(getattr(args, "dry_run", False))
    try:
        files = list_files()
        with use_layer(layer):
            verify_database(layer)
            check_immutable(layer, files)
            todo = pending_files(layer, files)
            chosen, sel = _selected_files(args, files)
            if getattr(args, "file", None) or getattr(args, "filename", None):
                names = {f.name for f in chosen}
                earlier = [f.name for f in todo if f.name not in names and any(f.order < g.order for g in chosen if g.name in {x.name for x in todo})]
                if earlier:
                    raise FwdError(f"{', '.join(earlier)} must be applied first (files are applied in numeric order)")
                todo = [f for f in todo if f.name in names]
            if not todo:
                print(f"layer {layer.name}: no pending FE files")
                return 0
            if dry:
                results = dry_run_files(todo, layer, force=getattr(args, "force", False), allow_data=getattr(args, "allow_data", False))
                print_results(layer, results, sel)
                return 1 if any(r["status"] in ("BLOCKED", "FAILED") for r in results) else 0
            results = []
            for f in todo:
                r = apply_file(f, layer, dry_run=False, force=getattr(args, "force", False), allow_data=getattr(args, "allow_data", False))
                results.append(r)
                print_results(layer, [r], sel)
                if r["status"] in ("BLOCKED", "FAILED"):
                    print(f"stopped at {f.name}: later files are not applied")
                    return 1
            return 0
    except (FwdError, LayerError) as e:
        print(f"Error: {e}")
        return 1


def cmd_promote(args):
    """Applies to the next layer the files the previous layer applied successfully, in the same order."""
    try:
        src, tgt = get_layer(args.from_layer), get_layer(args.to_layer)
        if tgt.upstream != src.name:
            raise LayerError(f"fwd-ddl promote moves ONE hop: {tgt.name!r} follows {tgt.upstream!r}, not {src.name!r}")
        if src.upstream is None:
            print(f"Error: {NOT_IN_BRONZE.format(name=src.name)}")
            return 2
        from sweep_layers import require_ddl
        require_ddl(tgt, args, logger, "fwd-ddl promote")
        files = list_files()
        with use_layer(src):
            src_state = layer_state(src, files)
        if src_state is None:
            raise FwdError(f"layer {src.name} is not migrated to schema version 2")
        applied_in_src = [f for f, s, _ in src_state if s == "APPLIED"]
        dry = bool(getattr(args, "dry_run", False))
        with use_layer(tgt):
            verify_database(tgt)
            check_immutable(tgt, files)
            todo = [f for f in pending_files(tgt, files) if f in applied_in_src]
            skipped = [f.name for f in pending_files(tgt, files) if f not in applied_in_src]
            if skipped:
                print(f"not promoted (not applied in {src.name} yet): {', '.join(skipped)}")
            chosen, sel = _selected_files(args, files)
            todo = [f for f in todo if f in chosen]
            if not todo:
                print(f"layer {tgt.name}: nothing to promote from {src.name}")
                return 0
            if dry:
                results = dry_run_files(todo, tgt, force=getattr(args, "force", False), allow_data=getattr(args, "allow_data", False))
                print_results(tgt, results, sel)
                return 1 if any(r["status"] in ("BLOCKED", "FAILED") for r in results) else 0
            for f in todo:
                r = apply_file(f, tgt, dry_run=False, force=getattr(args, "force", False), allow_data=getattr(args, "allow_data", False))
                print_results(tgt, [r], sel)
                if r["status"] in ("BLOCKED", "FAILED"):
                    print(f"stopped at {f.name}")
                    return 1
        return 0
    except (FwdError, LayerError) as e:
        print(f"Error: {e}")
        return 1


def cmd_check(args):
    """Dry-run of every pending file (and the compatibility check) in each layer; nothing is written. gold / gold_backup need --allow-ddl."""
    if getattr(args, "layer", None) and get_layer(args.layer).upstream is None:
        print(f"Error: {NOT_IN_BRONZE.format(name=get_layer(args.layer).name)}")
        return 2
    layers = [get_layer(args.layer)] if getattr(args, "layer", None) else fe_layers()
    try:
        files = list_files()
    except FwdError as e:
        print(f"Error: {e}")
        return 2
    sel = Selector.from_args(args)
    bad = 0
    for lay in layers:
        if not lay.allow_ddl and not getattr(args, "allow_ddl", False):
            print(f"== {lay.name}: skipped (allow_ddl=false: pass --allow-ddl to rehearse the pending files there)")
            continue
        try:
            with use_layer(lay):
                verify_database(lay)
                check_immutable(lay, files)
                todo = pending_files(lay, files)
                if not todo:
                    print(f"== {lay.name}: no pending FE files")
                    continue
                results = dry_run_files(todo, lay, force=True, allow_data=getattr(args, "allow_data", False))
        except (FwdError, LayerError, SystemExit) as e:
            print(f"== {lay.name}: {e}")
            bad += 1
            continue
        print(f"== {lay.name}: {len(todo)} pending file(s)")
        print_results(lay, results, sel)
        bad += sum(1 for r in results if r["status"] in ("BLOCKED", "FAILED"))
    return 1 if bad else 0


# -----------------------------------------------------------------------------
# what applied FE files changed (read by Steps 05 / 07, the validation steps and `layers`)
# -----------------------------------------------------------------------------
@dataclass
class FeIndex:
    rows: list = field(default_factory=list)                 # fwd_ddl_changes rows (dicts), oldest first
    dropped_tables: dict = field(default_factory=dict)       # (schema, table) -> file
    added_tables: dict = field(default_factory=dict)
    changed_tables: dict = field(default_factory=dict)       # any change on the table -> last file
    dropped_columns: dict = field(default_factory=dict)      # (schema, table) -> {column: file}
    added_columns: dict = field(default_factory=dict)
    changed_columns: dict = field(default_factory=dict)
    objects: dict = field(default_factory=dict)              # (kind, schema, object) -> (action, file) for constraints / indexes / views / triggers

    def table_dropped(self, schema, table):
        return (schema.lower(), table.lower()) in self.dropped_tables

    def table_touched(self, schema, table):
        return (schema.lower(), table.lower()) in self.changed_tables


def load_fe_changes(dbname=None):
    """FeIndex of the connected (or named) database; empty when the FE tables do not exist yet. A table dropped and added again is not dropped."""
    try:
        rows = query_db_dicts(f"SELECT file_name, kind, action, schema_name, object_name, detail::text AS detail FROM {CTL_SCHEMA}.fwd_ddl_changes "
                              f"WHERE kind <> 'rule' ORDER BY id", dbname=dbname)
    except RuntimeError:
        return FeIndex()
    idx = FeIndex(rows=rows)
    for r in rows:
        s, o, kind, action, f = (r["schema_name"] or "").lower(), (r["object_name"] or "").lower(), r["kind"], r["action"], r["file_name"]
        tbl = o.split(".")[0]
        if kind != "view" and kind != "sequence":
            idx.changed_tables[(s, tbl)] = f
        if kind == "table":
            if action == "dropped":
                idx.dropped_tables[(s, o)] = f
                idx.added_tables.pop((s, o), None)
            elif action == "added":
                idx.dropped_tables.pop((s, o), None)
                idx.added_tables[(s, o)] = f
        elif kind == "column":
            t, _, col = o.partition(".")
            if action == "dropped":
                idx.dropped_columns.setdefault((s, t), {})[col] = f
            elif action == "added":
                idx.dropped_columns.get((s, t), {}).pop(col, None)
                idx.added_columns.setdefault((s, t), {})[col] = f
            else:
                idx.changed_columns.setdefault((s, t), {})[col] = f
        else:
            idx.objects[(kind, s, o)] = (action, f)
    return idx


def fe_reason(fe, block):
    """Why a generated post-load statement is obsolete because of an applied FE file ('' = still valid)."""
    s, t, o, typ = (block.get("schema") or "").lower(), (block.get("table") or "").lower(), (block.get("object_name") or "").lower(), block.get("object_type", "")
    if t and fe.table_dropped(s, t):
        return f"table {s}.{t} was dropped by {fe.dropped_tables[(s, t)]}"
    kind = {"PK": "constraint", "UNIQUE": "constraint", "FK": "constraint", "CHECK": "constraint", "CONSTRAINT": "constraint",
            "FK_VALIDATE": "constraint", "CHECK_VALIDATE": "constraint", "INDEX": "index", "INDEX_ATTACH": "index", "VIEW": "view",
            "TRIGGER": "trigger"}.get(typ)
    if kind:
        key = (kind, s, (f"{t}.{o}" if kind in ("constraint", "index", "trigger") and t else o))
        hit = fe.objects.get(key)
        if hit and hit[0] == "dropped":
            return f"{kind} {o} was dropped by {hit[1]}"
    if t and fe.dropped_columns.get((s, t)):
        sql = block.get("sql", "")
        for col, f in fe.dropped_columns[(s, t)].items():
            if f'"{col}"' in sql.lower() or re.search(rf"\b{re.escape(col)}\b", blank_strings(sql).lower()):
                return f"column {col} of {s}.{t} was dropped by {f}"
    return ""
