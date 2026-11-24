#!/usr/bin/env python3
"""
sweep_layer_cmds.py - `sweep.py layers ...`: init-manifests, scan-models (F2); reconcile and check live in sweep_reconcile.py.

Manifests (layers/<layer>_tables.csv) say which tables a layer owns. They are DRAFTED from the inventory and the data-model
scripts and then reviewed by people; nothing here applies a draft on its own.
"""
import ast
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

from sweep_common import META_CONSTRAINTS_CSV, META_TABLES_CSV, VSAM_OUTPUTS_DIR, get_logger, norm_col
from sweep_layers import LayerError, MANIFEST_FIELDS, _root, chain, load_layers, read_manifest, write_manifest
from sweep_merge import atomic_write_csv, atomic_write_text

logger = get_logger("sweep_layers_cmds", to_file=False)


# -----------------------------------------------------------------------------
# init-manifests
# -----------------------------------------------------------------------------
def oracle_tables():
    """[(schema, table, keyed)] of every Oracle table of the Step 01 metadata."""
    keyed = set()
    try:
        with open(META_CONSTRAINTS_CSV, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if (r.get("ctype") or "").strip().upper() == "P":
                    keyed.add(((r.get("owner") or "").strip().lower(), norm_col(r.get("table_name", ""))))
    except OSError:
        logger.warning(f"{META_CONSTRAINTS_CSV} not found: Oracle tables are drafted as keyed (run Step 01 first)")
    out = {}
    try:
        with open(META_TABLES_CSV, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                key = ((r.get("owner") or "").strip().lower(), norm_col(r.get("table_name", "")))
                if key[0] and key[1]:
                    out[key] = True
    except OSError as e:
        raise LayerError(f"Oracle metadata {META_TABLES_CSV} not readable ({e}); run Step 01 first")
    return [(s, t, (not keyed) or (s, t) in keyed) for (s, t) in sorted(out)]


def vsam_tables():
    """[(schema, table, keyed)] of every VSAM target of the Step 02 load matrix."""
    lm = VSAM_OUTPUTS_DIR / "vsam_02_load_matrix.csv"
    if not lm.exists():
        lm = VSAM_OUTPUTS_DIR / "load_matrix.csv"
    out = {}
    try:
        with open(lm, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                key = ((r.get("target_schema") or "").strip().lower(), (r.get("target_table") or "").strip().lower())
                if key[0] and key[1]:
                    out[key] = out.get(key, False) or bool((r.get("key_columns") or "").strip())
    except OSError:
        logger.warning(f"{lm} not found: no VSAM tables in the drafts (run VSAM Steps 02-03 first)")
    return [(s, t, k) for (s, t), k in sorted(out.items())]


def _row(track, schema, table, keyed, enabled=True):
    mode = "upsert" if keyed else ("replace" if track == "vsam" else "insert_only")
    return {"track": track, "schema": schema, "table": table, "promote_mode": mode, "conflict_policy": "source_wins", "enabled": enabled}


def draft_manifests():
    """{layer: [rows]} per the layout: bronze = all Oracle + all VSAM; silver = all Oracle (VSAM / outputs come from review);
    gold_backup and gold = all Oracle (model outputs come from review)."""
    ora = [_row("oracle", s, t, k) for s, t, k in oracle_tables()]
    vsm = [_row("vsam", s, t, k) for s, t, k in vsam_tables()]
    layers = [l.name for l in load_layers()]
    drafts = {}
    for name in layers:
        drafts[name] = list(ora) + (vsm if name == layers[0] else [])
    return drafts


def init_manifests(force=False):
    """Writes the draft manifests. Existing manifests are kept unless force. Returns {layer: (path or None, rows)}."""
    result = {}
    for name, rows in draft_manifests().items():
        layer = next(l for l in load_layers() if l.name == name)
        if layer.manifest_path.exists() and not force:
            logger.warning(f"{layer.manifest_path} exists and is kept (reviewed by people); use --force to replace it")
            result[name] = (None, rows)
            continue
        path = write_manifest(layer, rows, force=True)
        logger.info(f"Drafted {path}: {len(rows)} table(s)")
        result[name] = (path, rows)
    try:                                           # the SWEEP_MODEL headers of data_models/ draft the silver VSAM tables and the model_output rows
        from sweep_models import models_dir
        if models_dir().is_dir() and any(models_dir().glob("*.py")):
            scan_models(models_dir())
    except LayerError as e:
        logger.warning(f"data models not scanned: {e}")
    logger.info("Review the drafts: silver also needs the VSAM tables the data models read and the model output tables "
                "(layers/silver_vsam_tables.draft.csv, layers/model_output_tables.draft.csv from the data_models/ headers); "
                "gold_backup / gold need the model output tables.")
    return result


# -----------------------------------------------------------------------------
# scan-models
# -----------------------------------------------------------------------------
_IDENT = r'"?[A-Za-z_][\w$]*"?'
_NAME = rf"({_IDENT}(?:\s*\.\s*{_IDENT})?)"
READ_RE = re.compile(rf"\b(?:FROM|JOIN)\s+(?!\(|LATERAL\b|UNNEST\b|ONLY\b){_NAME}", re.I)
WRITE_RE = re.compile(
    rf"\b(?:INSERT\s+INTO|MERGE\s+INTO|UPDATE(?!\s+SET)|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?(?:\s+ONLY)?|"
    rf"CREATE\s+(?:UNLOGGED\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?|CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW|"
    rf"ALTER\s+TABLE(?:\s+IF\s+EXISTS)?(?:\s+ONLY)?)\s+{_NAME}", re.I)
TEMP_RE = re.compile(rf"\bCREATE\s+(?:GLOBAL\s+|LOCAL\s+)?TEMP(?:ORARY)?\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+{_NAME}", re.I)
CTE_RE = re.compile(rf"(?:\bWITH\s+(?:RECURSIVE\s+)?|,\s*){_NAME}\s+AS\s*(?:NOT\s+)?(?:MATERIALIZED\s*)?\(", re.I)
SQL_WORDS = {"select", "lateral", "unnest", "generate_series", "values", "set", "only", "dual", "information_schema", "pg_catalog", "table"}


def _clean(name):
    n = re.sub(r"\s+", "", name).replace('"', "").lower()
    return n


def _split(name):
    n = _clean(name)
    return tuple(n.split(".", 1)) if "." in n else ("", n)


def _string_parts(node):
    """Text of a string literal / f-string / concatenation with '{}' standing in for every interpolated expression."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) else "x" for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _string_parts(node.left), _string_parts(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def sql_strings(source):
    """[(line, text)] of every string in a Python source that looks like SQL, plus pandas to_sql / read_sql_table calls."""
    tree = ast.parse(source)
    found, calls = [], []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Constant, ast.JoinedStr, ast.BinOp)):
            text = _string_parts(node)
            if text and re.search(r"\b(select|insert|update|delete|create|merge|truncate|alter|with)\b", text, re.I) and len(text) > 12:
                found.append((getattr(node, "lineno", 0), text))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("to_sql", "read_sql_table"):
            name = _string_parts(node.args[0]) if node.args else None
            schema = next((_string_parts(k.value) for k in node.keywords if k.arg == "schema"), "") or ""
            if name:
                calls.append((node.lineno, node.func.attr, schema, name))
    return found, calls


def scan_text(text):
    """(reads, writes) of one SQL text as sets of (schema, table); CTE and temporary table names are removed."""
    ctes = {_clean(m.group(1)) for m in CTE_RE.finditer(text)} | {_clean(m.group(1)) for m in TEMP_RE.finditer(text)}
    reads = {_split(m.group(1)) for m in READ_RE.finditer(text)}
    writes = {_split(m.group(1)) for m in WRITE_RE.finditer(text)}
    ok = lambda s, t: t and t not in SQL_WORDS and not (not s and t in ctes) and s not in ("pg_catalog", "information_schema")
    return {(s, t) for s, t in reads if ok(s, t)}, {(s, t) for s, t in writes if ok(s, t)}


def scan_models(models_dir=None, write=True):
    """
    Reads the data-model Python scripts (default: data_models/) and drafts, for HUMAN review. A script with a SWEEP_MODEL header is
    described by its header (inputs / outputs); only a script without one falls back to scanning the SQL inside its strings:
      layers/models_scan.json                  what every script reads and writes
      layers/silver_vsam_tables.draft.csv      VSAM tables the models read (candidates for the silver manifest)
      layers/model_output_tables.draft.csv     tables the models write (candidates for the model_output rows)
    Nothing is applied to a manifest. The scripts are never run or changed.
    """
    from sweep_models import ModelError, models_dir as default_models_dir, parse_header
    models_dir = Path(models_dir) if models_dir else default_models_dir()
    if not models_dir.is_dir():
        raise LayerError(f"models directory {models_dir} not found")
    scripts = sorted(p for p in models_dir.rglob("*.py") if p.is_file() and "work" not in p.relative_to(models_dir).parts)
    if not scripts:
        raise LayerError(f"no Python scripts under {models_dir}")
    detail, reads_by, writes_by, unparsed = {}, defaultdict(set), defaultdict(set), []
    for path in scripts:
        rel = str(path.relative_to(models_dir))
        try:
            header = parse_header(path)
        except ModelError as e:
            unparsed.append({"script": rel, "problem": str(e)})
            continue
        if header is not None:                    # the header is the truth: no guessing from SQL strings
            split = lambda names: {tuple(n.strip().lower().split(".", 1)) for n in (names or []) if "." in n}
            reads, writes, how = split(header.get("inputs")), split(header.get("outputs")), "header"
        else:
            try:
                strings, calls = sql_strings(path.read_text(encoding="utf-8", errors="replace"))
            except SyntaxError as e:
                unparsed.append({"script": rel, "problem": f"syntax error: {e}"})
                continue
            reads, writes, how = set(), set(), "scan"
            for _, text in strings:
                r, w = scan_text(text)
                reads |= r
                writes |= w
            for _, fn, schema, name in calls:
                (writes if fn == "to_sql" else reads).add((schema.lower(), name.lower()))
            reads -= writes                       # a table a script writes and then reads is its own working table
            unparsed.append({"script": rel, "problem": "no SWEEP_MODEL header: inputs / outputs guessed from the SQL strings (add a header)"})
        detail[rel] = {"source": how, "reads": sorted(f"{s}.{t}" if s else t for s, t in reads), "writes": sorted(f"{s}.{t}" if s else t for s, t in writes)}
        for k in reads:
            reads_by[k].add(rel)
        for k in writes:
            writes_by[k].add(rel)

    bronze = chain()[0]
    bronze_rows = read_manifest(bronze, include_disabled=True)
    vsam_known = {(r["schema"], r["table"]) for r in bronze_rows if r["track"] == "vsam"}
    vsam_by_name = defaultdict(set)
    for s, t in vsam_known:
        vsam_by_name[t].add(s)
    oracle_known = {(r["schema"], r["table"]) for r in bronze_rows if r["track"] == "oracle"}
    oracle_names = {t for _, t in oracle_known}
    outputs = {k for k in writes_by if not (k in oracle_known or (not k[0] and k[1] in oracle_names))}
    colliding = sorted(f"{s}.{t}" if s else t for (s, t) in writes_by if (s, t) in oracle_known or (s, t) in vsam_known)

    vsam_hits, ambiguous = {}, []
    for (s, t), scripts_ in reads_by.items():
        if (s, t) in outputs:
            continue
        if s and (s, t) in vsam_known:
            vsam_hits[(s, t)] = scripts_
        elif not s and t in vsam_by_name:
            for sch in vsam_by_name[t]:
                vsam_hits[(sch, t)] = scripts_
            if len(vsam_by_name[t]) > 1:
                ambiguous.append(f"{t}: read without a schema, exists in {', '.join(sorted(vsam_by_name[t]))} (all drafted)")

    root = _root() / "layers"
    report = {"models_dir": str(models_dir), "scripts": detail, "unparsed": unparsed, "ambiguous": ambiguous, "outputs_colliding_with_loaded_tables": colliding,
              "vsam_read": {f"{s}.{t}": sorted(v) for (s, t), v in sorted(vsam_hits.items())},
              "outputs_written": {f"{s}.{t}" if s else t: sorted(v) for (s, t), v in sorted(writes_by.items()) if (s, t) in outputs},
              "note": "DRAFTS for human review - nothing here is applied to a manifest"}
    vsam_rows = [{"track": "vsam", "schema": s, "table": t, "promote_mode": "upsert", "conflict_policy": "source_wins", "enabled": "false"}
                 for (s, t) in sorted(vsam_hits)]
    out_rows = [{"track": "model_output", "schema": s or "", "table": t, "promote_mode": "upsert", "conflict_policy": "source_wins",
                 "enabled": "false"} for (s, t) in sorted(outputs)]
    if write:
        root.mkdir(parents=True, exist_ok=True)
        atomic_write_text(root / "models_scan.json", json.dumps(report, indent=2))
        atomic_write_csv(root / "silver_vsam_tables.draft.csv", vsam_rows, MANIFEST_FIELDS)
        atomic_write_csv(root / "model_output_tables.draft.csv", out_rows, MANIFEST_FIELDS)
        logger.info(f"Scanned {len(scripts)} script(s): {len(vsam_rows)} VSAM table(s) read, {len(out_rows)} output table(s) written. "
                    f"Drafts in {root} (enabled=false): review them, then copy the rows you accept into the manifests.")
        for item in unparsed + [{"script": "-", "problem": a} for a in ambiguous]:
            logger.warning(f"{item['script']}: {item['problem']}")
    return report, vsam_rows, out_rows
