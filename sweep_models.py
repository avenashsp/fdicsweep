#!/usr/bin/env python3
"""
sweep_models.py - the data models run INSIDE the repository (part K): `sweep.py models list | run | status`.

A model is a Python script under data_models/ (`<NN>_<name>.py`). It runs in silver, after the bronze -> silver promote and before the
promote to gold_backup. The runner starts every model as its own subprocess (cwd = repository root) with ONLY the silver credentials
and a small contract of environment variables (docs/data_models.md):

    PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE      libpq connection to the silver database
    SWEEP_LAYER=silver  SWEEP_RUN_ID  SWEEP_MODEL  SWEEP_MODE (full|delta)
    SWEEP_MODEL_WORKDIR   data_models/work/<model>/<run_id>   generated files
    SWEEP_MODEL_CONFIG    data_models/config/<model>/         files people edit
    SWEEP_INPUT_DELTAS    JSON: per input table {"full": true} or {"full": false, "delta_table", "seq_from", "seq_to"}

and expects one line `SWEEP_MODEL_RESULT {json}` on stdout and exit code 0 on success.

The header of a script is a literal dict read with ast (the script is never imported):
    SWEEP_MODEL = {"name": ..., "inputs": [...], "outputs": [...], "depends_on": [...], "delta_strategy": "update_in_place",
                   "timeout": 3600, "update_key": [...]}
"""
import ast
import json
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path

import sweep_audit
from sweep_common import BASE_DIR, CTL_SCHEMA, clip_error, get_logger, get_timestamp, lit, parse_env_line, q, query_db_dicts, run_psql, run_psql_script
from sweep_layers import LayerError, get_layer, layer_conn, read_manifest, use_layer, verify_database
from sweep_merge import current_run_id
from sweep_platform import group_popen_kwargs, kill_process_group

logger = get_logger("sweep_models", to_file=False)

RESULT_PREFIX = "SWEEP_MODEL_RESULT"
STRATEGIES = ("insert_missing", "update_in_place")
HEADER_KEYS = ("name", "inputs", "outputs", "depends_on", "delta_strategy", "timeout", "update_key")
DEFAULT_TIMEOUT = 6 * 3600
SILVER = "silver"


class ModelError(LayerError):
    """A model script, its header or the model graph is invalid."""


# -----------------------------------------------------------------------------
# folders
# -----------------------------------------------------------------------------
def models_dir():
    return Path(os.environ.get("SWEEP_MODELS_DIR") or BASE_DIR / "data_models").resolve()


def _rel(path):
    """Path as given to the model: relative to the repository root when it lies inside it."""
    path = Path(path).resolve()
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return str(path)


# -----------------------------------------------------------------------------
# header (K2)
# -----------------------------------------------------------------------------
@dataclass
class Model:
    name: str
    file: Path
    prefix: int
    inputs: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    depends_on: list = field(default_factory=list)
    delta_strategy: str = "insert_missing"
    timeout: float = None
    update_key: list = field(default_factory=list)
    source: str = "header"                 # header | models.json | scan
    warnings: list = field(default_factory=list)


def parse_header(path):
    """The SWEEP_MODEL dict of a script (None when it has none). Read with ast.literal_eval: the script is never executed."""
    path = Path(path)
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError as e:
        raise ModelError(f"{path.name}: syntax error: {e}")
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        if any(isinstance(t, ast.Name) and t.id == "SWEEP_MODEL" for t in targets):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, SyntaxError):
                raise ModelError(f"{path.name}: SWEEP_MODEL must be a literal dict (no expressions, no names)")
            if not isinstance(value, dict):
                raise ModelError(f"{path.name}: SWEEP_MODEL must be a dict")
            return value
    return None


def _names(path, key, value):
    if value is None:
        return []
    if not isinstance(value, (list, tuple)) or not all(isinstance(v, str) for v in value):
        raise ModelError(f"{path.name}: SWEEP_MODEL[{key!r}] must be a list of strings")
    out = [v.strip().lower() for v in value if v.strip()]
    if key in ("inputs", "outputs"):
        bad = [v for v in out if v.count(".") != 1 or not all(v.split("."))]
        if bad:
            raise ModelError(f"{path.name}: SWEEP_MODEL[{key!r}] needs schema.table names; got {', '.join(bad)}")
    return out


def model_from_dict(path, d, source="header"):
    path = Path(path)
    m = re.match(r"(?i)(dm)?(\d+)[_-](.+)$", path.stem)
    if m and m.group(1):                       # DM03_bank: ordered by its number, named dm03_bank (the dm number stays in the name)
        prefix, stem = int(m.group(2)), path.stem.lower()
    else:
        prefix, stem = (int(m.group(2)), m.group(3)) if m else (10 ** 6, path.stem)
    warnings = [f"unknown key {k!r} in SWEEP_MODEL (ignored)" for k in d if k not in HEADER_KEYS]
    strategy = d.get("delta_strategy", "insert_missing" if source == "scan" else "update_in_place")
    if strategy not in STRATEGIES:
        raise ModelError(f"{path.name}: delta_strategy must be one of {', '.join(STRATEGIES)} (got {strategy!r})")
    timeout = d.get("timeout")
    if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0):
        raise ModelError(f"{path.name}: timeout must be a positive number of seconds")
    model = Model(name=str(d.get("name") or stem).strip().lower(), file=path, prefix=prefix, inputs=_names(path, "inputs", d.get("inputs")),
                  outputs=_names(path, "outputs", d.get("outputs")), depends_on=[x.lower() for x in _names(path, "depends_on", d.get("depends_on"))],
                  delta_strategy=strategy, timeout=timeout, update_key=[x.lower() for x in _names(path, "update_key", d.get("update_key"))],
                  source=source, warnings=warnings)
    if not model.outputs:
        model.warnings.append("no outputs declared: nothing can be verified or promoted for this model")
    if strategy == "update_in_place" and not model.update_key and source != "scan":
        model.warnings.append("update_in_place without update_key: the script derives its own key")
    return model


def _scan_header(path):
    """Fallback for a script without header: the static SQL scan of `layers scan-models` (inputs / outputs with a schema only)."""
    from sweep_layer_cmds import scan_text, sql_strings
    reads, writes = set(), set()
    strings, calls = sql_strings(path.read_text(encoding="utf-8", errors="replace"))
    for _, text in strings:
        r, w = scan_text(text)
        reads |= r
        writes |= w
    for _, fn, schema, name in calls:
        (writes if fn == "to_sql" else reads).add((schema.lower(), name.lower()))
    reads -= writes
    keep = lambda pairs: sorted(f"{s}.{t}" for s, t in pairs if s)
    return {"inputs": keep(reads), "outputs": keep(writes)}


def discover(directory=None):
    """
    Every model under the models folder, in file-name order. A script without a SWEEP_MODEL header falls back to the static scan
    (a warning says so). An optional data_models/models.json overrides header values per model name.
    """
    directory = Path(directory) if directory else models_dir()
    if not directory.is_dir():
        return []
    models, seen = [], {}
    for path in sorted(p for p in directory.glob("*.py") if p.is_file() and not p.name.startswith("_")):
        header = parse_header(path)
        if header is None:
            model = model_from_dict(path, _scan_header(path), source="scan")
            model.warnings.append("no SWEEP_MODEL header: inputs / outputs come from the static scan - add a header (docs/data_models.md)")
        else:
            model = model_from_dict(path, header)
        if model.name in seen:
            raise ModelError(f"model name {model.name!r} is used by {seen[model.name].name} and {path.name}")
        seen[model.name] = path
        models.append(model)
    cfg = directory / "models.json"
    if cfg.exists():
        try:
            data = json.loads(cfg.read_text(encoding="utf-8"))
        except ValueError as e:
            raise ModelError(f"{cfg}: not valid JSON: {e}")
        by_name = {m.name: m for m in models}
        for name, over in (data.get("models") or {}).items():
            m = by_name.get(name.lower())
            if m is None:
                raise ModelError(f"{cfg}: model {name!r} has no script in {directory}")
            merged = {k: getattr(m, k) for k in ("inputs", "outputs", "depends_on", "delta_strategy", "timeout", "update_key")}
            merged.update({k: v for k, v in over.items() if k in HEADER_KEYS})
            merged["name"] = m.name
            new = model_from_dict(m.file, merged, source="models.json")
            models[models.index(m)] = new
            by_name[m.name] = new
        for name in data.get("disabled") or []:
            models = [m for m in models if m.name != str(name).lower()]
    return models


# -----------------------------------------------------------------------------
# order (K3)
# -----------------------------------------------------------------------------
def order_models(models):
    """Topological order by depends_on; ties by numeric file prefix, then name. Unknown dependencies and cycles are errors."""
    by_name = {m.name: m for m in models}
    for m in models:
        for d in m.depends_on:
            if d not in by_name:
                raise ModelError(f"model {m.name} depends_on {d!r}, which is not a model in {models_dir()}")
    done, order, state = set(), [], {}

    def visit(m, path):
        if m.name in done:
            return
        if state.get(m.name) == "open":
            cyc = path[path.index(m.name):] + [m.name]
            raise ModelError("dependency cycle between data models: " + " -> ".join(cyc))
        state[m.name] = "open"
        for d in sorted(m.depends_on, key=lambda n: (by_name[n].prefix, n)):
            visit(by_name[d], path + [m.name])
        state[m.name] = "closed"
        done.add(m.name)
        order.append(m)

    for m in sorted(models, key=lambda m: (m.prefix, m.name)):
        visit(m, [])
    return order


def dependencies_of(models, names):
    by_name = {m.name: m for m in models}
    out, todo = set(), list(names)
    while todo:
        n = todo.pop()
        if n in out:
            continue
        out.add(n)
        todo.extend(by_name[n].depends_on)
    return out


def select_models(models, names=None, with_deps=False):
    by_name = {m.name: m for m in models}
    if not names:
        return list(models)
    wanted = [n.strip().lower() for n in names if n.strip()]
    unknown = [n for n in wanted if n not in by_name]
    if unknown:
        raise ModelError(f"unknown model(s): {', '.join(unknown)}. Known: {', '.join(sorted(by_name)) or 'none'}")
    keep = dependencies_of(models, wanted) if with_deps else set(wanted)
    return [m for m in models if m.name in keep]


# -----------------------------------------------------------------------------
# silver state: run log, delta sequences, FE changes
# -----------------------------------------------------------------------------
def silver_layer():
    return get_layer(SILVER)


def _db(layer=None):
    return (layer or silver_layer()).database


def last_success(model, layer=None):
    """Latest SUCCESS row of model_run_log (dict) or None."""
    try:
        rows = query_db_dicts(f"SELECT id, run_id, mode, ended_at::text AS ended_at, input_seqs::text AS input_seqs FROM {CTL_SCHEMA}.model_run_log "
                              f"WHERE model = {lit(model)} AND status = 'SUCCESS' ORDER BY id DESC LIMIT 1", dbname=_db(layer))
    except RuntimeError:
        return None
    if not rows:
        return None
    r = rows[0]
    r["input_seqs"] = json.loads(r["input_seqs"]) if r.get("input_seqs") else {}
    return r


def last_run(model, layer=None):
    try:
        rows = query_db_dicts(f"SELECT status, run_id, mode, ended_at::text AS ended_at, message, log_file FROM {CTL_SCHEMA}.model_run_log "
                              f"WHERE model = {lit(model)} ORDER BY id DESC LIMIT 1", dbname=_db(layer))
    except RuntimeError:
        return None
    return rows[0] if rows else None


def silver_seq(schema, table, layer=None):
    """Highest delta sequence of the table applied in silver (0 = none) and whether the registry knows the table at all."""
    rows = query_db_dicts(f"SELECT COALESCE(max(seq), 0) AS n, count(*) AS c FROM {CTL_SCHEMA}.delta_registry WHERE target_schema = {lit(schema)} "
                          f"AND target_table = {lit(table)} AND status = 'SUCCESS'", dbname=_db(layer))
    return int(rows[0]["n"]), int(rows[0]["c"]) > 0


def silver_deltas_since(schema, table, seq, layer=None):
    return query_db_dicts(f"SELECT seq, full_copy, delta_table, purged_at FROM {CTL_SCHEMA}.delta_registry WHERE target_schema = {lit(schema)} "
                          f"AND target_table = {lit(table)} AND status = 'SUCCESS' AND seq > {int(seq)} ORDER BY seq", dbname=_db(layer))


def fe_changed_since(schema, table, since, layer=None):
    """Name of the latest FE file that touched the table after `since` (a timestamp text), else ''."""
    try:
        rows = query_db_dicts(
            f"SELECT file_name FROM {CTL_SCHEMA}.fwd_ddl_changes WHERE kind <> 'rule' AND recorded_at > {lit(since)}::timestamptz AND schema_name = {lit(schema)} "
            f"AND (object_name = {lit(table)} OR object_name LIKE {lit(table + '.%')}) ORDER BY id DESC LIMIT 1", dbname=_db(layer))
    except RuntimeError:
        return ""
    return rows[0]["file_name"] if rows else ""


def table_exists(schema, table, layer=None):
    rows = query_db_dicts(f"SELECT to_regclass({lit(q(schema) + '.' + q(table))}) IS NOT NULL AS ok", dbname=_db(layer))
    return rows[0]["ok"] in ("t", "true")


def plan_model(model, models, full=False, force=False, ran_models=(), layer=None):
    """
    What `models run` would do with one model: {"run": bool, "mode": full|delta, "reasons": [..], "deltas": {input: spec}, "seqs": {input: seq}}.
    A model runs when it never succeeded, or an input table received a delta or an FE change since its last SUCCESS, or an upstream
    model it depends on ran in this run (the outputs are not delta captured: such an input is always read in full).
    """
    outputs_of_models = {o for m in models for o in m.outputs}
    prev = last_success(model.name, layer)
    reasons, deltas, seqs = [], {}, {}
    if force:
        reasons.append("--force")
    if prev is None:
        reasons.append("never succeeded")
    if full and prev is not None:
        reasons.append("--full")
    for dep in model.depends_on:
        if dep in ran_models:
            reasons.append(f"upstream model {dep} ran")
    for inp in model.inputs:
        schema, table = inp.split(".")
        cur, known = silver_seq(schema, table, layer)
        seqs[inp] = cur
        before = (prev["input_seqs"].get(inp) if prev else None)
        fe_file = fe_changed_since(schema, table, prev["ended_at"], layer) if prev else ""
        if fe_file:
            reasons.append(f"input {inp} changed by FE file {fe_file}")
        if prev is None or full or inp in outputs_of_models or not known or before is None or fe_file:
            deltas[inp] = {"full": True}
            continue
        rows = silver_deltas_since(schema, table, before, layer)
        usable = all(str(r["full_copy"]).lower() not in ("t", "true") and r["delta_table"] and not r["purged_at"] for r in rows)
        if rows:
            reasons.append(f"input {inp} received delta(s) {rows[0]['seq']}..{rows[-1]['seq']}")
        delta_table = f"sweep_delta.{schema}__{table}"
        if rows and (not usable or not table_exists("sweep_delta", f"{schema}__{table}", layer)):
            deltas[inp] = {"full": True}
        else:
            deltas[inp] = {"full": False, "delta_table": delta_table, "seq_from": int(before), "seq_to": cur}
    mode = "full" if (full or prev is None) else "delta"
    return {"run": bool(reasons), "mode": mode, "reasons": reasons, "deltas": deltas, "seqs": seqs, "previous": prev}


# -----------------------------------------------------------------------------
# preconditions (K8) and the output guard (A3)
# -----------------------------------------------------------------------------
def promote_problems(models, layer=None):
    """Bronze -> silver deltas of the input tables that are PENDING or FAILED (silver does not hold them yet)."""
    from sweep_promote import source_deltas
    layer = layer or silver_layer()
    bronze = get_layer(layer.upstream) if layer.upstream else None
    if bronze is None:
        return []
    promoted = {(r["schema"], r["table"]) for r in read_manifest(layer)}
    problems = []
    for inp in sorted({i for m in models for i in m.inputs}):
        schema, table = inp.split(".")
        if (schema, table) not in promoted:
            continue
        last, _ = silver_seq(schema, table, layer)
        try:
            pending = source_deltas(bronze, schema, table, last)
        except RuntimeError as e:
            problems.append(f"{inp}: the {bronze.name} delta registry cannot be read ({clip_error(e)})")
            continue
        if pending:
            failed = query_db_dicts(f"SELECT 1 FROM {CTL_SCHEMA}.promote_log WHERE target_schema = {lit(schema)} AND target_table = {lit(table)} "
                                    f"AND status = 'FAILED' AND seq > {int(last)} LIMIT 1", dbname=layer.database)
            state = "FAILED" if failed else "PENDING"
            problems.append(f"{inp}: {state} bronze -> silver delta(s) {pending[0]['seq']}..{pending[-1]['seq']} "
                            f"(run `python3 sweep.py promote --from {bronze.name} --to {layer.name}`)")
    return problems


def check_preconditions(models, skip_promote_check=False, layer=None):
    """Problems that stop the whole run before any model starts: (a) pending FE files, (b) pending / failed deltas, (c) inputs + guard, (d) outputs."""
    from sweep_fwd_ddl import pending_count
    layer = layer or silver_layer()
    problems = []
    try:
        verify_database(layer)                                                            # (c) the guard
    except SystemExit:
        problems.append(f"the connected database is not the {layer.name} database ({layer.database})")
        return problems
    try:
        capture_ok = bool(query_db_dicts("SELECT 1 AS x FROM pg_event_trigger WHERE evtname = 'sweep_capture_ddl' AND evtenabled <> 'D'", dbname=layer.database))
    except RuntimeError:
        capture_ok = False
    if not capture_ok:
        problems.append("(e) the DDL capture event trigger sweep_capture_ddl does not exist in " + layer.database + ": a data model's DDL could not be replayed in "
                        "gold backup / gold. Run `python3 sweep.py migrate --layer " + layer.name + "` as a user that may create event triggers (rds_superuser on Aurora)")
    pend = pending_count(layer.name)
    if pend is None:
        problems.append(f"(a) the FE state of {layer.name} cannot be read: run `python3 sweep.py migrate --layer {layer.name}`")
    elif pend:
        problems.append(f"(a) {layer.name} has {pend} pending FE file(s): run `python3 sweep.py fwd-ddl apply --layer {layer.name}` first")
    if not skip_promote_check:
        problems += [f"(b) {p}" for p in promote_problems(models, layer)]
    for m in models:
        for inp in m.inputs:
            if not table_exists(*inp.split("."), layer):
                problems.append(f"(c) model {m.name}: input table {inp} does not exist in {layer.name}")
        for out in m.outputs:
            if not table_exists(*out.split("."), layer):
                problems.append(f"(d) model {m.name}: output table {out} does not exist in {layer.name}")
    return problems


def loaded_tables():
    """Tables the loaders put data into (Oracle / VSAM manifests of bronze and silver, and the load logs of both databases)."""
    names = set()
    for lname in ("bronze", SILVER):
        lay = get_layer(lname)
        for r in read_manifest(lay, include_disabled=True):
            if r["track"] in ("oracle", "vsam"):
                names.add((r["schema"], r["table"]))
        for tbl in ("oracle_load_log", "vsam_load_log"):
            try:
                for r in query_db_dicts(f"SELECT DISTINCT lower(target_schema) AS s, lower(target_table) AS t FROM {CTL_SCHEMA}.{tbl}", dbname=lay.database):
                    for sch in r["s"].split(","):
                        names.add((sch.strip(), r["t"]))
            except RuntimeError:
                pass
    try:                       # a table the silver manifest lists with track model_output is an explicit model output (DM03: bcs.bank is also an Oracle table)
        names -= {(r["schema"], r["table"]) for r in read_manifest(get_layer(SILVER), include_disabled=True) if r["track"] == "model_output"}
    except LayerError:
        pass
    return names


def captured_ddl(layer, run_id, model):
    """[{command, type, schema, identity}] the DDL the model ran in its session (event trigger -> sweep_ctl.model_ddl_history)."""
    rows = query_db_dicts(f"SELECT command_tag AS command, object_type AS type, schema_name AS schema, object_identity AS identity "
                          f"FROM {CTL_SCHEMA}.model_ddl_history WHERE capture_tag = {lit(f'{run_id}:{model.name}')} ORDER BY id", dbname=_db(layer))
    return [dict(r) for r in rows]


def audit_post_step(model, layer, ddl, tag):
    """Every output table, and every table the model created or altered, carries the six canonical audit columns and the audit trigger."""
    pairs = {tuple(o.split(".")) for o in model.outputs}
    for d in ddl:
        if (d.get("type") or "") == "table" and d.get("identity"):
            sch, _, tbl = d["identity"].partition(".")
            pairs.add((sch.strip('"'), tbl.strip('"')))
    if pairs:
        values = ", ".join(f"({lit(s_)}, {lit(t_)})" for s_, t_ in sorted(pairs))
        res = run_psql_script(sweep_audit.ensure_for_query_sql(f"SELECT * FROM (VALUES {values}) v(s, t)"), dbname=_db(layer),
                              options=sweep_audit.session_options(sweep_audit.TAG_MODEL, capture=tag))      # captured like the model's own DDL
        if res.returncode != 0:
            raise RuntimeError(res.stderr or res.stdout or "psql failed")


def output_collisions(model, loaded):
    return [o for o in model.outputs if tuple(o.split(".")) in loaded]


# -----------------------------------------------------------------------------
# running one model (K1)
# -----------------------------------------------------------------------------
def _layer_file_env(layer, key):
    try:
        for raw in layer.env_path.read_text(encoding="utf-8").splitlines():
            kv = parse_env_line(raw)
            if kv and kv[0] == key:
                return kv[1]
    except OSError:
        pass
    return None


def model_env(model, layer, run_id, mode, deltas):
    """ONLY the silver credentials: everything PG* / SWEEP_* of the current process (other layers, other settings) is dropped."""
    conn = layer_conn(layer)
    env = {k: v for k, v in os.environ.items() if not (k.startswith("PG") or k.startswith("SWEEP_"))}
    alt_user = os.environ.get("SWEEP_MODEL_PGUSER") or _layer_file_env(layer, "SWEEP_MODEL_PGUSER")
    if alt_user:          # an optional account with write rights on the output tables (same silver database)
        user, password = alt_user, os.environ.get("SWEEP_MODEL_PGPASSWORD") or _layer_file_env(layer, "SWEEP_MODEL_PGPASSWORD")
    else:
        user, password = conn.user, conn.password
    env.update(PGHOST=str(conn.host), PGPORT=str(conn.port), PGUSER=str(user), PGDATABASE=str(conn.dbname))
    if password:
        env["PGPASSWORD"] = str(password)
    if conn.sslmode:
        env["PGSSLMODE"] = str(conn.sslmode)
    env["PGOPTIONS"] = sweep_audit.session_options(sweep_audit.TAG_MODEL, capture=f"{run_id}:{model.name}")      # model session + DDL capture tag
    root = models_dir()
    workdir = root / "work" / model.name / run_id
    config = root / "config" / model.name
    workdir.mkdir(parents=True, exist_ok=True)
    config.mkdir(parents=True, exist_ok=True)
    env.update(SWEEP_LAYER=layer.name, SWEEP_RUN_ID=run_id, SWEEP_MODEL=model.name, SWEEP_MODE=mode, SWEEP_MODEL_WORKDIR=_rel(workdir),
               SWEEP_MODEL_CONFIG=_rel(config) + os.sep, SWEEP_INPUT_DELTAS=json.dumps(deltas, sort_keys=True),
               PYTHONPATH=os.pathsep.join(filter(None, [str(BASE_DIR), os.environ.get("PYTHONPATH", "")])), PYTHONUNBUFFERED="1")
    return env


def parse_result(text):
    """The dict of the LAST `SWEEP_MODEL_RESULT {json}` line (None when there is none or it is not JSON)."""
    found = None
    for line in text.splitlines():
        if line.startswith(RESULT_PREFIX):
            try:
                found = json.loads(line[len(RESULT_PREFIX):].strip())
            except ValueError:
                found = None
    return found


def run_model_process(model, env, execute, mode, timeout, log_path):
    """Starts the script, writes its output to the log file, enforces the timeout. Returns (exit code, output text, timed_out)."""
    cmd = [sys.executable, _rel(model.file)] + (["--execute"] if execute else []) + ["--mode", mode]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(cmd, cwd=str(BASE_DIR), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                            errors="replace", **group_popen_kwargs())
    chunks, timed_out = [], threading.Event()

    def watchdog():
        if not done.wait(timeout):
            timed_out.set()
            kill_process_group(proc)
    done = threading.Event()
    t = threading.Thread(target=watchdog, daemon=True)
    t.start()
    with open(log_path, "w", encoding="utf-8") as fh:
        fh.write(f"# {' '.join(cmd)}\n# model {model.name} mode {mode} run {env.get('SWEEP_RUN_ID')}\n")
        for line in proc.stdout:
            chunks.append(line)
            fh.write(line)
            fh.flush()
        proc.wait()
        done.set()
        if timed_out.is_set():
            fh.write(f"\n# KILLED: timeout of {timeout}s exceeded\n")
    return proc.returncode, "".join(chunks), timed_out.is_set()


def verify_outputs(model, layer):
    """K9: every output table exists and has a primary key, or is insert_only in the manifest. Returns a list of problems."""
    manifest = {(r["schema"], r["table"]): r for r in read_manifest(layer, include_disabled=True)}
    problems = []
    for out in model.outputs:
        schema, table = out.split(".")
        if not table_exists(schema, table, layer):
            problems.append(f"output table {out} does not exist after the run")
            continue
        pk = query_db_dicts(f"SELECT 1 AS x FROM pg_index WHERE indrelid = to_regclass({lit(q(schema) + '.' + q(table))}) AND indisprimary", dbname=_db(layer))
        mode = (manifest.get((schema, table)) or {}).get("promote_mode")
        if not pk and mode != "insert_only":
            problems.append(f"output table {out} has no primary key and is not promote_mode=insert_only in layers/{layer.name}_tables.csv: "
                            f"it cannot be promoted safely")
    return problems


def record_run(layer, run_id, model, status, mode, started, exit_code, log_file, seqs, result, message, ddl_capture=None):
    j = lambda v: "NULL" if v is None else lit(json.dumps(v, sort_keys=True)) + "::jsonb"
    run_psql(f"INSERT INTO {CTL_SCHEMA}.model_run_log (run_id, model, status, mode, started_at, ended_at, exit_code, log_file, input_seqs, result, message, ddl_capture) "
             f"VALUES ({lit(run_id)}, {lit(model)}, {lit(status)}, {lit(mode)}, {lit(started)}::timestamptz, now(), "
             f"{'NULL' if exit_code is None else int(exit_code)}, {lit(log_file)}, {j(seqs)}, {j(result)}, {lit(message[:2000]) if message else 'NULL'}, "
             f"{lit(json.dumps(ddl_capture, sort_keys=True)) if ddl_capture else 'NULL'});",
             dbname=_db(layer))


def execute_model(model, layer, plan, run_id, loaded):
    """Runs one model and records the outcome. Returns {"model", "status", "message", "result"}."""
    started = time.strftime("%Y-%m-%d %H:%M:%S%z")
    mode, seqs = plan["mode"], plan["seqs"]
    log_file = models_dir() / "logs" / f"{layer.database}_{model.name}_{get_timestamp()}.log"
    clash = output_collisions(model, loaded)
    if clash:                                                     # A3: output tables never share a name with a loaded table
        msg = (f"output table(s) {', '.join(clash)} are also tables the loaders fill (Oracle / VSAM manifests or load logs): a model output "
               f"must have its own name; nothing was written")
        record_run(layer, run_id, model.name, "FAILED", mode, started, None, None, seqs, None, msg)
        return {"model": model.name, "status": "FAILED", "message": msg, "result": None}
    env = model_env(model, layer, run_id, mode, plan["deltas"])
    timeout = model.timeout or float(os.environ.get("SWEEP_MODEL_TIMEOUT") or DEFAULT_TIMEOUT)
    logger.info(f"model {model.name}: starting ({mode} mode; {'; '.join(plan['reasons'])}); log {_rel(log_file)}")
    t0 = time.time()
    try:
        code, out, timed_out = run_model_process(model, env, True, mode, timeout, log_file)
    except OSError as e:
        msg = f"cannot start {model.file.name}: {e}"
        record_run(layer, run_id, model.name, "FAILED", mode, started, None, _rel(log_file), seqs, None, msg)
        return {"model": model.name, "status": "FAILED", "message": msg, "result": None}
    result = parse_result(out)
    problems = []
    if timed_out:
        problems.append(f"timeout: killed after {int(timeout)}s")
    elif code != 0:
        tail = [l for l in out.strip().splitlines() if l.strip()][-3:]
        problems.append(f"exit code {code}: {' | '.join(tail)}"[:600])
    elif result is None:
        problems.append(f"the script exited 0 but printed no `{RESULT_PREFIX} {{json}}` line")
    elif str(result.get("status", "SUCCESS")).upper() not in ("SUCCESS", "OK"):
        problems.append(f"the script reported status {result.get('status')}: {result.get('error') or result.get('message') or ''}")
    ddl = []
    try:
        if not problems:
            audit_post_step(model, layer, captured_ddl(layer, run_id, model), f"{run_id}:{model.name}")      # audit columns + trigger on every output / created / altered table
        ddl = captured_ddl(layer, run_id, model)                                                            # the model's DDL + the post-step (replayed by promote)
    except RuntimeError as e:
        problems.append(f"audit post-step failed: {clip_error(e)}")
    if not problems:
        problems += verify_outputs(model, layer)
    status = "FAILED" if problems else "SUCCESS"
    message = "; ".join(problems)
    record_run(layer, run_id, model.name, status, mode, started, code, _rel(log_file), seqs, result, message, ddl_capture=ddl)
    logger.info(f"model {model.name}: {status} in {time.time() - t0:.1f}s" + (f" - {message}" if message else f" {json.dumps(result, sort_keys=True)}"))
    return {"model": model.name, "status": status, "message": message, "result": result}


# -----------------------------------------------------------------------------
# the run (K4)
# -----------------------------------------------------------------------------
def run_models_cmd(models=None, with_deps=False, full=False, force=False, workers=1, dry_run=False, skip_promote_check=False, **_unused):
    """`sweep.py models run`. Returns the exit code: 0 all fine, 1 a model failed or a precondition refused the run."""
    layer = silver_layer()
    try:
        everything = discover()
        order = order_models(everything)
        chosen = select_models(order, models if isinstance(models, (list, tuple)) else [m for m in (models or "").split(",") if m.strip()], with_deps)
    except LayerError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    chosen = [m for m in order if m in chosen]
    if not chosen:
        logger.info("models: no data models found (data_models/*.py)")
        return 0
    for m in chosen:
        for w in m.warnings:
            logger.warning(f"model {m.name}: {w}")
    run_id = current_run_id()
    with use_layer(layer):
        problems = check_preconditions(chosen, skip_promote_check)
        if problems:
            logger.error("models run refused - nothing was started:")
            for p in problems:
                logger.error(f"  {p}")
            return 1
        plans, results = {}, {}
        loaded = loaded_tables()

        # plan in order: whether a model runs depends on its upstream models (they may run in this very run)
        will_run = set()
        for m in chosen:
            p = plan_model(m, everything, full, force, ran_models=will_run, layer=layer)
            plans[m.name] = p
            if p["run"]:
                will_run.add(m.name)
        for m in chosen:
            p = plans[m.name]
            logger.info(f"model {m.name}: {'RUN' if p['run'] else 'up to date'} ({p['mode']})" + (f" - {'; '.join(p['reasons'])}" if p["reasons"] else ""))
        if dry_run:
            logger.info(f"models --dry-run: {len(will_run)} of {len(chosen)} model(s) would run; nothing was started")
            return 0

        def ready_state(m):
            """None = not yet decidable, 'go', or the name of a failed / skipped dependency."""
            for d in m.depends_on:
                if d in {x.name for x in chosen}:
                    st = results.get(d, {}).get("status")
                    if st is None:
                        return None
                    if st in ("FAILED", "SKIPPED"):
                        return d
            return "go"

        pending = [m for m in chosen]
        running = {}
        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            while pending or running:
                progressed = False
                for m in list(pending):
                    st = ready_state(m)
                    if st is None:
                        continue
                    pending.remove(m)
                    progressed = True
                    if st != "go":
                        msg = f"dependency {st} {results[st]['status'].lower()}"
                        record_run(layer, run_id, m.name, "SKIPPED", plans[m.name]["mode"], time.strftime("%Y-%m-%d %H:%M:%S%z"), None, None, None, None, msg)
                        results[m.name] = {"model": m.name, "status": "SKIPPED", "message": msg}
                        logger.warning(f"model {m.name}: SKIPPED ({msg})")
                    elif not plans[m.name]["run"]:
                        results[m.name] = {"model": m.name, "status": "UP_TO_DATE", "message": ""}
                    else:
                        running[pool.submit(execute_model, m, layer, plans[m.name], run_id, loaded)] = m
                if running:
                    done, _ = wait(list(running), timeout=0.2, return_when=FIRST_COMPLETED)
                    for fut in done:
                        m = running.pop(fut)
                        try:
                            results[m.name] = fut.result()
                        except Exception as e:  # noqa: BLE001 - a worker must never take the runner down
                            results[m.name] = {"model": m.name, "status": "FAILED", "message": clip_error(e)}
                            logger.error(f"model {m.name}: FAILED {e}")
                        progressed = True
                elif not progressed and pending:
                    break
    failed = [r for r in results.values() if r["status"] == "FAILED"]
    skipped = [r for r in results.values() if r["status"] == "SKIPPED"]
    ok = [r for r in results.values() if r["status"] == "SUCCESS"]
    logger.info(f"models: {len(ok)} succeeded, {len(failed)} failed, {len(skipped)} skipped, "
                f"{sum(1 for r in results.values() if r['status'] == 'UP_TO_DATE')} up to date")
    return 1 if failed or skipped else 0


# -----------------------------------------------------------------------------
# list / status
# -----------------------------------------------------------------------------
def models_list():
    try:
        order = order_models(discover())
    except LayerError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    if not order:
        print(f"No data models in {_rel(models_dir())}")
        return 0
    print(f"{'#':>3}  {'model':<22} {'file':<28} {'strategy':<15} {'header':<11} inputs -> outputs (depends_on)")
    for i, m in enumerate(order, 1):
        deps = f" (after {', '.join(m.depends_on)})" if m.depends_on else ""
        print(f"{i:>3}  {m.name:<22} {m.file.name:<28} {m.delta_strategy:<15} {m.source:<11} {', '.join(m.inputs) or '-'} -> {', '.join(m.outputs) or '-'}{deps}")
        for w in m.warnings:
            print(f"       WARNING: {w}")
    return 0


def models_status():
    """Last run of every model and whether it is up to date; read-only."""
    layer = silver_layer()
    try:
        everything = discover()
        order = order_models(everything)
    except LayerError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    rows = 0
    with use_layer(layer):
        print(f"models in {layer.name} ({layer.database})")
        for m in order:
            run = last_run(m.name, layer)
            try:
                plan = plan_model(m, everything, layer=layer)
                state = "UP TO DATE" if not plan["run"] else "DUE: " + "; ".join(plan["reasons"])
            except RuntimeError as e:
                state = f"unknown ({clip_error(e)})"
            last = f"{run['status']} {run['ended_at'][:19]} ({run['mode']}, run {run['run_id']})" if run else "never run"
            print(f"  {m.name:<22} last: {last:<60} {state}")
            if run and run["status"] == "FAILED" and run.get("message"):
                print(f"  {'':<22} {run['message'][:160]}")
            rows += 1
    return 0


# -----------------------------------------------------------------------------
# promote gate (K10)
# -----------------------------------------------------------------------------
def stale_outputs(layer):
    """{"schema.table": reason} of model outputs whose producing model has no SUCCESS newer than its latest silver input delta."""
    out = {}
    try:
        everything = discover()
    except LayerError:
        return out
    for m in everything:
        if not m.outputs:
            continue
        try:
            plan = plan_model(m, everything, layer=layer)
        except RuntimeError as e:
            for o in m.outputs:
                out[o] = f"model {m.name}: its state in {layer.name} cannot be read ({clip_error(e)})"
            continue
        if plan["run"]:
            for o in m.outputs:
                out[o] = f"model {m.name} has no SUCCESS in model_run_log newer than its inputs ({'; '.join(plan['reasons'])})"
    return out
