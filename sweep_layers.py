#!/usr/bin/env python3
"""
sweep_layers.py - the four medallion layers (bronze -> silver -> gold_backup -> gold), their databases and manifests.

  * sweep_layers.json lists the layers in chain order (database, env file, upstream, manifest, allow_ddl, trigger_mode,
    lock/statement timeouts, default conflict policy). No secrets: those live in the git-ignored sweep.<layer>.env files.
  * `--layer` is REQUIRED on every step that touches a database. `ensure_layer` selects the layer, loads its environment
    file and REFUSES to run when the connected database is not the one configured for the layer (so a bronze command can
    never touch gold by accident).
  * layers/<layer>_tables.csv is the manifest of a layer: a layer only ever touches the tables listed in its own manifest
    (tables created by the application or the FE team are never touched).
"""
import csv
import json
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import sweep_common as c
from sweep_common import BASE_DIR, get_logger, norm_col, parse_env_line
from sweep_merge import atomic_write_csv

logger = get_logger("sweep_layers", to_file=False)

LAYERS_FILE = Path(os.environ.get("SWEEP_LAYERS_FILE") or (BASE_DIR / "sweep_layers.json"))
MANIFEST_FIELDS = ["track", "schema", "table", "promote_mode", "conflict_policy", "enabled"]
TRACKS = ("oracle", "vsam", "model_output")
PROMOTE_MODES = ("upsert", "replace", "insert_only")
CONFLICT_POLICIES = ("source_wins", "keep_target")
EXIT_NO_LAYER = 2
EXIT_GUARD = 3


class LayerError(RuntimeError):
    """Bad layer configuration or a refused layer operation."""


@dataclass
class Layer:
    name: str
    database: str
    env_file: str
    upstream: str = None
    manifest: str = ""
    allow_ddl: bool = True
    trigger_mode: str = "auto"
    lock_timeout: str = "30s"
    statement_timeout: str = "0"
    conflict_policy: str = "source_wins"
    downstream: str = field(default=None)

    @property
    def manifest_path(self):
        p = Path(self.manifest or f"layers/{self.name}_tables.csv")
        return p if p.is_absolute() else _root() / p

    @property
    def env_path(self):
        p = Path(self.env_file or f"sweep.{self.name}.env")
        return p if p.is_absolute() else _root() / p


def _root():
    """Folder of sweep_layers.json (manifests and env files are resolved against it; the repository root by default)."""
    return LAYERS_FILE.parent


def load_layers(path=None):
    """The layers in chain order. Raises LayerError for a malformed file."""
    path = Path(path or LAYERS_FILE)
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise LayerError(f"cannot read the layer profiles {path}: {e}")
    layers = []
    for raw in doc.get("layers", []):
        known = {k: raw[k] for k in Layer.__dataclass_fields__ if k in raw}
        try:
            layers.append(Layer(**known))
        except TypeError as e:
            raise LayerError(f"layer entry {raw.get('name', raw)!r} in {path} is incomplete: {e}")
    if not layers:
        raise LayerError(f"no layers defined in {path}")
    names = [l.name for l in layers]
    if len(set(names)) != len(names):
        raise LayerError(f"duplicate layer names in {path}")
    for i, l in enumerate(layers):
        expected = names[i - 1] if i else None
        if l.upstream != expected:
            raise LayerError(f"layer {l.name!r}: upstream must be {expected!r} (layers are listed in chain order), got {l.upstream!r}")
        if i:
            layers[i - 1].downstream = l.name
    return layers


def layer_names():
    return [l.name for l in load_layers()]


def get_layer(name):
    name = str(name or "").strip().lower()
    for l in load_layers():
        if l.name == name:
            return l
    raise LayerError(f"unknown layer {name!r}: choose one of {', '.join(layer_names())}")


def chain(from_layer=None, to_layer=None):
    """Layers in chain order, optionally from `from_layer` to `to_layer` (inclusive)."""
    layers = load_layers()
    names = [l.name for l in layers]
    i = names.index(get_layer(from_layer).name) if from_layer else 0
    j = names.index(get_layer(to_layer).name) if to_layer else len(layers) - 1
    return layers[i:j + 1]


# -----------------------------------------------------------------------------
# Connections
# -----------------------------------------------------------------------------
@dataclass
class LayerConn:
    layer: str
    host: str
    port: str
    user: str
    password: str
    sslmode: str
    dbname: str


def layer_conn(layer):
    """
    Connection settings of a layer: host / port / user / password from its env file (when present) over the process
    environment; the DATABASE is the layer's own (its env file may name it, the process-wide PGDATABASE never overrides it).
    """
    layer = layer if isinstance(layer, Layer) else get_layer(layer)
    env = dict(os.environ)
    file_env = {}
    if layer.env_path.exists():
        for raw in layer.env_path.read_text(encoding="utf-8").splitlines():
            kv = parse_env_line(raw)
            if kv:
                file_env[kv[0]] = kv[1]
        env.update(file_env)
    return LayerConn(layer.name, env.get("PGHOST", "localhost"), env.get("PGPORT", "5432"), env.get("PGUSER", "postgres"),
                     env.get("PGPASSWORD") or env.get("PG_PASSWORD") or env.get("DB_PASSWORD") or "",
                     env.get("PGSSLMODE", "prefer"), file_env.get("PGDATABASE") or layer.database)


@contextmanager
def use_layer(layer):
    """Points the process-wide psql helpers (sweep_common.run_psql & co.) at `layer` for the duration of the block."""
    conn = layer_conn(layer)
    saved = {k: getattr(c, k) for k in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "PGSSLMODE")}
    saved_env = {k: os.environ.get(k) for k in ("PGPASSWORD", "PGDATABASE")}
    saved_opts = c.EXTRA_PGOPTIONS
    c.PGHOST, c.PGPORT, c.PGDATABASE, c.PGUSER, c.PGPASSWORD, c.PGSSLMODE = conn.host, conn.port, conn.dbname, conn.user, conn.password, conn.sslmode
    lay = layer if isinstance(layer, Layer) else get_layer(layer)
    opts = []
    if lay.lock_timeout and str(lay.lock_timeout) != "0":
        opts.append(f"-c lock_timeout={lay.lock_timeout}")       # no statement of ours waits for a lock longer than this (gold: 5s)
    if lay.statement_timeout and str(lay.statement_timeout) != "0":
        opts.append(f"-c statement_timeout={lay.statement_timeout}")
    c.EXTRA_PGOPTIONS = " ".join(opts)
    if conn.password:
        os.environ["PGPASSWORD"] = conn.password
    os.environ["PGDATABASE"] = conn.dbname
    try:
        yield conn
    finally:
        for k, v in saved.items():
            setattr(c, k, v)
        c.EXTRA_PGOPTIONS = saved_opts
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def same_credentials(a, b):
    """Cross-database transport spawns psql for the source with the target's credentials: they must be the same."""
    return (a.host, str(a.port), a.user, a.password) == (b.host, str(b.port), b.user, b.password)


# -----------------------------------------------------------------------------
# --layer handling for the steps
# -----------------------------------------------------------------------------
def add_layer_arg(parser):
    """`--layer` (falls back to SWEEP_LAYER); steps that touch a database call ensure_layer after parsing."""
    if "--layer" not in {o for a in parser._actions for o in a.option_strings}:
        parser.add_argument("--layer", default=None,
                            help="Medallion layer to work on: bronze, silver, gold_backup or gold (required for steps that touch a database)")
    if "--allow-ddl" not in {o for a in parser._actions for o in a.option_strings}:
        parser.add_argument("--allow-ddl", action="store_true",
                            help="Allow DDL (tables, keys, indexes, views, triggers) on a layer with allow_ddl=false (gold_backup, gold)")


def _fail(message, code, log=None):
    (log or logger).error(message)
    sys.exit(code)


def current_database():
    return (c.query_db_rows("SELECT current_database()") or [""])[0].strip()


def ensure_layer(args, log=None, *, required=True, db=True, ddl_what=None):
    """
    Selects the layer for a step. Returns the Layer (None when no layer was given and none is required).
    `db=True` also verifies the connected database is the one configured for the layer; a mismatch ends the process.
    `ddl_what` names a step that changes the schema: it is refused (before the database is touched) on a layer with allow_ddl=false
    unless --allow-ddl was given.
    """
    name = (getattr(args, "layer", None) or os.environ.get("SWEEP_LAYER") or "").strip().lower()
    if not name:
        if required:
            _fail("--layer is required for steps that touch a database: choose one of "
                  + ", ".join(layer_names()) + " (or set SWEEP_LAYER)", EXIT_NO_LAYER, log)
        return None
    try:
        layer = get_layer(name)
    except LayerError as e:
        _fail(str(e), EXIT_NO_LAYER, log)
    if layer.downstream is None and not getattr(args, "layer", None):
        _fail(f"layer {layer.name!r} (the live database the application works in) must be selected explicitly with --layer {layer.name}; "
              f"SWEEP_LAYER alone is not enough", EXIT_NO_LAYER, log)
    if c.LAYER and c.LAYER != layer.name:
        _fail(f"internal error: environment was loaded for layer {c.LAYER!r} but --layer is {layer.name!r}", EXIT_GUARD, log)
    if ddl_what:
        require_ddl(layer, args, log, ddl_what)
    if db:
        verify_database(layer, log)
    return layer


def verify_database(layer, log=None):
    """Refuses to continue when the connected database is not the layer's configured database."""
    try:
        actual = current_database()
    except RuntimeError as e:
        _fail(f"cannot connect to the database of layer {layer.name} ({layer.database}): {e}", EXIT_GUARD, log)
    if actual.lower() != layer.database.lower():
        _fail(f"layer guard: connected to database {actual!r}, but layer {layer.name!r} is configured as {layer.database!r}. "
              f"Check PGDATABASE / {layer.env_file}; nothing was changed.", EXIT_GUARD, log)
    (log or logger).info(f"Layer {layer.name}: database {actual} verified")


def require_ddl(layer, args, log=None, what="this step"):
    """Steps that change the schema refuse to run on gold / gold_backup unless --allow-ddl is given explicitly."""
    if layer is not None and not layer.allow_ddl and not getattr(args, "allow_ddl", False):
        _fail(f"{what} changes the schema, and layer {layer.name!r} has allow_ddl=false (schema changes there come only from "
              f"controlled migrations by the Database team). Pass --allow-ddl explicitly if this is such a change.",
              EXIT_NO_LAYER, log)


def require_bronze(layer, log=None, what="Step 06"):
    """S3 files are read ONCE, into bronze only; the other layers are filled by `sweep.py promote`."""
    if layer is not None and layer.upstream is not None:
        _fail(f"{what} reads S3 and only loads the first layer ({load_layers()[0].name}); layer {layer.name!r} is filled from "
              f"{layer.upstream!r} with: python3 sweep.py promote --from {layer.upstream} --to {layer.name}", EXIT_NO_LAYER, log)


# -----------------------------------------------------------------------------
# Manifests
# -----------------------------------------------------------------------------
def _truthy(v):
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y")


def read_manifest(layer, include_disabled=False):
    """Rows of layers/<layer>_tables.csv (schema / table lower-cased). [] when the manifest does not exist."""
    layer = layer if isinstance(layer, Layer) else get_layer(layer)
    path = layer.manifest_path
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        rows = []
        for r in csv.DictReader(fh):
            if not (r.get("schema") or "").strip() or not (r.get("table") or "").strip():
                continue
            row = {
                "track": (r.get("track") or "oracle").strip().lower(),
                "schema": r["schema"].strip().lower(),
                "table": r["table"].strip().lower(),
                "promote_mode": (r.get("promote_mode") or "upsert").strip().lower(),
                "conflict_policy": (r.get("conflict_policy") or layer.conflict_policy).strip().lower(),
                "enabled": _truthy(r.get("enabled", "true")),
            }
            if include_disabled or row["enabled"]:
                rows.append(row)
    return rows


def validate_manifest_rows(rows, name="manifest"):
    problems = []
    for r in rows:
        if r["track"] not in TRACKS:
            problems.append(f"{r['schema']}.{r['table']}: unknown track {r['track']!r}")
        if r["promote_mode"] not in PROMOTE_MODES:
            problems.append(f"{r['schema']}.{r['table']}: unknown promote_mode {r['promote_mode']!r}")
        if r["conflict_policy"] not in CONFLICT_POLICIES:
            problems.append(f"{r['schema']}.{r['table']}: unknown conflict_policy {r['conflict_policy']!r}")
    if problems:
        raise LayerError(f"{name}: " + "; ".join(problems))


def manifest_tables(layer):
    """{(schema, table): row} of the ENABLED tables of a layer."""
    return {(r["schema"], r["table"]): r for r in read_manifest(layer)}


def write_manifest(layer, rows, *, force=False):
    """Writes layers/<layer>_tables.csv; an existing manifest is only replaced with force (a .bak is kept)."""
    layer = layer if isinstance(layer, Layer) else get_layer(layer)
    path = layer.manifest_path
    if path.exists() and not force:
        raise LayerError(f"{path} exists: it is reviewed by people, so it is not overwritten (use --force to replace it; a .bak is kept)")
    if path.exists():
        (path.with_name(path.name + ".bak")).write_bytes(path.read_bytes())
    ordered = sorted(rows, key=lambda r: (r["track"], r["schema"], r["table"]))
    atomic_write_csv(path, [{**r, "enabled": "true" if r.get("enabled", True) else "false"} for r in ordered], MANIFEST_FIELDS)
    return path


def restrict_to_manifest(layer, pairs, log=None, what="DDL"):
    """
    Keeps only the (schema, table) pairs the layer's manifest lists. A layer with allow_ddl=false needs a manifest; for the
    others a missing manifest means "no restriction" (with a warning).
    """
    pairs = list(pairs)
    if layer is None:
        return pairs
    listed = manifest_tables(layer)
    if not layer.manifest_path.exists():
        if not layer.allow_ddl:
            _fail(f"layer {layer.name!r} has no manifest ({layer.manifest_path}); {what} is limited to the tables of the manifest",
                  EXIT_NO_LAYER, log)
        (log or logger).warning(f"layer {layer.name}: no manifest {layer.manifest_path}; {what} is not restricted to a table list")
        return pairs
    kept = [(s, t) for s, t in pairs if (str(s).lower(), norm_col(t)) in listed]
    dropped = len(pairs) - len(kept)
    if dropped:
        (log or logger).info(f"layer {layer.name}: {dropped} table(s) are not in the manifest and are left alone")
    return kept
