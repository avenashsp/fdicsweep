#!/usr/bin/env python3
"""
sweep_audit.py - the SIX canonical audit columns of every table in every layer (single source of truth; sweep_common re-exports it).

    createdby      varchar(40)  NOT NULL DEFAULT SESSION_USER
    createddate    timestamp    NOT NULL DEFAULT now()
    modifiedby     varchar(40)  NOT NULL DEFAULT SESSION_USER
    modifieddate   timestamp    NOT NULL DEFAULT now()
    createddate_m  timestamp    NULL     DEFAULT <now() only in a loader / model session, else NULL>
    createdby_m    varchar(40)  NULL     DEFAULT nullif(current_setting('sweep.loader', true), '')

Rules (docs/audit_columns.md): createdby / modifiedby are ALWAYS SESSION_USER (never CURRENT_USER, never a literal); only createdby_m may carry a
hard-coded literal - `dcc_migration` (rows the Oracle / VSAM loaders load and promote copies) or `fe_migration` (rows data models create); only the two
`_m` columns may be NULL (a row inserted by anybody else gets NULL there). First insert: createddate = modifieddate. An update changes ONLY
modifiedby / modifieddate. The session setting `sweep.loader` ('dcc_migration' / 'fe_migration') is what tells the database that the code - not a
person - is writing: loader, promote and model sessions set it, nothing else does.

This module has no dependencies (the data models import it without the rest of the tool).
"""
LOADER_SETTING = "sweep.loader"          # session setting: 'dcc_migration' (loaders, promote of oracle / vsam tables) or 'fe_migration' (data models)
CAPTURE_SETTING = "sweep.capture"        # session setting: '<run id>:<model>' - the DDL of this session is recorded in sweep_ctl.ddl_history
TAG_LOADER = "dcc_migration"
TAG_MODEL = "fe_migration"
TAGS = (TAG_LOADER, TAG_MODEL)

TRIGGER_NAME = "trg_sweep_audit"
FUNCTION = "sweep_ctl.fn_audit_cols"
SETTING_EXPR = f"nullif(current_setting('{LOADER_SETTING}', true), '')"

TS = "timestamp without time zone"
VC = "character varying(40)"

# (name, type, not null, default expression)
AUDIT_COLUMNS = (
    ("createdby", VC, True, "SESSION_USER"),
    ("createddate", TS, True, "now()"),
    ("modifiedby", VC, True, "SESSION_USER"),
    ("modifieddate", TS, True, "now()"),
    ("createddate_m", TS, False, f"CASE WHEN {SETTING_EXPR} IS NOT NULL THEN now() END"),
    ("createdby_m", VC, False, SETTING_EXPR),
)
AUDIT_NAMES = tuple(c[0] for c in AUDIT_COLUMNS)
AUDIT_SET = frozenset(AUDIT_NAMES)
AUDIT_TYPES = {c[0]: c[1] for c in AUDIT_COLUMNS}
# the columns a loader / model never overwrites on an update of an existing row
CREATED_NAMES = ("createdby", "createddate", "createdby_m", "createddate_m")
# legacy variants of the audit columns: never touched by the DDL code; models that still carry them fill them with CURRENT_TIMESTAMP / SESSION_USER
LEGACY_VARIANTS = {
    "created_by": "SESSION_USER", "created_date": "CURRENT_TIMESTAMP", "created_at": "CURRENT_TIMESTAMP",
    "modified_by": "SESSION_USER", "modified_date": "CURRENT_TIMESTAMP", "modified_at": "CURRENT_TIMESTAMP",
    "updated_by": "SESSION_USER", "updated_date": "CURRENT_TIMESTAMP", "updated_at": "CURRENT_TIMESTAMP",
}


def q(name):
    return '"' + str(name).replace('"', '""') + '"'


def column_def(spec):
    name, typ, notnull, default = spec
    return f"{q(name)} {typ}{' NOT NULL' if notnull else ''} DEFAULT {default}"


def create_table_lines(skip=(), indent="  "):
    """The six column definitions for a CREATE TABLE (a name in `skip` is left out)."""
    return [indent + column_def(c) for c in AUDIT_COLUMNS if c[0] not in skip]


def add_columns_sql(schema, table):
    """Idempotent: ADD COLUMN IF NOT EXISTS for the six columns (a NOT NULL column on a non-empty table is added with its DEFAULT, so it succeeds)."""
    tgt = f"{q(schema)}.{q(table)}"
    return "".join(f"ALTER TABLE {tgt} ADD COLUMN IF NOT EXISTS {column_def(c)};\n" for c in AUDIT_COLUMNS)


def trigger_sql(schema, table):
    """Idempotent: the audit trigger of one table (only valid when the table carries all six columns)."""
    tgt = f"{q(schema)}.{q(table)}"
    return (f"DROP TRIGGER IF EXISTS {TRIGGER_NAME} ON {tgt};\n"
            f"CREATE TRIGGER {TRIGGER_NAME} BEFORE INSERT OR UPDATE ON {tgt} FOR EACH ROW EXECUTE FUNCTION {FUNCTION}();\n")


FUNCTION_SQL = f"""CREATE OR REPLACE FUNCTION {FUNCTION}() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE loader text := {SETTING_EXPR};
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
"""


def insert_exprs(tag):
    """Explicit values of an INSERT written by the code (loaders, promote, models): {column: SQL expression}. One now() per statement, so created = modified."""
    return {"createdby": "SESSION_USER", "createddate": "now()", "modifiedby": "SESSION_USER", "modifieddate": "now()",
            "createddate_m": "now()", "createdby_m": "'" + tag + "'"}


def update_assignments():
    """What an UPDATE written by the code sets: ONLY modifiedby / modifieddate."""
    return {"modifiedby": "SESSION_USER", "modifieddate": "now()"}


def session_options(tag, capture=None):
    """PGOPTIONS text that makes a session a loader / model session (a custom setting works with `-c name=value`)."""
    opts = f"-c {LOADER_SETTING}={tag}"
    if capture:
        opts += f" -c {CAPTURE_SETTING}={capture}"
    return opts


def assert_loader_sql():
    """For a fast-bulk COPY, which relies on the column defaults: refuse to run in a session that did not set sweep.loader."""
    return (f"DO $a$ BEGIN IF {SETTING_EXPR} IS NULL THEN RAISE EXCEPTION "
            f"'{LOADER_SETTING} is not set: a loader session must set it (createdby_m would be NULL)'; END IF; END $a$;\n")


def ensure_for_query_sql(query):
    """
    An idempotent DO block that gives every table returned by `query` (columns: s = schema, t = table; duplicates and tables that do not exist,
    views, partition children and sweep_ctl / sweep_delta / system schemas are skipped) the six audit columns and the audit trigger. The
    post-step of fwd-ddl (tables an FE file created or altered) and of the models runner (tables a model wrote) is built from it.
    """
    alters = "".join(f"ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS {column_def(c)};\n" for c in AUDIT_COLUMNS)
    trig = (f"DROP TRIGGER IF EXISTS {TRIGGER_NAME} ON %I.%I;\n"
            f"CREATE TRIGGER {TRIGGER_NAME} BEFORE INSERT OR UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION {FUNCTION}();")
    n_alter = len(AUDIT_COLUMNS)
    args_alter = ", ".join(["r.s, r.t"] * n_alter)
    return (f"DO $audit$ DECLARE r record; BEGIN\n"
            f"  FOR r IN SELECT DISTINCT x.s, x.t FROM ({query}) x\n"
            f"    JOIN pg_class c ON c.oid = to_regclass(format('%I.%I', x.s, x.t))\n"
            f"   WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition AND x.s NOT IN ('sweep_ctl', 'sweep_delta', 'sweep_meta', 'pg_catalog', 'information_schema')\n"
            f"  LOOP\n"
            f"    EXECUTE format($q${alters}$q$, {args_alter});\n"
            f"    EXECUTE format($q${trig}$q$, r.s, r.t, r.s, r.t);\n"
            f"  END LOOP;\nEND $audit$;\n")
