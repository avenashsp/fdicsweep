"""
_dm_support.py - shared helpers of the real data models DM01-DM06 (a file starting with "_" is never a model: the runner skips it).

The models are standalone scripts (`python3 data_models/DM03_bank.py --execute`) that import this module from their own folder.
It keeps the audit-column handling of every model identical (docs/audit_columns.md):

  * the six canonical columns come from sweep_audit (the single source of truth of the repository),
  * the session is a model session: `sweep.loader = fe_migration` (PGOPTIONS), so a plain default fills createdby_m,
  * INSERT  : createdby = modifiedby = SESSION_USER, createddate = modifieddate = ONE now(), createddate_m = now(), createdby_m = 'fe_migration'
  * UPDATE  : data columns + modifiedby = SESSION_USER + modifieddate = now(); nothing else (DM03 also flips createdby_m to 'fe_migration')
  * a legacy variant of an audit column (created_at, created_by, modified_date, ...) that a target table carries is filled with
    CURRENT_TIMESTAMP / SESSION_USER and never relabelled, renamed or dropped.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import sweep_audit  # noqa: E402  (after the path set-up)

TAG = sweep_audit.TAG_MODEL                    # 'fe_migration': the only literal a model ever writes (createdby_m)
RESULT_PREFIX = "SWEEP_MODEL_RESULT"
CANONICAL = sweep_audit.AUDIT_NAMES
LEGACY = sweep_audit.LEGACY_VARIANTS


def libpq_defaults():
    """Connection defaults from the libpq environment (no host, user or password is ever hard-coded)."""
    return {"host": os.environ.get("PGHOST") or "localhost", "port": int(os.environ.get("PGPORT") or 5432),
            "user": os.environ.get("PGUSER") or "postgres", "password": os.environ.get("PGPASSWORD") or "",
            "db": os.environ.get("PGDATABASE") or "postgres"}


def init_session():
    """Makes every psql session of this process a model session (sweep.loader = fe_migration). Call once, before any psql."""
    opt = sweep_audit.session_options(TAG)
    cur = os.environ.get("PGOPTIONS", "")
    if f"{sweep_audit.LOADER_SETTING}=" not in cur:
        os.environ["PGOPTIONS"] = (cur + " " + opt).strip()


def ensure_audit_sql(target):
    """Idempotent DDL that makes `target` ("schema.table") carry the six audit columns and the audit trigger. Always safe to re-run."""
    schema, table = target.split(".", 1)
    return sweep_audit.add_columns_sql(schema, table) + sweep_audit.trigger_sql(schema, table)


def insert_audit(all_cols):
    """
    [(column, data type, SQL expression)] to append to an INSERT list. `all_cols` = {lower name: (real name, data type)} of the target.
    Every canonical column is listed (ensure_audit_sql has run before the INSERT); legacy variants only when the table has them.
    """
    ex = sweep_audit.insert_exprs(TAG)
    out = [(c, sweep_audit.AUDIT_TYPES[c], ex[c]) for c in CANONICAL]
    for name, expr in LEGACY.items():
        if name in all_cols and name not in ex:
            out.append((all_cols[name][0], all_cols[name][1], expr))
    return out


def update_audit(all_cols, flip_created_by_m=False):
    """[(column, SQL expression)] an UPDATE sets besides the data columns: modifiedby / modifieddate (+ createdby_m for DM03)."""
    out = [("modifiedby", "SESSION_USER"), ("modifieddate", "now()")]
    for name in ("modified_by", "modified_date", "modified_at", "updated_by", "updated_date", "updated_at"):
        if name in all_cols:
            out.append((all_cols[name][0], LEGACY[name]))
    if flip_created_by_m:
        out.append(("createdby_m", f"'{TAG}'"))
    return out


def own_rows_where():
    """Rows this model family created (the model's own rows): createdby_m = 'fe_migration'."""
    return f'"createdby_m" = \'{TAG}\''


def result_line(payload):
    import json
    return f"{RESULT_PREFIX} {json.dumps(payload, sort_keys=True)}"
