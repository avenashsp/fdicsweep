# The six canonical audit columns (6.3.0)

Every table in every layer (bronze, silver, gold backup, gold) carries the same six audit columns. They record **who** created and
changed a row, **when**, and - for rows the migration tools wrote - **which tool** wrote them. The single source of truth is the module
`sweep_audit.py` (re-exported by `sweep_common.py`); every generator, loader, promote, model and check reads the definitions from there.

| Column | Type | NULL? | Default |
|---|---|---|---|
| `createdby` | varchar(40) | NOT NULL | `SESSION_USER` |
| `createddate` | timestamp | NOT NULL | `now()` |
| `modifiedby` | varchar(40) | NOT NULL | `SESSION_USER` |
| `modifieddate` | timestamp | NOT NULL | `now()` |
| `createddate_m` | timestamp | NULL allowed | `now()` when `sweep.loader` is set, otherwise NULL |
| `createdby_m` | varchar(40) | NULL allowed | the value of `sweep.loader`, otherwise NULL |

## The rules

1. **`createdby` and `modifiedby` are always `SESSION_USER`** - never `CURRENT_USER`, never a literal.
2. **Only `createdby_m` may hold a literal**, and only one of two values:
   * `'dcc_migration'` - rows written by the Oracle / VSAM loaders and by promote of the oracle / vsam tracks;
   * `'fe_migration'` - rows written by the data models (DM01-DM06), FE-created data, promote of model outputs.
3. **Only the two `_m` columns may be NULL.** A row inserted by a third party (an application, a DBA) has `createddate_m` and `createdby_m` NULL
   and `createdby` / `modifiedby` = that user. The four mandatory columns are never NULL.
4. On the **first insert** `createddate = modifieddate` (one `now()`).
5. An **update changes only `modifiedby` and `modifieddate`**. `createdby`, `createddate`, `createddate_m` and `createdby_m` of an existing row are
   never overwritten by a loader (the trigger keeps them; the loaders' `ON CONFLICT DO UPDATE` lists none of them and the `IS DISTINCT FROM`
   guard excludes all six). Exception: DM03 flips `createdby_m` of a bank it enriched from `dcc_migration` to `fe_migration`; `createddate_m` stays.
6. A **source column that is named like one of the six** (an Oracle table with a `CREATEDBY` column) is kept as data but takes the standard type,
   NOT NULL (for the four) and default; NULLs in the file are filled with the defaults.
7. **Legacy variants** (`created_at`, `created_by`, `modified_date`, `updated_at`, ...) are never touched by any DDL code - no rename, relabel or drop.
   A model whose target carries them fills them with `CURRENT_TIMESTAMP` / `SESSION_USER`.
8. There is **no** `audit-columns apply`, no `--drop-legacy`, no migration of existing tables: the data is reloaded fresh into tables that are
   created with the six columns.

## The trigger

`sweep_ctl.fn_audit_cols()` (created by migration 3 / `sweep_meta.sql`) is attached to every table as `trg_sweep_audit BEFORE INSERT OR UPDATE`:

* INSERT: `createdby` / `modifiedby` default to `SESSION_USER`, `createddate` to `now()`, `modifieddate` to `coalesce(NEW.modifieddate, NEW.createddate)`.
* UPDATE: `createdby` / `createddate` keep their old values; the two `_m` columns keep their old values unless the session is a loader / model session
  (`sweep.loader` set); `modifiedby := SESSION_USER`, `modifieddate := now()`.

The trigger is created idempotently on every table the code creates or alters: the Oracle step 04 / VSAM step 04 DDL, the post-step of `fwd-ddl`
(after every FE file, on each table the file created or altered; the FE file itself stays untouched and FE runs in silver only) and the post-step of
the models runner (every output table and every table a model created or altered). Control tables of `sweep_ctl` / `sweep_delta` are excluded.

## The loader session setting `sweep.loader`

`createdby_m` / `createddate_m` get their values from the **session setting** `sweep.loader`:

* The Oracle / VSAM loaders (Step 06) run their psql sessions with `PGOPTIONS='-c sweep.loader=dcc_migration'` (also the fast-bulk `\copy`
  sessions, which rely on the column defaults). Promote sets `dcc_migration` for the oracle / vsam tracks and `fe_migration` for `model_output`.
* The models runner and a model run by hand (`init_session()` in `data_models/_dm_support.py`) use `fe_migration`.
* A plain session (no setting) leaves both `_m` columns NULL - that is how a third-party insert is told apart.
* Fast bulk **asserts** the setting before the `COPY` and fails loudly when it is missing (otherwise `createdby_m` would be NULL).

## Who sets what

| Writer | createdby / modifiedby | createddate / modifieddate | createddate_m | createdby_m |
|---|---|---|---|---|
| Oracle / VSAM loader (staged, explicit INSERT list) | `SESSION_USER` | one `now()` | `now()` | `'dcc_migration'` |
| Oracle fast bulk (defaults) | `SESSION_USER` | `now()` | `now()` | `'dcc_migration'` (from the session setting) |
| Loader UPDATE of an existing row | `modifiedby = SESSION_USER` | `modifieddate = now()` | unchanged | unchanged |
| Promote | copies the six as data (never writes a literal into the four mandatory ones) | | copied | copied (`dcc_migration` / `fe_migration` as loaded) |
| Data model INSERT | `SESSION_USER` | one `now()` | `now()` | `'fe_migration'` |
| Data model UPDATE | `modifiedby = SESSION_USER` | `modifieddate = now()` | unchanged | unchanged (DM03: `'fe_migration'`) |
| Third-party insert | the user | `now()` | NULL | NULL |

## Checks

`sweep.py layers check` has the check **audit columns** (WARN for every manifest table that lacks one of the six) and `layers validate` lists
`MISSING_AUDIT_COLUMN` rows in the schema comparison. Delta detection, the row fingerprints of `layers reconcile` and the models' self-compare
ignore the six columns (`sweep_delta.TOOL_AUDIT_COLS`), so a different `modifiedby` never counts as a data difference.

## FAQ

**Why is `createddate_m` filled by a CASE default instead of a plain `DEFAULT NULL`?** So that a fast-bulk `COPY` (which relies on defaults) gets `now()`
in a loader session and NULL for a third party, with one definition for both.

**Why do the loaders list the six columns explicitly?** The staged load / upsert and promote run with triggers off, so they set the values themselves.

**Can I see which rows were written by the tools?** `SELECT createdby_m, count(*) FROM <table> GROUP BY 1` - NULL = application rows.

**Why `SESSION_USER` and not `CURRENT_USER`?** `CURRENT_USER` changes inside `SECURITY DEFINER` functions; the audit must name the account that logged in.
