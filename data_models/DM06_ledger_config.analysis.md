# DM06_ledger_config.py - analysis note

Real data model Ledger Config (spec: Change L + M, 6.3.0). The user's standalone original (DM06_ledger_config.txt) is the reference; the adaptations are listed below.

## What the script does

For each of the nine schemas (bcs, bss01001, bss01104, bss01105, bss01108, bss01110, bss01112, bss01115, bss01116) reads `<schema>.stg_transcodedescptrec_v` and inserts the rows whose `(broker_id, tran_code)` is not in `<schema>.ledger_config` yet (`WHERE NOT EXISTS` + `ON CONFLICT DO NOTHING`); `--replace` deletes the model's own rows first, `--truncate` empties the table.

`is_active` defaults to TRUE: that is a business default, not an audit column, and is kept.

Target: `<schema>.ledger_config (9 schemas)` - natural key (broker_id, tran_code) = pk_ledger_config1. DRY RUN unless `--execute`.

## What was adapted (and nothing else)

| Topic | Change |
|---|---|
| Names | no test-copy suffix on any table name (the originals pointed at test copies); the source schema is `bss00098` |
| Connection | no hard-coded host / user / password: libpq environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`), same CLI overrides |
| Runner | `SWEEP_MODEL` header (name, inputs, outputs, depends_on, delta_strategy, update_key, timeout), `--mode full|delta` (both idempotent), `SWEEP_MODEL_WORKDIR` for generated files, last line `SWEEP_MODEL_RESULT {json}`; still runs standalone: `python3 data_models/DM06_ledger_config.py --execute` |
| Audit columns | the `CREATED_BY` constant (`FE_MIGRATION_TEAM` / `DATA_MIGRATION`) and every literal in createdby / createddate / modifiedby / modifieddate are gone. Each run first executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for the six canonical audit columns and creates the `trg_sweep_audit` trigger, then: INSERT = createdby = modifiedby = `SESSION_USER`, createddate = modifieddate = one `now()`, createddate_m = `now()`, createdby_m = `'fe_migration'`. INSERT only (insert-if-missing); `--replace` deletes the rows with `createdby_m = 'fe_migration'`. |
| Session | `sweep.loader = 'fe_migration'` is set for every psql session of the script (PGOPTIONS), so a default fills createdby_m |
| Own rows | a row of the model family is recognised by `createdby_m = 'fe_migration'` (not by a name in createdby) |
| Legacy columns | a target that carries `created_at`, `created_by`, `modified_date`, ... gets CURRENT_TIMESTAMP / SESSION_USER in them; they are never renamed, relabelled or dropped |
