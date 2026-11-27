# DM05_program_bank_rate_tier.py - analysis note

Real data model Program Bank Rate Tier (spec: Change L + M, 6.3.0). The user's standalone original (DM05_program_bank_rate_tier.txt) is the reference; the adaptations are listed below.

## What the script does

Joins the live `bcs.program_bank_rate` with `bcs.cusip`, `bcs.cusipcusipgroup`, `bcs.rate`, `bcs.ratetier`, `bcs.flattier` and `bcs.indexedtier`, classifies each tier NEW / CHANGED / UNCHANGED on `(program_bank_rate_id, tier_type)` and applies the delta.

DM05 depends on DM04 (`depends_on` in the header): it reads the program_bank_rate rows DM04 writes, so the runner orders it after DM04. It no longer auto-detects a backup copy of program_bank_rate; the live table is used (`--pbr-table` can name another).

Target: `bcs.program_bank_rate_tier` - natural key (program_bank_rate_id, tier_type). DRY RUN unless `--execute`.

## What was adapted (and nothing else)

| Topic | Change |
|---|---|
| Names | no test-copy suffix on any table name (the originals pointed at test copies); the source schema is `bss00098` |
| Connection | no hard-coded host / user / password: libpq environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`), same CLI overrides |
| Runner | `SWEEP_MODEL` header (name, inputs, outputs, depends_on, delta_strategy, update_key, timeout), `--mode full|delta` (both idempotent), `SWEEP_MODEL_WORKDIR` for generated files, last line `SWEEP_MODEL_RESULT {json}`; still runs standalone: `python3 data_models/DM05_program_bank_rate_tier.py --execute` |
| Audit columns | the `CREATED_BY` constant (`FE_MIGRATION_TEAM` / `DATA_MIGRATION`) and every literal in createdby / createddate / modifiedby / modifieddate are gone. Each run first executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for the six canonical audit columns and creates the `trg_sweep_audit` trigger, then: INSERT = createdby = modifiedby = `SESSION_USER`, createddate = modifieddate = one `now()`, createddate_m = `now()`, createdby_m = `'fe_migration'`. UPDATE sets the value fields (flat_rate, lower_limit, upper_limit, effective_date, expiration_date, published_index_id, ...) + modifiedby / modifieddate. |
| Session | `sweep.loader = 'fe_migration'` is set for every psql session of the script (PGOPTIONS), so a default fills createdby_m |
| Own rows | a row of the model family is recognised by `createdby_m = 'fe_migration'` (not by a name in createdby) |
| Legacy columns | a target that carries `created_at`, `created_by`, `modified_date`, ... gets CURRENT_TIMESTAMP / SESSION_USER in them; they are never renamed, relabelled or dropped |
