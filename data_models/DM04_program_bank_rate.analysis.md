# DM04_program_bank_rate.py - analysis note

Real data model Program Bank Rate (spec: Change L + M, 6.3.0). The user's standalone original (DM04_program_bank_rate.txt) is the reference; the adaptations are listed below.

## What the script does

Joins `bcs.rate`, `bcs.cusipcusipgroup`, `bcs.cusip`, `bcs.bank`, `bcs.daysinyear` and `bcs.ratetier` (6 tables), classifies each row NEW / CHANGED / UNCHANGED on `(bank_id, cusip_id)` (value fields: days_in_year, effective_date, expiration_date, balance_distribution, use_daily_rate) and applies the delta in one transaction.

The target is the live table `bcs.program_bank_rate` (the originals pointed at a test copy with a backup suffix; that suffix is removed).

Target: `bcs.program_bank_rate` - natural key (bank_id, cusip_id). DRY RUN unless `--execute`.

## What was adapted (and nothing else)

| Topic | Change |
|---|---|
| Names | no test-copy suffix on any table name (the originals pointed at test copies); the source schema is `bss00098` |
| Connection | no hard-coded host / user / password: libpq environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`), same CLI overrides |
| Runner | `SWEEP_MODEL` header (name, inputs, outputs, depends_on, delta_strategy, update_key, timeout), `--mode full|delta` (both idempotent), `SWEEP_MODEL_WORKDIR` for generated files, last line `SWEEP_MODEL_RESULT {json}`; still runs standalone: `python3 data_models/DM04_program_bank_rate.py --execute` |
| Audit columns | the `CREATED_BY` constant (`FE_MIGRATION_TEAM` / `DATA_MIGRATION`) and every literal in createdby / createddate / modifiedby / modifieddate are gone. Each run first executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for the six canonical audit columns and creates the `trg_sweep_audit` trigger, then: INSERT = createdby = modifiedby = `SESSION_USER`, createddate = modifieddate = one `now()`, createddate_m = `now()`, createdby_m = `'fe_migration'`. UPDATE sets balance_distribution, use_daily_rate, days_in_year, effective_date, expiration_date, is_active + modifiedby / modifieddate. |
| Session | `sweep.loader = 'fe_migration'` is set for every psql session of the script (PGOPTIONS), so a default fills createdby_m |
| Own rows | a row of the model family is recognised by `createdby_m = 'fe_migration'` (not by a name in createdby) |
| Legacy columns | a target that carries `created_at`, `created_by`, `modified_date`, ... gets CURRENT_TIMESTAMP / SESSION_USER in them; they are never renamed, relabelled or dropped |
