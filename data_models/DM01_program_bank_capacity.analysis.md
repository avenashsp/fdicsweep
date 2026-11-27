# DM01_program_bank_capacity.py - analysis note

Real data model Program Bank Capacity (spec: Change L + M, 6.3.0). The user's standalone original (DM01_program_bank_capacity.txt) is the reference; the adaptations are listed below.

## What the script does

Reads `bcs.capacityplan` joined to `bcs.cusipcusipgroup`, `bcs.cusip` and `bcs.bank`, classifies every source row NEW / CHANGED / UNCHANGED against the target on `(broker_id, bank_id)`, writes `stage.csv` with the delta rows and one transaction (`load.sql`) that UPDATEs the CHANGED rows and INSERTs the NEW rows.

The business override `(broker 108, bank 1707) -> bank_capacity 25000000.0000` is kept (a hard business rule of the source specification).

Target: `bcs.program_bank_capacity` - natural key (broker_id, bank_id). DRY RUN unless `--execute`.

## What was adapted (and nothing else)

| Topic | Change |
|---|---|
| Names | no test-copy suffix on any table name (the originals pointed at test copies); the source schema is `bss00098` |
| Connection | no hard-coded host / user / password: libpq environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`), same CLI overrides |
| Runner | `SWEEP_MODEL` header (name, inputs, outputs, depends_on, delta_strategy, update_key, timeout), `--mode full|delta` (both idempotent), `SWEEP_MODEL_WORKDIR` for generated files, last line `SWEEP_MODEL_RESULT {json}`; still runs standalone: `python3 data_models/DM01_program_bank_capacity.py --execute` |
| Audit columns | the `CREATED_BY` constant (`FE_MIGRATION_TEAM` / `DATA_MIGRATION`) and every literal in createdby / createddate / modifiedby / modifieddate are gone. Each run first executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for the six canonical audit columns and creates the `trg_sweep_audit` trigger, then: INSERT = createdby = modifiedby = `SESSION_USER`, createddate = modifieddate = one `now()`, createddate_m = `now()`, createdby_m = `'fe_migration'`. UPDATE sets bank_capacity, overflow_capacity_percent, capacity_percent_low_limit, capacity_percent_high_limit, is_active + modifiedby / modifieddate. |
| Session | `sweep.loader = 'fe_migration'` is set for every psql session of the script (PGOPTIONS), so a default fills createdby_m |
| Own rows | a row of the model family is recognised by `createdby_m = 'fe_migration'` (not by a name in createdby) |
| Legacy columns | a target that carries `created_at`, `created_by`, `modified_date`, ... gets CURRENT_TIMESTAMP / SESSION_USER in them; they are never renamed, relabelled or dropped |
