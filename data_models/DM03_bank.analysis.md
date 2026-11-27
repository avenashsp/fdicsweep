# DM03_bank.py - analysis note

Real data model Bank mnemonic enrichment (spec: Change L + M, 6.3.0). The user's standalone original (DM03_bank.txt) is the reference; the adaptations are listed below.

## What the script does

Adds `bank_statement_mnemonic varchar(10)` to `bcs.bank` when it is missing (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`; the alias `bank_statemnt_mnemonic` is detected), fills it from `bcs.stg_program_bank_primary_rec_v` (match on upper(trim(displayname)) = upper(trim(bank_name))) and applies the five manual overrides (First Bank & Trust -> FRSTBKTR, East West Bank -> ESTWST, American Bank of Dallas -> AMERICAN, Union Bank & Trust -> UNIONBK, Metropolitan Commercial -> METCOMM).

`bcs.bank` is both an Oracle table (loaded with createdby_m = 'dcc_migration') and an output of this model. When the model updates a bank, `createdby_m` flips from 'dcc_migration' to 'fe_migration' and `createddate_m` is left as loaded: the row now carries the model's data. The ALTER TABLE is captured by the runner (docs/data_models.md, DDL capture) and replayed by promote into gold backup and gold before the rows are copied.

Target: `bcs.bank` - natural key bankid. DRY RUN unless `--execute`.

## What was adapted (and nothing else)

| Topic | Change |
|---|---|
| Names | no test-copy suffix on any table name (the originals pointed at test copies); the source schema is `bss00098` |
| Connection | no hard-coded host / user / password: libpq environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`), same CLI overrides |
| Runner | `SWEEP_MODEL` header (name, inputs, outputs, depends_on, delta_strategy, update_key, timeout), `--mode full|delta` (both idempotent), `SWEEP_MODEL_WORKDIR` for generated files, last line `SWEEP_MODEL_RESULT {json}`; still runs standalone: `python3 data_models/DM03_bank.py --execute` |
| Audit columns | the `CREATED_BY` constant (`FE_MIGRATION_TEAM` / `DATA_MIGRATION`) and every literal in createdby / createddate / modifiedby / modifieddate are gone. Each run first executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for the six canonical audit columns and creates the `trg_sweep_audit` trigger, then: INSERT = createdby = modifiedby = `SESSION_USER`, createddate = modifieddate = one `now()`, createddate_m = `now()`, createdby_m = `'fe_migration'`. UPDATE sets the mnemonic + modifiedby / modifieddate + createdby_m = 'fe_migration'; createdby, createddate and createddate_m never change. |
| Session | `sweep.loader = 'fe_migration'` is set for every psql session of the script (PGOPTIONS), so a default fills createdby_m |
| Own rows | a row of the model family is recognised by `createdby_m = 'fe_migration'` (not by a name in createdby) |
| Legacy columns | a target that carries `created_at`, `created_by`, `modified_date`, ... gets CURRENT_TIMESTAMP / SESSION_USER in them; they are never renamed, relabelled or dropped |
