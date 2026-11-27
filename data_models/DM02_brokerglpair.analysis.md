# DM02_brokerglpair.py - analysis note

Real data model BrokerGLPair (spec: Change L + M, 6.3.0). The user's standalone original (DM02_brokerglpair.txt) is the reference; the adaptations are listed below.

## What the script does

Reads `<schema>.stg_genledgeinterface_v` for the Hogan product codes 811, 812, 826, 827, 818, 819, maps every code to a broker through `hogan_broker_map.csv` and `bcs.broker`, and inserts the rows whose composite key is not in `bcs.brokerglpair` yet (an existing row is never updated; a re-run inserts nothing).

`hogan_broker_map.csv` is a maintained file: `data_models/config/dm02_brokerglpair/hogan_broker_map.csv` (or `$SWEEP_MODEL_CONFIG`, or `--map`). If it is missing the script stops with a clear message; it never creates a map with default names and never inserts without one. DM02 replaces the former `37_brokerglpair.py` (and its tests).

Target: `bcs.brokerglpair` - natural key (hogan_product_code, total_number). DRY RUN unless `--execute`.

## What was adapted (and nothing else)

| Topic | Change |
|---|---|
| Names | no test-copy suffix on any table name (the originals pointed at test copies); the source schema is `bss00098` |
| Connection | no hard-coded host / user / password: libpq environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`), same CLI overrides |
| Runner | `SWEEP_MODEL` header (name, inputs, outputs, depends_on, delta_strategy, update_key, timeout), `--mode full|delta` (both idempotent), `SWEEP_MODEL_WORKDIR` for generated files, last line `SWEEP_MODEL_RESULT {json}`; still runs standalone: `python3 data_models/DM02_brokerglpair.py --execute` |
| Audit columns | the `CREATED_BY` constant (`FE_MIGRATION_TEAM` / `DATA_MIGRATION`) and every literal in createdby / createddate / modifiedby / modifieddate are gone. Each run first executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for the six canonical audit columns and creates the `trg_sweep_audit` trigger, then: INSERT = createdby = modifiedby = `SESSION_USER`, createddate = modifieddate = one `now()`, createddate_m = `now()`, createdby_m = `'fe_migration'`. INSERT only (insert-if-missing); `--replace` (by hand only, refused under the runner) deletes the rows with `createdby_m = 'fe_migration'` of these codes first. |
| Session | `sweep.loader = 'fe_migration'` is set for every psql session of the script (PGOPTIONS), so a default fills createdby_m |
| Own rows | a row of the model family is recognised by `createdby_m = 'fe_migration'` (not by a name in createdby) |
| Legacy columns | a target that carries `created_at`, `created_by`, `modified_date`, ... gets CURRENT_TIMESTAMP / SESSION_USER in them; they are never renamed, relabelled or dropped |
