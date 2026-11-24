# 37_brokerglpair.py - analysis note (rule R8)

Written BEFORE the script was changed. The original script is kept unchanged as the golden reference of the tests
(`tests/data/models/37_brokerglpair_original.py`, only its docstring names were corrected by A1 / A2).

## What the script does (core logic - NOT changed, rule R1)

| Step | What | Kept as is |
|---|---|---|
| 1 | reads `<schema>.stg_genledgeinterface_v` (20 `m921_*` columns), only rows whose `trim(m921_kfund_kproduct)` is one of 811, 812, 826, 827, 818, 819 | product code filter, `SRC_COLS` |
| 2 | maps every Hogan product code to a broker name through `hogan_broker_map.csv` and the name to `brokerid` in `bcs.broker` (exact match, then upper-case match; exactly one row or the code is a problem) | `DEFAULT_BROKERS`, lookup rules, `--allow-null-broker` |
| 3 | inspects the target (`bcs.brokerglpair`): 23 business columns (with `COLUMN_ALIASES`), the six audit columns it finds (createddate, createdby, modifieddate, modifiedby, createddate_m, createdby_m) | alias resolution, audit detection |
| 4 | builds the business rows: `product_id = 2`, `is_active = true`, `total_number = clean_total(m921_ktotal)`; collapses duplicates on the whole business tuple | `clean_total`, de-duplication |
| 5 | one transaction: `LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE` with `lock_timeout = '60s'`, `\copy` into a temp stage table, typed insert | the lock, the single transaction |
| 6 | dry run unless `--execute`; "NOT EXECUTED" when there are no source rows or the broker lookup is incomplete | the checks |

## Findings

1. **Source schema.** The docstring named a schema ending in 099, the code default is `bss00098`. Decision A1: it is `bss00098` everywhere.
2. **Target.** The docstring and the argparse description named a testing copy of the table (the name with a backup suffix), the default is `bcs.brokerglpair`.
   Decision A2: a name with that suffix is a testing table; the real table is `bcs.brokerglpair`. Docstring, description, header and tests use it.
3. **Connection** was hard-coded (`localhost`, `postgres`). Now libpq environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`) with the same CLI overrides.
4. **Files.** Everything was written to `VSAM_OUT/brokerglpair`. Generated files (`source_rows.csv`, `preview.csv`, `broker_list.csv`, `stage.csv`, `load.sql`, `run.log`) go to
   `SWEEP_MODEL_WORKDIR`; the file people edit (`hogan_broker_map.csv`) lives in `SWEEP_MODEL_CONFIG` (`data_models/config/brokerglpair/`).
5. **Re-runs.** The script refused to run twice (DATA_MIGRATION rows exist) unless `--replace`, which DELETES the earlier rows and inserts again. That is destructive and breaks
   the ids and the audit columns, so it cannot be a delta strategy. Replaced by update-in-place (decision A4).
6. **Delta.** The source is read in full each time. With `SWEEP_INPUT_DELTAS` only the source rows that changed (the silver delta table of the input) are processed.

## The update key (decision A4) - what could and could not be verified

* Natural key of the output: **`(hogan_product_code, total_number)`** - the product code of the source row and the cleaned total number (`m921_ktotal`, leading zeroes
  removed). Every other business column is a description of that pair (organisation, account, cost centre, key code, transaction code, concept code, description for
  the debit and the credit side) plus the broker (derived from the product code) and two constants.
* **Not verifiable here:** the real databases are not available in this repository's test environment, so it is NOT known that the pair is unique in the real
  `stg_genledgeinterface_v` rows, or that `bcs.brokerglpair` carries a unique index on it. The script therefore checks at run time and fails loudly:
  * two distinct business rows with the same key in the source -> `UPDATE KEY NOT UNIQUE`, nothing is written (also in the dry run);
  * an update key that matches more than one existing target row -> the transaction is rolled back;
  * no unique index on the key in the target -> a warning (the key is still honoured).
  If the key turns out not to be unique, STOP and decide the real key (or `insert_missing`) before the next run.

## Behaviour of the new apply step

| Situation | Action |
|---|---|
| no output row has the key | INSERT (audit columns as before) |
| row exists and `createdby = 'DATA_MIGRATION'` | UPDATE the non-key business columns only where a value differs (NULL-safe), `modifiedby = 'DATA_MIGRATION'`, `modifieddate = CURRENT_TIMESTAMP` |
| row exists, created by someone else (or no `createdby` column) | untouched, counted as `skipped_foreign` |
| the key of a source row changed | a new row is inserted, the old row stays (nothing is ever deleted) |

`id`, `createddate` and `createdby` never change. Re-running the same delta changes nothing. Full mode on a non-empty target uses the same rule.
The result line reports `inserted`, `updated`, `unchanged`, `skipped_foreign`, `affected_source_rows`.
