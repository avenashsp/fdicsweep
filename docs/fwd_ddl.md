# FE SQL files - `sweep.py fwd-ddl` (v6.3.0)

Forward engineering (FE) asks for tables to be created and Oracle / VSAM tables to be changed: add, drop or rename columns, retype, new constraints, indexes, views, triggers. Those
changes are **SQL files in `fwd_ddl/`** that the Database team applies in **silver** and then moves on to **gold_backup** and **gold** (`silver -> gold_backup -> gold`), schema first, then data. **They never run in bronze:** bronze is the raw mirror of the source (the S3 files are loaded into it as they are); `promote` bridges the difference between the raw bronze tables and the FE-changed silver tables (section 2). The files contain **structure only**.

## 1. The files

```
fwd_ddl/V1__add_customer_segment.sql
fwd_ddl/V2__drop_legacy_column.sql
```

* Name: `V<number>__<description>.sql`. Files are applied **in numeric order**; two files with the same number are an error.
* **An applied file is immutable.** Its SHA-256 is stored in `sweep_ctl.fwd_ddl_log`; a file that changed after it was applied is refused (`CHANGED`). A correction is a **new file**, never an edit.
* **One transaction per file.** Any error rolls the whole file back and the file stays pending (`FAILED`). `-- sweep:no-transaction` runs a file statement by statement (for `CREATE INDEX CONCURRENTLY` and similar).
* **Data statements are refused** (`INSERT`, `UPDATE`, `DELETE`, `COPY`, `TRUNCATE`, `MERGE`, `CREATE TABLE AS` with data) unless `--allow-data` is given.
* Every applied file records what it really changed: the catalog is snapshotted before and after inside the same transaction and the difference (tables, columns, constraints, indexes, views, triggers, sequences) is stored in `sweep_ctl.fwd_ddl_changes`. The loaders, the validation steps and `models run` read it.
* The control tables (`fwd_ddl_log`, `fwd_ddl_changes`, `fwd_ddl_rules`, `load_history`, `model_run_log`) come with **migration 2** (`python3 sweep.py migrate --all-layers`); the `sweep_meta.sql` files create them for a fresh install.

## 2. Pragmas

Comments of the form `-- sweep:<pragma> <arguments>` at the top level of a file:

| Pragma | Meaning |
|---|---|
| `-- sweep:layers silver,gold_backup,gold` | Apply this file only in these layers (others report `NOT_APPLICABLE`) |
| `-- sweep:ignore-column schema.table.column` | The loaders **ignore** this column of the S3 file (the table no longer has it). `*` as the schema matches every schema |
| `-- sweep:rename-column schema.table.old -> new` | The S3 column `old` loads into the table column `new` |
| `-- sweep:default-column schema.table.column = <expression>` | A column the source does not have gets this value on **insert**; an existing row keeps its value |
| `-- sweep:skip-table schema.table` | The table was dropped: its S3 files are skipped (reported `SKIPPED`), the table is **not** recreated by Steps 04 / 05 |
| `-- sweep:no-transaction` | Run the file without a surrounding transaction |

The rules are stored in `sweep_ctl.fwd_ddl_rules` of **each layer's database**. **`promote` reads the rules of the TARGET layer** and applies them while it stages a delta: an ignored column is not copied into the table, a renamed column loads into its new name, a default column gets its expression on insert (existing rows keep their value), a skip-table table is reported `SKIPPED`. A source column that is not in the table and has no rule is still **schema drift** and fails that table - an unexpected change is never hidden - and the message names these pragmas. The Step 06 loaders load bronze and have **no** rules; an unknown column in an S3 file fails the file.

## 3. Commands

```
python3 sweep.py fwd-ddl status [--layer L]
python3 sweep.py fwd-ddl apply  --layer silver [--file F] [--dry-run] [--allow-data] [--force]
python3 sweep.py fwd-ddl promote --from silver --to gold_backup [--dry-run] [--allow-ddl]     # then --from gold_backup --to gold
python3 sweep.py fwd-ddl check  [--layer L]
```

* **bronze is refused:** `apply --layer bronze`, `promote --from bronze ...` and `check --layer bronze` end with exit code 2 and the message `FE files are never applied in bronze`; `status` says so for bronze and, without `--layer`, lists only silver, gold_backup and gold.
* **status** (read-only; silver, gold_backup and gold without `--layer`): per file APPLIED / PENDING / FAILED / CHANGED in each layer, and which layers are behind.
* **apply** applies the pending files of one layer in order. `--dry-run` rehearses all pending files in **one transaction that is rolled back** and prints the catalog difference of each file.
* **promote** applies to the next layer the files the previous layer applied successfully, in the same order (the layers are separate databases, so each one needs the files).
* **check** dry-runs every pending file with the compatibility check; nothing is written (read-only).
* **`--allow-ddl` is required on `gold_backup` and `gold`** (they have `allow_ddl=false`) for `apply` and `promote`. `status` and `check` are read-only and work without `--layer` (every layer is read; `check` skips layers with `allow_ddl=false` unless `--allow-ddl` is given).

## 4. Compatibility check (before a file is applied)

For every table a file touches, the **source columns** (Oracle: the Step 01 metadata; VSAM: the Step 02 column map) are compared with the table **as it would be after the file**, inside the same transaction. The apply is **BLOCKED** (the file is rolled back) when the next delta load would fail:

| Case | Reason |
|---|---|
| (a) | a source column is neither in the table nor covered by an `ignore-column` / `rename-column` rule |
| (b) | a `NOT NULL` column without default is absent from the source and has no `default-column` rule |
| (c) | the table was dropped and there is no `skip-table` rule |

`--force` applies the file anyway (the warnings stay in the output). Fix the file (new file) or add the missing pragma instead whenever you can.

## 5. Order of a Day-2 run

```
load bronze from S3                    (raw, no FE files)
  -> data promote bronze -> silver      (the FE rules of silver are applied here)
  -> fwd-ddl apply (silver)             (schema AFTER the raw data arrived)
  -> STOP: the data models run in silver (python3 sweep.py models run)
  -> fwd-ddl promote + data promote (gold_backup, deltas and model outputs together)
  -> fwd-ddl promote + data promote (gold)
```

* **Data `promote` refuses a hop between FE layers** (silver -> gold_backup, gold_backup -> gold) while the target layer has pending FE files that the source layer has already applied (`--ignore-pending-fwd` overrides, for a baseline or a repair). **bronze -> silver is never gated:** bronze has no FE files, and the silver files may be applied right after the data.
* **`chain`** follows exactly this order and stops at silver: it never continues past silver while silver has pending FE files. FE files for gold_backup / gold are applied only with `chain --allow-ddl`.
* **Steps 04 / 05 / 07** do not recreate or fail on objects an FE file dropped or changed: they report `FE_CHANGED`. (Oracle Step 04 only writes DDL files and never touches the database, so the `FE_CHANGED` report of the Oracle track is made by Step 05 and Step 07.)
* **Validation:** differences that an applied FE file explains are `FE_ADDED` / `FE_CHANGED` / `FE_DROPPED`, count as PASS and are listed in the `FE_Changes` sheet of the Oracle Step 08, VSAM Step 07, `layers reconcile`, `layers check` and `validate` reports.
* `python3 sweep.py monitor --layer L` shows the pending FE files of a layer.
