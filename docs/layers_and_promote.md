# Medallion layers, promote and migrate (v6.2.2)

SWEEP now fills **four PostgreSQL databases in ONE Aurora cluster**, one per layer, in a fixed chain:

| Layer | Database | Holds | Filled from |
|---|---|---|---|
| `bronze` | `SWEEPB` | all Oracle tables + all VSAM tables (raw) | **S3 (the only layer that reads S3)** |
| `silver` | `SWEEPS` | all Oracle tables + the VSAM tables the data models read + the model **output** tables | bronze |
| `gold_backup` | `SWEEPGB` | all Oracle tables + the model output tables (the gate / rehearsal) | silver |
| `gold` | `SWEEPG` | all Oracle tables + the model output tables - **the only database the application and the FE team use** | gold_backup |

Load order is always **bronze -> silver -> gold_backup -> gold**. S3 files are read **once** (into bronze), for Day-1 and for every Day-2 delta / snapshot. The Database team owns every layer; the application and the FE team change data, tables, columns and triggers directly in **gold**, so the pipeline must never damage gold: nothing is ever deleted, nothing in gold is ever truncated or replaced, and gold / gold_backup never get DDL from the pipeline.

## 1. Layer profiles - `sweep_layers.json`

`sweep_layers.json` (no secrets) lists the layers in chain order with `name`, `database`, `env_file`, `upstream`, `manifest`, `allow_ddl`, `trigger_mode`, `lock_timeout`, `statement_timeout`, `conflict_policy`.

* **Environment files:** `sweep.<layer>.env` (`sweep.bronze.env`, `sweep.silver.env`, `sweep.gold_backup.env`, `sweep.gold.env`), same cluster host. They are git-ignored; commit only the `*.sample` files. `promote` copies between databases with the credentials of the target layer, so host, port, user and password must be the same in all four files (or use `~/.pgpass`).
* **`--layer {bronze,silver,gold_backup,gold}` is REQUIRED** on every step that touches a database (Oracle 05-08, VSAM 05-07, VSAM 04 with `--execute`, `run-all`). `SWEEP_LAYER` may supply it - except for `gold`, which must always be named explicitly with `--layer gold`. Steps 01-03 (metadata only) are layer independent; Step 04 takes `--layer` only to name its output folder.
* **Database guard:** a step refuses to run (exit code 3) when `current_database()` is not the database configured for its layer. A bronze command can never touch gold by accident.
* **Per-layer outputs:** generated DDL goes to `outputs/.../<layer>/` (`pre_load/<layer>`, `post_load/<layer>`, `gen_ddl/<layer>`, `ddl_fragments/<layer>`); reports carry the database name.
* **`allow_ddl=false` (gold_backup, gold):** Steps 04, 05 and 07 refuse to run (exit code 2) unless `--allow-ddl` is given explicitly. Schema changes there come only from controlled migrations by the Database team. Even with `--allow-ddl` a step only touches tables of the layer's manifest.
* **Step 06 loads bronze only:** `--layer silver|gold_backup|gold` is refused with the `promote` command to use instead.

## 2. Manifests - `layers/<layer>_tables.csv`

Columns: `track` (`oracle` | `vsam` | `model_output`), `schema`, `table`, `promote_mode` (`upsert` | `replace` | `insert_only`), `conflict_policy` (`source_wins` | `keep_target`), `enabled`.

A layer only ever touches the tables of **its own manifest** - tables the application or FE team created in gold are never touched.

| Command | What it does |
|---|---|
| `python3 sweep.py layers init-manifests [--force]` | Drafts the manifests from the inventory: bronze = all Oracle + all VSAM, silver / gold_backup / gold = all Oracle. Existing (reviewed) manifests are kept unless `--force` (a `.bak` is kept). Keyed tables get `upsert`, keyless Oracle tables `insert_only`, keyless VSAM tables `replace`. |
| `python3 sweep.py layers scan-models [--models-dir <path>]` | Reads the data models of `data_models/` (default) - the `SWEEP_MODEL` header of each script is the truth, only a script without a header falls back to scanning the SQL inside its strings - and **drafts** `layers/silver_vsam_tables.draft.csv` (VSAM tables the models read) and `layers/model_output_tables.draft.csv` (tables they write), plus `layers/models_scan.json`. Drafts have `enabled=false`; **nothing is applied automatically** - a person reviews them and copies the rows they accept into the manifests. The scripts are only read, never run. `layers init-manifests` also writes these drafts from the headers, and `models_scan.json` lists outputs that collide with a loaded table (a model output must have its own name). See [data_models.md](data_models.md). |

## 3. Capturing the delta while bronze loads

While Step 06 loads a file into bronze, the rows that were **really applied** (inserted or changed, after the "only if different" filter) are copied into an **unlogged table** of the `sweep_delta` schema of the same database - `sweep_delta."<schema>__<table>"` with the table's columns plus `_seq`, `_op` (`'I'`/`'U'`) and `_row_no` - and registered in `sweep_ctl.delta_registry` (run id, strictly increasing sequence number per table, source URI(s), rows inserted / updated, captured columns, status).

If the target table was **empty** before the load (a Day-1 style load) or `--full` is given, no copy is stored: the registry row says `full_copy` and the promote hop copies the whole source table (no doubled storage). Keyless VSAM tables (snapshot replace) are always registered as full copies; keyed VSAM tables capture a delta per destination schema.

## 4. `promote` - moving data one hop

```
python3 sweep.py promote --from <layer> --to <layer> [--tables a,b] [--full] [--dry-run] [--workers N] [--transport auto|program|stream]
python3 sweep.py promote --status [--tables ...] [--limit N]
python3 sweep.py promote --purge [--dry-run]
```

* **One hop only** (`--from silver --to gold` is refused). Tables are processed **parent-first, level by level** (FK metadata of Step 01 + live FKs), workers in parallel inside a level.
* **Transport:** the target `psql` runs `\copy _raw FROM PROGRAM 'psql -d <source> -c "COPY (...) TO STDOUT"'` (client side, no extension). If `FROM PROGRAM` cannot run, `--transport stream` (or `SWEEP_PROMOTE_TRANSPORT=stream`; `auto` falls back by itself) makes Python pipe the source `psql` into the target `psql`'s stdin - same staged script, same transaction. `postgres_fdw` was considered and not used: it needs an extension, a server object and user mappings in every database (DDL the Database team would have to grant on gold).
* **Same staged pipeline as Step 06:** validation (`pg_input_is_valid`), rejects in `sweep_ctl.load_rejects`, only-if-different upsert, row-count check (`read = inserted + updated + unchanged + rejected + duplicates`), **one transaction per table**, `sweep_ctl.promote_log` row (run id, source / target layer, table, mode, inserted, updated, unchanged, rejected, status, timestamps). The target records **its own delta** for the next hop.
* **Order and gating:** deltas are applied per table in **strict sequence order** (N+1 never before N; a gap is an error that asks for `--full`). A layer takes a table only if the previous layer applied that delta successfully; a failure stops that table's chain while other tables continue. **gold_backup is the gate:** gold only reads what gold_backup applied. Re-running is idempotent and resumable.
* **Modes:** `upsert` (changed rows only), `insert_only` (keyless: insert rows the target does not have; keyed: new keys only), `replace` (atomic `TRUNCATE` + `INSERT` in one transaction, for VSAM keyless tables in silver - **refused on gold and gold_backup**). A keyless **model output** table is skipped with a warning unless its manifest says `insert_only`.
* **Model outputs** (`track=model_output`): the data models of `data_models/` run in silver (`python3 sweep.py models run`, see [data_models.md](data_models.md)); then `promote --from silver --to gold_backup`, then `--to gold`. Upsert on the primary key of the **changed rows only** (`IS DISTINCT FROM`); never a replace or truncate. **`promote --from silver` refuses a model output** whose model has no SUCCESS newer than its latest silver input delta (`--ignore-model-status` overrides).
* **`--status`** prints the delta x layer matrix (`SUCCESS` / `FAILED` / `PENDING` / `BLOCKED`). **`--purge`** removes delta tables once **every** later layer applied them plus `SWEEP_DELTA_RETENTION_DAYS` (default 14); the registry rows stay.
* **`--full`** copies the whole source table (baseline or resynchronization) instead of the deltas.
* **FE files and promote:** FE files run in silver (never in bronze) and move on with `fwd-ddl promote`. A hop silver -> gold_backup / gold_backup -> gold is refused while the target layer has pending FE files that the source layer already applied (`python3 sweep.py fwd-ddl promote --from L --to L` first; `--ignore-pending-fwd` overrides). **bronze -> silver applies the FE rules of silver** (`ignore-column`, `rename-column`, `default-column`, `skip-table`) while it stages the bronze delta; a bronze column that is missing in silver and has no rule is still schema drift. See [fwd_ddl.md](fwd_ddl.md).
* **`sweep_ctl.load_history`:** every load attempt - an S3 file (Oracle / VSAM Step 06) or a promote hop - is one append-only row (run id, track, layer, source, uri, table, SUCCESS / FAILED, rows read / inserted / updated / unchanged / rejected / duplicates, `rows_before` on the first row of a table = its ledger baseline, S3 metadata), written **inside the load transaction**; a failed attempt gets a FAILED row of its own.

### Gold safeguards (gold is live)

* **Schema drift check before every table:** a column missing in the target fails **that table** with a clear message (no automatic `ALTER`); extra target columns are fine when nullable or defaulted - an extra `NOT NULL` column without a default fails the table and is listed.
* **Conflict policy per table:** `source_wins` (default) overwrites rows with the same key (only if different); `keep_target` inserts new keys only (`ON CONFLICT DO NOTHING`). `--dry-run` shows inserted / updated / unchanged per table without writing.
* **Sequences:** after every apply the identity / serial sequences of the touched table are advanced to `GREATEST(current value, max(column))` - never backwards.
* **Triggers:** `trigger_mode` per layer (default `auto` = `session_replication_role = replica`, else the user triggers of the table are disabled inside the transaction). **FE / application triggers do not fire during pipeline loads.**
* **Locks:** gold and gold_backup use a short `lock_timeout` (5 s) and a `statement_timeout` for every statement, with retry and exponential backoff; one small transaction per table; no lock is held across tables.

## 5. `chain`

```
python3 sweep.py chain [--until bronze|silver|models|gold_backup|gold] [--skip-models] [--models a,b] [--force-models] [--allow-ddl] [--tracks oracle,vsam] [--skip-load] [--workers N] [--tables ...] [--dry-run]
```

The order of a Day-2 run, **stopping at the first failure**:

```
load bronze from S3 (raw, no FE files) -> promote bronze -> silver -> fwd-ddl apply (silver) -> RUN THE DATA MODELS in silver
  -> fwd-ddl promote + promote (gold_backup: deltas and model outputs together) -> fwd-ddl promote + promote (gold)
```

It never goes past silver while silver has pending FE files. `--until models` stops after the models so the result can be reviewed; `--until gold_backup` before gold. `--skip-models` promotes without running the models. A failed model stops the chain: nothing is promoted to gold_backup. FE files for gold_backup / gold are applied only with `--allow-ddl`.

## 6. `migrate`

```
python3 sweep.py migrate [--layer X | --all-layers] [--dry-run]
```

Applies versioned, idempotent, transactional migrations and records them in `sweep_ctl.schema_version` (version, name, applied_at, applied_by); `--all-layers` runs in chain order. **Migration 1** creates `oracle_load_log` (PK `uri`) and `vsam_load_log` (PK `uri, target_schema` - the real URI, no more `<uri>#<schema>`), copies the rows of the old `sweep_ctl.load_log` (`uri LIKE '%#%'` = VSAM, split at the last `#`; others = Oracle) and renames the old table to `load_log_legacy` (never dropped), adds the `s3_*` columns, adds `track` to `load_rejects` and `load_audit`, and creates `delta_registry`, `promote_log`, `schema_version` and the `sweep_delta` schema. Step 06 and `promote` refuse to run on a database that is not migrated. The `sweep_meta.sql` files create the same objects for a fresh install.

## 7. Baseline and checks (Day-1 was loaded into each database separately)

```
python3 sweep.py layers reconcile --from <layer> --to <layer> [--tables ...] [--exact]
python3 sweep.py layers check [--layer X]
```

* **reconcile:** per table row counts plus a key-hash comparison (`--exact` hashes every row; the default compares a 1 percent sample of the keys), Excel report, exit code. Only columns of the **same type** are compared; type differences are reported separately (`type_differences`, status `TYPE_DIFFERENCE`). Bronze vs silver vs gold_backup must match for the Oracle tables and model outputs, and **VSAM tables are enforced in silver** (informational in gold_backup). **gold uses a superset check:** every key of gold_backup must still exist in gold and `count(gold) >= count(gold_backup)` (the keys are streamed from gold_backup into gold through the same cross-database pipe `promote` uses; a 1 percent sample by default, all keys with `--exact`) - the application may add and change rows in gold, never remove them. Differences an applied FE file explains are `FE_ADDED` / `FE_CHANGED` / `FE_DROPPED`, count as PASS and appear in the `FE_Changes` sheet. Tool-set audit columns (`createddate_m`, `createdby_m`) are not compared. **Run and review it before the first chained delta.**
* **check:** database reachable, PostgreSQL >= 16, privilege to set `session_replication_role`, `sweep_ctl` migrated, schema drift summary against the upstream layer (columns / tables an applied FE file dropped are not drift), sequences behind `max(column)`.

### Validate one layer and monitor

```
python3 sweep.py validate --layer L [--tables a,b] [--exact] [--tracks oracle,vsam]
python3 sweep.py monitor [--layer L] [-w]
```

* **validate** writes one Excel report with a verdict per component and for the layer (exit code 0 = PASS). **bronze** = Oracle Step 08 + VSAM Step 07 (their own reports) + the ledger; **every other layer** = reconcile with its upstream (gold: superset check) + a column-by-column schema comparison with FE-explained differences + the ledger. Sheets: `Summary`, `Reconcile`, `Schema`, `Ledger`, `FE_Changes`.
* **ledger check** (Oracle Step 08, VSAM Step 07, `validate`): `count(table) == rows_before + sum(rows_inserted)` from `sweep_ctl.load_history` (exact counts), status `LEDGER_MATCH` / `LEDGER_MISMATCH` (columns in `Row_Count_Audit` / `RowCounts`, sheet `Ledger`): a row deleted or added outside the loaders shows up. A keyless VSAM table is replaced by every run, so its ledger is the rows of the latest snapshot.
* **monitor --layer** adds the layer and database name, a compact last-promote line and the number of pending FE files to the dashboard header.

## 8. Recommended first run

1. `python3 sweep.py migrate --all-layers`
2. `python3 sweep.py layers init-manifests`, review the manifests; review the drafts written from the `data_models/` headers (`layers scan-models`), copy accepted rows.
3. `python3 sweep.py layers check`
4. `python3 sweep.py layers reconcile --from bronze --to silver`, `--from silver --to gold_backup`, `--from gold_backup --to gold` (informational), review the reports.
5. Day-2: `python3 sweep.py chain --until gold_backup` (FE files, promote to silver, the data models, promote to gold_backup), review (`python3 sweep.py validate --layer gold_backup`), then `python3 sweep.py chain --until gold --skip-load` or `python3 sweep.py promote --from gold_backup --to gold`.
