# Data models (v6.0.0) - `data_models/` and `sweep.py models`

The data models are **Python scripts in this repository** (`data_models/<NN>_<name>.py`). They run in **silver**, after the bronze -> silver promote and **before** the promote to gold_backup. The runner starts each script in
its own subprocess and gives it only the silver credentials.

```
python3 sweep.py models list                     # models in run order with inputs, outputs, dependencies
python3 sweep.py models run [--models a,b] [--with-deps] [--full] [--force] [--workers N] [--dry-run] [--skip-promote-check]
python3 sweep.py models status                   # last run of every model and whether it is due
python3 sweep.py chain [--until bronze|silver|models|gold_backup|gold] [--skip-models] [--models a,b] [--force-models]
```

## 1. Layout

```
data_models/37_brokerglpair.py         the script (committed)
data_models/37_brokerglpair.analysis.md the analysis note written before the script was changed (rule R8)
data_models/models.json                optional per-model overrides (see 3)
data_models/config/<model>/            files people edit (committed), SWEEP_MODEL_CONFIG
data_models/work/<model>/<run_id>/     files the script generates (git-ignored), SWEEP_MODEL_WORKDIR
data_models/logs/<db>_<model>_<ts>.log output of every run (git-ignored)
```

## 2. The run contract

Every model runs as `python3 data_models/<file> [--execute] [--mode full|delta]`, working directory = repository root. `--execute` is omitted only by a person doing a dry run by hand: the runner always passes it.

| Environment variable | Content |
|---|---|
| `PGHOST` `PGPORT` `PGUSER` `PGPASSWORD` `PGDATABASE` | libpq connection to the **silver** database (credentials from `sweep.silver.env` ONLY - every other `PG*` / `SWEEP_*` variable of the parent is dropped) |
| `SWEEP_LAYER` | `silver` |
| `SWEEP_RUN_ID` | the run id (also written to `model_run_log` and the `load_history` rows) |
| `SWEEP_MODEL` / `SWEEP_MODE` | model name / `full` or `delta` |
| `SWEEP_MODEL_WORKDIR` | `data_models/work/<model>/<run_id>` (created) |
| `SWEEP_MODEL_CONFIG` | `data_models/config/<model>/` (created) |
| `SWEEP_INPUT_DELTAS` | JSON per input table: `{"full": true}` or `{"full": false, "delta_table": "sweep_delta.<schema>__<table>", "seq_from": N, "seq_to": M}` (delta rows: `_seq > N AND _seq <= M`, columns of the table + `_seq`, `_op`, `_row_no`) |

A model prints exactly one line `SWEEP_MODEL_RESULT {json}` on stdout and exits `0`; any error exits non-zero. Without the result line a run counts as failed. The optional `SWEEP_MODEL_PGUSER` / `SWEEP_MODEL_PGPASSWORD` (in `sweep.silver.env` or the environment) give a model another account of the same silver database, for example one that may write the output tables. `SWEEP_MODEL_TIMEOUT` (seconds, default 21600 = 6 h) or the `timeout` of the header ends a run: the process group is killed and the run is `FAILED`.

## 3. The header (`SWEEP_MODEL`)

A literal dict at the top of the script, read with `ast.literal_eval` (the script is never imported or executed to read it):

```python
SWEEP_MODEL = {
    "name": "brokerglpair",
    "inputs": ["bss00098.stg_genledgeinterface_v", "bcs.broker"],
    "outputs": ["bcs.brokerglpair"],
    "depends_on": [],
    "delta_strategy": "update_in_place",       # or insert_missing
    "update_key": ["hogan_product_code", "total_number"],
    "timeout": 3600,
}
```

`data_models/models.json` (optional) overrides header values by model name (`{"models": {"<name>": {"depends_on": [...]}}, "disabled": ["<name>"]}`). A script **without** a header falls back to the static scan of its SQL strings, with a warning.

## 4. Order, dependencies, failures

Models run in numeric file prefix order, but a model always after the models in its `depends_on` (a cycle is an error naming the cycle). `--workers N` runs independent models in parallel. **A failed model blocks only its dependents** (they are recorded `SKIPPED`); the others continue; the exit code is 1.

## 5. When does a model run?

When it **never succeeded**, or an input table received a **delta or an FE change** since its last SUCCESS, or an upstream model it depends on ran in this run. `--force` runs always, `--full` runs in full mode. A failed run **does not advance** its input sequences (they are stored only with a SUCCESS in `sweep_ctl.model_run_log`), so the next run sees the same deltas again. An input that is the output of another model, whose delta was purged, or that an FE file changed, is read **in full**.

## 6. Preconditions (checked before any model starts)

| | |
|---|---|
| (a) | no pending FE files in silver |
| (b) | no PENDING or FAILED bronze -> silver delta for the input tables (`--skip-promote-check` skips this one only) |
| (c) | the inputs exist in silver and the layer guard passes (the connected database is the silver database) |
| (d) | the output tables exist |

A violation refuses the whole run and lists every problem. **Output guard:** an output table that is also a table the loaders fill (Oracle / VSAM manifests of bronze or silver, or a load log) makes **that model fail with a clear message and nothing is written**.

## 7. After a model exits 0

The runner verifies every output table: it exists and has a **primary key** or is `promote_mode=insert_only` in the silver manifest; otherwise the run is `FAILED`. The outcome (status, mode, exit code, log file, input sequences, the result line) is in `sweep_ctl.model_run_log`.

## 8. Promote gate

`promote --from silver --to gold_backup` refuses a `model_output` table whose producing model has no SUCCESS newer than the latest delta of its silver inputs (message names the model and the reason); `--ignore-model-status` overrides. `chain --skip-models` promotes without running the models (and without this check).

## 9. Rules for adapting a script (R1 - R13)

1. **R1** Do not change the core logic. Prove it: a test compares the adapted script's output rows with the original script's (`tests/test_brokerglpair.py`, golden comparison).
2. **R2** Add the `SWEEP_MODEL` header.
3. **R3** The connection comes from the libpq environment; command-line options still override.
4. **R4** `--execute` (dry run without it) and `--mode full|delta`.
5. **R5** Generated files go to `SWEEP_MODEL_WORKDIR`; files people edit go to `SWEEP_MODEL_CONFIG`.
6. **R6** Delta mode reads the delta tables of `SWEEP_INPUT_DELTAS`.
7. **R7** Print the `SWEEP_MODEL_RESULT` line and exit non-zero on error.
8. **R8** Write an analysis note (`data_models/<script>.analysis.md`) before changing a script.
9. **R9 (source schema)** The source schema is always `bss00098`. Never `bss00099`.
10. **R10 (testing tables)** A table name ending in `_backup` is a testing table: the real name drops the suffix (the target of `37_brokerglpair.py` is `bcs.brokerglpair`). An **input** name ending in `_backup` is not changed but reported.
11. **R11 (outputs)** An output table never has the name of a loaded table (the runner guard of section 6).
12. **R12 (idempotent, never destructive)** A model never truncates, drops, or deletes and re-inserts. Default strategy: **`update_in_place`** with a key derived from the source key - insert when no output row has the key; update the non-key business columns only where a value differs (NULL-safe) in a row the model created (audit `modifiedby` / `modifieddate`); leave a row somebody else created alone and count it as `skipped_foreign`; `id`, `createddate` and `createdby` never change. Re-running the same delta changes nothing. A legacy `--replace` is refused when `SWEEP_RUN_ID` is set. If the key is not unique or the mapping is unclear, **STOP and ask**.
13. **R13** Use `insert_missing` only when no key can be derived, and only after asking.

A test fails when `bss00099` or an output name ending in `_backup` appears in `data_models/` or in a `SWEEP_MODEL` header.

## 10. `37_brokerglpair.py`

Source `bss00098.stg_genledgeinterface_v` (product codes 811, 812, 826, 827, 818, 819) and `bcs.broker` -> `bcs.brokerglpair`. The Hogan code -> broker mapping is `data_models/config/brokerglpair/hogan_broker_map.csv` (edit it there). Update key `(hogan_product_code, total_number)`.
The result line reports `inserted`, `updated`, `unchanged`, `skipped_foreign`, `affected_source_rows`. In delta mode only the source rows of the delta window are processed; a changed `bcs.broker` or an input read in full affects every row.
**Not verified against real data:** that the key pair is unique in the real source and that the real target has a unique index on it. The script checks at run time (`UPDATE KEY NOT UNIQUE` stops the run, also in the dry run; a key matching several target rows rolls the transaction back; a missing unique index is a warning) - see `data_models/37_brokerglpair.analysis.md`.
