# SWEEP: Unified Data Migration Framework (v6.0.0)

SWEEP migrates large financial datasets into **AWS RDS Aurora PostgreSQL** from two sources:

- **Oracle**: DDL dumps plus data files in S3
- **VSAM**: flat files described by COBOL copybooks, in S3

Data is streamed from S3 straight into PostgreSQL (`aws s3 cp - | gzip -dc | psql \copy`) with no local staging.

Since 6.0.0 the target is a **medallion chain of four databases in one Aurora cluster**: `bronze` (SWEEPB, raw) -> `silver` (SWEEPS) -> `gold_backup` (SWEEPGB, the gate) -> `gold` (SWEEPG, what the application uses). S3 is read **once**, into bronze; the other layers are filled by `sweep.py promote` (or `sweep.py chain`) from the previous layer, never from S3. See [layers and promote](docs/layers_and_promote.md).

## Requirements

- Python 3.9+ with `pandas`, `numpy`, `openpyxl`, `boto3`, `python-dateutil` (`pip install -r requirements.txt`)
- `psql` (PostgreSQL client) and the AWS CLI on `PATH`
- PostgreSQL **16 or later** on the target (the loaders validate values with `pg_input_is_valid`)
- Network access to the Aurora cluster and the S3 buckets

## Setup

```bash
pip install -r requirements.txt   # or: pip install -e .   (adds the `sweep` command)
cp sweep.env.sample sweep.env     # single database: edit PGHOST, PGPASSWORD, S3 buckets, workers
# layered setup: one git-ignored env file per layer (commit only the *.sample files)
for l in bronze silver gold_backup gold; do cp sweep.$l.env.sample sweep.$l.env; done   # then edit each
python3 sweep.py check            # pre-flight: deps, DB, AWS, S3, input files (exit 1 if unusable)
```

`sweep.env` and `sweep.<layer>.env` hold credentials and are git-ignored. Never commit them. Variables already exported in your shell
override `sweep.env`. Instead of `PGPASSWORD` you can use `~/.pgpass`, `PGSERVICE`, or `SWEEP_ALLOW_NO_PASSWORD=1`.

## Usage

```bash
python3 sweep.py oracle run-all --workers 8     # full Oracle pipeline (stops at the first failing step)
python3 sweep.py vsam run-all --workers 8       # full VSAM pipeline
python3 sweep.py run-all --track all --bg       # both tracks, as one background job

python3 sweep.py status                         # background jobs (RUNNING / FINISHED / ERRORED / STOPPED)
python3 sweep.py monitor -w                     # live ingestion dashboard
python3 sweep.py logs -f                        # tail logs
python3 sweep.py stop                           # stop jobs (and their child processes)
```

Individual steps are modules, run from the repository root:

```bash
python3 sweep.py vsam load-data --workers 4
python3 -m vsam.vsam_06_load_data --workers 4      # same thing, without the CLI wrapper
```

Every step exits `0` on success and non-zero on failure (`2` for an invalid selection or a missing / refused `--layer`, `3` when the connected database is not the layer's database).

> **Loads (both tracks):** every file is loaded in one transaction through temp staging tables; bad rows are rejected and
> reported (`sweep_ctl.load_rejects`, `Reject_Summary` / `Rejected_Rows` sheets) and never fail a file unless
> `--max-rejects N` is given. A file is loaded again when its S3 LastModified / ETag / size changed **or its target table is empty**
> (or with `--reload`). Both tracks write their own log table (`sweep_ctl.oracle_load_log` / `vsam_load_log`).
> Oracle loads parent tables first. A VSAM table without a primary key is replaced by the newest snapshot; in a *partial*
> run (`--company` / `--tables` / `--limit-files`) replacing a populated table needs `--force-truncate`. `--dry-run` lists
> what would be loaded.

### Layers, promote and migrate

```bash
python3 sweep.py migrate --all-layers                          # versioned sweep_ctl migrations (first thing on every database)
python3 sweep.py layers init-manifests                         # draft layers/<layer>_tables.csv for review
python3 sweep.py layers scan-models --models-dir <path>        # draft the silver VSAM list and the model output tables
python3 sweep.py layers check                                  # reachable, PostgreSQL >= 16, migrated, drift, sequences
python3 sweep.py layers reconcile --from bronze --to silver    # counts + key hash; run it BEFORE the first chained delta

python3 sweep.py oracle load-data --layer bronze               # S3 is read into bronze only (also: vsam load-data)
python3 sweep.py promote --from bronze --to silver             # one hop, applying the captured deltas in sequence order
python3 sweep.py promote --from silver --to gold_backup --dry-run
python3 sweep.py chain --until gold_backup                     # load bronze, promote to gold_backup, review ...
python3 sweep.py promote --from gold_backup --to gold          # ... then gold (explicit; gold is the live database)
python3 sweep.py promote --status                              # delta x layer status matrix
```

`--layer {bronze,silver,gold_backup,gold}` is **required** on every step that touches a database; a step refuses to run when the connected
database is not the one configured for the layer in `sweep_layers.json`. gold and gold_backup refuse DDL steps (04, 05, 07) without `--allow-ddl`.

### Targeted runs

Every step takes the same selectors - `--tables`, `--schemas`, `--exclude-tables`, `--file` / `--filename`, `--company`, `--dry-run` - which are
validated up front (unknown table or file = clear message, exit code 2). A run **with** filters merges its outputs (file maps, gate results,
metadata, per-table DDL fragments) and leaves every other entry untouched; a run **without** filters is a full run and replaces them. See
[targeting and outputs](docs/targeting_and_outputs.md).

### Pipeline stages

| # | Oracle | VSAM |
|---|--------|------|
| 1 | `extract` metadata | `extract` copybooks |
| 2 | `inventory` S3 files | `inventory` S3 files |
| 3 | `gate` type check | `type-gate` |
| 4 | `gen-ddl` | `gen-ddl` |
| 5 | `apply-ddl` | `apply-ddl` |
| 6 | `load-data` (one transaction per file) | `load-data` |
| 7 | `load-obj` (PK, indexes, FK, views, triggers) | `validate` |
| 8 | `validate` | n/a |

## Layout

```
sweep.py                    master CLI
sweep_common.py             config/env, psql helpers (timeouts, retries), logging, Excel, type mapping
sweep_platform.py           all OS-specific code (process groups, PID checks, locks, atomic writes)
sweep_streaming.py          safe S3 -> stdout helpers (no shell interpolation)
sweep_stream.sh             S3 -> stdout streaming pipe
sweep_layers.py / .json     layer profiles, --layer guard, manifests;  sweep_layer_cmds.py  init-manifests / scan-models
sweep_promote.py            promote / chain / status / purge;  sweep_delta.py  delta capture + registry
sweep_migrate.py            versioned sweep_ctl migrations;  sweep_reconcile.py  layers reconcile / check
sweep_targeting.py          uniform --tables/--schemas/... selectors;  sweep_merge.py / sweep_ddlfrag.py  merge-safe outputs
sweep_bg.py + sweep_jobs.py, sweep_metrics.py, sweep_dashboard.py, sweep_format.py   background jobs and monitor
oracle/                     Oracle track (steps 01-08) + inputs/
vsam/                       VSAM track (steps 01-07) + inputs/ (mapping and override workbooks)
tests/                      pytest suite (integration tests need a reachable PostgreSQL)
docs/                       overview, command and flag references, output descriptions
```

Generated `logs/`, `reports/` and `outputs/` folders are git-ignored.

## Development

```bash
pip install -r requirements-dev.txt
python3 -m pyflakes .
python3 -m pytest                     # PG* env vars (or SWEEP_ALLOW_NO_PASSWORD=1) enable the integration tests
python3 tools/make_package.py         # fdicsweep_package_v<version>.docx in package/ (git-tracked files, zip + base64, self-extracting commands)
```

## Documentation

- [Project overview](docs/project_overview.md)
- [Oracle commands](docs/commands_oracle.md)
- [VSAM commands](docs/commands_vsam.md)
- [Layers, promote, migrate](docs/layers_and_promote.md)
- [Targeted runs, merge-safe outputs, log tables](docs/targeting_and_outputs.md)
- [Flag and environment reference](docs/flag_arguments.md)
- [Sample outputs](docs/sweep_outputs.md)
- [Changelog](CHANGELOG_v6.0.0.md)
