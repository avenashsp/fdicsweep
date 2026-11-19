# SWEEP: Unified Data Migration Framework (v5.3.0)

SWEEP migrates large financial datasets into **AWS RDS Aurora PostgreSQL** from two sources:

- **Oracle**: DDL dumps plus data files in S3
- **VSAM**: flat files described by COBOL copybooks, in S3

Data is streamed from S3 straight into PostgreSQL (`aws s3 cp - | gzip -dc | psql \copy`) with no local staging.

## Requirements

- Python 3.9+ with `pandas`, `numpy`, `openpyxl`, `boto3`, `python-dateutil` (`pip install -r requirements.txt`)
- `psql` (PostgreSQL client) and the AWS CLI on `PATH`
- Network access to the Aurora cluster and the S3 buckets

## Setup

```bash
pip install -r requirements.txt   # or: pip install -e .   (adds the `sweep` command)
cp sweep.env.sample sweep.env     # then edit: PGHOST, PGPASSWORD, S3 buckets, workers
python3 sweep.py check            # pre-flight: deps, DB, AWS, S3, input files (exit 1 if unusable)
```

`sweep.env` holds credentials and is git-ignored. Never commit it. Variables already exported in your shell
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

Every step exits `0` on success and non-zero on failure.

> **VSAM reloads:** a table that already holds rows but has no primary key is never truncated implicitly. Re-run with
> `--force-truncate` to truncate it once per run and reload. `--dry-run` lists what would be loaded.

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
```

## Documentation

- [Project overview](docs/project_overview.md)
- [Oracle commands](docs/commands_oracle.md)
- [VSAM commands](docs/commands_vsam.md)
- [Flag and environment reference](docs/flag_arguments.md)
- [Sample outputs](docs/sweep_outputs.md)
- [Changelog](CHANGELOG_v5.3.0.md)
