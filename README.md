# SWEEP: Unified Data Migration Framework (v5.0)

SWEEP migrates large financial datasets into **AWS RDS Aurora PostgreSQL** from two sources:

- **Oracle**: DDL dumps plus data files in S3
- **VSAM**: flat files described by COBOL copybooks, in S3

Data is streamed from S3 straight into PostgreSQL (`aws s3 cp - | gzip -dc | psql \copy`) with no local staging.

## Requirements

- Python 3.9+ with `pandas`, `numpy`, `openpyxl`, `boto3`, `python-dateutil`
- `psql` (PostgreSQL client) and the AWS CLI on `PATH`
- Network access to the Aurora cluster and the S3 buckets

## Setup

```bash
cp sweep.env.sample sweep.env     # then edit: PGHOST, PGPASSWORD, S3 buckets, workers
python3 sweep.py check            # pre-flight: deps, DB, AWS, S3, input files
```

`sweep.env` holds credentials and is git-ignored. Never commit it.

## Usage

```bash
python3 sweep.py oracle run-all --workers 8     # full Oracle pipeline
python3 sweep.py vsam run-all --workers 8       # full VSAM pipeline
python3 sweep.py run-all --track all --bg       # both tracks, as a background daemon

python3 sweep.py status                         # background jobs
python3 sweep.py monitor -w                     # live ingestion dashboard
python3 sweep.py logs -f                        # tail logs
python3 sweep.py cancel                         # stop jobs
```

### Pipeline stages

| # | Oracle | VSAM |
|---|--------|------|
| 1 | `extract` metadata | `extract` copybooks |
| 2 | `inventory` S3 files | `inventory` S3 files |
| 3 | `gate` type check | `type-gate` |
| 4 | `gen-ddl` | `gen-ddl` |
| 5 | `apply-ddl` | `apply-ddl` |
| 6 | `load-data` | `load-data` |
| 7 | `load-obj` (PK, indexes, FK, views, triggers) | `validate` |
| 8 | `validate` | n/a |

## Layout

```
sweep.py            master CLI
sweep_bg.py         background jobs, status, monitor, stop
sweep_common.py     shared config, psql/S3 helpers, type mapping
sweep_stream.sh     S3 -> stdout streaming pipe
oracle/             Oracle track (steps 01-08) + inputs/
vsam/               VSAM track (steps 01-07) + inputs/ (mapping and override workbooks)
docs/               overview, command and flag references, output descriptions
```

Generated `logs/`, `reports/` and `outputs/` folders are git-ignored.

## Documentation

- [Project overview](docs/project_overview.md)
- [Oracle commands](docs/commands_oracle.md)
- [VSAM commands](docs/commands_vsam.md)
- [Flag reference](docs/flag_arguments.md)
- [Sample outputs](docs/sweep_outputs.md)
- [Changelog](CHANGELOG_v5.0.md)
