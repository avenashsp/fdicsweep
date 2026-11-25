# SWEEP CLI Command Reference - Oracle Migration Track (v5.4.2)

Complete command reference for the **Oracle Migration Track** of the SWEEP Unified Data Migration Framework.

---

## 1. Universal Operations & Diagnostics

### Pre-Flight Diagnostics (`check`)
Validates python dependencies, RDS Aurora PostgreSQL connectivity, AWS CLI/Boto3 credentials, S3 bucket access, and required input files:

```bash
# Standard pre-flight diagnostic check
python3 sweep.py check

# Check against a custom Oracle S3 bucket URI
python3 sweep.py check --s3 s3://your-migration-bucket/oracle-migration-qa/
```

### Live Process Status (`status`)
Inspects active background daemons, start timestamps, PIDs, elapsed execution times, and exit statuses:

```bash
python3 sweep.py status
```

### Real-Time Ingestion Dashboard (`monitor`)
Renders a live terminal dashboard showing active workers, streaming file progress, committed rows, throughput (rows/sec), and in-flight streams:

```bash
# Dual-track live dashboard (single snapshot render)
python3 sweep.py monitor

# Continuous live refresh (every 3 seconds; press Ctrl+C to exit)
python3 sweep.py monitor -w

# Monitor Oracle track only with custom refresh interval (e.g. 5 seconds)
python3 sweep.py monitor --track oracle --interval 5
```

### Execution Log Viewer (`logs`)
Follows live logs or inspects recent execution output:

```bash
# Tail and follow the latest active background log across all tracks
python3 sweep.py logs -f

# Follow logs filtered strictly to the Oracle migration track
python3 sweep.py logs --track oracle -f

# View the last 100 lines of the latest Oracle log
python3 sweep.py logs --track oracle -n 100

# View a specific log file in the oracle logs directory
python3 sweep.py logs --file oracle/logs/SWEEPB_oracle_06_load_data_20261108_154430.log
```

### Clean Cancellation (`stop`)
Safely terminates running background jobs *and every child process they started* (psql, aws, gzip), after verifying each PID still belongs to a SWEEP supervisor:

```bash
# Stop all running background migration processes
python3 sweep.py stop

# Stop a specific job by name
python3 sweep.py stop --name oracle_06_load_data

# Stop background processes AND end in-flight PostgreSQL sessions opened by SWEEP
# (only sessions with application_name = 'sweep'; other users' sessions are left alone)
python3 sweep.py stop --kill-db
```

### Automated Full Pipeline (`run-all`)
Executes the Oracle pipeline end-to-end (Steps 01 through 08) in sequence:

```bash
# Run Oracle pipeline end-to-end in background daemon with 4 workers
python3 sweep.py oracle run-all --workers 4 --bg

# Run in the foreground; stops at the first step that exits non-zero
python3 sweep.py oracle run-all --workers 4
```

> Every step exits **non-zero on failure** (missing inputs, failed DDL, failed file loads, failed parity audit), so a pipeline never continues past a failed step.

---

## 2. Oracle Migration Step-by-Step Command Reference

### Step 01: Extract Metadata from Oracle DDL Dump (`extract`)
Parses Oracle SQL DDL dumps to extract table structures, columns, datatypes, primary keys, and indexes:

```bash
# Standard extraction across all schemas
python3 sweep.py oracle extract

# Run extraction in background daemon
python3 sweep.py oracle extract --bg

# Filter extraction to specific Oracle schema owners
python3 sweep.py oracle extract --owners BSS01001,BSS01002

# Specify a custom Oracle DDL dump file path
python3 sweep.py oracle extract --file oracle/inputs/oracle_dump/oracle_master_ddl.sql

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_01_extract_mtd --owners BSS01001 --bg
```

* **Inputs**: `oracle/inputs/oracle_dump/*.sql` (or `--file`)
* **Outputs**: `oracle/outputs/oracle_metadata/oracle_01_metadata_{tables,constraints,indexes,partitions,table_stats,sequences,objects}.csv`
* **Report**: `oracle/reports/<db>_oracle_01_extract_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_01_extract_<timestamp>.log`

---

### Step 02: S3 Inventory & Small-to-Large Queue (`inventory`)
Scans `ORACLE_S3_BUCKET`, maps S3 CSV files to Oracle tables, matches column headers, and sorts files from smallest to largest:

```bash
# Standard inventory discovery
python3 sweep.py oracle inventory

# Run inventory in background daemon
python3 sweep.py oracle inventory --bg

# Scan a custom S3 URI prefix
python3 sweep.py oracle inventory --s3 "s3://your-migration-bucket/oracle-migration-qa/"

# Restrict scan to specific schema owners or tables
python3 sweep.py oracle inventory --owners BSS01001 --tables BROKERACCOUNTDETAIL

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_02_inventory --owners BSS01001
```

* **Inputs**: `ORACLE_S3_BUCKET`, Step 01 metadata extracts
* **Outputs**: `oracle/outputs/oracle_02_s3_file_map.csv` (file-to-table map, ordered smallest to largest; a legacy copy `s3_file_map.csv` is also written)
* **Report**: `oracle/reports/<db>_oracle_02_inventory_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_02_inventory_<timestamp>.log`

---

### Step 03: Pre-Flight Data Quality Gate (`gate`)
Samples S3 CSV data rows and compares actual values against expected PostgreSQL data types to catch anomalies prior to loading:

```bash
# Standard type gate (sample 1,000 rows per file)
python3 sweep.py oracle gate

# Sample 2,000 rows per file in background
python3 sweep.py oracle gate --rows 2000 --bg

# Test a subset of specific tables
python3 sweep.py oracle gate --tables BROKERACCOUNTDETAIL,SECURITYMASTER

# Limit testing to the first 10 tables
python3 sweep.py oracle gate --max-tables 10

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_03_type_gate --rows 1000
```

* **Inputs**: S3 CSV files, Step 01 extracts, `oracle_02_s3_file_map.csv`
* **Outputs**: `oracle/outputs/oracle_03_gate_results.csv`, `oracle_03_gate_pass_tables.txt` (tables with no BLOCK findings; unprefixed legacy copies are also written)
* **Report**: `oracle/reports/<db>_oracle_03_type_gate_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_03_type_gate_<timestamp>.log`

---

### Step 04: Generate Target DDL (`gen-ddl` / `ddl`)
Generates PostgreSQL DDL split into pre-load bare tables (with audit columns) and post-load objects (PKs, indexes, views, triggers):

```bash
# Generate DDL for all discovered tables
python3 sweep.py oracle gen-ddl

# Run DDL generation in background
python3 sweep.py oracle gen-ddl --bg

# Generate DDL for specific schema owners or tables
python3 sweep.py oracle gen-ddl --owners BSS01001 --tables BROKERACCOUNTDETAIL

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_04_generate_ddl --owners BSS01001
```

* **Inputs**: Step 01 metadata extracts (and the dump file for view/trigger translation)
* **Outputs** (one set per schema):
  - `oracle/outputs/pre_load/<schema>_tables.sql` (bare tables + partitions)
  - `oracle/outputs/post_load/<schema>_unique_and_pk.sql`, `_alter_statements.sql`, `_indexes.sql`, `_fk_not_valid.sql`, `_fk_validate.sql`, `_views.sql`, `_triggers.sql`
* **Report**: `oracle/reports/<db>_oracle_04_generate_ddl_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_04_generate_ddl_<timestamp>.log`

---

### Step 05: Apply DDL & Provision Target Tables (`apply-ddl`)
Executes pre-load DDL in PostgreSQL to create target schemas, control tables (`sweep_ctl`), and unconstrained bare tables:

```bash
# Apply DDL for all schemas
python3 sweep.py oracle apply-ddl

# Apply DDL in background daemon
python3 sweep.py oracle apply-ddl --bg

# Apply DDL for specific schemas only
python3 sweep.py oracle apply-ddl --schemas BSS01001,BSS01002

# Skip re-applying sweep_meta.sql
python3 sweep.py oracle apply-ddl --skip-meta

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_05_apply_ddl --schemas BSS01001
```

* **Inputs**: `oracle/inputs/sweep_meta.sql`, `oracle/outputs/pre_load/*_tables.sql`
* **Target DB**: Provisions `sweep_ctl.load_log`, target schemas, and bare tables
* **Exit code**: non-zero if any DDL script failed (the remaining scripts are still attempted and listed in the report)
* **Report**: `oracle/reports/<db>_oracle_05_apply_ddl_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_05_apply_ddl_<timestamp>.log`

---

### Step 06: Multi-Worker Streaming Bulk Load (`load-data` / `load`)
Streams CSV chunks directly from S3 into PostgreSQL using zero-disk Unix pipes via `sweep_stream.sh`:

```bash
# Full bulk load with 4 workers in background daemon
python3 sweep.py oracle load-data --workers 4 --bg

# Load ONLY tables that passed the pre-flight type gate
python3 sweep.py oracle load-data --gate-pass --workers 4 --bg

# Smoke test: Ingest only the first 2 small files
python3 sweep.py oracle load-data --limit-files 2

# Dry-run: Test S3 accessibility and stream pipes without writing to DB
python3 sweep.py oracle load-data --dry-run

# Load a specific table
python3 sweep.py oracle load-data --tables BROKERACCOUNTDETAIL --workers 2

# Exclude specific heavy tables from a run
python3 sweep.py oracle load-data --exclude-tables AUDITLOG,TRANSACTIONDETAIL

# Ingest a specific S3 file URI directly
python3 sweep.py oracle load-data --file "s3://your-migration-bucket/oracle-migration-qa/BSS01001.BROKERACCOUNTDETAIL_001.csv.gz"

# Re-load files even though S3 shows no change since the last load (upserts on the key, appends when there is none)
python3 sweep.py oracle load-data --reload --tables BROKERACCOUNTDETAIL

# Ingest only small reference files (<= 100 MB)
python3 sweep.py oracle load-data --max-size-mb 100 --workers 4 --bg

# Ingest only large chunk files (>= 1000 MB)
python3 sweep.py oracle load-data --min-size-mb 1000 --workers 2 --bg

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_06_load_data --workers 4 --bg
```

* **Inputs**: S3 CSV files, `oracle/outputs/oracle_02_s3_file_map.csv`, the foreign keys of `oracle/outputs/metadata/oracle_01_metadata_constraints.csv`, `sweep_stream.sh`
* **Outputs**: Data committed to Aurora PostgreSQL; audit logged in `sweep_ctl.load_log`; rejected rows in `sweep_ctl.load_rejects`
* **Load order**: parent tables first. The dependency graph comes from the foreign keys extracted in Step 01 (the target tables have none yet on Day-1) plus any live foreign keys in PostgreSQL. Tables are loaded **level by level** (level 0 = tables that reference nobody); all files of a level finish before the next level starts, with parallel workers inside a level. Self-references and cycles share one level. Foreign keys stay enabled and the `FK_MISSING` check stays; the ordering removes the false orphans. With `--fast-bulk` the files are loaded smallest to largest instead.
* **Which files**: a file counts as already loaded only when `sweep_ctl.load_log` has a `SUCCESS` row whose stored S3 `LastModified`, `ETag` and size (columns `s3_last_modified`, `s3_etag`, `s3_size`) all equal what S3 reports now. Step 02 writes them to the file map; Step 06 re-checks the object (HEAD) right before loading and stores what it saw, so a stale file map does not matter. A re-delivered file (same name and path, different metadata) is loaded again; `--reload`, `--file` and `--filename` force a load. Rows from earlier versions without metadata are treated as loaded (use `--reload` to re-verify them). `load_log` keeps one row per URI.
* **Triggers**: switched off for the load transaction (`SET LOCAL session_replication_role = replica`, or `DISABLE TRIGGER` / `ENABLE TRIGGER` inside the transaction as a fallback - `--trigger-mode`), so audit columns are set explicitly on updates.
* **Upsert**: `INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE (target columns) IS DISTINCT FROM (file columns)` - only rows that actually differ are updated. A `modifieddate` / `modifiedby` that comes from the file is source data and counts as a change. Counts: `inserted`, `updated`, `unchanged` (identical rows, not touched), `rejected`, `duplicates` (in-file, last row wins); `read = inserted + updated + unchanged + rejected + duplicates` is checked before the commit.
* **Bad rows**: `NOT_NULL`, `CHECK`, `BAD_VALUE`, `FK_MISSING` and `ALT_KEY_CONFLICT` rows are rejected, not fatal; they are listed in `sweep_ctl.load_rejects` and the `Reject_Summary` / `Rejected_Rows` sheets. **By default (`--max-rejects 0`) rejected rows never fail a file**; only technical errors (a row-count mismatch, a broken stream) roll it back. `--max-rejects N` fails and rolls back a file at N rejected rows.
* **Temp storage / chunked load**: the staged path keeps the rows in temp tables, which live on the instance's local storage. Files of at least `--chunk-min-mb` (default 256 MB compressed) are therefore staged in chunks of `--chunk-rows` records (default 2,000,000) **inside the same single transaction**: each chunk is validated and applied, then its temp tables are emptied, so temp storage holds one chunk instead of the whole file. Counts, rejects and the reject limit are cumulative over all chunks and the row-count check covers the whole file. One difference from a single pass: a duplicate key that appears in two different chunks is applied as an update (last row wins) instead of being dropped as `DUP_IN_FILE`; the loaded data is the same. The report column `load_path` shows `fast bulk`, `staged` or `staged, chunked` per file.
* **`--fast-bulk`**: direct `COPY` into an empty table, **no temp staging**. NOT NULL and identity columns do not block it (COPY enforces NOT NULL itself - one NULL fails the file and rolls it back; identity columns are `GENERATED BY DEFAULT`). A primary / unique key, CHECK or foreign key, a non-empty table, or a `GENERATED ALWAYS` identity column in the file send the table to the staged path; the log says why (`--fast-bulk skipped for schema.table - using the staged load: ...`).
* **Atomicity**: each file is one transaction (staging, validation, upsert, `load_log` row, `COMMIT`); a failed or truncated stream leaves no rows behind. File names are shell-quoted, so keys with spaces, quotes or `;` are safe.
* **Safety**: a failed "is the table empty?" / "does it have a PK?" lookup fails the file instead of guessing. Each file is bounded by `SWEEP_LOAD_TIMEOUT` (default 6 h). PostgreSQL 16 or later is required (`pg_input_is_valid`).
* **Exit code**: non-zero if any file failed
* **Report**: `oracle/reports/<db>_oracle_06_load_data_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_06_load_data_<timestamp>.log`

---

### Step 07: Post-Load Keys, Indexes, Views & Triggers (`load-obj` / `post-load`)
Compiles primary keys, secondary indexes, foreign keys, views, and triggers across target tables:

```bash
# Build all objects across all loaded tables in background
python3 sweep.py oracle load-obj --stage all --bg

# Execute a specific object stage:
python3 sweep.py oracle load-obj --stage pk        # Primary keys / unique constraints only
python3 sweep.py oracle load-obj --stage alter     # Check constraints and ALTER statements only
python3 sweep.py oracle load-obj --stage index     # Secondary indexes only
python3 sweep.py oracle load-obj --stage fk        # Foreign keys only
python3 sweep.py oracle load-obj --stage views     # Views only
python3 sweep.py oracle load-obj --stage triggers  # Triggers only

# Restrict object build to specific schemas
python3 sweep.py oracle load-obj --stage all --schemas BSS01001 --bg

# Only build objects for tables that actually have loaded data
python3 sweep.py oracle load-obj --stage all --only-loaded

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_07_load_objects --stage all --bg
```

* **Inputs**: `oracle/outputs/post_load/<schema>_*.sql` (parsed by `oracle/oracle_07_parser.py`)
* **Target DB**: Builds PK constraints, indexes, foreign keys, views, triggers
* **Report**: `oracle/reports/<db>_oracle_07_load_objects_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_07_load_objects_<timestamp>.log`

---

### Step 08: End-to-End Oracle Dump vs PostgreSQL Parity Audit (`validate` / `verify`)
Performs complete row count, column structure, datatype, and primary key reconciliation:

```bash
# Fast validation using PostgreSQL system catalog approximations (instant execution)
python3 sweep.py oracle validate

# Full exact parity audit using live COUNT(*) queries
python3 sweep.py oracle validate --exact-counts --bg

# Validate specific schemas
python3 sweep.py oracle validate --schemas BSS01001

# Validate specific tables
python3 sweep.py oracle validate --tables BROKERACCOUNTDETAIL,SECURITYMASTER

# Specify custom master Oracle SQL dump file (auto-extracts if metadata CSVs are missing)
python3 sweep.py oracle validate --file oracle/inputs/oracle_dump/oracle_master_ddl.sql

# Standalone module equivalent (run from the repository root):
python3 -m oracle.oracle_08_validate --exact-counts
```

* **Inputs**: Oracle metadata extracts, PostgreSQL catalog, `sweep_ctl.load_log`
* **Outputs**: Multi-tab parity audit Excel report (10 tabs, see `docs/sweep_outputs.md`)
* **Exit code**: `0` only when the audit verdict is `PASS - 100% PARITY ACHIEVED`; `1` otherwise
* **Report**: `oracle/reports/<db>_oracle_08_validate_report_<timestamp>.xlsx`
* **Log**: `oracle/logs/<db>_oracle_08_validate_<timestamp>.log`

---

## 3. Practical Operational Recipes

### Recipe 1: Recovering from Network Interruption
```bash
# 1. Inspect status
python3 sweep.py status

# 2. If needed, cancel stale sessions:
python3 sweep.py stop --kill-db

# 3. Resume ingestion:
# The loader automatically checks sweep_ctl.load_log and skips already loaded files!
python3 sweep.py oracle load-data --workers 4 --bg
```

### Recipe 2: Progressive Small-to-Large Ingestion
```bash
# Stage 1: Load all small reference files (<= 100 MB)
python3 sweep.py oracle load-data --max-size-mb 100 --workers 4

# Stage 2: Load medium files (100 MB to 1000 MB)
python3 sweep.py oracle load-data --min-size-mb 100 --max-size-mb 1000 --workers 4 --bg

# Stage 3: Load large chunk files (> 1000 MB) with lower concurrency
python3 sweep.py oracle load-data --min-size-mb 1000 --workers 2 --bg
```

### Step 06 options (v5.4.2)

```bash
python3 sweep.py oracle load-data                           # default: rejected rows never fail a file (--max-rejects 0)
python3 sweep.py oracle load-data --max-rejects 10          # fail and roll back a file that reaches 10 rejected rows
python3 sweep.py oracle load-data --fast-bulk               # direct COPY (no temp staging) for empty tables without keys / CHECK / FK; size order
python3 sweep.py oracle load-data --chunk-rows 1000000      # chunk size for big staged files (default 2,000,000; 0 = never chunk)
python3 sweep.py oracle load-data --chunk-min-mb 100        # chunk files of 100 MB (compressed) and up (default 256)
python3 sweep.py oracle load-data --trigger-mode disable    # when session_replication_role is not permitted
```
