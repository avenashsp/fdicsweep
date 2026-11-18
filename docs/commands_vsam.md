# SWEEP CLI Command Reference - VSAM Migration Track (v5.1)

Complete command reference for the **VSAM Migration Track** of the SWEEP Unified Data Migration Framework.

---

## 1. Universal Operations & Diagnostics

### Pre-Flight Diagnostics (`check`)
Validates python dependencies, RDS Aurora PostgreSQL connectivity, AWS CLI/Boto3 credentials, S3 bucket access, and required input files:

```bash
# Standard pre-flight diagnostic check
python3 sweep.py check

# Check against a custom VSAM S3 bucket URI
python3 sweep.py check --s3 s3://your-migration-bucket/vsam-migration-qa/
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

# Monitor VSAM track only with custom refresh interval (e.g. 5 seconds)
python3 sweep.py monitor --track vsam --interval 5
```

### Execution Log Viewer (`logs`)
Follows live logs or inspects recent execution output:

```bash
# Tail and follow the latest active background log across all tracks
python3 sweep.py logs -f

# Follow logs filtered strictly to the VSAM migration track
python3 sweep.py logs --track vsam -f

# View the last 100 lines of the latest VSAM log
python3 sweep.py logs --track vsam -n 100

# View a specific log file in the vsam logs directory
python3 sweep.py logs --file vsam/logs/SWEEPB_vsam_06_load_data_20261108_154430.log
```

### Clean Cancellation (`stop`)
Safely terminates running background jobs *and every child process they started* (psql, aws, gzip), after verifying each PID still belongs to a SWEEP supervisor:

```bash
# Stop all running background migration processes
python3 sweep.py stop

# Stop a specific job by name
python3 sweep.py stop --name vsam_06_load_data

# Stop background processes AND end in-flight PostgreSQL sessions opened by SWEEP
# (only sessions with application_name = 'sweep'; other users' sessions are left alone)
python3 sweep.py stop --kill-db
```

### Automated Full Pipeline (`run-all`)
Executes the VSAM pipeline end-to-end (Steps 01 through 07) in sequence:

```bash
# Run VSAM pipeline end-to-end in background daemon with 4 workers
python3 sweep.py vsam run-all --workers 4 --bg
```

---

## 2. VSAM Step-by-Step Command Reference (Steps 01 - 07)

### Step 01: Extract Copybook Metadata (`extract` / `extract-copybooks`)
Parses all COBOL copybooks (`.txt`) in `vsam/inputs/copybooks/` to extract column metadata, Cobol PICTURE datatypes, and identify candidate primary keys (fields grouped under `KEY` or `KEM`, or containing `KEY` in field name).

```bash
# Extract metadata from all copybooks
python3 sweep.py vsam extract

# Run extraction in background daemon
python3 sweep.py vsam extract --bg

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_01_extract_copybooks
```

* **Outputs Generated**:
  - `vsam/outputs/vsam_01_copybook_extracts.csv`: every copybook field (SQL type, lengths, key / filler flags).
  - `vsam/reports/<db>_vsam_01_extract_report_<timestamp>.xlsx`: sheets `Summary` (per copybook) and `FieldExtract`.
* **Exit code**: non-zero if any copybook failed to parse or the copybook directory is missing.

---

### Step 02: S3 Discovery & Inventory Mapping (`inventory`)
Discovers all S3 VSAM flat files, maps them to copybooks and target schemas/tables using `vsam_master_mapping.xlsx`. Evaluates multiple file versions and prunes older versions so only the single latest version per file is retained in the load matrix.

```bash
# Standard discovery and inventory mapping
python3 sweep.py vsam inventory

# Scan a custom S3 bucket prefix in background
python3 sweep.py vsam inventory --s3 "s3://your-migration-bucket/vsam-flat-files/" --bg

# Restrict discovery to a specific source company (e.g. FDIC or AGILETIC)
python3 sweep.py vsam inventory --company FDIC

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_02_inventory --company FDIC
```

* **Outputs Generated**:
  - `vsam/outputs/vsam_02_inventory.csv`, `vsam_02_load_matrix.csv`, `vsam_02_latest_file_headers.csv` (unprefixed legacy copies are also written).
  - `vsam/reports/<db>_vsam_02_inventory_report_<timestamp>.xlsx`: sheets `Summary`, `FileInventory`, `LatestFileHeaders`.

> `--latest-only` is available on the standalone module (`python3 -m vsam.vsam_02_inventory --latest-only`); `sweep.py vsam inventory` exposes `--s3` and `--company`.

---

### Step 03: Type Gate, PK Verification & Schema Column Detection (`type-gate` / `gate`)
Reads actual CSV data from S3 to verify candidate primary keys, profile data types using exact column headers from line 1 (`header=0`), and classify schema columns. Processes the single latest version per file:
- **Sentinel & Blank Record Filtering**: Completely ignores and purges records where all column values are NULL, empty strings `""`, whitespace `'   '`, sentinel placeholder values (`"9"`, `"9999"`, `"9999-99-99"`, `"9999_99_99"`, `"999999"`), or literal nulls (`nan`, `none`, `null`). They are excluded from analysis, never counted in `rows_checked` or `rows_total`, and never evaluated for PK uniqueness or type errors. Files with only invalid/sentinel data rows are classified as **`EMPTY_NO_DATA`**.
- **No Synthetic `col_1`/`col_2`**: Reads line 1 as headers (`header=0`) to map true column names directly.
- **Regex Date Classification**: Classifies hyphen dates (`YYYY-MM-DD`) as PostgreSQL `DATE` and underscore dates (`YYYY_MM_DD`, sentinels `9999_99_99`) as `VARCHAR(10)` via regex profiling (zero hardcoded column names).
- **Dynamic Varchar Auto-Widening**: Automatically widens varchar columns when actual data lengths exceed declared copybook widths (`varchar(max(declared, actual_max + 4))`).
- **Korg / Schema Column Extraction**:
  - Extracted directly from the `korg` column in the `copybook_mapping` sheet of `vsam_master_mapping.xlsx`.
  - For `AGILETIC` source company: auto-detects schema/key columns in positional order if no copybook mapping is provided.
  - Fallback universal regex detection if korg is not declared in mapping sheet (checks for column names ending in `org`, `korg`, `bank`, `nbr`, `no`).
- **Missing Mapping Indicators**: If copybook mapping or korg mapping is blank in the mapping sheet, the report outputs **`NO_COPYBOOK`** and **`NO_KORG`** respectively.
- **Schema Column to PK Relationship (`schema_col_is_pk`)**: Classifies relationship as `SOLE_PK`, `COMPOSITE_PK_PART`, `NOT_IN_PK`, or `NO_SCHEMA_COL`.
- **Automatic Fallback PK Auto-Discovery (Up to 8 Columns)**: Automatically discovers unique primary keys (single or composite, strictly in positional left-to-right order up to **8** columns) if copybook keys contain duplicates or nulls. PK column names come from the actual file header—never synthetic labels. If no unique combination is found → `NO_PK`.

```bash
# Run type gate across all discovered sources with 4 workers
python3 sweep.py vsam type-gate --workers 4

# Run in background daemon
python3 sweep.py vsam type-gate --workers 4 --bg

# Filter by source company
python3 sweep.py vsam type-gate --company FDIC

# Standalone module equivalent (adds --latest-only, which the sweep.py wrapper does not expose):
python3 -m vsam.vsam_03_type_gate --workers 4 --latest-only
```

* **Outputs Generated**:
  - `vsam/outputs/vsam_03_type_gate.json` and `vsam_03_type_gate_status.csv`: type profiling results, PK candidates, schema routing metadata and column definitions.
  - `vsam/reports/<db>_vsam_03_type_gate_report_<timestamp>.xlsx`: sheets `TypeGateSummary` (11 columns: `S3 file name`, `source key`, `schema`, `copybook`, `table`, `has PK?`, `PK Columns`, `is PK valid`, `has schema_col?`, `schema_col`, `Full S3 URI`), `GateSummary`, `PKVerification`, `SchemaColumnDetect`, `TypeIssues`.
  - A source that fails to profile is recorded as `BLOCKED` with the error text; the step itself still completes.

---

### Step 04: Generate PostgreSQL DDL (`gen-ddl` / `ddl`)
Generates consolidated and per-schema PostgreSQL DDL inside `vsam/outputs/gen_ddl/`:
- `all_tables.sql`: `CREATE SCHEMA` and `CREATE TABLE` statements (including all **6 native audit columns**: `createddate`, `createdby`, `modifieddate`, `modifiedby`, `createddate_m`, `createdby_m`).
- `all_pk.sql`: `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY` statements.
- Per-schema files: `<schema>.sql` and `<schema>_pk.sql`.
- Provisions the fallback **`bcs` schema** alongside all company-specific schemas.
- **Duplicate Column Deduplication**: `build_table_ddl()` deduplicates columns (first occurrence wins). If a copybook defines the same field twice (e.g. `REDEFINES`), only the first occurrence is emitted and a WARNING is logged. Data columns matching any of the 6 native audit column names are automatically de-duplicated.
- **AGILETIC Source Company Handling**: For `AGILETIC` sources, column names and datatypes are directly inferred from the S3 bucket CSV files and data. Key columns are retrieved from the mapped copybook, or auto-detected in positional order if no copybook is present.

```bash
# Generate DDL scripts in vsam/outputs/gen_ddl/
python3 sweep.py vsam gen-ddl

# Generate and immediately execute table DDL in target PostgreSQL database
python3 sweep.py vsam gen-ddl --execute

# Generate DDL for a specific source company
python3 sweep.py vsam gen-ddl --company FDIC

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_04_generate_ddl --execute
```

* **Outputs Generated**:
  - `vsam/outputs/gen_ddl/all_tables.sql`: Consolidated table creation DDL.
  - `vsam/outputs/gen_ddl/all_pk.sql`: Consolidated primary key constraint DDL.
  - `vsam/outputs/gen_ddl/<schema>.sql`: Per-schema table DDL.
  - `vsam/outputs/gen_ddl/<schema>_pk.sql`: Per-schema primary key DDL.

---

### Step 05: Apply Table DDL (`apply-ddl`)
Deploys `sweep_meta.sql` (`sweep_ctl` and `sweep_meta` control schemas) and provisions all target schemas and bare tables in PostgreSQL from `vsam/outputs/gen_ddl/all_tables.sql`.

```bash
# Apply table DDL across all schemas
python3 sweep.py vsam apply-ddl

# Apply table DDL in background daemon
python3 sweep.py vsam apply-ddl --bg

# Skip re-deploying sweep_meta.sql control schema
python3 sweep.py vsam apply-ddl --skip-meta

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_05_apply_ddl
```

* **Outputs Generated**:
  - Log records in `vsam/logs/` detailing schema creation and table provisioning.
  - `vsam/reports/<db>_vsam_05_apply_ddl_report_<timestamp>.xlsx` (sheet `ProvisionedTables`).
  - PostgreSQL target tables ready for streaming bulk load.
* **Exit code**: non-zero if the generated DDL is missing or any statement fails (the whole script runs with `ON_ERROR_STOP`).

---

### Step 06: Schema-Segregated Streaming Ingestion & Dynamic Upsert (`load-data` / `load`)
Streams VSAM flat files directly from S3 into PostgreSQL with live database constraint inspection and flexible `--mode`:

#### Ingestion Modes (`--mode {default, data}`):
- **Default Mode (`python3 sweep.py vsam load-data` or `--mode default`)**:
  * **Fresh Table**: Applies PK constraints from `gen_ddl/<schema>_pk.sql` to available tables in PostgreSQL *before* loading data, validates every record against the PK constraint (`record_passes_pk_validation`), and inserts only valid records.
  * **Existing Table with PK**: Upserts incoming data using the PK constraint (`ON CONFLICT (pk_cols) DO UPDATE SET ...`).
  * **Existing Table without PK**: **Refuses to touch the table unless `--force-truncate` is given** (see below).
- **Data Mode (`python3 sweep.py vsam load-data --mode data`)**:
  * **Fresh Table**: Does NOT apply PK constraints. Performs direct bulk load.
  * **Existing Table with PK**: Upserts incoming data using the PK constraint.
  * **Existing Table without PK**: **Refuses to touch the table unless `--force-truncate` is given** (see below).

#### Truncation Safety (`--force-truncate`)
Several source files can feed the same destination table (for example different companies routed to the shared `bcs` schema). SWEEP therefore:
- never truncates implicitly: a table that already contains rows and has **no primary key** makes the file fail with *"Refusing to TRUNCATE ... without --force-truncate"*;
- with `--force-truncate`, truncates each such table **once per run** - the first file to reach the table "claims" it, and every later file mapped to the same table **appends** (so earlier files are never wiped);
- fails the file if the `TRUNCATE` itself fails (it is never ignored).

```bash
# Re-load tables that already contain data and have no PK (truncate once, then load every file)
python3 sweep.py vsam load-data --mode data --force-truncate --workers 4

# Preview which files would be loaded, without touching the database
python3 sweep.py vsam load-data --dry-run
```

> VSAM Step 06 loads in 10,000-row `INSERT` batches (not one transaction per file) and does **not** skip files that were loaded earlier; re-running is idempotent for tables with a PK (upsert) and needs `--force-truncate` for tables without one. Load results are logged per destination schema in `sweep_ctl.load_log` (keyed `<uri>#<schema>`); if that log write fails the file is reported `FAILED`.

#### Schema Routing Rules:
- **No copybook and no korg column**: Loaded **directly into the `bcs` schema**.
- **No copybook, but korg column present**: Segregates records by korg value and routes them into the matching schemas (or `bcs` if unmatched/inactive).
- **With copybook and korg column**: Segregates records and inserts them into mapped schemas based on korg values. Unmapped/inactive schemas are routed to `bcs`.

```bash
# Default mode: apply PKs to fresh tables, validate records, dynamic upsert (4 workers, background)
python3 sweep.py vsam load-data --workers 4 --bg

# Explicit default mode
python3 sweep.py vsam load-data --mode default --workers 4 --bg

# Data mode: direct load for fresh tables without applying PK constraints
python3 sweep.py vsam load-data --mode data --workers 4 --bg

# Load only the single latest file version per table (based on timestamp in filename)
python3 sweep.py vsam load-data --latest-only --workers 4

# Smoke test: Ingest only the first 2 files
python3 sweep.py vsam load-data --limit-files 2

# Ingest data for a specific source company
python3 sweep.py vsam load-data --company FDIC --workers 4

# Ingest data for specific tables only
python3 sweep.py vsam load-data --tables vsamkorg,stg_ftxref_v --workers 4

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_06_load_data --mode default --workers 4
```

#### Generated Excel Workbook (`vsam/reports/<db>_vsam_06_load_data_report_<timestamp>.xlsx`, plus `vsam/outputs/vsam_06_load_log_<run_id>.csv`):
1. **`LoadSummary` Sheet (17 Columns)**:
   - `s3 filename`: Source S3 file basename.
   - `tablename`: Destination table name.
   - `copybook`: Copybook name or `NO_COPYBOOK`.
   - `korg column`: Korg column name or `NO_KORG`.
   - `total recs`: Total raw records in file.
   - `loaded recs`: Successfully committed records.
   - `skipped recs`: Skipped records (duplicates/invalid PKs).
   - `records loaded in mapped schema`: Records routed to declared mapped schemas.
   - `records loaded in unmapped schema`: Records routed to non-mapped target schemas.
   - `records loaded in bcs schema`: Records routed to the fallback `bcs` schema.
   - `load %`: Percentage of records loaded into the mapped schema out of total file records: `(mapped_loaded / total_records) * 100`.
   - `has PK?`: `YES` or `NO`.
   - `PK columns`: Primary key column names.
   - `skipped/failed reason`: Error message or reason if skipped/failed.
   - `duration`: Execution duration in seconds.
   - `status`: `SUCCESS`, `SKIPPED`, or `FAILED`.
   - `full s3 uri`: Complete S3 URI.
2. **`SchemaBreakdown` Sheet**:
   - Pivot matrix displaying **S3 filenames** and **tablenames** as rows.
   - Destination **schemas** as columns (`bcs`, `bss01104`, `bss01105`, `bss01112`, etc.) plus a `Total` column.
   - Record counts populated at each file/schema intersection.
   - Grand `TOTAL` summary row at the bottom.
3. **`RunOverview` Sheet**: run id, task / success / failed counts, rows loaded and skipped, `--latest-only` flag, worker count.

* **Exit code**: non-zero if any file failed to load.

---

### Step 07: End-to-End Validation & Parity Audit (`validate` / `verify`)
Performs thorough reconciliation and audit:
- Reconciles S3 files against `sweep_ctl.load_log`.
- Executes exact PostgreSQL row counts (`COUNT(*)` with `--exact-counts`).
- Verifies column structures, data types, lengths, precision, scale, and PK constraint status against copybook specifications (or reports `TABLE_UNCONSTRAINED` if PK constraints were not deployed). Tables whose columns were inferred from the data (no copybook) are audited too.
- A file routed entirely to the fallback `bcs` schema counts as loaded.
- **Exit code**: `1` when a file is not loaded, a column is missing, a type mismatches, or the database catalog cannot be queried; `0` otherwise.

```bash
# Fast validation using PostgreSQL catalog approximations
python3 sweep.py vsam validate

# Full exact parity audit using live COUNT(*) queries in background
python3 sweep.py vsam validate --exact-counts --bg

# Validate specific tables
python3 sweep.py vsam validate --tables vsamkorg --exact-counts

# Validate specific schema
python3 sweep.py vsam validate --schema bss01001 --exact-counts

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_07_validate --exact-counts
```

* **Outputs Generated**:
  - `vsam/reports/<db>_vsam_07_validate_report_<timestamp>.xlsx`: sheets `FileReconciliation`, `RowCounts`, `ColumnAudit`, `TypeAudit`, `PKAudit`.

---

## 3. Practical Operational Recipes

### Recipe 1: Recovering from Network Interruption
If a network disconnect occurs during VSAM ingestion:
```bash
# 1. Inspect status
python3 sweep.py status

# 2. Check if the job is still alive or stopped
# If needed, terminate stale database sessions:
python3 sweep.py stop --kill-db

# 3. Resume ingestion:
# VSAM re-processes every file in the load matrix: tables with a PK are upserted (idempotent);
# tables without a PK need --force-truncate so rows from the interrupted run are not duplicated.
python3 sweep.py vsam load-data --workers 4 --bg
python3 sweep.py vsam load-data --mode data --force-truncate --workers 4 --bg   # PK-less tables
```

### Recipe 2: Initial Fresh Load without Constraints followed by Constrained Upsert
To load high-volume initial data rapidly without constraint overhead, then enforce constraints on subsequent runs:
```bash
# Step 1: Initial fast load in data mode (no PK constraints applied to fresh tables)
python3 sweep.py vsam load-data --mode data --workers 4 --bg

# Step 2: Validate ingested row counts
python3 sweep.py vsam validate --exact-counts

# Step 3: Subsequent delta ingestion in default mode (applies PKs and upserts)
python3 sweep.py vsam load-data --mode default --workers 4 --bg
```

### Recipe 3: Multi-Track Overnight Ingestion
To run Oracle and VSAM pipelines sequentially overnight with automated logging:
```bash
python3 sweep.py run-all --track all --workers 4 --bg
python3 sweep.py monitor -w

# A failing step stops the pipeline and the job finishes as ERRORED (check `python3 sweep.py status`)
```
