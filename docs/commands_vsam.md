# SWEEP CLI Command Reference - VSAM Migration Track (v5.4.1)

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
Parses all COBOL copybooks (`.txt`, `.cbl`, `.cob`) in `vsam/inputs/copybooks/` (or `VSAM_COPYBOOK_DIR`) to extract column metadata, Cobol PICTURE datatypes, and identify candidate primary keys (fields grouped under `KEY` or `KEM`, or containing `KEY` in field name).

The reader (`vsam/common_cobol.py`) copes with real mainframe layouts:
- fixed-format **sequence areas** - line/change numbers in columns 1-6 (`106618`) and in columns 73-80 (`00040001`), including numbers glued to the closing period (`VALUE SPACE.00050001`);
- comment lines (`*` / `/` in column 7, `*>` inline comments) and banner lines (`=====`);
- `COPY ... REPLACING` placeholders such as `:USA090:-KORG` or `:NEXT-MODEL:-KEY` (the colons are removed: `USA090-KORG`);
- statements that continue over several lines (`VALUE LOW-VALUES.` on the next line), quoted literals containing periods, fields that start with digits (`06BALCUR`), `COMP-3`, `OCCURS`, and `REDEFINES` (the redefining field is not duplicated).

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

### Step 02: S3 Discovery, Inventory Mapping & Column Mapping (`inventory`)
Discovers all S3 VSAM flat files, maps them to copybooks and target schemas/tables using `vsam_master_mapping.xlsx`. Evaluates multiple file versions and prunes older versions so only the single latest version per file is retained in the load matrix.

File names are `FDICSWP_<COMPANY>_<SOURCE>_FLAT_<timestamp>.csv[.gz]`; the timestamp may be `MMDDYYYY`, `YYYYMMDD`, `YYMMDD`, `YYYYMMDDHHMMSS`, `YYYYMMDD_HHMMSS` or `YYYY-MM-DD[THH:MM:SS]` and decides which file is the newest snapshot.

#### Column mapping (S3 headers -> copybook columns)
Every VSAM file has a header row, but the header names and their order do **not** match the copybook. Step 02 therefore maps each S3 header to a copybook column:

1. **`column_overrides` sheet** of `vsam_master_overrides.xlsx` (`sourcecompany`, `filename` = source name or S3 file name, `header`, `copybook_column`; blank `copybook_column` forces *unmapped*) - a human decision always wins.
2. **Sticky**: headers that were matched in the previous run keep their match, so a new file version cannot silently re-shuffle columns.
3. **Exact name** after normalisation.
4. **Similarity + optimal assignment** (Kuhn-Munkres / Hungarian) over the remaining headers and copybook columns: name segmentation, mainframe abbreviations (`ACCT`, `NBR`, `DT`...), acronyms, `PREV`/`TODAY`/`FEE`/`DIV`/`STM` guards, occurrence-index checks, optional single-letter prefixes (`b_korg` = `KORG`).

Each match carries a score and a status: **ACCEPTED** (score >= 0.70, exact, override or sticky), **REVIEW** (0.35-0.70, mapped but flagged), **UNMAPPED** (header with no copybook column - loaded as its own column, type inferred from data), and **COPYBOOK_ONLY** (copybook column that is not in the file). Thresholds can be tuned with `VSAM_MAP_ACCEPT` / `VSAM_MAP_MIN`.

#### Table column names: `--tbl-col`
| Value | Table column names | Datatype |
| :--- | :--- | :--- |
| `headers` (default) | the S3 file headers | from the mapped copybook column |
| `copybook` | the mapped copybook column names (unmapped headers keep their own name) | from the mapped copybook column |

The default can also be set with `VSAM_TBL_COL`. The choice is stored in `vsam/outputs/vsam_02_column_map.csv`, which Steps 03, 04, 06 and 07 read, so they cannot disagree. Switching later means re-running Steps 02-05 on fresh tables (existing tables are not renamed).

#### Keys
A primary key is applied only when it comes from a **copybook KEY/KEM group** (every key column must be an ACCEPTED match) or from a person (`korg_overrides` sheet). Unique columns that the code finds by itself are listed as `candidate key (not applied)` in the report and never become a primary key - a guessed key can be wrong. Tables without a key are replaced by each new snapshot (see Step 06).

```bash
# Standard discovery and inventory mapping
python3 sweep.py vsam inventory

# Scan a custom S3 bucket prefix in background
python3 sweep.py vsam inventory --s3 "s3://your-migration-bucket/vsam-flat-files/" --bg

# Restrict discovery to a specific source company (e.g. FDIC or AGILETIC)
python3 sweep.py vsam inventory --company FDIC

# Name the table columns after the copybook instead of the S3 headers (default: --tbl-col headers)
python3 sweep.py vsam inventory --tbl-col copybook

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_02_inventory --company FDIC
```

* **Outputs Generated**:
  - `vsam/outputs/vsam_02_inventory.csv`, `vsam_02_load_matrix.csv`, `vsam_02_latest_file_headers.csv` (unprefixed legacy copies are also written).
  - `vsam/outputs/vsam_02_column_map.csv`: one row per header (and per unused copybook column) - see the `ColumnMapping` sheet below.
  - `vsam/reports/<db>_vsam_02_inventory_report_<timestamp>.xlsx`: sheets `Summary`, `FileInventory` (adds `key source`, `candidate key (not applied)`, `mapped / review / unmapped columns`), `LatestFileHeaders` and **`ColumnMapping`** (`source_company`, `source_filename`, `latest_filename`, `copybook_name`, `header_position`, `header`, `table_column`, `copybook_field`, `copybook_col`, `pic`, `usage`, `copybook_type`, `pg_type`, `is_key`, `score`, `confidence`, `status`, `method`, `tbl_col_mode`).

> Only the newest file version per (company, source file) is kept, always (VSAM files are full snapshots). `sweep.py vsam inventory` exposes `--s3` and `--company`.

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

# Standalone module equivalent:
python3 -m vsam.vsam_03_type_gate --workers 4
```

* **Outputs Generated**:
  - `vsam/outputs/vsam_03_type_gate.json` and `vsam_03_type_gate_status.csv`: type profiling results, PK candidates, schema routing metadata and column definitions.
  - `vsam/reports/<db>_vsam_03_type_gate_report_<timestamp>.xlsx`: sheets `TypeGateSummary` (11 columns: `S3 file name`, `source key`, `schema`, `copybook`, `table`, `has PK?`, `PK Columns`, `is PK valid`, `has schema_col?`, `schema_col`, `Full S3 URI`), `GateSummary`, `PKVerification`, `SchemaColumnDetect`, `TypeIssues`.
  - A source that fails to profile is recorded as `BLOCKED` with the error text; the step itself still completes.
  - Types come from the Step 02 column map. A column whose values do not fit its copybook type (for example a header matched to the wrong numeric column) is loaded as `text` instead of turning the values into NULL; the report lists `TYPE_FALLBACK_TEXT`.
  - Keys found by positional discovery are reported as `KEY_CANDIDATE_NOT_APPLIED` and are never applied.

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
Since 5.4.0 the VSAM loader works exactly like the Oracle loader: temp staging tables, bulk `\copy`, set-based validation in SQL and **one transaction per file** (`vsam/vsam_06_pipeline.py`; shared helpers in `sweep_loadpipe.py`). A failed file leaves no partial data.

Per file, inside one `BEGIN ... COMMIT`:
1. `\copy` the file into an all-text temp table (columns named after the table columns, using the Step 02 column map);
2. each row is routed to its destination schema in SQL (see *Schema Routing Rules*);
3. bad rows are rejected and recorded (see *Rejected rows*);
4. typed temp table, then the upsert (keyed tables) or the snapshot replace (tables without a key);
5. row-count check `read = inserted + updated + unchanged + rejected + duplicates`, otherwise the file rolls back;
6. rejected rows go to `sweep_ctl.load_rejects`, the `sweep_ctl.load_log` rows (with the S3 metadata) are written, `COMMIT`.

**Keys.** Only a primary key from the copybook KEY group (every key column an ACCEPTED match) or from `korg_overrides` is applied - never a guessed one. The key is applied to the destination tables before the load; if it cannot be applied (for example duplicates in the data, or key columns missing from the file) that is an **error**: the source is not loaded (Step 03 reports `PK_ERROR`, Step 06 fails the file). A file without a copybook is loaded **without** a key.

**Keyed tables** are upserted: `INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE (table columns) IS DISTINCT FROM (file columns)`, so only rows that actually differ are updated (`inserted`, `updated`, `unchanged` are counted separately). Rows missing from a newer snapshot are **never deleted**.

**Tables without a key (snapshot replace).** VSAM files are full snapshots, so a keyless table is **replaced**. All files that feed one keyless table are loaded in a **single transaction on one connection** (one `\copy` per file into temp tables, then `TRUNCATE` + `INSERT`, the load_log rows and `COMMIT`). If any file of the table fails, the whole transaction rolls back and the table keeps its old data (every file of the table is reported `FAILED`).

**Which files are loaded.** Only the newest version per (company, source file). A file counts as already loaded only when `sweep_ctl.load_log` has `SUCCESS` rows for it and the stored S3 `LastModified`, `ETag` and size all equal what S3 reports now (Step 06 checks the object right before loading and stores the values). A changed file is reloaded even with the same name and path; `--reload` forces a reload. A keyless shared table reloads **all** of its files whenever **any** one of them changed; keyed tables load only the changed files. A file older than a snapshot of the same source that is already loaded is `SKIPPED`.

In a **partial run** (`--company`, `--tables`, `--limit-files`) other source files of a shared keyless table may be missing, so replacing a table that already holds rows needs `--force-truncate`. Full runs need no flag.

#### Rejected rows
| Reason | Meaning |
|---|---|
| `BAD_VALUE` | a value does not fit the column type (`pg_input_is_valid`); detail = column and value. Dates such as `9999-12-31` are valid and kept. |
| `NOT_NULL` | NOT NULL column without a value |
| `PK_BLANK` | keyed table: a primary key column is NULL / empty / `nan` / `none` / `null` |
| `PK_SENTINEL` | keyed table: a primary key column is all 9s (`9999`, `9999-99-99`, ...) |
| `ALL_NULL` | keyed table: every column is empty, blank or a 0s / 9s placeholder |

Tables without a key load every row (nothing is rejected for blank or placeholder values). Rejected rows are stored in `sweep_ctl.load_rejects` (reason, detail, record number, the row as JSON) and shown in the `Reject_Summary` and `Rejected_Rows` sheets. **By default a rejected row never fails the file** (`--max-rejects 0`); `--max-rejects N` fails and rolls back a file that reaches N rejected rows.

```bash
# Replace a populated keyless table from a filtered (partial) run - only when you know no other source feeds it
python3 sweep.py vsam load-data --company FDIC --force-truncate --workers 4

# Preview which files would be loaded, without touching the database
python3 sweep.py vsam load-data --dry-run

# Load again even though S3 shows no change / fail files that reach 25 rejected rows
python3 sweep.py vsam load-data --reload
python3 sweep.py vsam load-data --max-rejects 25
```

PostgreSQL **16 or later** is required (`pg_input_is_valid`); the loader stops with a clear message otherwise.

#### Schema Routing Rules:
- **No copybook and no korg column**: Loaded **directly into the `bcs` schema**.
- **No copybook, but korg column present**: Segregates records by korg value and routes them into the matching schemas (or `bcs` if unmatched/inactive).
- **With copybook and korg column**: Segregates records and inserts them into mapped schemas based on korg values. Unmapped/inactive schemas are routed to `bcs`.

```bash
# Apply PKs to fresh tables, validate records, dynamic upsert (4 workers, background)
python3 sweep.py vsam load-data --workers 4 --bg

# Smoke test: Ingest only the first 2 files
python3 sweep.py vsam load-data --limit-files 2

# Ingest data for a specific source company
python3 sweep.py vsam load-data --company FDIC --workers 4

# Ingest data for specific tables only
python3 sweep.py vsam load-data --tables vsamkorg,stg_ftxref_v --workers 4

# Standalone module equivalent (run from the repository root):
python3 -m vsam.vsam_06_load_data --workers 4
```

#### Generated Excel Workbook (`vsam/reports/<db>_vsam_06_load_data_report_<timestamp>.xlsx`, plus `vsam/outputs/vsam_06_load_log_<run_id>.csv`):
1. **`LoadSummary` Sheet**:
   - `s3 filename`, `tablename`, `copybook` (name or `NO_COPYBOOK`), `korg column` (name or `NO_KORG`)
   - `total recs`: records read from the file
   - `loaded recs`: valid records applied to the tables (`inserted` + `updated` + `unchanged`)
   - `inserted` / `updated` / `unchanged`: new rows / rows whose values changed / identical rows that were not touched
   - `skipped recs`: rejected records (all reasons, duplicates not included)
   - `rejected by reason`: per-reason counts, e.g. `ALL_NULL = 1, BAD_VALUE = 2, PK_BLANK = 3`
   - `duplicates dropped`: duplicate key rows inside the file (last row wins)
   - `records loaded in mapped schema` / `records loaded in unmapped schema`: per schema as text, `bss01104 = 20, bss01112 = 34` (blank when none)
   - `records loaded in bcs schema`, `load %` (`mapped_loaded / total_records * 100`)
   - `has PK?`, `PK columns`
   - `skipped/failed reason`: the reject summary or the error message, up to 1,000 characters per cell (longer text ends with `...[+N chars]`)
   - `duration`, `status` (`SUCCESS`, `SKIPPED` or `FAILED`), `full s3 uri`
2. **`SchemaBreakdown` Sheet**: pivot of file/table rows by destination schema (`bcs`, `bss01104`, ...) plus `Total`, with a `TOTAL` row.
3. **`Reject_Summary` Sheet**: one row per file, table and reason with the row count and a sample detail.
4. **`Rejected_Rows` Sheet**: the rejected rows (file line number, destination schema, reason, detail, row as JSON, up to 10,000 rows; all of them are in `sweep_ctl.load_rejects`).
5. **`RunOverview` Sheet**: run id, task / success / failed counts, skipped (unchanged / older snapshot), rows loaded, rejected and duplicate counts, reject limit, worker count.

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
# VSAM re-processes every file in the load matrix: tables with a PK are upserted, tables without a PK are
# replaced by the snapshot (the interrupted run never touched them, thanks to the staging swap).
python3 sweep.py vsam load-data --workers 4 --bg
```

### Recipe 2: Initial Load followed by Delta Loads
Primary keys are applied before the first load, so later snapshots upsert:
```bash
# Step 1: initial load (PKs applied to fresh tables)
python3 sweep.py vsam load-data --workers 4 --bg

# Step 2: validate ingested row counts
python3 sweep.py vsam validate --exact-counts

# Step 3: subsequent delta ingestion upserts on the PKs
python3 sweep.py vsam load-data --workers 4 --bg
```

### Recipe 3: Multi-Track Overnight Ingestion
To run Oracle and VSAM pipelines sequentially overnight with automated logging:
```bash
python3 sweep.py run-all --track all --workers 4 --bg
python3 sweep.py monitor -w

# A failing step stops the pipeline and the job finishes as ERRORED (check `python3 sweep.py status`)
```
