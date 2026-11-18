# SWEEP Migration Framework - Complete CLI Flags Reference (v5.0)

Complete flags reference for all CLI commands and python scripts in the **SWEEP Unified Data Migration Framework** across both **Oracle** and **VSAM** tracks.

Scripts can be invoked in two ways:
1. **Via Master CLI**: `python3 sweep.py <track> <command> [flags]` *(Recommended: includes `--bg` daemon management, logging, and unified environment configuration)*
2. **As Standalone Scripts**: `python3 <track>/<script>.py [flags]`

---

## 1. Quick Flag Reference Matrix

### Oracle Migration Track (8 Steps)

| Step | Script / Command | Available Flags |
| :---: | :--- | :--- |
| **01** | `oracle_01_extract_mtd.py`<br>`python3 sweep.py oracle extract` | `'--file FILE'`, `'--owners OWNERS'`, `'--bg'`, `'-h, --help'` |
| **02** | `oracle_02_inventory.py`<br>`python3 sweep.py oracle inventory` | `'--s3 S3'`, `'--owners OWNERS'`, `'--tables TABLES'`, `'--bg'`, `'-h, --help'` |
| **03** | `oracle_03_type_gate.py`<br>`python3 sweep.py oracle gate` | `'--rows ROWS'`, `'--tables TABLES'`, `'--max-tables MAX_TABLES'`, `'--bg'`, `'-h, --help'` |
| **04** | `oracle_04_generate_ddl.py`<br>`python3 sweep.py oracle gen-ddl` | `'--owners OWNERS'`, `'--tables TABLES'`, `'--bg'`, `'-h, --help'` |
| **05** | `oracle_05_apply_ddl.py`<br>`python3 sweep.py oracle apply-ddl` | `'--schemas SCHEMAS'`, `'--skip-meta'`, `'--bg'`, `'-h, --help'` |
| **06** | `oracle_06_load_data.py`<br>`python3 sweep.py oracle load-data` | `'--workers WORKERS'`, `'--tables TABLES'`, `'--exclude-tables EXCLUDE_TABLES'`, `'--file FILE'`, `'--filename FILENAME'`, `'--gate-pass'`, `'--limit-files LIMIT_FILES'`, `'--max-size-mb MAX_SIZE_MB'`, `'--min-size-mb MIN_SIZE_MB'`, `'--dry-run'`, `'--bg'`, `'-h, --help'` |
| **07** | `oracle_07_load_objects.py`<br>`python3 sweep.py oracle load-obj` | `'--stage {all,pk,index,fk,validate,views,triggers}'`, `'--schemas SCHEMAS'`, `'--bg'`, `'-h, --help'` |
| **08** | `oracle_08_validate.py`<br>`python3 sweep.py oracle validate` | `'--exact-counts'`, `'--tables TABLES'`, `'--schemas SCHEMAS'`, `'--file FILE'`, `'--bg'`, `'-h, --help'` |
| **All** | `python3 sweep.py oracle run-all` | `'--workers WORKERS'`, `'--bg'`, `'-h, --help'` |

---

### VSAM Migration Track (7 Steps)

| Step | Script / Command | Available Flags |
| :---: | :--- | :--- |
| **01** | `vsam_01_extract_copybooks.py`<br>`python3 sweep.py vsam extract` | `'--bg'`, `'-h, --help'` |
| **02** | `vsam_02_inventory.py`<br>`python3 sweep.py vsam inventory` | `'--s3 S3'`, `'--company COMPANY'`, `'--latest-only'`, `'--bg'`, `'-h, --help'` |
| **03** | `vsam_03_type_gate.py`<br>`python3 sweep.py vsam type-gate` | `'--workers WORKERS'`, `'--company COMPANY'`, `'--latest-only'`, `'--bg'`, `'-h, --help'` |
| **04** | `vsam_04_generate_ddl.py`<br>`python3 sweep.py vsam gen-ddl` | `'--company COMPANY'`, `'--execute'`, `'--bg'`, `'-h, --help'` |
| **05** | `vsam_05_apply_ddl.py`<br>`python3 sweep.py vsam apply-ddl` | `'--skip-meta'`, `'--bg'`, `'-h, --help'` |
| **06** | `vsam_06_load_data.py`<br>`python3 sweep.py vsam load-data` | `'--mode {default,data}'`, `'--workers WORKERS'`, `'--company COMPANY'`, `'--tables TABLES'`, `'--limit-files LIMIT_FILES'`, `'--latest-only'`, `'--bg'`, `'-h, --help'` |
| **07** | `vsam_07_validate.py`<br>`python3 sweep.py vsam validate` | `'--exact-counts'`, `'--tables TABLES'`, `'--schema SCHEMA'`, `'--bg'`, `'-h, --help'` |
| **All** | `python3 sweep.py vsam run-all` | `'--workers WORKERS'`, `'--bg'`, `'-h, --help'` |

---

### Universal Framework Operations

| Command | Available Flags |
| :--- | :--- |
| `python3 sweep.py --version` | `'-v, --version'` |
| `python3 sweep.py check` | `'--s3 S3'`, `'-h, --help'` |
| `python3 sweep.py status` | `'-h, --help'` |
| `python3 sweep.py monitor` | `'-w, --watch'`, `'--interval INTERVAL'`, `'--track {all,oracle,vsam}'`, `'--no-color'`, `'-h, --help'` |
| `python3 sweep.py logs` | `'-f, --follow'`, `'-n, --lines LINES'`, `'--track {oracle,vsam}'`, `'--file FILE'`, `'-h, --help'` |
| `python3 sweep.py cancel` *(alias: `stop`)* | `'--job NAME'`, `'--kill-db'`, `'-h, --help'` |
| `python3 sweep.py run-all` | `'--track {oracle,vsam,all}'`, `'--workers WORKERS'`, `'--bg'`, `'-h, --help'` |

---

## 2. Oracle Track Detailed Flag Specifications

### Step 01: Extract Metadata
* **Script:** `oracle/oracle_01_extract_mtd.py`
* **CLI:** `python3 sweep.py oracle extract`

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle extract -h` |
| `--file FILE` | string | `inputs/*.sql` | Specific master Oracle SQL dump file path | `--file oracle/inputs/oracle_dump/master_dump.sql` |
| `--owners OWNERS`| string | all owners | Comma-separated Oracle schema owners to filter | `--owners BSS01001,BSS01104` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle extract --bg` |

---

### Step 02: S3 Inventory & Ordering
* **Script:** `oracle/oracle_02_inventory.py`
* **CLI:** `python3 sweep.py oracle inventory`

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle inventory -h` |
| `--s3 S3` | string | `ORACLE_S3_BUCKET` | S3 URI prefix to scan | `--s3 s3://my-bucket/prefix/` |
| `--owners OWNERS`| string | all owners | Comma-separated schema owners to filter | `--owners BSS01001` |
| `--tables TABLES`| string | all tables | Comma-separated table names to filter | `--tables BROKERACCOUNTDETAIL,SECURITY` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle inventory --bg` |

---

### Step 03: Pre-Flight Data Quality Gate
* **Script:** `oracle/oracle_03_type_gate.py`
* **CLI:** `python3 sweep.py oracle gate`

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle gate -h` |
| `--rows ROWS` | integer | `1000` | Number of uncompressed rows to sample per file | `--rows 2000` |
| `--tables TABLES`| string | all tables | Comma-separated table names to validate | `--tables BROKERACCOUNTDETAIL` |
| `--max-tables N` | integer | none | Limit total number of tables sampled | `--max-tables 10` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle gate --bg` |

---

### Step 04: Generate Target DDL
* **Script:** `oracle/oracle_04_generate_ddl.py`
* **CLI:** `python3 sweep.py oracle gen-ddl` *(alias: `ddl`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle gen-ddl -h` |
| `--owners OWNERS`| string | all owners | Comma-separated owners to generate DDL for | `--owners BSS01001` |
| `--tables TABLES`| string | all tables | Comma-separated tables to generate DDL for | `--tables BROKERACCOUNTDETAIL` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle gen-ddl --bg` |

---

### Step 05: Apply DDL & Provision Target Tables
* **Script:** `oracle/oracle_05_apply_ddl.py`
* **CLI:** `python3 sweep.py oracle apply-ddl`

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle apply-ddl -h` |
| `--schemas SCHEMAS`| string | all schemas | Comma-separated target schemas to apply | `--schemas bss01001,bss01104` |
| `--skip-meta` | flag | false | Skip re-executing `sweep_meta.sql` | `--skip-meta` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle apply-ddl --bg` |

---

### Step 06: Multi-Worker Streaming Bulk Load
* **Script:** `oracle/oracle_06_load_data.py`
* **CLI:** `python3 sweep.py oracle load-data` *(alias: `load`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle load-data -h` |
| `--workers WORKERS`| integer | `4` | Parallel ingestion workers | `--workers 8` |
| `--tables TABLES`| string | all tables | Comma-separated tables to load | `--tables BROKERACCOUNTDETAIL` |
| `--exclude-tables`| string | none | Comma-separated tables to exclude | `--exclude-tables AUDITLOG` |
| `--file S3_URI` | string | none | Ingest a single specific S3 file URI | `--file "s3://.../file.csv.gz"` |
| `--filename NAME`| string | none | Ingest a specific file by filename | `--filename "file.csv.gz"` |
| `--gate-pass` | flag | false | Load ONLY tables that passed Step 03 Gate | `--gate-pass` |
| `--limit-files N`| integer | all files | Limit total number of files loaded (testing) | `--limit-files 2` |
| `--max-size-mb MB`| float | none | Only load files with size <= MB | `--max-size-mb 100` |
| `--min-size-mb MB`| float | none | Only load files with size >= MB | `--min-size-mb 1000` |
| `--dry-run` | flag | false | Verify S3 streams without inserting to DB | `--dry-run` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle load-data --bg` |

---

### Step 07: Post-Load Keys, Indexes, Views & Triggers
* **Script:** `oracle/oracle_07_load_objects.py`
* **CLI:** `python3 sweep.py oracle load-obj` *(alias: `post-load`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle load-obj -h` |
| `--stage STAGE` | choice | `all` | Specific stage: `all`, `pk`, `index`, `fk`, `validate`, `views`, `triggers` | `--stage pk` |
| `--schemas SCHEMAS`| string | all schemas | Comma-separated schemas to build objects for | `--schemas bss01001` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle load-obj --bg` |

---

### Step 08: End-to-End Parity Audit
* **Script:** `oracle/oracle_08_validate.py`
* **CLI:** `python3 sweep.py oracle validate` *(alias: `verify`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle validate -h` |
| `--exact-counts` | flag | false | Execute live `COUNT(*)` queries on PostgreSQL | `--exact-counts` |
| `--tables TABLES`| string | all tables | Comma-separated tables to validate | `--tables BROKERACCOUNTDETAIL` |
| `--schemas SCHEMAS`| string | all schemas | Comma-separated schemas to validate | `--schemas bss01001,bss01104` |
| `--file FILE` | string | `inputs/*.sql` | Master Oracle SQL dump file | `--file oracle/inputs/oracle_dump/master_dump.sql` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle validate --bg` |

---

## 3. VSAM Track Detailed Flag Specifications

### Step 01: Extract Copybook Metadata
* **Script:** `vsam/vsam_01_extract_copybooks.py`
* **CLI:** `python3 sweep.py vsam extract` *(alias: `extract-copybooks`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py vsam extract -h` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py vsam extract --bg` |

---

### Step 02: S3 Discovery & Inventory Mapping
* **Script:** `vsam/vsam_02_inventory.py`
* **CLI:** `python3 sweep.py vsam inventory`

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py vsam inventory -h` |
| `--s3 S3` | string | `VSAM_S3_BUCKET`| S3 prefix to discover flat files | `--s3 s3://my-bucket/vsam/` |
| `--company COMPANY`| string | all companies | Comma-separated company tokens to filter | `--company FDIC` |
| `--latest-only` | flag | false | Keep only latest file version per table (based on timestamp) | `--latest-only` |
| `--bg` | flag | false | Run in background daemon *(CLI only)* | `python3 sweep.py vsam inventory --bg` |

---

### Step 03: Type Gate, PK Verification & Schema Column Detection
* **Script:** `vsam/vsam_03_type_gate.py`
* **CLI:** `python3 sweep.py vsam type-gate` *(alias: `gate`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py vsam type-gate -h` |
| `--workers WORKERS`| integer | `4` | Parallel profiling worker threads | `--workers 8` |
| `--company COMPANY`| string | all companies | Comma-separated company tokens to filter | `--company FDIC` |
| `--latest-only` | flag | false | Process only latest file version per table | `--latest-only` |
| `--bg` | flag | false | Run in background daemon *(CLI only)* | `python3 sweep.py vsam type-gate --bg` |

---

### Step 04: Generate Target DDL (Tables + PK in gen_ddl)
* **Script:** `vsam/vsam_04_generate_ddl.py`
* **CLI:** `python3 sweep.py vsam gen-ddl` *(alias: `ddl`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py vsam gen-ddl -h` |
| `--company COMPANY`| string | all companies | Comma-separated company filter | `--company FDIC` |
| `--execute` | flag | false | Apply table DDL immediately to PostgreSQL | `--execute` |
| `--bg` | flag | false | Run in background daemon | `python3 sweep.py vsam gen-ddl --bg` |

---

### Step 05: Apply Table DDL & Provision Target Tables
* **Script:** `vsam/vsam_05_apply_ddl.py`
* **CLI:** `python3 sweep.py vsam apply-ddl`

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py vsam apply-ddl -h` |
| `--skip-meta` | flag | false | Skip re-deploying `sweep_meta.sql` | `--skip-meta` |
| `--bg` | flag | false | Run in background daemon | `python3 sweep.py vsam apply-ddl --bg` |

---

### Step 06: Schema-Segregated Streaming Ingestion & Dynamic Upsert (Merged Step)
* **Script:** `vsam/vsam_06_load_data.py`
* **CLI:** `python3 sweep.py vsam load-data` *(alias: `load`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py vsam load-data -h` |
| `--mode MODE` | choice: `default`, `data` | `default` | Ingestion mode: `default` (apply PK to fresh tables, validate records, upsert) or `data` (no PK on fresh tables, direct load) | `--mode default` / `--mode data` |
| `--workers WORKERS`| integer | `4` | Parallel ingestion workers | `--workers 8` |
| `--company COMPANY`| string | all companies | Comma-separated company filter | `--company FDIC` |
| `--tables TABLES`| string | all tables | Comma-separated target tables to load | `--tables vsamkorg` |
| `--limit-files N`| integer | all files | Limit total number of files loaded (testing) | `--limit-files 2` |
| `--latest-only` | flag | false | Load only the latest file version per table (based on timestamp in filename) | `--latest-only` |
| `--bg` | flag | false | Run in background daemon | `python3 sweep.py vsam load-data --bg` |

---

### Step 07: End-to-End Validation & Parity Audit
* **Script:** `vsam/vsam_07_validate.py`
* **CLI:** `python3 sweep.py vsam validate` *(alias: `verify`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py vsam validate -h` |
| `--exact-counts` | flag | false | Perform exact `COUNT(*)` queries on PostgreSQL | `--exact-counts` |
| `--tables TABLES`| string | all tables | Comma-separated tables to validate | `--tables vsamkorg` |
| `--schema SCHEMA` | string | all schemas | Validate only this schema | `--schema bss01001` |
| `--bg` | flag | false | Run in background daemon | `python3 sweep.py vsam validate --bg` |

---

## 4. Universal Framework Operations: Detailed Flag Specifications

### `python3 sweep.py check`
Pre-flight host environment, AWS S3, and RDS Aurora diagnostic check.

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py check -h` |
| `--s3 S3` | string | `ORACLE_S3_BUCKET`| Custom S3 bucket URI to test connectivity against | `python3 sweep.py check --s3 s3://my-bucket/` |

---

### `python3 sweep.py monitor`
Live operational throughput and streaming ingestion dashboard.

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py monitor -h` |
| `-w, --watch` | flag | false | Continuously refresh dashboard (Ctrl+C to exit) | `python3 sweep.py monitor -w` |
| `--interval SEC` | integer | `3` | Refresh interval in seconds when `--watch` is used | `python3 sweep.py monitor -w --interval 5` |
| `--track TRACK` | choice | `all` | Filter monitor to `all`, `oracle`, or `vsam` | `python3 sweep.py monitor --track vsam -w` |
| `--no-color` | flag | false | Disable ANSI colors in dashboard | `python3 sweep.py monitor --no-color` |

---

### `python3 sweep.py logs` *(alias: `log`)*
View or stream execution logs.

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py logs -h` |
| `-f, --follow` | flag | false | Stream and follow live output | `python3 sweep.py logs -f` |
| `-n, --lines N` | integer | `40` | Number of lines to tail | `python3 sweep.py logs -n 100` |
| `--track TRACK` | choice | all | Filter logs to specific track (`oracle` or `vsam`) | `python3 sweep.py logs --track oracle -f` |
| `--file FILE` | string | latest active | Specific log file path to view | `python3 sweep.py logs --file oracle/logs/job.log -f` |

---

### `python3 sweep.py cancel` *(alias: `stop`)*
Terminate running migration jobs cleanly.

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py cancel -h` |
| `--job NAME` | string | all jobs | Specific background job name to terminate | `python3 sweep.py cancel --job vsam_06_load_data` |
| `--kill-db` | flag | false | Also terminate in-flight PostgreSQL COPY sessions | `python3 sweep.py cancel --kill-db` |

---

### `python3 sweep.py run-all`
Automated end-to-end execution across tracks.

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py run-all -h` |
| `--track TRACK` | choice | `all` | Track to execute: `oracle`, `vsam`, or `all` | `python3 sweep.py run-all --track oracle` |
| `--workers WORKERS`| integer | `4` | Parallel workers for ingestion steps | `python3 sweep.py run-all --workers 8` |
| `--bg` | flag | false | Run full pipeline as detached background daemon | `python3 sweep.py run-all --track all --bg` |

---

## 5. Environment Variables (`sweep.env`)

All parameters can be pre-configured via `sweep.env` located in the root repository folder:

| Variable | Description | Example / Default |
| :--- | :--- | :--- |
| `PGHOST` | Target Aurora PostgreSQL host address | `aurora-cluster.xyz.us-east-2.rds.amazonaws.com` |
| `PGPORT` | Target PostgreSQL port | `5432` |
| `PGDATABASE` | Target database name | `sweep` |
| `PGUSER` | Target database master user | `postgres` |
| `PGPASSWORD` | Target database master password | `secret_password` |
| `PGSSLMODE` | PostgreSQL SSL connection mode | `prefer` (or `require`) |
| `AWS_DEFAULT_REGION` | AWS Region for S3 buckets | `us-east-2` |
| `ORACLE_S3_BUCKET` | S3 URI for Oracle track flat files | `s3://your-migration-bucket/oracle-migration-qa/` |
| `VSAM_S3_BUCKET` | S3 URI for VSAM track flat files | `s3://your-migration-bucket/vsam-migration-qa/` |
| `SWEEP_WORKERS` | Default worker threads for Oracle track | `4` |
| `VSAM_WORKERS` | Default worker threads for VSAM track | `4` |
| `VSAM_CHUNK_ROWS` | Streaming batch size for VSAM load | `200000` |
