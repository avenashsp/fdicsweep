# SWEEP Migration Framework - Complete CLI Flags Reference (v5.4.0)

Complete flags reference for all CLI commands and python scripts in the **SWEEP Unified Data Migration Framework** across both **Oracle** and **VSAM** tracks.

Steps can be invoked in two ways:
1. **Via Master CLI**: `python3 sweep.py <track> <command> [flags]` *(Recommended: includes `--bg` daemon management, logging, and unified environment configuration)*
2. **As standalone modules** from the repository root: `python3 -m <track>.<script> [flags]` (for example `python3 -m oracle.oracle_06_load_data --workers 4`). The `--bg` flag is only honoured by `sweep.py`. Running the file path directly (`python3 oracle/oracle_06_load_data.py`) is not supported because the steps use package imports; alternatively run `pip install -e .` once.

Every step exits with **status 0 on success and non-zero on failure**, so shell scripts, CI and `run-all` can rely on the exit code.

---

## 1. Quick Flag Reference Matrix

### Oracle Migration Track (8 Steps)

| Step | Script / Command | Available Flags |
| :---: | :--- | :--- |
| **01** | `oracle.oracle_01_extract_mtd`<br>`python3 sweep.py oracle extract` | `'--file FILE'`, `'--owners OWNERS'`, `'--bg'`, `'-h, --help'` |
| **02** | `oracle.oracle_02_inventory`<br>`python3 sweep.py oracle inventory` | `'--s3 S3'`, `'--owners OWNERS'`, `'--tables TABLES'`, `'--bg'`, `'-h, --help'` |
| **03** | `oracle.oracle_03_type_gate`<br>`python3 sweep.py oracle gate` | `'--rows ROWS'`, `'--tables TABLES'`, `'--max-tables MAX_TABLES'`, `'--bg'`, `'-h, --help'` |
| **04** | `oracle.oracle_04_generate_ddl`<br>`python3 sweep.py oracle gen-ddl` | `'--owners OWNERS'`, `'--tables TABLES'`, `'--bg'`, `'-h, --help'` |
| **05** | `oracle.oracle_05_apply_ddl`<br>`python3 sweep.py oracle apply-ddl` | `'--schemas SCHEMAS'`, `'--skip-meta'`, `'--bg'`, `'-h, --help'` |
| **06** | `oracle.oracle_06_load_data`<br>`python3 sweep.py oracle load-data` | `'--workers WORKERS'`, `'--tables TABLES'`, `'--exclude-tables EXCLUDE_TABLES'`, `'--file FILE'`, `'--filename FILENAME'`, `'--gate-pass'`, `'--limit-files LIMIT_FILES'`, `'--max-size-mb MAX_SIZE_MB'`, `'--min-size-mb MIN_SIZE_MB'`, `'--dry-run'`, `'--reload'`, `'--max-rejects MAX_REJECTS'`, `'--fast-bulk'`, `'--trigger-mode TRIGGER_MODE'`, `'--bg'`, `'-h, --help'` |
| **07** | `oracle.oracle_07_load_objects`<br>`python3 sweep.py oracle load-obj` | `'--stage {all,pk,alter,index,fk,validate,views,triggers}'`, `'--schemas SCHEMAS'`, `'--only-loaded'`, `'--bg'`, `'-h, --help'` |
| **08** | `oracle.oracle_08_validate`<br>`python3 sweep.py oracle validate` | `'--exact-counts'`, `'--tables TABLES'`, `'--schemas SCHEMAS'`, `'--file FILE'`, `'--bg'`, `'-h, --help'` |
| **All** | `python3 sweep.py oracle run-all` | `'--workers WORKERS'`, `'--bg'`, `'-h, --help'` |

---

### VSAM Migration Track (7 Steps)

| Step | Script / Command | Available Flags |
| :---: | :--- | :--- |
| **01** | `vsam.vsam_01_extract_copybooks`<br>`python3 sweep.py vsam extract` | `'--bg'`, `'-h, --help'` |
| **02** | `vsam.vsam_02_inventory`<br>`python3 sweep.py vsam inventory` | `'--s3 S3'`, `'--company COMPANY'`, `'--tbl-col {headers,copybook}'`, `'--bg'`, `'-h, --help'` |
| **03** | `vsam.vsam_03_type_gate`<br>`python3 sweep.py vsam type-gate` | `'--workers WORKERS'`, `'--company COMPANY'`, `'--bg'`, `'-h, --help'` |
| **04** | `vsam.vsam_04_generate_ddl`<br>`python3 sweep.py vsam gen-ddl` | `'--company COMPANY'`, `'--execute'`, `'--bg'`, `'-h, --help'` |
| **05** | `vsam.vsam_05_apply_ddl`<br>`python3 sweep.py vsam apply-ddl` | `'--skip-meta'`, `'--bg'`, `'-h, --help'` |
| **06** | `vsam.vsam_06_load_data`<br>`python3 sweep.py vsam load-data` | `'--workers WORKERS'`, `'--company COMPANY'`, `'--tables TABLES'`, `'--limit-files LIMIT_FILES'`, `'--force-truncate'`, `'--reload'`, `'--max-rejects MAX_REJECTS'`, `'--dry-run'`, `'--bg'`, `'-h, --help'` |
| **07** | `vsam.vsam_07_validate`<br>`python3 sweep.py vsam validate` | `'--exact-counts'`, `'--tables TABLES'`, `'--schema SCHEMA'`, `'--bg'`, `'-h, --help'` |
| **All** | `python3 sweep.py vsam run-all` | `'--workers WORKERS'`, `'--force-truncate'`, `'--tbl-col {headers,copybook}'`, `'--bg'`, `'-h, --help'` |

---

### Universal Framework Operations

| Command | Available Flags |
| :--- | :--- |
| `python3 sweep.py --version` | `'-v, --version'` |
| `python3 sweep.py check` | `'--s3 S3'`, `'-h, --help'` |
| `python3 sweep.py status` | `'-h, --help'` |
| `python3 sweep.py monitor` | `'-w, --watch'`, `'--interval INTERVAL'`, `'--track {all,oracle,vsam}'`, `'--no-color'`, `'-h, --help'` |
| `python3 sweep.py logs` | `'-f, --follow'`, `'-n, --lines LINES'`, `'--track {oracle,vsam}'`, `'--file FILE'`, `'-h, --help'` |
| `python3 sweep.py stop` | `'--name NAME'`, `'--kill-db'`, `'-h, --help'` |
| `python3 sweep.py run-all` | `'--track {oracle,vsam,all}'`, `'--workers WORKERS'`, `'--force-truncate'`, `'--tbl-col {headers,copybook}'`, `'--bg'`, `'-h, --help'` |

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
| `--dry-run` | flag | false | List the files that would load without inserting to DB | `--dry-run` |
| `--reload` | flag | false | Load files again even when their S3 LastModified / ETag / size are unchanged since the last load (upsert on the key, append when there is none) | `--reload` |
| `--max-rejects` | int | 0 | Oracle Step 06: fail and roll back a file when this many rows are rejected (NOT NULL / CHECK / bad value / FK / alternate-key conflict). `0` (default) = rejected rows never fail a file | `--max-rejects 50` |
| `--fast-bulk` | flag | false | Oracle Step 06: direct COPY into an empty table without constraints (no bad-row isolation); also loads smallest to largest instead of parent tables first | `--fast-bulk` |
| `--trigger-mode` | choice | auto | Oracle Step 06: how triggers are switched off while loading: `replica` (session_replication_role, no lock), `disable` (ALTER TABLE DISABLE TRIGGER), `auto` | `--trigger-mode disable` |
| `--bg` | flag | false | Run as detached background daemon *(CLI only)* | `python3 sweep.py oracle load-data --bg` |

---

### Step 07: Post-Load Keys, Indexes, Views & Triggers
* **Script:** `oracle/oracle_07_load_objects.py`
* **CLI:** `python3 sweep.py oracle load-obj` *(alias: `post-load`)*

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py oracle load-obj -h` |
| `--stage STAGE` | choice | `all` | Specific stage: `all`, `pk`, `alter`, `index`, `fk`, `validate`, `views`, `triggers` | `--stage pk` |
| `--schemas SCHEMAS`| string | all schemas | Comma-separated schemas to build objects for | `--schemas bss01001` |
| `--only-loaded` | flag | false | Only build objects for tables that have loaded data | `--only-loaded` |
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
| `--tbl-col MODE` | choice: `headers`, `copybook` | `headers` (or `VSAM_TBL_COL`) | Table column names: the S3 file headers, or the mapped copybook column names. Datatypes always come from the mapped copybook column. Stored in `vsam_02_column_map.csv` for Steps 03-07 | `--tbl-col copybook` |
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
| `--workers WORKERS`| integer | `4` | Parallel ingestion workers | `--workers 8` |
| `--company COMPANY`| string | all companies | Comma-separated company filter | `--company FDIC` |
| `--tables TABLES`| string | all tables | Comma-separated target tables to load | `--tables vsamkorg` |
| `--limit-files N`| integer | all files | Limit total number of files loaded (testing) | `--limit-files 2` |
| `--force-truncate` | flag | false | Tables without a PK are replaced by the newest snapshot automatically in a full run. In a PARTIAL run (`--company`, `--tables`, `--limit-files`) replacing a table that already has rows needs this flag, because other source files of the table may be missing | `--force-truncate` |
| `--reload` | flag | false | VSAM Step 06: load files again even when their S3 LastModified / ETag / size are unchanged | `--reload` |
| `--max-rejects` | int | 0 | VSAM Step 06: fail and roll back a file when this many rows are rejected (`BAD_VALUE`, `NOT_NULL`, `PK_BLANK`, `PK_SENTINEL`, `ALL_NULL`). `0` (default) = rejected rows never fail a file | `--max-rejects 25` |
| `--dry-run` | flag | false | List the files that would be loaded; no database changes | `--dry-run` |
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
Pre-flight host environment, AWS S3, and RDS Aurora diagnostic check. Exits non-zero when `psql` is missing, credentials are absent, or the database connection fails.

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

### `python3 sweep.py stop`
Terminate running background migration jobs cleanly: each registered PID is verified to be a SWEEP supervisor (a stale/re-used PID is never signalled), then the supervisor's whole process group (psql, aws, gzip children included) is terminated.

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py stop -h` |
| `--name NAME` | string | all jobs | Specific background job name to terminate | `python3 sweep.py stop --name vsam_06_load_data` |
| `--kill-db` | flag | false | Also terminate in-flight PostgreSQL sessions opened by SWEEP (`application_name = 'sweep'`); other users' sessions are never touched | `python3 sweep.py stop --kill-db` |

---

### `python3 sweep.py run-all`
Automated end-to-end execution across tracks.

| Flag | Type | Default | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `-h, --help` | flag | - | Show help message and exit | `python3 sweep.py run-all -h` |
| `--track TRACK` | choice | `all` | Track to execute: `oracle`, `vsam`, or `all` | `python3 sweep.py run-all --track oracle` |
| `--workers WORKERS`| integer | `4` | Parallel workers for ingestion steps | `python3 sweep.py run-all --workers 8` |
| `--force-truncate` | flag | false | Forwarded to the VSAM load step (see Step 06) | `python3 sweep.py run-all --force-truncate` |
| `--tbl-col MODE` | choice | `headers` | Forwarded to VSAM Step 02 | `python3 sweep.py run-all --tbl-col copybook` |
| `--bg` | flag | false | Run full pipeline as one detached background job (stops at the first failing step) | `python3 sweep.py run-all --track all --bg` |

---

## 5. Environment Variables (`sweep.env`)

All parameters can be pre-configured via `sweep.env` located in the root repository folder (copy `sweep.env.sample`).
**Variables already exported in your shell win over `sweep.env`**, so `VAR=value python3 sweep.py ...` always works.
`sweep.env` is git-ignored; never commit it.

| Variable | Description | Example / Default |
| :--- | :--- | :--- |
| `PGHOST` | Target Aurora PostgreSQL host address | `aurora-cluster.xyz.us-east-2.rds.amazonaws.com` |
| `PGPORT` | Target PostgreSQL port | `5432` |
| `PGDATABASE` | Target database name | `sweep` |
| `PGUSER` | Target database master user | `postgres` |
| `PGPASSWORD` | Target database password. Optional if `~/.pgpass`, `PGPASSFILE` or `PGSERVICE` is configured | `secret_password` |
| `PGSSLMODE` | PostgreSQL SSL connection mode | `prefer` (or `require`) |
| `SWEEP_ALLOW_NO_PASSWORD` | Set to `1` for trust / peer authentication (no password, no `.pgpass`) | unset |
| `SWEEP_PSQL_TIMEOUT` | Timeout in seconds for every `psql` call (queries, DDL files) | `7200` |
| `SWEEP_LOAD_TIMEOUT` | Timeout in seconds for one file load (`\copy` pipeline); `0` disables. The load and its S3/gzip children are killed on expiry | `21600` |
| `SWEEP_HEAD_THREADS` | Parallel S3 `HEAD` requests used to compare LastModified / ETag / size of already-loaded files before a load | `16` |
| `SWEEP_CONNECT_RETRIES` | Retries (exponential backoff) for connection-establishment failures only; statements that actually ran are never retried | `3` |
| `AWS_DEFAULT_REGION` | AWS Region for S3 buckets | `us-east-2` |
| `ORACLE_S3_BUCKET` | S3 URI for Oracle track flat files | `s3://your-migration-bucket/oracle-migration-qa/` |
| `VSAM_S3_BUCKET` | S3 URI for VSAM track flat files | `s3://your-migration-bucket/vsam-migration-qa/` |
| `ORACLE_WORKERS` / `SWEEP_WORKERS` / `VSAM_WORKERS` | Default worker threads (first one set wins, in that order) | `4` |
| `VSAM_DELIM` / `VSAM_QUOTE` / `VSAM_ENCODING` | Delimiter / quoting / encoding handling for VSAM files | `auto` |
| `VSAM_TBL_COL` | Default for `--tbl-col`: `headers` (S3 file headers) or `copybook` (copybook column names) | `headers` |
| `VSAM_MAP_ACCEPT` / `VSAM_MAP_MIN` | Column-mapping thresholds: scores >= ACCEPT are ACCEPTED, >= MIN are REVIEW, below MIN UNMAPPED | `0.70` / `0.35` |
| `VSAM_COMMON_SCHEMA_CASE` | Case of the shared fallback schema name | `lower` |
| `VSAM_FOLDERS` | Comma-separated S3/local folders to scan (overrides discovery) | unset |
| `VSAM_BASE` | Local directory scanned for VSAM files when not using S3 | repository root |
| `VSAM_COPYBOOK_DIR` | Directory holding the COBOL copybooks | `vsam/inputs/copybooks` |
| `VSAM_MASTER_MAPPING` / `VSAM_MASTER_OVERRIDES` | Paths of the mapping / override workbooks | `vsam/inputs/vsam_master_mapping.xlsx` / `vsam_master_overrides.xlsx` |
| `VSAM_ANALYZE_MAX_ROWS` | Cap on rows read per file when profiling (`0` = all) | `0` |
| `PSQL` | `psql` executable to use | `psql` |
| `NO_COLOR` | Disable ANSI colours in the dashboard | unset |

> `sweep.env.sample` also lists `VSAM_CHUNK_ROWS` and `VSAM_ENABLE_FINTEC`; the current code does not read them, so they have no effect.
