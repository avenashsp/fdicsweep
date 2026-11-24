# SWEEP Unified Data Migration Framework (v5.4.1)

## 1. Executive Summary

**SWEEP** (v5.4.1) is an enterprise-grade, high-throughput unified database migration framework built to migrate multi-terabyte financial datasets from both **Oracle SQL DDL dumps** and **VSAM flat files (COBOL copybooks)** into **AWS RDS Aurora PostgreSQL** with per-file transactional atomicity on the Oracle track, loud failures instead of silent data loss, and high ingestion velocity.

Unlike traditional migration tools that stage gigabytes or terabytes of CSV data onto local bastion disks or perform slow row-by-row ORM inserts, SWEEP uses a **zero-disk streaming pipeline** that pulls compressed data directly from Amazon S3, uncompresses it in memory via Unix pipes, and streams it into PostgreSQL with the CSV `\copy` protocol (Oracle track; the VSAM track routes rows by schema and sends them as batched `INSERT`s - see "Reliability & Safety Guarantees").

```
                               ┌────────────────────────────────────────────────────────┐
                               │                    sweep.py (Root)                     │
                               │               Single Unified Master CLI                │
                               └───────────────────────────┬────────────────────────────┘
                                                           │
                      ┌────────────────────────────────────┼────────────────────────────────────┐
                      ▼                                    ▼                                    ▼
       ┌──────────────────────────────┐     ┌──────────────────────────────┐     ┌──────────────────────────────┐
       │     python3 sweep.py oracle  │     │      python3 sweep.py vsam   │     │      Shared Operations       │
       └──────────────┬───────────────┘     └──────────────┬───────────────┘     └──────────────┬───────────────┘
                      │                                    │                                    │
                      ▼                                    ▼                                    ▼
       ┌──────────────────────────────┐     ┌──────────────────────────────┐     ┌─────────────────────────┐
       │           oracle/            │     │            vsam/             │     │  • sweep.py check       │
       │ • oracle_01_extract_mtd.py   │     │ • vsam_01_extract_copybooks. │     │  • sweep.py status      │
       │ • oracle_02_inventory.py     │     │ • vsam_02_inventory.py      │     │  • sweep.py monitor     │
       │ • oracle_03_type_gate.py     │     │ • vsam_03_type_gate.py      │     │  • sweep.py logs -f     │
       │ • oracle_04_generate_ddl.py  │     │ • vsam_04_generate_ddl.py   │     │  • sweep.py stop        │
       │ • oracle_05_apply_ddl.py     │     │ • vsam_05_apply_ddl.py      │     │  • sweep.py run-all     │
       │ • oracle_06_load_data.py     │     │ • vsam_06_load_data.py      │     └─────────────────────────┘
       │ • oracle_07_load_objects.py  │     │ • vsam_07_validate.py       │
       │ • oracle_08_validate.py      │     │                             │
       └──────────────────────────────┘     └──────────────────────────────┘
```

---

## 2. Core Architectural Principles & Innovations

### 1. Zero-Disk S3 Streaming Pipe (`sweep_stream.sh`)
- Direct Unix pipe `aws s3 cp s3://... - | gzip -dc | psql ... "\copy ..."` streams multi-gigabyte S3 chunks directly into Aurora PostgreSQL without writing a single byte to local disk or consuming Python RAM.
- Employs streaming sub-processes that decouple S3 transfer speeds from database write commit latency.

### 2. Pre-Load vs. Post-Load Object Splitting
To achieve maximum ingestion velocity, heavy database constraints and indexes are decoupled from data loading:
- **Phase A (Pre-Load)**: Target tables are provisioned in PostgreSQL with column definitions and audit columns, but *without* secondary indexes, foreign keys, views, or triggers.
- **Phase B (Streaming Load)**: Raw data streams into the unindexed tables at native Aurora storage speeds (up to 20,000+ rows/sec per worker).
- **Phase C (Post-Load)**: Secondary indexes, unique constraints, foreign keys, views, and triggers are compiled in parallel across available workers once data ingestion is complete.

### 3. Merged Ingestion Step in VSAM
In the VSAM pipeline, Step 06 (`vsam load-data`) applies primary keys itself and loads with the same staged, one-transaction-per-file workflow as the Oracle track: PK constraints are applied to the destination tables before loading, bad rows (`BAD_VALUE`, `NOT_NULL`, `PK_BLANK`, `PK_SENTINEL`, `ALL_NULL`) are rejected and reported, and valid rows are upserted via `ON CONFLICT (pk_cols) DO UPDATE ... WHERE ... IS DISTINCT FROM ...`. Only the newest file version per (company, source file) is loaded.
- **VSAM files are full snapshots.** A table **with** a primary key is upserted in place (rows missing from a newer snapshot are not deleted). A table **without** one is **replaced** by each new snapshot: all files of the table are loaded in a single transaction on one connection (temp staging, `TRUNCATE` + `INSERT`, `load_log` rows, `COMMIT`); if any file fails the table keeps its old data. Partial runs (`--company`, `--tables`, `--limit-files`) need `--force-truncate` to replace a populated table, because other source files of a shared table may be missing.
- **Keys are never guessed.** A primary key comes only from a copybook KEY/KEM group (with every key column reliably mapped) or from a person (`korg_overrides`); positional auto-discovery only produces a *candidate* that is reported, not applied. A key that cannot be applied is an error.
- **A failed VSAM file leaves no partial data** (one transaction per file); a file is reloaded only when its S3 LastModified / ETag / size changed (or with `--reload`).

### 3b. Header-to-Copybook Column Mapping (VSAM Step 02)
VSAM files always have a header row, but its names and column order differ from the copybook. Step 02 maps every S3 header to a copybook column (overrides sheet -> previous run -> exact name -> similarity + Hungarian optimal assignment) and writes the result to `vsam_02_column_map.csv` and a `ColumnMapping` sheet. **Table columns are named after the S3 headers by default (`--tbl-col headers`); datatypes come from the mapped copybook column**, and `--tbl-col copybook` names the columns after the copybook instead. A column whose data does not fit its copybook type is loaded as `text`, so a wrong match can never turn values into NULL.

### 4. Dynamic Schema Segregation & Suffix Matching
- **VSAM Schema Segregation**: Flat files mapped to multiple schemas are segregated dynamically by matching the trailing 3 digits of the schema discriminator column (`schema_col` / `korg`) to the target active schemas (e.g. `104` $\to$ `bss01104`, `112` $\to$ `bss01112`).
- **Direct `bcs` Loading Rule**: When a file has no copybook mapping and no `korg` column in the mapping sheet (and no `korg` column detected in data), it is routed 100% directly into the fallback `bcs` schema (`bcs.<table_name>`).
- **KORG Segregation Rule**: When a file has no copybook but a `korg` column is present, columns and types are inferred directly from data, key columns are auto-discovered sequentially, and records are segregated by `korg` trailing digits into active company schemas (unmatched/sentinels to `bcs`).
- **Active Schema Cross-Routing**: If a record contains an active schema identifier from Agiletics (even if not explicitly mapped to that file, e.g. FDIC file containing `110` / `bss01110`), records route directly into that active schema. Inactive schemas and invalid sentinel values route to `bcs`.

### 5. Key Candidates (never applied)
- When copybooks are missing, unique columns are searched sequentially from left to right (up to 8 columns) and **reported as candidates only** (`candidate key (not applied)`); a guessed key is never applied. A key becomes a primary key only when it comes from a copybook KEY group (every key column an ACCEPTED match) or from `korg_overrides`.
- A copybook key that cannot be applied (duplicates in the data, key columns missing from the file) is an **error**: Step 03 reports `PK_ERROR` and Step 06 does not load that source. A file without a copybook is loaded without a key (keyless).
- For the **AGILETIC** source company, column names and datatypes are inferred from S3 CSV headers and data.

### 6. Transaction Logging & Idempotency (`sweep_ctl.load_log`)
- **Both tracks work the same way:** each file is loaded in **one transaction** through temp staging tables - bulk `\copy` into an all-text temp table, set-based validation in SQL (`pg_input_is_valid`, NOT NULL, CHECK, FK, key rules), upsert, row-count check (`read = inserted + updated + unchanged + rejected + duplicates`), `load_rejects` and `load_log` rows, `COMMIT`. A truncated or corrupt stream, a count mismatch or any error rolls the whole file back and leaves no rows behind.
- **Bad rows are reported, not fatal:** they are stored in `sweep_ctl.load_rejects` and shown in the `Reject_Summary` / `Rejected_Rows` sheets. The default (`--max-rejects 0`) never fails a file for rejected rows; `--max-rejects N` fails a file that reaches N.
- **Upserts update only rows that differ:** `ON CONFLICT (key) DO UPDATE ... WHERE (target) IS DISTINCT FROM (file)`; identical rows are counted as `unchanged` and not touched.
- **A file is "already loaded" only while its S3 `LastModified`, `ETag` and size still equal the values stored in `load_log`** (`s3_last_modified`, `s3_etag`, `s3_size`). A re-delivered file with the same name and path is loaded again; `--reload` forces a reload.
- **Oracle temp storage:** big files are staged in chunks (default 2,000,000 rows, files of 256 MB+) inside the one transaction, so local temp storage holds one chunk; `--fast-bulk` loads empty tables without keys / CHECK / FK directly with no staging at all (NOT NULL and identity columns are fine).
- **Oracle load order:** parent tables first, level by level, from the foreign keys extracted in Step 01 (plus live FKs); `--fast-bulk` loads smallest to largest instead. Triggers are switched off for the load.
- **VSAM:** a `load_log` row per destination schema (keyed `<uri>#<schema>`) is written inside the file's transaction. Tables without a key are replaced by the newest snapshot: all files of the table load in a single transaction, or the table keeps its old data. Rows missing from a newer snapshot of a keyed table are never deleted.
- PostgreSQL **16 or later** is required (`pg_input_is_valid`); the loaders and `sweep.py check` verify it.
- The primary key of `sweep_ctl.load_log` is `uri`; see `docs/sweep_outputs.md` for the full column list.

### 7. Reliability & Safety Guarantees (v5.4.1)
- **Exit codes:** every step exits non-zero when it cannot do its job (missing inputs, failed loads, failed DDL, failed validation), so `run-all` and CI stop at the first failing step.
- **No shell injection:** S3 keys and file names are never interpolated into shell command lines. Python callers use argument lists; the one `\copy ... FROM PROGRAM '...'` command string is built with `shlex.quote`. Keys containing spaces, quotes, `;` or `$()` load correctly.
- **Stream errors are visible:** S3 / gzip failures surface as errors (not as "empty file"); header and sample peeks check the pipeline's exit status and stderr.
- **Timeouts and retries:** every `psql` call has a timeout (`SWEEP_PSQL_TIMEOUT`, default 2 h); file loads have `SWEEP_LOAD_TIMEOUT` (default 6 h) and are killed together with their S3/gzip child processes. Connection-establishment failures (statement never ran) are retried with backoff (`SWEEP_CONNECT_RETRIES`, default 3).
- **Credentials:** a password is no longer mandatory - `~/.pgpass`, `PGPASSFILE`, `PGSERVICE` or `SWEEP_ALLOW_NO_PASSWORD=1` (trust/peer auth) also work. Values exported in the real environment override `sweep.env`.
- **Safe job control:** background jobs run in their own process group; `stop` verifies the PID still belongs to a SWEEP supervisor (no killing of re-used PIDs), terminates the whole group, and `--kill-db` only ends sessions tagged `application_name = 'sweep'`. The job registry is updated under a file lock with atomic writes.
- **Lookup failures never guess:** a failed "is the table empty?" or "does it have a PK?" query fails the file instead of falling back to a bulk append that could create duplicates.

### 8. Dual-Track Operational Monitoring
- `python3 sweep.py monitor`: Real-time dashboard showing live throughput, in-flight streams, progress bars, committed row totals, and average speeds across Oracle and VSAM tracks simultaneously.
- `python3 sweep.py status`: Real-time process table showing background daemon state, runtime, PIDs, and finish codes.
- `python3 sweep.py check`: Pre-flight environment diagnostics validating Python modules, PostgreSQL connectivity, AWS CLI/Boto3, and input directory structures.

---

## 3. Unified Directory Structure

```
fdicsweep/
├── sweep.py                     # Root unified CLI entrypoint (v5.4.1)
├── sweep.env                    # Environment variables (RDS endpoint, credentials, S3 buckets) - git-ignored
├── sweep.env.sample             # Sample environment template
├── sweep_common.py              # Config/env loading, psql helpers (timeouts, retries), logging, Excel, type mapping
├── sweep_platform.py            # ALL OS-specific code: process groups, PID checks, file locks, atomic writes
├── sweep_loadpipe.py            # Shared by both Step 06 loaders: reject table, upsert SQL, table metadata, output markers
├── sweep_s3meta.py              # S3 LastModified / ETag / size: capture, HEAD, 'already loaded' comparison
├── sweep_streaming.py           # Safe S3 -> stdout helpers (argument lists, shlex quoting, stderr/exit checks)
├── sweep_stream.sh              # Zero-disk S3-to-PostgreSQL streaming Unix pipe
├── sweep_bg.py                  # Facade + background supervisor entry point (`_runner`)
├── sweep_jobs.py                # Job registry (locked, atomic), start / status / logs / stop
├── sweep_metrics.py             # Throughput & progress metrics for the monitor
├── sweep_dashboard.py           # Terminal rendering of the live dashboard
├── sweep_format.py              # Duration / number / size formatters
├── pyproject.toml               # Packaging (`pip install -e .`), pytest config
├── requirements.txt             # Runtime dependencies (requirements-dev.txt adds pytest, pyflakes)
│
├── oracle/                      # ORACLE MIGRATION TRACK (Steps 01-08)
│   ├── oracle_01_extract_mtd.py # Step 01: Extract metadata from Oracle dump SQL
│   ├── oracle_02_inventory.py   # Step 02: Scan S3, match tables, build small-to-large queue
│   ├── oracle_03_type_gate.py   # Step 03: Profile S3 CSVs against Oracle DDL types
│   ├── oracle_04_generate_ddl.py# Step 04: Generate PostgreSQL DDL
│   ├── oracle_05_apply_ddl.py   # Step 05: Provision target schemas and bare tables
│   ├── oracle_06_load_data.py   # Step 06: Multi-worker S3 streaming ingestion (one transaction per file)
│   ├── oracle_06_pipeline.py    #   - SQL pipeline: staging, validation, upsert, reconciliation
│   ├── oracle_06_order.py       #   - parent-first load levels from foreign keys
│   ├── oracle_07_load_objects.py# Step 07: Compile post-load PKs, indexes, FKs, views, triggers
│   ├── oracle_07_parser.py      #   - splits generated post-load SQL into executable object blocks
│   ├── oracle_08_validate.py    # Step 08: Parity audit orchestration (returns pass/fail)
│   ├── oracle_08_catalog.py     #   - PostgreSQL catalog queries (one function per catalog)
│   ├── oracle_08_audit.py       #   - comparison levels: schemas, tables, columns, constraints, indexes, ...
│   ├── inputs/                  # Oracle DDL SQL dumps, sweep_meta.sql
│   ├── outputs/                 # CSV outputs, generated SQL scripts (git-ignored)
│   ├── reports/                 # Formatted Excel audit reports (git-ignored)
│   └── logs/                    # Timestamped execution logs (git-ignored)
│
├── vsam/                        # VSAM MIGRATION TRACK (Steps 01-07)
│   ├── vsam_01_extract_copybooks.py # Step 01: Parse COBOL copybooks (*.txt)
│   ├── vsam_02_inventory.py     # Step 02: Discover S3 files, map copybooks & schemas, map headers to copybook columns
│   ├── vsam_03_type_gate.py     # Step 03: Verify PK uniqueness & extract schema_col (korg)
│   ├── vsam_04_generate_ddl.py  # Step 04: Generate PostgreSQL DDL in gen_ddl/
│   ├── vsam_05_apply_ddl.py     # Step 05: Provision target schemas and bare tables
│   ├── vsam_06_load_data.py     # Step 06: Schema-segregated staged ingestion & upsert (one transaction per file)
│   ├── vsam_06_pipeline.py      #   - SQL pipeline: staging, routing, validation, upsert / snapshot replace
│   ├── vsam_07_validate.py      # Step 07: Post-migration catalog & parity validation
│   ├── vsam_common.py           # Compatibility facade re-exporting the modules below
│   ├── common_cobol.py          #   copybook text -> field tree (sequence areas, :TAG: names, multi-line statements)
│   ├── common_colmap.py         #   header -> copybook column mapping (Hungarian), overrides, --tbl-col, effective columns
│   ├── common_config.py         #   paths, settings, folder discovery, Excel reads
│   ├── common_mapping.py        #   master mapping / override workbooks, schema routing
│   ├── common_records.py        #   record validation, key discovery, schema-column detection
│   ├── common_copybook.py       #   copybook parsing, SQL types, profile merging
│   ├── common_schema.py         #   target DDL and live PostgreSQL inspection
│   ├── common_files.py          #   S3/local file access, delimiters, CSV/JSON helpers
│   ├── inputs/                  # vsam_master_mapping.xlsx, overrides, copybooks/
│   ├── outputs/                 # CSV outputs, gen_ddl/ consolidated SQL scripts (git-ignored)
│   ├── reports/                 # Formatted Excel audit reports (git-ignored)
│   └── logs/                    # Timestamped execution logs (git-ignored)
│
├── tests/                       # pytest suite (unit + PostgreSQL integration tests)
├── .github/workflows/ci.yml     # CI: compile, pyflakes, pytest against PostgreSQL 16
├── docs/                        # DOCUMENTATION (v5.4.1)
│   ├── project_overview.md      # Architecture, executive summary, design principles
│   ├── commands_oracle.md       # Complete CLI command reference for Oracle track
│   ├── commands_vsam.md         # Complete CLI command reference for VSAM track
│   ├── flag_arguments.md        # Comprehensive CLI flags & environment variables reference
│   └── sweep_outputs.md         # Operational monitoring sample outputs & Excel structures
└── CHANGELOG_v5.4.1.md                 # Release changelog and version history (newest first)
```

> **How steps are launched.** Steps are modules run from the repository root: `python3 -m oracle.oracle_06_load_data ...`
> (`python3 sweep.py oracle load-data ...` does exactly this for you). Running `python3 oracle/oracle_06_load_data.py`
> directly is no longer supported because imports are package-based; alternatively install once with `pip install -e .`.

---

## 4. Pipeline Execution Summary

### Oracle Pipeline Workflow
| Step | Command | Focus Area | Output Artifacts |
| :---: | :--- | :--- | :--- |
| **01** | `python3 sweep.py oracle extract` | Parse Oracle DDL SQL | Table, column, type, PK, index metadata CSVs |
| **02** | `python3 sweep.py oracle inventory` | S3 discovery & queue | `oracle_02_inventory.csv`, `load_matrix.csv` |
| **03** | `python3 sweep.py oracle gate` | Pre-flight quality gate | `oracle_03_type_gate.json`, Excel report |
| **04** | `python3 sweep.py oracle gen-ddl` | Generate PostgreSQL DDL | `pre_load/<schema>_tables.sql`, `post_load/*.sql` |
| **05** | `python3 sweep.py oracle apply-ddl`| Provision DB schemas | Target schemas and bare tables in PostgreSQL |
| **06** | `python3 sweep.py oracle load-data` | Multi-worker streaming | Data committed via `\copy`; `sweep_ctl.load_log` |
| **07** | `python3 sweep.py oracle load-obj`  | Compile DB objects | PKs, indexes, FKs, views, triggers compiled |
| **08** | `python3 sweep.py oracle validate`  | Parity reconciliation | Multi-tab reconciliation audit Excel report |

### VSAM Pipeline Workflow
| Step | Command | Focus Area | Output Artifacts |
| :---: | :--- | :--- | :--- |
| **01** | `python3 sweep.py vsam extract` | Parse COBOL copybooks | `vsam_01_copybook_extracts.csv`, Excel report |
| **02** | `python3 sweep.py vsam inventory` | S3 discovery, mapping, **column mapping** | `vsam_02_inventory.csv`, `load_matrix.csv`, `vsam_02_column_map.csv`, Excel report incl. `ColumnMapping` |
| **03** | `python3 sweep.py vsam type-gate` | PK & KORG validation | `vsam_03_type_gate.json`, 11-column Excel report |
| **04** | `python3 sweep.py vsam gen-ddl` | Consolidated DDL | `gen_ddl/all_tables.sql`, `all_pk.sql`, per-schema SQL |
| **05** | `python3 sweep.py vsam apply-ddl`| Provision DB schemas | Schemas & bare tables in PostgreSQL |
| **06** | `python3 sweep.py vsam load-data` | Staged load & upsert| Data committed; `LoadSummary`, `SchemaBreakdown`, `Reject_Summary`, `Rejected_Rows` |
| **07** | `python3 sweep.py vsam validate`  | Parity reconciliation | Multi-tab reconciliation audit Excel report |
