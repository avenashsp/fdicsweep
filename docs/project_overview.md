# SWEEP Unified Data Migration Framework (v5.0)

## 1. Executive Summary

**SWEEP** (v5.0) is an enterprise-grade, high-throughput unified database migration framework built to migrate multi-terabyte financial datasets from both **Oracle SQL DDL dumps** and **VSAM flat files (COBOL copybooks)** into **AWS RDS Aurora PostgreSQL** with zero data loss, guaranteed transactional atomicity, and maximum ingestion velocity.

Unlike traditional migration tools that stage gigabytes or terabytes of CSV data onto local bastion disks or perform slow row-by-row ORM inserts, SWEEP uses a **zero-disk streaming pipeline** that pulls compressed data directly from Amazon S3, uncompresses it in memory via Unix pipes, and streams it into PostgreSQL using binary-optimized `\copy` protocol.

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
       │ • oracle_04_generate_ddl.py  │     │ • vsam_04_generate_ddl.py   │     │  • sweep.py stop/cancel │
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

### 3. Merged Ingestion Step with `--mode` in VSAM
In the VSAM pipeline, Step 06 (`vsam load-data`) incorporates primary key constraint management directly into streaming ingestion:
- **`--mode default` (Quality & Upsert Mode)**: Applies PK constraints to fresh tables before loading data. Every incoming record is validated against PK constraints (`record_passes_pk_validation`). Records passing validation are upserted dynamically via `ON CONFLICT (pk_cols) DO UPDATE SET ...`. Tables with existing data but no PK constraints are safely truncated in the destination mapped schema and loaded.
- **`--mode data` (Data Priority Mode)**: Fresh tables are loaded bare without PK constraints. If an existing table already has PK constraints, it upserts via the PK; otherwise, it truncates the destination schema and loads bare data.

### 4. Dynamic Schema Segregation & Suffix Matching
- **VSAM Schema Segregation**: Flat files mapped to multiple schemas are segregated dynamically by matching the trailing 3 digits of the schema discriminator column (`schema_col` / `korg`) to the target active schemas (e.g. `104` $\to$ `bss01104`, `112` $\to$ `bss01112`).
- **Direct `bcs` Loading Rule**: When a file has no copybook mapping and no `korg` column in the mapping sheet (and no `korg` column detected in data), it is routed 100% directly into the fallback `bcs` schema (`bcs.<table_name>`).
- **KORG Segregation Rule**: When a file has no copybook but a `korg` column is present, columns and types are inferred directly from data, key columns are auto-discovered sequentially, and records are segregated by `korg` trailing digits into active company schemas (unmatched/sentinels to `bcs`).
- **Active Schema Cross-Routing**: If a record contains an active schema identifier from Agiletics (even if not explicitly mapped to that file, e.g. FDIC file containing `110` / `bss01110`), records route directly into that active schema. Inactive schemas and invalid sentinel values route to `bcs`.

### 5. Positional Sequential PK Auto-Discovery
- When copybooks are missing, composite primary keys are discovered dynamically by scanning actual data sequentially from left to right ($c_1$, $c_1+c_2$, ..., up to 8 columns) to establish strict uniqueness with zero NULLs or blank values.
- For the **AGILETIC** source company, column names and datatypes are directly inferred from S3 CSV headers and data, extracting key columns from mapped copybooks or positional auto-discovery if copybooks are absent.

### 6. Atomic Transaction Logging & Idempotency (`sweep_ctl.load_log`)
- Every file ingestion records an atomic commit entry into `sweep_ctl.load_log` recording S3 URI, target schema, target table, byte size, loaded row count, seconds elapsed, and timestamp.
- On rerun or restart, files marked `SUCCESS` in `sweep_ctl.load_log` are automatically skipped, enabling instant resumption without data duplication.

### 7. Dual-Track Operational Monitoring
- `python3 sweep.py monitor`: Real-time dashboard showing live throughput, in-flight streams, progress bars, committed row totals, and average speeds across Oracle and VSAM tracks simultaneously.
- `python3 sweep.py status`: Real-time process table showing background daemon state, runtime, PIDs, and finish codes.
- `python3 sweep.py check`: Pre-flight environment diagnostics validating Python modules, PostgreSQL connectivity, AWS CLI/Boto3, and input directory structures.

---

## 3. Unified Directory Structure

```
fdicsweep/
├── sweep.py                     # Root Unified CLI entrypoint (v5.0)
├── sweep.env                    # Environment variables (RDS endpoint, credentials, S3 buckets)
├── sweep.env.sample             # Sample environment template
├── sweep_common.py              # Core shared utilities, Excel styling, DB helpers
├── sweep_stream.sh              # Zero-disk S3-to-PostgreSQL streaming Unix pipe
│
├── oracle/                      # ORACLE MIGRATION TRACK (Steps 01-08)
│   ├── oracle_common.py         # Shared Oracle regex parsers, DDL builders, type mappers
│   ├── oracle_01_extract_mtd.py # Step 01: Extract metadata from Oracle dump SQL
│   ├── oracle_02_inventory.py   # Step 02: Scan S3, match tables, build small-to-large queue
│   ├── oracle_03_type_gate.py   # Step 03: Profile S3 CSVs against Oracle DDL types
│   ├── oracle_04_generate_ddl.py# Step 04: Generate PostgreSQL DDL in gen_ddl/
│   ├── oracle_05_apply_ddl.py   # Step 05: Provision target schemas and bare tables
│   ├── oracle_06_load_data.py   # Step 06: Multi-worker S3 streaming ingestion
│   ├── oracle_07_load_objects.py# Step 07: Compile post-load PKs, indexes, views, triggers
│   ├── oracle_08_validate.py    # Step 08: End-to-end parity reconciliation audit
│   ├── inputs/                  # Oracle DDL SQL dumps, sweep_meta.sql
│   ├── outputs/                 # CSV outputs, gen_ddl/ SQL scripts
│   ├── reports/                 # Formatted Excel audit reports
│   └── logs/                    # Timestamped execution logs
│
├── vsam/                        # VSAM MIGRATION TRACK (Steps 01-07)
│   ├── vsam_common.py           # Shared COBOL copybook parser, dynamic korg mappings
│   ├── vsam_01_extract_copybooks.py # Step 01: Parse COBOL copybooks (*.txt)
│   ├── vsam_02_inventory.py     # Step 02: Discover S3 files, map copybooks & schemas
│   ├── vsam_03_type_gate.py     # Step 03: Verify PK uniqueness & extract schema_col (korg)
│   ├── vsam_04_generate_ddl.py  # Step 04: Generate PostgreSQL DDL in gen_ddl/
│   ├── vsam_05_apply_ddl.py     # Step 05: Provision target schemas and bare tables
│   ├── vsam_06_load_data.py     # Step 06: Schema-segregated streaming ingestion & upsert (--mode)
│   ├── vsam_07_validate.py      # Step 07: Post-migration catalog & parity validation
│   ├── inputs/                  # vsam_master_mapping.xlsx, overrides, copybooks/
│   ├── outputs/                 # CSV outputs, gen_ddl/ consolidated SQL scripts
│   ├── reports/                 # Formatted Excel audit reports
│   └── logs/                    # Timestamped execution logs
│
├── docs/                        # DOCUMENTATION (v5.0)
│   ├── project_overview.md      # Architecture, executive summary, design principles
│   ├── commands_oracle.md       # Complete CLI command reference for Oracle track
│   ├── commands_vsam.md         # Complete CLI command reference for VSAM track
│   ├── flag_arguments.md        # Comprehensive CLI flags & environment variables reference
│   └── sweep_outputs.md         # Operational monitoring sample outputs & Excel structures
│
├── package/                     # Offline deployment artifacts
│   └── fdicsweep_package_v5.0.docx # Enterprise distribution package
└── CHANGELOG_v5.0.md            # Release changelog and version history
```

---

## 4. Pipeline Execution Summary

### Oracle Pipeline Workflow
| Step | Command | Focus Area | Output Artifacts |
| :---: | :--- | :--- | :--- |
| **01** | `python3 sweep.py oracle extract` | Parse Oracle DDL SQL | Table, column, type, PK, index metadata CSVs |
| **02** | `python3 sweep.py oracle inventory` | S3 discovery & queue | `oracle_02_inventory.csv`, `load_matrix.csv` |
| **03** | `python3 sweep.py oracle gate` | Pre-flight quality gate | `oracle_03_type_gate.json`, Excel report |
| **04** | `python3 sweep.py oracle gen-ddl` | Generate PostgreSQL DDL | `gen_ddl/all_tables.sql`, `all_post_load.sql` |
| **05** | `python3 sweep.py oracle apply-ddl`| Provision DB schemas | Target schemas and bare tables in PostgreSQL |
| **06** | `python3 sweep.py oracle load-data` | Multi-worker streaming | Data committed via `\copy`; `sweep_ctl.load_log` |
| **07** | `python3 sweep.py oracle load-obj`  | Compile DB objects | PKs, indexes, FKs, views, triggers compiled |
| **08** | `python3 sweep.py oracle validate`  | Parity reconciliation | Multi-tab reconciliation audit Excel report |

### VSAM Pipeline Workflow
| Step | Command | Focus Area | Output Artifacts |
| :---: | :--- | :--- | :--- |
| **01** | `python3 sweep.py vsam extract` | Parse COBOL copybooks | `vsam_01_copybook_extracts.csv`, Excel report |
| **02** | `python3 sweep.py vsam inventory` | S3 discovery & mapping | `vsam_02_inventory.csv`, `load_matrix.csv`, Excel report |
| **03** | `python3 sweep.py vsam type-gate` | PK & KORG validation | `vsam_03_type_gate.json`, 11-column Excel report |
| **04** | `python3 sweep.py vsam gen-ddl` | Consolidated DDL | `gen_ddl/all_tables.sql`, `all_pk.sql`, per-schema SQL |
| **05** | `python3 sweep.py vsam apply-ddl`| Provision DB schemas | Schemas & bare tables in PostgreSQL |
| **06** | `python3 sweep.py vsam load-data` | Stream & upsert (`--mode`)| Data committed; 17-col `LoadSummary` & `SchemaBreakdown` |
| **07** | `python3 sweep.py vsam validate`  | Parity reconciliation | Multi-tab reconciliation audit Excel report |
