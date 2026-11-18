# Changelog

All notable changes to the **SWEEP Unified Data Migration Framework** (Oracle & VSAM to AWS RDS Aurora PostgreSQL) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [5.0.0] - 2026-11-17

### Added
- **Direct `bcs` Loading Rule (No Copybook & No KORG)**:
  - For any file where there is no copybook mapping and no `korg` mapping in `vsam_master_mapping.xlsx` (and no `korg` column detected in the file header), records stream 100% directly into the fallback `bcs` schema (`bcs.<table_name>`).
  - Target table DDL is generated in the `bcs` schema.
- **KORG Segregation Rule (No Copybook BUT KORG Present)**:
  - When a file has no copybook but a `korg` column is present (either specified in `copybook_mapping` or present in the file header e.g. `b_korg` or ending in `_korg`):
    - Columns and datatypes are inferred directly from the file data.
    - Key columns are auto-discovered sequentially in positional order.
    - Records are segregated by `korg` values: matching trailing 3 digits to active schemas (e.g. `104` -> `bss01104`, `112` -> `bss01112`), with unmatched or sentinel values routed directly into `bcs`.
- **Standardized `NO_COPYBOOK` and `NO_KORG` Reporting**:
  - Whenever copybook mapping or `korg` mapping is blank, null, or empty in the mapping sheet, Excel reports explicitly display `NO_COPYBOOK` and `NO_KORG`:
    - **Step 02 Inventory Report**: `copybook map` shows `NO_COPYBOOK`, and `korg mapping` shows `NO_KORG`.
    - **Step 03 Type Gate Report**: Primary `TypeGateSummary` 11-column sheet displays `copybook = NO_COPYBOOK` and `schema_col = NO_KORG` (with `has schema_col? = NO`).
    - **Step 04 DDL Report**: `copybook = NO_COPYBOOK`, `schema_col = NO_KORG`, with details annotated.
    - **Step 06 Load Data Report**: Explicit `copybook` and `korg` columns display `NO_COPYBOOK` and `NO_KORG`.
- **VSAM Data Load Excel Report Enhancements (`vsam_06_load_data`)**:
  - **`LoadSummary` Sheet (Standardized 17-Column Layout)**:
    - `s3 filename`, `tablename`, `copybook`, `korg column`, `total recs`, `loaded recs`, `skipped recs`
    - `records loaded in mapped schema`: Sum of records loaded into target mapped schemas.
    - `records loaded in unmapped schema`: Sum of records routed into other active schemas via cross-routing.
    - `records loaded in bcs schema`: Sum of records routed into fallback `bcs` schema.
    - `load %`: Percentage of records loaded in mapped schema relative to total file records (e.g. `75%`).
    - `has PK?`, `PK columns`, `skipped/failed reason`, `duration`, `status`, `full s3 uri`
  - **`SchemaBreakdown` Sheet (Intersection Pivot Matrix)**:
    - Rows list `s3 filename` and `tablename`.
    - Columns list each destination schema name (`bss01104`, `bss01105`, ..., `bcs`) and `Total`.
    - Cells display loaded record count at the intersection of file/table and destination schema.
    - Includes summary grand `TOTAL` row across all tables.
- **Merged Single Step 06 Load Data (`python3 sweep.py vsam load-data`) with `--mode` Support**:
  - Merged former Step 06 (`load-obj`) and Step 07 (`load-data`) into unified Step 06.
  - `--mode default` (default):
    - Fresh table: applies PK constraints before loading, validates every record against PK requirements, and inserts valid records.
    - Existing table with PK constraint: dynamically upserts incoming records via `ON CONFLICT (pk_cols) DO UPDATE SET ...`.
    - Existing table without PK constraint: truncates destination schema and loads data.
  - `--mode data`:
    - Fresh table: does not apply PK constraints; loads bare data directly.
    - Existing table with PK constraint: dynamic upsert using active PKs.
    - Existing table without PK constraint: truncates destination schema and loads data.
- **Consolidated DDL Generation**:
  - Removed legacy `pre_load` and `post_load` folders.
  - All SQL files, table DDLs, and PK constraint scripts are generated consolidated inside `vsam/outputs/gen_ddl/`.
- **AGILETIC Source Company Handling**:
  - For `AGILETIC` source company, directly infers column names and SQL datatypes from S3 CSV headers and data.
  - Extracts key columns from mapped copybook, or auto-detects composite PK in sequential positional order up to 8 columns if no copybook exists.

### Changed
- Refactored `vsam_common.py` with `load_master_copybook_info()` and dynamic korg discriminator mappings.
- Cleaned up all unused imports across all 25 repository scripts in root, `vsam/`, and `oracle/` (0 pyflakes warnings repo-wide).
- Renamed legacy forwarders and updated documentation in `docs/` and package files.
