# Changelog

All notable changes to the **SWEEP Unified Data Migration Framework** (Oracle & VSAM to AWS RDS Aurora PostgreSQL) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Maintenance rule:** every change set gets its own version heading. The **newest release is always at the top**; older releases keep their text unchanged below it. Version numbers only increase (`MAJOR.MINOR.PATCH`). The **file name carries the current version** (`CHANGELOG_v<MAJOR.MINOR>.md`); when a new release is added, rename the file to the new version (`git mv CHANGELOG_v5.1.md CHANGELOG_v5.2.md`) and update the links in `README.md` and `docs/project_overview.md`.

---

## [5.2.1] - 2026-11-19

### Changed
- **VSAM `LoadSummary` sheet:** `records loaded in mapped schema` and `records loaded in unmapped schema` now list the count per schema instead of one total, e.g. `bss01104 = 20, bss01112 = 34` (sorted by schema, blank when nothing was loaded). The `bcs` column, `load %` and the `SchemaBreakdown` sheet are unchanged.
- **Error messages in the Excel reports are no longer cut at 100-300 characters.** They are kept whole up to **1,000 characters** per cell (`sweep_common.clip_error`; longer text ends with `...[+N chars]`). Fixed in: Oracle Step 06 (was 300 and only the *last line* of psql's output - now the full ERROR/DETAIL/CONTEXT text), Oracle Step 07 (200 / 250), Oracle Step 05, VSAM Step 06 (300 / 200), VSAM Step 03 (200), VSAM Step 01 (100). The VSAM `skipped/failed reason` summary is clipped the same way; the "first 5 skipped rows" sample is unchanged.
- Version 5.2.1; changelog renamed `CHANGELOG_v5.2.1.md`.

---

## [5.2.0] - 2026-11-18

VSAM column handling and loading release. The Oracle track is unchanged.

### Added
- **Header-to-copybook column mapping in VSAM Step 02** (`vsam/common_colmap.py`). VSAM files always have a header row whose names and order differ from the copybook, so every S3 header is matched to a copybook column:
  - precedence: `column_overrides` sheet (new, in `vsam_master_overrides.xlsx`) -> previous run ("sticky", so new file versions cannot re-shuffle columns) -> exact name -> similarity score + **Kuhn-Munkres / Hungarian optimal one-to-one assignment** over what is left (numpy solver, 500x500 in ~0.2 s; verified optimal against brute force);
  - adapted from the supplied column-mapper prototype: mainframe abbreviation lexicon, name segmentation, acronym and `PREV`/`TODAY`/`FEE`/`DIV`/`STM` guards, occurrence-index checks; **stricter acronym rule** (the whole name must be the acronym, so `FIRST_NAME` no longer matches `FN-ADDR` at 0.98), optional single-letter prefixes on both sides (`b_korg` = `KORG`), cached canonical forms and an exact-match pass before the global assignment;
  - result per header: score, confidence (`EXACT`/`HIGH`/`MEDIUM`), status `ACCEPTED` (>= 0.70) / `REVIEW` (0.35-0.70) / `UNMAPPED`, plus `COPYBOOK_ONLY` rows for copybook columns that are not in the file (thresholds `VSAM_MAP_ACCEPT` / `VSAM_MAP_MIN`);
  - **new `ColumnMapping` sheet** in the Step 02 Excel report and `vsam/outputs/vsam_02_column_map.csv`; `FileInventory` gains `key source`, `candidate key (not applied)`, `mapped / review / unmapped columns`.
- **`--tbl-col {headers,copybook}`** (`sweep.py vsam inventory`, `vsam run-all`, `run-all`, `VSAM_TBL_COL`): table columns are named after the **S3 headers** (default) or after the mapped **copybook columns**; the datatype always comes from the mapped copybook column (unmapped headers become `text`, refined by data profiling). The choice is stored in the column map, which Steps 03, 04, 06 and 07 read, so they cannot disagree.
- **Snapshot replace for VSAM tables without a primary key.** VSAM files are full snapshots, so a keyless table is replaced by the newest snapshot automatically: all files of the table load into a staging table (`<table>__sweep_stg`) and one transaction truncates, copies and writes the `load_log` rows when every file succeeded; if any file fails the table keeps its old data. A file older than a snapshot already loaded is `SKIPPED`. Tables with a primary key still upsert (rows missing from a newer snapshot are not deleted).
- Shared tables: when several sources feed one table, columns matched to the same copybook column are named alike and Step 04 creates the union of their columns.
- `vsam/common_cobol.py`, tests for the four reported copybook layouts (`tests/data/copybooks/`), the mapping (Hungarian optimality, precedence, keys, overrides), snapshot replace (unit + PostgreSQL) and an end-to-end Steps 01-07 run on a throw-away copy of the repository (headers differing from the copybook, `--tbl-col copybook`, snapshot replacement). 214 tests (223 as of 5.2.1).

### Fixed
- **Copybooks that Step 01 could not parse** (0 fields or a crash): fixed-format sequence areas in columns 1-6 and 73-80 (`00040001`, change tags such as `106618`), sequence numbers glued to the closing period (`VALUE SPACE.00050001`), `*` comment lines with change tags (`154130****`), `====` banners, `COPY ... REPLACING` names such as `:USA090:-KORG` / `:NEXT-MODEL:-KEY`, multi-line `VALUE LOW-VALUES.`, fields that start with digits, quoted literals containing periods. The two duplicate parsers now share one reader.
- **Headers that did not match the copybook were loaded as NULL** (rows were keyed by header name, columns came from the copybook, and `row.get()` found nothing). Rows are now keyed through the column map.
- **Values that do not fit a numeric/date copybook type were silently turned into NULL at load time.** Step 03 now loads such columns as `text` and reports `TYPE_FALLBACK_TEXT`.
- **File names with other timestamp formats were rejected as `UNPARSEABLE_FILENAME`.** `FDICSWP_<CO>_<SOURCE>_FLAT_<timestamp>` now accepts `MMDDYYYY`, `YYYYMMDD`, `YYMMDD`, `YYYYMMDDHHMMSS`, `YYYYMMDD_HHMMSS` and ISO dates; `parse_filename` previously read every 8-digit stamp as `MMDDYYYY` and returned no date for `YYYYMMDD`.
- **Auto-discovered keys were applied as primary keys.** A guessed key can be wrong or unstable (a too-narrow key makes upserts merge different records). Positional discovery now only reports a *candidate* (`KEY_CANDIDATE_NOT_APPLIED`); keys come from the copybook (every key column must be an ACCEPTED match) or from `korg_overrides`. Without a copybook the mapping-sheet `korg` column is no longer used as a key either (it is a schema discriminator).
- `VSAM_COPYBOOK_DIR` is now honoured by Step 01 (it was documented but ignored).

### Changed
- **`--force-truncate` semantics:** keyless tables are replaced automatically in a full run. The flag is now only needed to replace a populated table in a **partial** run (`--company`, `--tables`, `--limit-files`), where other source files of a shared table might be missing. (In 5.1 it was required for every keyless table that already held rows.)
- Docs (`docs/`, README) updated for all of the above; version 5.2.0; changelog renamed to `CHANGELOG_v5.2.md`.

### Known limitations
- Changing `--tbl-col` later means re-running Steps 02-05 on fresh tables; existing tables are not renamed.
- VSAM loads into keyed tables are still batched `INSERT`s (not one transaction per file).
- The mapping lexicon is domain-specific; unknown abbreviations score as `REVIEW`/`UNMAPPED` and are corrected through the `column_overrides` sheet.
- AGILETIC sources keep their v5.0 behaviour (columns and types inferred from the data, no copybook mapping).

---

## [5.1.0] - 2026-11-18

Reliability, safety and maintainability release. No change to the migration rules (schema routing, `bcs` / KORG
behaviour, report layouts); the pipelines now fail loudly instead of continuing after errors, cannot silently lose data,
and no longer build shell commands from file names.

### Fixed - data loss / silent wrong results
- **Failed steps no longer exit 0.** Every step script now returns a real exit status (`sys.exit(0 if ok else 1)`); previously 10 of 15 steps never set an exit status (only an uncaught crash returned non-zero), so `run-all` carried on after a failed DDL or load step.
  - Oracle 05 (apply DDL) now fails when *any* DDL script failed; Oracle 08 and VSAM 07 exit `1` when the parity audit does not pass; VSAM 01 exits `1` when a copybook fails to parse; `sweep.py check` exits `1` when the connection or `psql` is unusable.
- **VSAM load could wipe earlier files' data.** Truncation state was per file, so when several source files mapped to the same table (e.g. different companies routed to `bcs`) every later file truncated what the earlier files had loaded - and all reported `SUCCESS`. Truncation is now tracked per run: a table is claimed by the first file that touches it, later files append.
- **VSAM `TRUNCATE` is now opt-in** (`--force-truncate`) and a failed `TRUNCATE` fails the file instead of being logged at debug level and ignored (which duplicated data).
- **VSAM `load_log` writes are no longer swallowed.** The old code always tried `ON CONFLICT (uri, target_schema)` (no such constraint), fell back to a second statement and ignored any error. It now writes the `<uri>#<schema>` row directly and a failed write fails the file.
- **Oracle loader no longer guesses on lookup errors.** A failed "is the table empty?" query was treated as "empty" (bulk-appending into a populated table) and a failed primary-key lookup was treated and cached as "no PK" (turning an upsert into a blind append). Both now fail the file.
- **VSAM Step 07 crashed** with `KeyError: 'len_display'` for tables whose columns were inferred from data (no copybook). It now audits them.
- **VSAM Step 07 reported files routed to the fallback `bcs` schema as `NOT_LOADED`**; a `bcs` load-log row now counts as loaded.

### Security
- **Shell command injection removed.** S3 keys / file names were interpolated into shell strings (`subprocess.run(..., shell=True)`, unquoted `\copy ... FROM PROGRAM '<cmd>'`). A key such as `x; $(touch PWNED).csv.gz` executed commands and broke the load. Callers now pass argument lists; the single unavoidable command string is built with `shlex.quote`; URIs with control characters are rejected. Keys with spaces, quotes, `;`, backticks or `$()` now load correctly.
- **`stop` can no longer kill unrelated processes.** It trusted PIDs from a registry file (PID re-use), signalled only the supervisor (children such as psql/aws kept running) and `--kill-db` terminated *every* session whose query began with `copy` (other users' too). It now verifies the PID's command line, terminates the whole process group, and `--kill-db` only ends sessions with `application_name = 'sweep'`.
- Every SWEEP `psql` session now sets `standard_conforming_strings=on`; `lit()` strips NUL characters.

### Added
- **Timeouts and retries.** Every `psql` / `aws` / `ps` call has a timeout (`SWEEP_PSQL_TIMEOUT` default 7200 s, `SWEEP_LOAD_TIMEOUT` default 21600 s for file loads). Connection-establishment failures - where the statement provably never ran - are retried with backoff (`SWEEP_CONNECT_RETRIES`, default 3). A timed-out load kills `psql` and its S3/gzip pipeline together (own process group).
- **Credentials without a password:** `~/.pgpass`, `PGPASSFILE`, `PGSERVICE` and `SWEEP_ALLOW_NO_PASSWORD=1` are accepted; `PGPASSWORD` is no longer mandatory (`has_db_auth()`).
- **VSAM load flags:** `--force-truncate` and `--dry-run` (also `sweep.py vsam load-data`, `vsam run-all --force-truncate`, `run-all --force-truncate`).
- **Pipeline stops on failure** in the foreground (`run-all` exits with the failing step's code) and as a background job (job ends `ERRORED`).
- **Locked, atomic job registry** (`registry_transaction`, `atomic_write_json`) shared by the CLI, the monitor and the supervisors.
- **Test suite** (`tests/`, 128 tests): unit tests for helpers, process/lock/atomic-write behaviour, shell-quoting round-trips, truncate-once logic, job control, exit codes, module/facade imports, and PostgreSQL integration tests (hostile file names, atomic load of a truncated gzip, `--kill-db` isolation, timeouts, load-log keys). Integration tests skip automatically when no PostgreSQL is reachable.
- **CI** (`.github/workflows/ci.yml`): compile + pyflakes + pytest on Python 3.9 and 3.12 against PostgreSQL 16.
- **Packaging:** `pyproject.toml` (`pip install -e .`, `sweep` console script), `requirements.txt`, `requirements-dev.txt`.
- `README.md` and `.gitignore` (secrets, logs, reports, outputs, caches, `.sweep_bg_jobs.json`).

### Changed
- **Steps run as modules from the repository root** (`python3 -m oracle.oracle_06_load_data`); `sweep.py` does this for you (cwd + `PYTHONPATH` set). The per-file `sys.path` patching and import-time path hacks were removed; `from vsam_common import ...` became `from vsam.vsam_common import ...`. Running `python3 oracle/oracle_06_load_data.py` directly is no longer supported.
- **`sweep.py run-all` no longer writes `run_all_tracks.py` / `run_oracle_all.py` / `run_vsam_all.py`** into the repository and executes them; pipelines run in-process (foreground) or as `sweep.py ... run-all` (background).
- **Environment file precedence:** variables already exported in the real environment now win over `sweep.env` (previously the file always overwrote them). The duplicated env-loading / `PG*` setup in `sweep_common.py` was merged.
- **`COPY ... TO STDOUT` for `query_db_dicts`** instead of `\copy` on one joined line: multi-line queries, trailing `--` comments and embedded newlines in values are preserved (CSV parsed with `io.StringIO`).
- **Exception handling:** ~94 blanket `except Exception` handlers narrowed to specific groups (`DB_ERRORS`, `FILE_READ_ERRORS`, `AWS_ERRORS`, `PARSE_ERRORS` in `sweep_common`); formerly silent handlers now log. Remaining broad handlers are worker/diagnostic boundaries and are annotated.
- **Oracle step 03 / `extract_file_header` / VSAM `S3Text`** read S3 through `sweep_streaming` (argument lists, exit-status and stderr checks). The `PYTHONPATH=/usr/local/lib/python3.9/...` workaround in `oracle_03_type_gate.py` went away with the shell pipeline; the one in `sweep_stream.sh` is unchanged.
- **Windows-only code** (`msvcrt`, `ctypes`, `taskkill`, `tasklist`, ANSI enabling, process detachment, shared-read file access) moved into `sweep_platform.py`.
- **Large files split** (public names unchanged):
  - `vsam_common.py` (1,877 lines) -> `vsam/common_config.py`, `common_mapping.py`, `common_records.py`, `common_copybook.py`, `common_schema.py`, `common_files.py` behind a re-exporting facade.
  - `sweep_bg.py` (1,409 lines) -> `sweep_jobs.py`, `sweep_metrics.py`, `sweep_dashboard.py`, `sweep_format.py` (+ facade and `_runner` entry point).
  - `oracle_08_validate.py` (1,439 lines; one 870-line function) -> `oracle_08_catalog.py` (one function per catalog), `oracle_08_audit.py` (one function per comparison level) and a 190-line orchestrator. Output verified identical to v5.0 on a sample dump.
  - `oracle_07_load_objects.py` -> `oracle_07_parser.py` (SQL block parser).
  - VSAM Step 06: duplicated row-routing loops merged, report building moved into `build_load_summary` / `build_schema_breakdown`, dead code (`_load_via_pipe`, `_load_filtered`, `_is_schema_match`) removed.
- **Formatting:** `oracle_02_inventory.py` and `oracle_06_load_data.py` had a blank line after every line (1,267 -> 636 and 1,083 -> 531 lines, AST unchanged); module docstrings now come first in every script.
- Version bumped to 5.1; changelog file renamed `CHANGELOG_v5.0.md` -> `CHANGELOG_v5.1.md` (the file name carries the current version).
- **Documentation (`docs/`, README) rewritten against the real code:** correct commands (`stop --name`, `monitor --track/--interval`, `logs --track/--file`), real output file names and report sheets, real `sweep_ctl.load_log` definition, VSAM truncation semantics, new flags and environment variables, module layout, exit codes and the run-as-module convention.

### Removed
- `sweep.py oracle patch-identity | patch-audit | patch-fk` (and their parsers): the scripts they launched (`oracle_patch_*.py`) do not exist in the repository.
- `sweep_latest.log` (a runtime symlink that had been committed) - now git-ignored.

### Known limitations (unchanged by this release)
- VSAM Step 06 still loads with batched `INSERT ... VALUES` (not one transaction per file and not `\copy`); a file that fails part-way can be partially loaded. Re-run with the PK upsert or `--force-truncate`.
- Oracle-to-PostgreSQL type mapping rules (`oracle_to_pg`) are unchanged and have not been re-reviewed.
- `sweep_stream.sh` still prepends hard-coded Python 3.9 site-packages paths to `PYTHONPATH`.
- A few very long functions that are one big loop remain (for example the per-folder loop in `vsam_02_inventory.main`, the stage loop in `oracle_07.apply_post_load`).

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
