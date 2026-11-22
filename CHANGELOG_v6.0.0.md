# Changelog

All notable changes to the **SWEEP Unified Data Migration Framework** (Oracle & VSAM to AWS RDS Aurora PostgreSQL) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Maintenance rule:** every change set gets its own version heading. The **newest release is always at the top**; older releases keep their text unchanged below it. Version numbers only increase (`MAJOR.MINOR.PATCH`). The **file name carries the current version** (`CHANGELOG_v<MAJOR.MINOR.PATCH>.md`); when a new release is added, rename the file to the new version (`git mv CHANGELOG_v5.4.0.md CHANGELOG_v6.0.0.md`) and update the links in `README.md` and `docs/project_overview.md`.

---

## [6.0.0] - 2026-11-22

The target becomes a **medallion chain of four databases in one Aurora cluster** (bronze SWEEPB -> silver SWEEPS -> gold_backup SWEEPGB -> gold SWEEPG). S3 is read once, into bronze; the other layers are filled by `promote` from the previous layer. Every step gets the same targeting flags and merge-safe outputs, and the load log is split per track. See `docs/layers_and_promote.md` and `docs/targeting_and_outputs.md`.

### Added - layers, promote, migrate
- **Layer profiles** (`sweep_layers.json`, `sweep_layers.py`, `sweep.<layer>.env.sample`): `--layer {bronze,silver,gold_backup,gold}` is **required** on every step that touches a database (`SWEEP_LAYER` may supply it, except for gold); a step **refuses to run when the connected database is not the layer's database** (exit code 3). `allow_ddl=false` on gold_backup / gold: Steps 04, 05, 07 (Oracle) and 04, 05 (VSAM) refuse to run without `--allow-ddl`. Step 06 loads **bronze only**. Generated DDL goes to per-layer folders.
- **Manifests** `layers/<layer>_tables.csv` (`track`, `schema`, `table`, `promote_mode`, `conflict_policy`, `enabled`): a layer only touches tables of its own manifest. `sweep.py layers init-manifests` drafts them; `sweep.py layers scan-models --models-dir <path>` scans the (external) data-model scripts and drafts the silver VSAM list and the model output tables for **human review** - nothing is applied automatically.
- **Delta capture** (`sweep_delta.py`): while bronze loads, the rows actually applied (after the only-if-different filter) are copied into unlogged `sweep_delta."<schema>__<table>"` tables and registered in `sweep_ctl.delta_registry` (strictly increasing sequence per table). An empty target table or `--full` registers a full copy instead (no second copy of the data). New `--full` flag on Step 06.
- **`sweep.py promote --from L --to L`** (`sweep_promote.py`): one hop, parent-first level by level, transport `\copy ... FROM PROGRAM 'psql ... COPY ... TO STDOUT'` with a Python-pipe fallback (`--transport stream`), the same staged script as Step 06, one transaction per table, `sweep_ctl.promote_log`, own delta for the next hop; deltas strictly in sequence order, gated layer by layer (gold only after gold_backup), failures stop only that table's chain, idempotent and resumable. `--dry-run` (inserted / updated / unchanged per table), `--full`, `--status` (delta x layer matrix), `--purge` (after every layer applied a delta plus `SWEEP_DELTA_RETENTION_DAYS`, default 14). Model outputs are upserted on the key, changed rows only; keyless outputs are skipped unless `insert_only`; `replace` (VSAM keyless, silver) is refused on gold and gold_backup. `postgres_fdw` was evaluated and not used (needs an extension and server objects in every database).
- **Gold safeguards:** schema-drift check before every table (missing column fails that table, extra NOT NULL column without default fails and is listed, no automatic ALTER), conflict policy per table (`source_wins` / `keep_target`), identity / serial sequences advanced to `GREATEST(current, max(column))` and never backwards, `trigger_mode` per layer (FE / application triggers do not fire during pipeline loads), short `lock_timeout` (5 s) + `statement_timeout` with retry and backoff, one small transaction per table.
- **`sweep.py chain [--until L]`**: load bronze from S3, then promote silver -> gold_backup -> gold, stopping at the first failure (`--until gold_backup` stops for review).
- **`sweep.py migrate [--layer X | --all-layers] [--dry-run]`** (`sweep_migrate.py`): versioned, idempotent, transactional migrations recorded in `sweep_ctl.schema_version`. Migration 1 creates the two log tables, copies the old `load_log` (renamed `load_log_legacy`, never dropped), adds `track` to `load_rejects` / `load_audit`, creates `delta_registry`, `promote_log`, `schema_version` and the `sweep_delta` schema. Both `sweep_meta.sql` files create the same objects for a fresh install. Step 06 and `promote` refuse to run on an unmigrated database.
- **`sweep.py layers reconcile --from L --to L [--tables] [--exact]`**: per-table row counts + key hash (exact or 1 percent sample), Excel report, exit code (gold and VSAM informational). **`layers check [--layer X]`**: reachable, PostgreSQL >= 16, `session_replication_role` privilege, migrated, drift summary, sequence status.

### Added - targeting and merge-safe outputs
- **Uniform targeting flags in every step** (`sweep_targeting.py`): `--tables`, `--schemas` (aliases `--owners`, `--schema`), `--exclude-tables`, `--file` / `--filename`, `--company`, `--copybook`, `--dry-run`. Gaps filled: Oracle 05 / 07 `--tables`, VSAM 01 `--copybook`, VSAM 04 / 05 `--tables` / `--schemas`, VSAM 06 `--file` / `--filename` / `--exclude-tables`. Selectors are validated up front: unknown table, no matching file or unparseable name = clear message and exit code 2 before anything runs. `run-all` passes the selectors on.
- **Merge-safe outputs** (`sweep_merge.py`, `sweep_ddlfrag.py`): a targeted run replaces only its own rows / keys / fragments in the file maps, `vsam_03_type_gate.json`, gate outputs, Step 01 metadata CSVs, the VSAM load matrix and column map; generated DDL is written **per table** (fragment + manifest) and the per-schema scripts are assembled from the fragments. A run without filters is a full run and replaces everything. Writes are atomic and locked, one `.bak` is kept, merged CSV rows carry `_run_id` / `_updated_at`, JSON entries a `_meta` block, and the log says `targeted run: merged N entries, kept M`.

### Changed
- **Separate load log tables:** `sweep_ctl.oracle_load_log` (PK `uri`) and `sweep_ctl.vsam_load_log` (PK `uri, target_schema`, the **real URI** - no more `<uri>#<schema>`, no `#` stripping); `LOAD_LOG_TABLE` is replaced by `ORACLE_LOAD_LOG_TABLE` / `VSAM_LOAD_LOG_TABLE`. `load_rejects` and `load_audit` stay shared and get a `track` column.
- **"Already loaded" also requires a non-empty target table** (Oracle and VSAM Step 06, Oracle Step 08): an unchanged file whose table was truncated by hand is loaded again. A keyless VSAM table still reloads all its files when any changed.
- **VSAM Step 04 never applies a key taken from the Step 03 profile** (the old `pk_cols` fallback is gone): only the copybook KEY group (all key columns ACCEPTED) or `korg_overrides`; a source without a trusted key is created and loaded without a primary key.
- The VSAM load log CSV is now `<db>_vsam_06_load_log_<run_id>.csv`; the leftover `get_pipeline_mode` / `VSAM_MODE` helper is removed.
- Version 6.0.0; the changelog file is `CHANGELOG_v6.0.0.md`.

### Not done on purpose
- No row is ever deleted by the pipeline in any layer (no delete logic for keys missing from a newer snapshot); no option to bulk-load Day-1 with keys applied first; foreign keys are never disabled (parent-first ordering instead); no automatic ALTER / DDL on gold or gold_backup; the data-model scripts are never run or changed; QA databases are never copied to PROD.

### Tests
- New: targeting selectors, merge safety (byte-identical other entries, concurrent runs, atomic writes, full run replaces), targeted Oracle 01-04 and VSAM 01-07 runs, log-table migration (legacy rows, idempotency, rollback, fresh vs migrated schema), layer guard, manifests and model scan, delta capture, the promote chain over four temporary databases (apply order, gating, idempotent re-run, drift, keep_target, sequences, replace refusal, model outputs, stream transport, lock timeout, purge), reconcile / check / migrate commands, hand-truncated tables. Integration tests create temporary databases and skip when no PostgreSQL is reachable.

---

## [5.4.0] - 2026-11-19

Oracle Step 06 refinements, a VSAM Step 06 rewrite on the same workflow, and housekeeping.

### Removed
- **VSAM `--mode {default,data}`**: only the former `default` behaviour remains (primary keys are always applied before loading). The `DIRECT_INSERT` branch of the load strategy is gone.
- **`--latest-only` / `--latest-version-only`** (VSAM Steps 02, 03, 06, `sweep.py`, report field `latest_only_mode`): "newest version per (company, source file)" is now **always on** - VSAM files are full snapshots.
- **`VSAM_VERSION_MODE`, the load-policy code and the `loadpolicy_overrides` sheet** of `vsam_master_overrides.xlsx` (`version_mode_for`, `load_policy`, `load_policy.csv`).

### Changed - Oracle Step 06
- **`--max-rejects` now defaults to `0`**: valid rows always load; rejected rows never fail a file. Only technical errors (a row-count mismatch, a broken stream) roll a file back. `--max-rejects N` still fails a file that reaches N rejects.
- **Only rows that differ are updated:** `ON CONFLICT (key) DO UPDATE ... WHERE (target columns) IS DISTINCT FROM (file columns)`. A `modifieddate` / `modifiedby` that comes from the file counts as a change; values the tool sets itself are not compared. `unchanged` = identical rows (not touched); the row-count check still balances. Inserts and updates are now told apart by the keys that existed before the statement, so the counts are also correct on **partitioned tables** (`RETURNING xmax` failed there with "cannot retrieve a system column in this context").
- **Parent tables first** (`oracle/oracle_06_order.py`): the load order comes from the foreign keys extracted in Step 01 (plus live FKs in PostgreSQL) and runs **level by level** - all files of a level finish before the next level starts, parallel workers inside a level; self-references and cycles share a level. Foreign keys stay on and the `FK_MISSING` check stays. `--fast-bulk` loads smallest to largest instead. New `load_level` column in `File_Execution_Details`.
- **New / re-delivered S3 files are detected by S3 metadata, not just by URI:** Step 02 writes `s3_last_modified`, `s3_etag`, `s3_size` to the file map; `sweep_ctl.load_log` gets the same three columns (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in both `sweep_meta.sql` files and the VSAM control DDL); a file is already loaded only if its status is `SUCCESS` and all three values equal what S3 reports now. Step 06 checks the object (HEAD) right before loading and stores what it saw; Step 08 uses the same comparison (`s3_files_current / changed / pending`, parity status `S3_CHANGED_NOT_RELOADED`). `--reload`, `--file`, `--filename` still force a load. `load_log` stays one row per URI. Rows loaded by earlier versions (no metadata) are treated as loaded; use `--reload` to re-verify them.

### Changed - VSAM Step 06 (rewrite, same workflow as Oracle)
- **Staged, set-based, one transaction per file** (`vsam/vsam_06_pipeline.py`): bulk `\copy` into a raw temp table (columns via the Step 02 column map), validation and routing in SQL, upsert, row-count check (`read = inserted + updated + unchanged + rejected + duplicates`), `load_log` rows, `COMMIT`. No more 10,000-row `INSERT ... VALUES` batches; a failed file leaves no partial data.
- **Schema routing in SQL:** `korg` last 3 digits -> active schema; unmatched, sentinel or inactive values -> `bcs`; files without copybook and korg go entirely to `bcs`.
- **Keys:** only the copybook KEY-group key (every key column ACCEPTED) or a `korg_overrides` key is applied - never a guessed one. A key that cannot be applied (duplicates, key columns missing from the file) is an **error**: Step 03 reports `PK_ERROR` (the key stays visible instead of silently becoming "no PK"), Step 06 does not load the source. A file without a copybook is loaded keyless. Step 03's uniqueness check ignores blank / placeholder keys (they are rejected at load).
- **Keyless tables (snapshot replace):** all files of one keyless table are loaded in a **single transaction on one connection** (several `\copy` into temp tables, `TRUNCATE` + `INSERT`, `load_log` rows, `COMMIT`); if any file fails, the table keeps its old data. The persistent `<table>__sweep_stg` table is gone. An older snapshot never overwrites a newer one; partial runs still need `--force-truncate`.
- **Bad values are rejected, not NULLed:** `BAD_VALUE` (detail = column and value) goes to `sweep_ctl.load_rejects`. Dates such as `9999-12-31` are valid and kept.
- **Blank / placeholder key rows are reported:** every skipped row is written to `load_rejects` with `PK_BLANK`, `PK_SENTINEL` or `ALL_NULL` and the row as JSON. New sheets `Reject_Summary` and `Rejected_Rows`; `LoadSummary` gains `inserted`, `updated`, `unchanged`, `rejected by reason`, `duplicates dropped`.
- **`--max-rejects` (default 0)** and **`--reload`** are available for VSAM. Unchanged files (S3 LastModified + ETag + size) are skipped; a keyless shared table reloads **all** of its files when **any** of them changed, keyed tables load only the changed ones. Keyed tables never delete rows that are missing from a newer snapshot (intentional).
- Shared building blocks moved to `sweep_loadpipe.py` (reject table, upsert SQL, table metadata, output markers) and `sweep_s3meta.py` (S3 metadata).

### Added
- **PostgreSQL 16+ start-up check** (`check_postgres_version`) in both Step 06 loaders and `sweep.py check`, with a clear message (`pg_input_is_valid` is required).
- **Cleaner end-of-step summary:** every step now ends with one compact, aligned summary block (a single log record with dotted leaders, grouped sections for Steps 06) instead of a box of separate log lines.
- Tests: parent-first ordering, `IS DISTINCT FROM` counts (incl. partitioned tables), S3-metadata change detection, VSAM staged load (one transaction, no partial data), `BAD_VALUE` / `PK_*` / `ALL_NULL` rejects, keyless shared-table replace, SQL ports of the Python PK / routing rules, version check, summary format.

### Fixed
- User-facing messages and help texts say `python3` (Amazon Linux), e.g. `python3 sweep.py oracle gate`.
- `python3 sweep.py oracle gate` hint in Step 06 pointed to a non-existent `gate` top-level command.

---

## [5.3.0] - 2026-11-19

Oracle Step 06 robust load release. The VSAM track is unchanged apart from the shared control-schema DDL.

### Added
- **Triggers are off while code loads data.** Each file load runs `SET LOCAL session_replication_role = replica` (transaction-scoped, no table lock), so user triggers do not fire and are back on automatically at commit/rollback. When the role may not set it (non-superuser), the load falls back to `ALTER TABLE ... DISABLE TRIGGER <name>` / `ENABLE TRIGGER` inside the same transaction (locks the table until commit; needs ownership). `--trigger-mode auto|replica|disable` (default `auto`, probed once per run). `modifiedby` / `modifieddate` are set explicitly on updates because the audit trigger no longer runs.
- **Alternate / composite unique keys** are part of dedup and conflict handling: duplicates inside a file are collapsed on the conflict key (last row wins); a row whose alternate key already belongs to a *different* existing row is rejected (`ALT_KEY_CONFLICT`) rather than updating the wrong row.
- **Relaxed staging + validation of staged rows.** The stage table has no NOT NULL / CHECK / defaults; rows are validated before touching the target for `NOT_NULL`, `CHECK`, `BAD_VALUE` (`pg_input_is_valid`, PostgreSQL 16+), `FK_MISSING`.
- **Bad rows are reported, not fatal.** Rejected rows are stored in `sweep_ctl.load_rejects` (reason, detail, row number, row as JSON) and shown in new Excel sheets `Reject_Summary` and `Rejected_Rows`; the file still loads in **ONE transaction** (data + `load_log` commit together). A file **fails and rolls back completely when 10 rows are rejected** (`--max-rejects N`, default 10, `0` = never fail); in-file duplicates are not failures. Files with rejects below the limit stay `SUCCESS` and the exit code is unchanged.
- **Row-count reconciliation** per file: `read = inserted + updated + unchanged + rejected + duplicates`; a mismatch aborts and rolls back the file. New columns in `File_Execution_Details`: `rows_read`, `rows_inserted`, `rows_updated`, `rows_unchanged`, `rows_rejected`, `duplicates_dropped`; new Summary metrics.
- **One pipeline for every load** (always staged, new module `oracle/oracle_06_pipeline.py`). `--fast-bulk` opts in to the old direct `COPY` for empty tables without constraints (fastest initial load; one bad row fails the file).
- `sweep_ctl.load_rejects` added to both `sweep_meta.sql` files; new tests `tests/test_oracle_load_pipeline.py`.

### Changed
- `oracle_06_load_data.load_single_file` returns a `LoadOutcome` (status, rows, seconds, error + counts).
- Staging temp tables live on instance-local storage, so size ~ file size; use `--fast-bulk` for very large initial loads.
- Version 5.3.0; changelog renamed `CHANGELOG_v5.4.0.md`.

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
