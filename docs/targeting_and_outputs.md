# Targeted runs, merge-safe outputs and the load log tables (v6.2.1)

## 1. Uniform targeting flags

Every step - Oracle 01-08, VSAM 01-07, `run-all` and the layer commands - understands the same selectors (implemented once in `sweep_targeting.py`):

| Flag | Meaning |
|---|---|
| `--tables a,b` | `SCHEMA.TABLE` or a bare table name, case-insensitive. A bare name matches that table in every schema. |
| `--schemas s1,s2` | Schemas / owners. Aliases: `--owners` (Oracle steps), `--schema` (VSAM Step 07). |
| `--exclude-tables a,b` | Same syntax as `--tables`; an excluded table is never selected. |
| `--file` / `--filename` | An S3 file (full URI, key suffix or file name) for the steps that work on S3 files: Oracle 02, 03, 06; VSAM 02, 03, 06. |
| `--company` | VSAM company tokens. |
| `--copybook` | VSAM Step 01: copybook files (name with or without extension). |
| `--dry-run` | Print what would be touched; **write nothing** (every step). |
| `--layer` | See [layers_and_promote.md](layers_and_promote.md). |

New in 6.0.0: Oracle 05 and 07 accept `--tables`; VSAM 01 has `--copybook`; VSAM 04 and 05 accept `--tables` / `--schemas`; VSAM 06 accepts `--file` / `--filename` / `--exclude-tables`. `run-all` passes the selectors to every step that understands them (use `--track oracle|vsam` when targeting; `--file` is never passed on by `run-all`).

**Validated up front:** an unknown table or schema, a file that matches nothing, an unknown company / copybook or an unparseable name is a clear message ("Invalid selection: unknown table 'x' ... did you mean: ...") and **exit code 2 before anything runs**. The exit code of a targeted run reflects failure of the targeted item.

A run **without any filter is a full run**; a run with filters is a **targeted run**.

Notes: VSAM `--schemas` in Step 06 selects the files that load into those schemas; each selected file is still loaded completely (all destination schemas). VSAM Step 02 processes all sources that feed the same shared table together so their columns stay unified.

## 2. Merge-safe outputs

Before 6.0.0 every step overwrote its whole output. Now a **targeted run changes only its own entries** (`sweep_merge.py`):

| Output | Targeted run |
|---|---|
| File maps: `oracle_02_s3_file_map.csv`, `s3_file_map.csv`; VSAM `vsam_02_inventory.csv`, `vsam_02_load_matrix.csv`, `vsam_02_latest_file_headers.csv`, `vsam_02_column_map.csv` (and their legacy copies) | rows of the targeted tables / files are removed and replaced, all other rows kept (the Oracle `load_order_rank` is renumbered small to large across the merged map) |
| `vsam_03_type_gate.json` / `type_gate.json` | only the `"company|source"` keys of the run are updated; every entry gets a `_meta` block (`run_id`, `timestamp`) |
| Oracle gate outputs (`gate_results.csv`, pass list), Step 01 metadata CSVs, VSAM `vsam_01_copybook_extracts.csv`, `vsam_03_type_gate_status.csv`, the VSAM load matrix updated by Step 03 | merged by table / owner / copybook / source |
| Generated DDL (Step 04) | written **per table** as fragments (`<root>/fragments/<schema>/<table>.<kind>.sql` + `manifest.json`); the per-schema scripts are assembled from the fragments; a targeted run rewrites only its own fragments, a full run also prunes fragments of tables that no longer exist. Oracle 05 / VSAM 05 apply selected tables straight from their fragments. |
| Excel reports | per run (timestamped, database name in the file name): nothing to merge |

Safeguards for every merge: a run without filters replaces the whole file; writes are **atomic and locked** (`file_lock` + temp file + `os.replace`, never a half-written file); **one `.bak`** of the previous version is kept before every merge; every merged CSV row carries `_run_id` and `_updated_at`; the log says `targeted run: merged N entries, kept M`. Owner-level Step 01 files (sequences, views / PL-SQL objects) are left alone by a run that names tables.

Layer-dependent outputs (generated DDL) live under per-layer folders (`outputs/<...>/<layer>/`) and reports / the VSAM load log CSV carry the database name; layer-independent metadata (Steps 01-03) stays shared.

## 3. Log tables

> **6.1.0:** every load attempt (Oracle / VSAM Step 06 and `promote`) is also one append-only row of `sweep_ctl.load_history`, written in the load transaction (failed attempts get a FAILED row). Oracle Step 08, VSAM Step 07 and `validate` compare each table with it (`count == rows_before + sum(rows_inserted)`; a keyless VSAM table with its latest snapshot). FE rules from `fwd_ddl/` (`ignore-column`, `rename-column`, `default-column`, `skip-table`) apply to the targeted loads too. See [fwd_ddl.md](fwd_ddl.md).

| Table | Key | Content |
|---|---|---|
| `sweep_ctl.oracle_load_log` | `uri` | one row per Oracle S3 file |
| `sweep_ctl.vsam_load_log` | `(uri, target_schema)` | one row per VSAM file and destination schema; the **real URI** is stored (no `<uri>#<schema>`, no `#` stripping in Step 07) |
| `sweep_ctl.load_rejects`, `sweep_ctl.load_audit` | | shared by both tracks, with a `track` column (`oracle` / `vsam`) |
| `sweep_ctl.load_log_legacy` | | the old single `load_log`, kept (renamed) by migration 1 |

Both log tables have the S3 metadata columns `s3_last_modified`, `s3_etag`, `s3_size`: a file is "already loaded" only when its row has status `SUCCESS`, all three values equal what S3 reports now **and the target table is not empty**. See [layers_and_promote.md](layers_and_promote.md) for `sweep.py migrate` and the delta / promote tables (`delta_registry`, `promote_log`, `schema_version`).
