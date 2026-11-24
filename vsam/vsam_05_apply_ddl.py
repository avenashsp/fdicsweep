#!/usr/bin/env python3
"""
=============================================================================
vsam/vsam_05_apply_ddl.py - Step 05: Apply Pre-Load DDL (Tables Only, No PK)
=============================================================================
Applies the control schema (sweep_ctl) and executes the target table DDL
(CREATE TABLE only, without primary key constraints) from Step 04.

PK constraints are added separately in Step 07 (Load Objects).

Generates:
  - reports/vsam/<db>_vsam_05_apply_ddl_report_<ts>.xlsx
=============================================================================
"""
import sys
import argparse
from pathlib import Path

from sweep_common import (
    log_summary,
    VSAM_OUTPUTS_DIR, VSAM_GEN_DDL_DIR, VSAM_PRE_LOAD_DIR, VSAM_REPORTS_DIR, SWEEP_META_SQL,
    get_logger, get_timestamp, get_db_name, generate_excel_report, run_psql, query_db_rows
)

from sweep_common import layer_dir
from sweep_ddlfrag import FragmentStore
from sweep_targeting import add_targeting_args, selector_or_exit, validate_or_exit
from sweep_fwd_ddl import load_fe_changes
from sweep_layers import add_layer_arg, ensure_layer, restrict_to_manifest

logger = get_logger("vsam_05_apply_ddl", "vsam_05_apply_ddl")


def main():
    parser = argparse.ArgumentParser(description="Step 05: Apply pre-load DDL (tables only, no PK) to PostgreSQL.")
    parser.add_argument("--skip-meta", action="store_true", help="Skip re-deploying sweep_meta.sql control schema")
    parser.add_argument("--bg", action="store_true", help="Run in background")
    add_targeting_args(parser)
    add_layer_arg(parser)
    args = parser.parse_args()
    sel = selector_or_exit(args, logger)
    layer = ensure_layer(args, logger, ddl_what="Step 05 (apply DDL)")
    store = FragmentStore(layer_dir(VSAM_OUTPUTS_DIR / "ddl_fragments"))
    known = [(sc, t) for sc in store.schemas() for t in store.tables(sc)]
    manifest_driven = layer is not None and layer.manifest_path.exists()      # only the tables of the layer's manifest
    if sel.filtered or manifest_driven:
        if not known:
            logger.error("No per-table DDL found; run 'sweep.py vsam gen-ddl' first")
            sys.exit(1)
        validate_or_exit(sel.validate_tables, known, what="the generated DDL", logger=logger)
        picked = restrict_to_manifest(layer, [(sc, t) for sc, t in known if sel.matches(sc, t)], logger, "table DDL")
        if sel.dry_run:
            for sc, t in picked:
                logger.info(f"[dry-run] would apply {sc}.{t}")
            logger.info(f"[dry-run] {sel.describe()}: {len(picked)} table(s); nothing applied")
            return True
    elif sel.dry_run:
        logger.info(f"[dry-run] would apply the whole DDL script ({len(known)} table fragment(s)); nothing applied")
        return True

    logger.info("=" * 75)
    logger.info("SWEEP VSAM MIGRATION - STEP 05: TARGET DDL DEPLOYMENT (TABLES ONLY)")
    logger.info("=" * 75)

    # 1. Apply Control & Metadata Schema
    if not args.skip_meta:
        if SWEEP_META_SQL.exists():
            logger.info(f"Deploying control schema: {SWEEP_META_SQL}...")
            run_psql(SWEEP_META_SQL, is_file=True)
            logger.info("[+] sweep_ctl & sweep_meta deployed.")

    # 2. Apply Pre-Load DDL (tables without PK). A targeted run applies only the selected tables' fragments. A table an applied
    #    FE file dropped (or renamed away) is NOT recreated: it is reported as FE_CHANGED.
    fe = load_fe_changes()
    use_fragments = sel.filtered or manifest_driven or bool(fe.dropped_tables)
    if use_fragments:
        if not (sel.filtered or manifest_driven):
            picked = list(known)
        for sc, t in [(sc, t) for sc, t in picked if fe.table_dropped(sc, t)]:
            logger.info(f"FE_CHANGED: {sc}.{t} was dropped by {fe.dropped_tables[(sc, t)]}; it is not recreated")
        picked = [(sc, t) for sc, t in picked if not fe.table_dropped(sc, t)]
        for sc in sorted({sc for sc, _ in picked}):
            run_psql(store.script_for(sc, "pre", [t for s_, t in picked if s_ == sc]))
        logger.info(f"[+] {len(picked)} target table(s) provisioned.")
    else:
        ddl_file = (
            VSAM_GEN_DDL_DIR / "vsam_04_all.sql" if (VSAM_GEN_DDL_DIR / "vsam_04_all.sql").exists() else
            VSAM_GEN_DDL_DIR / "all_tables.sql"
        )
        if not ddl_file.exists():
            ddl_file = VSAM_PRE_LOAD_DIR / "all_tables.sql"
        if not ddl_file.exists():
            ddl_file = VSAM_OUTPUTS_DIR / "ddl" / "all.sql"

        if not ddl_file.exists():
            logger.error(f"DDL file not found at {ddl_file}. Run 'sweep.py vsam gen-ddl' first.")
            sys.exit(1)

        logger.info(f"Executing table DDL: {ddl_file}...")
        run_psql(ddl_file, is_file=True)
        logger.info("[+] Target tables and schemas provisioned.")
    logger.info("[i] Primary key constraints are managed in Step 06 (load-data) for fresh tables or upserts.")

    # 3. Catalog verification
    logger.info("Verifying provisioned tables in PostgreSQL catalog...")
    sql_check = """
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'sweep_ctl', 'sweep_meta')
    ORDER BY table_schema, table_name;
    """
    rows = query_db_rows(sql_check)
    res = []
    for r in rows:
        parts = r.split("|")
        if len(parts) >= 2:
            res.append({"schema": parts[0], "table": parts[1], "status": "EXISTS"})

    ts = get_timestamp()
    report_file = VSAM_REPORTS_DIR / f"{get_db_name()}_vsam_05_apply_ddl_report_{ts}.xlsx"
    VSAM_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generate_excel_report(report_file, {
        "ProvisionedTables": res
    })

    logger.info(f"Target Provisioning Complete: {len(res)} tables verified in catalog.")
    logger.info(f"Excel Report: {report_file}")

    log_summary(logger, Path(__file__).name, [
        ('tables provisioned in db', len(res)),
        ('note', 'PK constraints added in Step 07'),
    ])


if __name__ == "__main__":
    sys.exit(0 if main() is not False else 1)
