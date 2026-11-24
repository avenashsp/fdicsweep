#!/usr/bin/env python3
"""
=============================================================================
SWEEP Data Migration Framework - Unified Command Line Interface (CLI)
=============================================================================
Unified CLI orchestrating multi-terabyte data migration pipelines from
Oracle dumps and VSAM flat files to AWS RDS Aurora PostgreSQL.

Usage:
  python3 sweep.py <track> <command> [options]
  python3 sweep.py <command> [options]

Tracks:
  oracle       Oracle to PostgreSQL migration pipeline (Steps 01 to 08)
  vsam         VSAM to PostgreSQL migration pipeline (Steps 01 to 07/08)

Universal Operations:
  check        Pre-flight host environment, AWS S3, and RDS Aurora check
  status       Display live status of active/completed background migration jobs
  monitor      Launch real-time live ingestion monitoring dashboard (-w)
  logs         View or follow the latest execution logs (-f, -n 50)
  stop         Cleanly terminate running migration processes & database sessions
  run-all      Execute complete end-to-end pipeline (--track oracle|vsam|all)
=============================================================================
"""
import os
import sys
import argparse
import subprocess
from pathlib import Path

from sweep_common import (
    __version__,
    BASE_DIR, ORACLE_LOGS_DIR, VSAM_LOGS_DIR,
    ORACLE_INPUTS_DIR, ORACLE_OUTPUTS_DIR, ORACLE_REPORTS_DIR,
    VSAM_INPUTS_DIR, VSAM_OUTPUTS_DIR, VSAM_COPYBOOK_DIR, VSAM_REPORTS_DIR,
    PGHOST, PGPORT, PGDATABASE, PGUSER, has_db_auth, AWS_REGION, S3_BUCKET, ORACLE_S3_BUCKET, VSAM_S3_BUCKET,
    DEFAULT_WORKERS, PSQL_BIN, check_postgres_version, get_logger, run_psql
)
import sweep_bg

logger = get_logger("sweep_cli", to_file=False)


# -----------------------------------------------------------------------------
# Pre-Flight Environment & Connectivity Check
# -----------------------------------------------------------------------------
def cmd_check(args):
    """Performs pre-flight checks on EC2 host, AWS S3, and PostgreSQL."""
    def fmt_line(label: str, value: str, width: int = 33) -> str:
        prefix = f"[+] {label} "
        return prefix.ljust(width, "-") + " " + str(value)

    print("-" * 80)
    print("Pre-flight environment and connectivity diagnostics:\n")
    print("=" * 75)
    print(f"SWEEP UNIFIED PRE-FLIGHT ENVIRONMENT & CONNECTIVITY CHECK (v{__version__})")
    print("=" * 75)
    print(fmt_line("Framework Version", f"SWEEP v{__version__}"))
    print()

    # 1. Python & Core Dependencies (Zero psycopg2 dependency)
    print(fmt_line("Python Version", f"{sys.version.split()[0]} on {sys.platform}"))
    for mod in ["openpyxl", "pandas", "boto3", "dateutil", "sqlparse"]:
        try:
            __import__(mod)
            print(fmt_line(f"Module {mod}", "AVAILABLE"))
        except ImportError:
            print(fmt_line(f"Module {mod}", f"MISSING (pip install {mod})"))
    print()

    # 2. PostgreSQL Client CLI (psql) & Database Connectivity
    import shutil
    psql_path = shutil.which(PSQL_BIN)
    if psql_path:
        print(fmt_line("PSQL CLI check", f"SUCCESS ({psql_path})"))
    else:
        print(fmt_line("PSQL CLI check", f"WARNING ({PSQL_BIN} not found on PATH; required for all database activities)"))

    # Host & Port
    if PGHOST and PGPORT:
        print(fmt_line("Host & Port", f"SUCCESS ({PGHOST}:{PGPORT})"))
    else:
        print(fmt_line("Host & Port", "MISSING (PGHOST or PGPORT not set in sweep.env)"))

    # Username
    if PGUSER:
        print(fmt_line("Username", f"SUCCESS ({PGUSER})"))
    else:
        print(fmt_line("Username", "MISSING (PGUSER not set in sweep.env)"))

    # Target Database
    ssl_mode = os.getenv("PGSSLMODE", "prefer")
    if PGDATABASE:
        print(fmt_line("Target Database", f"SUCCESS ({PGDATABASE}) (SSL: {ssl_mode})"))
    else:
        print(fmt_line("Target Database", "MISSING (PGDATABASE not set in sweep.env)"))

    # Connection Test
    check_ok = bool(psql_path)
    if not has_db_auth():
        print(fmt_line("Connection", "FAILED (no credentials: set PGPASSWORD in sweep.env, or use ~/.pgpass / PGSERVICE)"))
        check_ok = False
    else:
        try:
            run_psql("SELECT 1;")
            print(fmt_line("Connection", "SUCCESS"))
            try:
                print(fmt_line("PostgreSQL version", f"SUCCESS (PostgreSQL {check_postgres_version()}; 16 or later required)"))
            except RuntimeError as e:
                print(fmt_line("PostgreSQL version", f"FAILED ({str(e)[:90]})"))
                check_ok = False
        except RuntimeError as e:
            err_line = str(e).splitlines()[0] if str(e) else "Connection failed"
            print(fmt_line("Connection", f"FAILED ({err_line[:60]})"))
            check_ok = False
    print()

    # 3. AWS S3 Access
    print(fmt_line("Region", AWS_REGION))

    oracle_b = getattr(args, "s3", None) or ORACLE_S3_BUCKET or S3_BUCKET
    if oracle_b and oracle_b.startswith("s3://"):
        print(fmt_line("Oracle S3 Bucket", f"SUCCESS ({oracle_b})"))
    elif oracle_b:
        print(fmt_line("Oracle S3 Bucket", f"WARNING (Invalid URI: {oracle_b})"))
    else:
        print(fmt_line("Oracle S3 Bucket", "NOT CONFIGURED (ORACLE_S3_BUCKET not set in sweep.env)"))

    vsam_b = getattr(args, "s3", None) or VSAM_S3_BUCKET
    if vsam_b and vsam_b.startswith("s3://"):
        print(fmt_line("VSAM S3 Bucket", f"SUCCESS ({vsam_b})"))
    elif vsam_b:
        print(fmt_line("VSAM S3 Bucket", f"WARNING (Invalid URI: {vsam_b})"))
    else:
        print(fmt_line("VSAM S3 Bucket", "NOT CONFIGURED (VSAM_S3_BUCKET not set in sweep.env)"))

    # Test bucket for Boto3 / AWS CLI
    test_bucket = vsam_b if (vsam_b and vsam_b.startswith("s3://")) else (oracle_b if (oracle_b and oracle_b.startswith("s3://")) else None)
    if test_bucket:
        b_name = test_bucket[5:].split("/")[0]
        try:
            import boto3
            s3_client = boto3.client("s3", region_name=AWS_REGION)
            s3_client.head_bucket(Bucket=b_name)
            print(fmt_line("Boto3 SDK Check", "SUCCESS"))
        except Exception as be:  # noqa: BLE001 - botocore raises many unrelated error types; this is a diagnostic
            err_clean = (str(be).splitlines() or ["check failed"])[0]
            print(fmt_line("Boto3 SDK Check", f"WARNING ({err_clean[:50]})"))

        try:
            res = subprocess.run(["aws", "s3api", "head-bucket", "--bucket", b_name, "--region", AWS_REGION],
                                 capture_output=True, text=True, timeout=60)
            if res.returncode == 0:
                print(fmt_line("AWS CLI Check", "SUCCESS"))
            else:
                err_clean = res.stderr.strip().splitlines()[0] if res.stderr else "HeadBucket failed"
                print(fmt_line("AWS CLI Check", f"WARNING ({err_clean[:50]})"))
        except (OSError, subprocess.SubprocessError) as e:
            print(fmt_line("AWS CLI Check", f"WARNING ({str(e)[:50]})"))
    else:
        print(fmt_line("Boto3 SDK Check", "SKIPPED (No S3 bucket configured)"))
        print(fmt_line("AWS CLI Check", "SKIPPED (No S3 bucket configured)"))
    print()

    # 4. Folder Structure & Recursive Input File Checks
    print(fmt_line("Workstation Dirs", str(BASE_DIR)))

    # Ensure output, report, and log directories exist silently
    for d in [ORACLE_OUTPUTS_DIR, ORACLE_REPORTS_DIR, ORACLE_LOGS_DIR, VSAM_OUTPUTS_DIR, VSAM_REPORTS_DIR, VSAM_LOGS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    def find_file_in_tree(base_dir: Path, target_name: str):
        if not base_dir.exists():
            return None, False
        target_lower = target_name.lower()
        direct = base_dir / target_name
        if direct.exists() and direct.is_file():
            return direct, True
        for p in base_dir.rglob("*"):
            if p.is_file() and p.name.lower() == target_lower:
                return p, True
        return None, False

    # Check oracle/inputs and subfolders
    oracle_items = []
    oracle_all_ok = True

    p_meta, ok_meta = find_file_in_tree(ORACLE_INPUTS_DIR, "sweep_meta.sql")
    if ok_meta:
        oracle_items.append("    - sweep_meta.sql")
    else:
        oracle_items.append("    - sweep_meta.sql - MISSING (not found in oracle/inputs or subfolders)")
        oracle_all_ok = False

    p_ddl, ok_ddl = find_file_in_tree(ORACLE_INPUTS_DIR, "oracle_master_ddl.sql")
    if not ok_ddl:
        for ext in ("*.sql", "*.ddl"):
            for f in ORACLE_INPUTS_DIR.rglob(ext):
                if f.is_file() and f.name.lower() != "sweep_meta.sql":
                    p_ddl, ok_ddl = f, True
                    break
            if ok_ddl:
                break

    if ok_ddl:
        oracle_items.append(f"    - {p_ddl.name}")
    else:
        oracle_items.append("    - oracle_master_ddl.sql - MISSING (not found in oracle/inputs or subfolders)")
        oracle_all_ok = False

    oracle_status = "AVAILABLE" if (oracle_all_ok and ORACLE_INPUTS_DIR.exists()) else "MISSING"
    print(fmt_line("oracle/inputs", oracle_status))
    for itm in oracle_items:
        print(itm)

    # Check vsam/inputs and subfolders
    vsam_items = []
    vsam_all_ok = True

    p_vmeta, ok_vmeta = find_file_in_tree(VSAM_INPUTS_DIR, "sweep_meta.sql")
    if ok_vmeta:
        vsam_items.append("    - sweep_meta.sql")
    else:
        vsam_items.append("    - sweep_meta.sql - MISSING (not found in vsam/inputs or subfolders)")
        vsam_all_ok = False

    p_vmap, ok_vmap = find_file_in_tree(VSAM_INPUTS_DIR, "vsam_master_mapping.xlsx")
    if ok_vmap:
        vsam_items.append("    - vsam_master_mapping.xlsx")
    else:
        vsam_items.append("    - vsam_master_mapping.xlsx - MISSING (not found in vsam/inputs or subfolders)")
        vsam_all_ok = False

    p_vovr, ok_vovr = find_file_in_tree(VSAM_INPUTS_DIR, "vsam_master_overrides.xlsx")
    if ok_vovr:
        vsam_items.append("    - vsam_master_overrides.xlsx")
    else:
        vsam_items.append("    - vsam_master_overrides.xlsx - MISSING (not found in vsam/inputs or subfolders)")
        vsam_all_ok = False

    cb_dir = VSAM_COPYBOOK_DIR
    if not cb_dir.exists():
        cb_candidates = [d for d in VSAM_INPUTS_DIR.rglob("copybooks") if d.is_dir()]
        cb_dir = cb_candidates[0] if cb_candidates else cb_dir

    cb_count = 0
    if cb_dir.exists():
        cb_files = [f for f in cb_dir.rglob("*") if f.is_file() and not f.name.startswith(".")]
        cb_count = len(cb_files)

    if cb_count > 0:
        vsam_items.append(f"    - copybooks ({cb_count})")
    else:
        vsam_items.append("    - copybooks (0) - MISSING (no copybook files found in vsam/inputs/copybooks)")
        vsam_all_ok = False

    vsam_status = "AVAILABLE" if (vsam_all_ok and VSAM_INPUTS_DIR.exists()) else "MISSING"
    print(fmt_line("vsam/inputs", vsam_status))
    for itm in vsam_items:
        print(itm)

    print("=" * 75)
    return check_ok


# -----------------------------------------------------------------------------
# Dispatch Execution Helper
# -----------------------------------------------------------------------------
ORACLE_STEPS = [
    "oracle_01_extract_mtd", "oracle_02_inventory", "oracle_03_type_gate", "oracle_04_generate_ddl",
    "oracle_05_apply_ddl", "oracle_06_load_data", "oracle_07_load_objects", "oracle_08_validate",
]
VSAM_STEPS = [
    "vsam_01_extract_copybooks", "vsam_02_inventory", "vsam_03_type_gate", "vsam_04_generate_ddl",
    "vsam_05_apply_ddl", "vsam_06_load_data", "vsam_07_validate",
]


def step(track, module, *args):
    """
    Command line for one pipeline step. Steps run as modules from the repository root
    (`python3 -m oracle.oracle_05_apply_ddl`), so imports resolve without any sys.path patching.
    """
    return [sys.executable, "-m", f"{track}.{module}", *[str(a) for a in args]]


def child_env():
    """Environment for step subprocesses: repository root first on PYTHONPATH."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(BASE_DIR), env.get("PYTHONPATH", "")]))
    return env


def run_or_bg(job_name, cmd_args, run_in_bg=False):
    """Executes a command synchronously (exit code propagated) or spawns a background daemon supervisor."""
    if run_in_bg:
        sweep_bg.start_background_job(job_name, cmd_args)
    else:
        res = subprocess.run(cmd_args, cwd=str(BASE_DIR), env=child_env())
        if res.returncode != 0:
            sys.exit(res.returncode)


def run_pipeline(label, commands):
    """Runs step commands in order, stopping at the first failure. Returns the exit code."""
    for cmd in commands:
        print(f"\n=== EXECUTING: {' '.join(cmd)} ===")
        res = subprocess.run(cmd, cwd=str(BASE_DIR), env=child_env())
        if res.returncode != 0:
            print(f"Step failed with code {res.returncode}. Aborting pipeline.")
            return res.returncode
    print(f"\n[+] {label} Pipeline Completed Successfully!")
    return 0


def pipeline_or_bg(job_name, label, commands, bg_command, run_in_bg):
    """Runs a full pipeline in the foreground (exiting non-zero on failure) or as one background job."""
    if run_in_bg:
        sweep_bg.start_background_job(job_name, bg_command)
        return
    code = run_pipeline(label, commands)
    if code != 0:
        sys.exit(code)


# -----------------------------------------------------------------------------
# Oracle Track Command Handlers
# -----------------------------------------------------------------------------
def handle_oracle(args):
    cmd = args.oracle_cmd
    if not cmd:
        print("Usage: python3 sweep.py oracle <command> [options]")
        print("Commands: extract, inventory, gate, gen-ddl, apply-ddl, load-data, load-obj, validate, run-all")
        sys.exit(0)

    if cmd == "extract":
        c = step("oracle", "oracle_01_extract_mtd")
        if getattr(args, "file", None): c.extend(["--file", args.file])
        if getattr(args, "owners", None): c.extend(["--owners", args.owners])
        run_or_bg("oracle_01_extract_mtd", c, getattr(args, "bg", False))

    elif cmd == "inventory":
        c = step("oracle", "oracle_02_inventory")
        if getattr(args, "s3", None): c.extend(["--s3", args.s3])
        if getattr(args, "owners", None): c.extend(["--owners", args.owners])
        if getattr(args, "tables", None): c.extend(["--tables", args.tables])
        run_or_bg("oracle_02_inventory", c, getattr(args, "bg", False))

    elif cmd == "gate":
        c = step("oracle", "oracle_03_type_gate", "--rows", str(getattr(args, "rows", 1000)))
        if getattr(args, "tables", None): c.extend(["--tables", args.tables])
        if getattr(args, "max_tables", None): c.extend(["--max-tables", str(args.max_tables)])
        run_or_bg("oracle_03_type_gate", c, getattr(args, "bg", False))

    elif cmd in ("gen-ddl", "ddl"):
        c = step("oracle", "oracle_04_generate_ddl")
        if getattr(args, "owners", None): c.extend(["--owners", args.owners])
        if getattr(args, "tables", None): c.extend(["--tables", args.tables])
        run_or_bg("oracle_04_generate_ddl", c, getattr(args, "bg", False))

    elif cmd == "apply-ddl":
        c = step("oracle", "oracle_05_apply_ddl")
        if getattr(args, "schemas", None): c.extend(["--schemas", args.schemas])
        if getattr(args, "skip_meta", False): c.append("--skip-meta")
        run_or_bg("oracle_05_apply_ddl", c, getattr(args, "bg", False))

    elif cmd in ("load-data", "load"):
        c = step("oracle", "oracle_06_load_data", "--workers", str(getattr(args, "workers", DEFAULT_WORKERS)))
        if getattr(args, "tables", None): c.extend(["--tables", args.tables])
        if getattr(args, "exclude_tables", None): c.extend(["--exclude-tables", args.exclude_tables])
        if getattr(args, "file", None): c.extend(["--file", args.file])
        if getattr(args, "filename", None): c.extend(["--filename", args.filename])
        if getattr(args, "gate_pass", False): c.append("--gate-pass")
        if getattr(args, "limit_files", None): c.extend(["--limit-files", str(args.limit_files)])
        if getattr(args, "max_size_mb", None) is not None: c.extend(["--max-size-mb", str(args.max_size_mb)])
        if getattr(args, "min_size_mb", None) is not None: c.extend(["--min-size-mb", str(args.min_size_mb)])
        if getattr(args, "dry_run", False): c.append("--dry-run")
        if getattr(args, "reload", False): c.append("--reload")
        if getattr(args, "max_rejects", None) is not None: c.extend(["--max-rejects", str(args.max_rejects)])
        if getattr(args, "fast_bulk", False): c.append("--fast-bulk")
        if getattr(args, "chunk_rows", None) is not None: c.extend(["--chunk-rows", str(args.chunk_rows)])
        if getattr(args, "chunk_min_mb", None) is not None: c.extend(["--chunk-min-mb", str(args.chunk_min_mb)])
        if getattr(args, "trigger_mode", None): c.extend(["--trigger-mode", args.trigger_mode])
        run_or_bg("oracle_06_load_data", c, getattr(args, "bg", False))

    elif cmd in ("load-obj", "post-load"):
        c = step("oracle", "oracle_07_load_objects", "--stage", getattr(args, "stage", "all"))
        if getattr(args, "schemas", None): c.extend(["--schemas", args.schemas])
        if getattr(args, "only_loaded", False): c.append("--only-loaded")
        run_or_bg("oracle_07_load_objects", c, getattr(args, "bg", False))

    elif cmd in ("validate", "verify"):
        c = step("oracle", "oracle_08_validate")
        if getattr(args, "exact_counts", False): c.append("--exact-counts")
        if getattr(args, "tables", None): c.extend(["--tables", args.tables])
        if getattr(args, "schemas", None): c.extend(["--schemas", args.schemas])
        if getattr(args, "file", None): c.extend(["--file", args.file])
        run_or_bg("oracle_08_validate", c, getattr(args, "bg", False))

    elif cmd == "run-all":
        workers = getattr(args, "workers", DEFAULT_WORKERS)
        commands = [step("oracle", m, *(["--workers", workers] if m == "oracle_06_load_data" else [])) for m in ORACLE_STEPS]
        bg_cmd = [sys.executable, str(BASE_DIR / "sweep.py"), "oracle", "run-all", "--workers", str(workers)]
        pipeline_or_bg("oracle_full_pipeline", "Oracle Full", commands, bg_cmd, getattr(args, "bg", False))


# -----------------------------------------------------------------------------
# VSAM Track Command Handlers
# -----------------------------------------------------------------------------
def handle_vsam(args):
    cmd = args.vsam_cmd
    if not cmd:
        print("Usage: python3 sweep.py vsam <command> [options]")
        print("Commands: extract, inventory, type-gate, gen-ddl, apply-ddl, load-data, load-obj, validate, run-all")
        sys.exit(0)

    # Step 01: Extract copybook metadata
    if cmd in ("extract", "extract-copybooks"):
        c = step("vsam", "vsam_01_extract_copybooks")
        run_or_bg("vsam_01_extract_copybooks", c, getattr(args, "bg", False))

    # Step 02: Inventory & S3 mapping
    elif cmd == "inventory":
        c = step("vsam", "vsam_02_inventory")
        if getattr(args, "s3", None): c.extend(["--s3", args.s3])
        if getattr(args, "company", None): c.extend(["--company", args.company])
        if getattr(args, "tbl_col", None): c.extend(["--tbl-col", args.tbl_col])
        run_or_bg("vsam_02_inventory", c, getattr(args, "bg", False))

    # Step 03: Type Gate - PK verification & schema col detection
    elif cmd in ("type-gate", "gate"):
        c = step("vsam", "vsam_03_type_gate")
        if getattr(args, "workers", None): c.extend(["--workers", str(args.workers)])
        if getattr(args, "company", None): c.extend(["--company", args.company])
        run_or_bg("vsam_03_type_gate", c, getattr(args, "bg", False))

    # Step 04: Generate DDL (pre_load + post_load)
    elif cmd in ("gen-ddl", "ddl"):
        c = step("vsam", "vsam_04_generate_ddl")
        if getattr(args, "company", None): c.extend(["--company", args.company])
        if getattr(args, "execute", False): c.append("--execute")
        run_or_bg("vsam_04_generate_ddl", c, getattr(args, "bg", False))

    # Step 05: Apply pre-load DDL (tables only, no PK)
    elif cmd == "apply-ddl":
        c = step("vsam", "vsam_05_apply_ddl")
        if getattr(args, "skip_meta", False): c.append("--skip-meta")
        run_or_bg("vsam_05_apply_ddl", c, getattr(args, "bg", False))

    # Step 06: Load data with dynamic upsert and schema segregation (includes PK application in default mode)
    elif cmd in ("load-data", "load", "load-obj", "load-objects", "post-load"):
        c = step("vsam", "vsam_06_load_data", "--workers", str(getattr(args, "workers", DEFAULT_WORKERS)))
        if getattr(args, "company", None): c.extend(["--company", args.company])
        if getattr(args, "tables", None): c.extend(["--tables", args.tables])
        if getattr(args, "limit_files", None): c.extend(["--limit-files", str(args.limit_files)])
        if getattr(args, "force_truncate", False): c.append("--force-truncate")
        if getattr(args, "dry_run", False): c.append("--dry-run")
        if getattr(args, "reload", False): c.append("--reload")
        if getattr(args, "max_rejects", None) is not None: c.extend(["--max-rejects", str(args.max_rejects)])
        run_or_bg("vsam_06_load_data", c, getattr(args, "bg", False))

    # Step 07: Validate
    elif cmd in ("validate", "verify"):
        c = step("vsam", "vsam_07_validate")
        if getattr(args, "exact_counts", False): c.append("--exact-counts")
        if getattr(args, "tables", None): c.extend(["--tables", args.tables])
        if getattr(args, "schema", None): c.extend(["--schema", args.schema])
        run_or_bg("vsam_07_validate", c, getattr(args, "bg", False))

    elif cmd == "run-all":
        workers = getattr(args, "workers", DEFAULT_WORKERS)
        extra = ["--force-truncate"] if getattr(args, "force_truncate", False) else []
        tbl = ["--tbl-col", args.tbl_col] if getattr(args, "tbl_col", None) else []
        commands = [step("vsam", m, *(["--workers", workers, *extra] if m == "vsam_06_load_data" else tbl if m == "vsam_02_inventory" else [])) for m in VSAM_STEPS]
        bg_cmd = [sys.executable, str(BASE_DIR / "sweep.py"), "vsam", "run-all", "--workers", str(workers), *extra, *tbl]
        pipeline_or_bg("vsam_full_pipeline", "VSAM Full", commands, bg_cmd, getattr(args, "bg", False))


# -----------------------------------------------------------------------------
# Global Run-All Command
# -----------------------------------------------------------------------------
def cmd_run_all(args):
    track = getattr(args, "track", "all").lower()
    workers = getattr(args, "workers", DEFAULT_WORKERS)
    if track == "oracle":
        args.oracle_cmd = "run-all"
        handle_oracle(args)
    elif track == "vsam":
        args.vsam_cmd = "run-all"
        handle_vsam(args)
    else:
        logger.info("Executing End-to-End Migration for BOTH Oracle and VSAM tracks...")
        extra = ["--force-truncate"] if getattr(args, "force_truncate", False) else []
        tbl = ["--tbl-col", args.tbl_col] if getattr(args, "tbl_col", None) else []
        oracle_cmds = [step("oracle", m, *(["--workers", workers] if m == "oracle_06_load_data" else [])) for m in ORACLE_STEPS]
        vsam_cmds = [step("vsam", m, *(["--workers", workers, *extra] if m == "vsam_06_load_data" else tbl if m == "vsam_02_inventory" else [])) for m in VSAM_STEPS]
        bg_cmd = [sys.executable, str(BASE_DIR / "sweep.py"), "run-all", "--track", "all", "--workers", str(workers), *extra, *tbl]
        pipeline_or_bg("sweep_all_tracks", "Oracle + VSAM", oracle_cmds + vsam_cmds, bg_cmd, getattr(args, "bg", False))


# -----------------------------------------------------------------------------
# Main Argument Parser
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        prog="sweep",
        description="Unified SWEEP CLI for Oracle and VSAM to AWS RDS Aurora PostgreSQL Migration",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-v", "--version", action="version", version=f"SWEEP Unified Migration Framework v{__version__}")
    subparsers = parser.add_subparsers(dest="command", help="Primary commands or tracks")

    # check
    p_check = subparsers.add_parser("check", help="Pre-flight environment and connection check")
    p_check.add_argument("--s3", help="Target S3 URI to test")

    # run-all
    p_all = subparsers.add_parser("run-all", help="Execute complete pipeline end-to-end")
    p_all.add_argument("--track", choices=["oracle", "vsam", "all"], default="all", help="Migration track to execute (default: all)")
    p_all.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    p_all.add_argument("--force-truncate", action="store_true", help="Pass --force-truncate to the VSAM load step")
    p_all.add_argument("--tbl-col", choices=["headers", "copybook"], help="VSAM table column names: S3 file headers (default) or copybook column names")
    p_all.add_argument("--bg", action="store_true", help="Run full pipeline as detached background daemon")

    # Background operations
    subparsers.add_parser("status", help="Show live status of background migration jobs")
    p_mon = subparsers.add_parser("monitor", help="Live throughput and progress dashboard")
    p_mon.add_argument("-w", "--watch", action="store_true", help="Continuously refresh dashboard")
    p_mon.add_argument("--interval", type=int, default=3, help="Refresh interval in seconds (default: 3)")
    p_mon.add_argument("--no-color", action="store_true", help="Disable colored output")
    p_mon.add_argument("--track", choices=["all", "oracle", "vsam"], default="all", help="Track to monitor: oracle, vsam, or all (default: all)")

    p_log = subparsers.add_parser("logs", aliases=["log"], help="View or tail execution logs")
    p_log.add_argument("-n", "--lines", type=int, default=40, help="Lines to tail (default: 40)")
    p_log.add_argument("-f", "--follow", action="store_true", help="Follow live output")
    p_log.add_argument("--track", choices=["oracle", "vsam"], help="Filter logs by track (oracle or vsam)")
    p_log.add_argument("--file", help="Specific log file path to view")

    p_stop = subparsers.add_parser("stop", help="Stop running migration jobs")
    p_stop.add_argument("--name", help="Specific job name to stop")
    p_stop.add_argument("--kill-db", action="store_true", help="Also terminate in-flight PostgreSQL sessions opened by SWEEP (application_name 'sweep')")

    # -------------------------------------------------------------------------
    # Track: ORACLE
    # -------------------------------------------------------------------------
    p_oracle = subparsers.add_parser("oracle", help="Oracle to PostgreSQL migration pipeline")
    o_sub = p_oracle.add_subparsers(dest="oracle_cmd", help="Oracle subcommands")

    o_ext = o_sub.add_parser("extract", help="Step 01: Extract metadata from master Oracle SQL dump")
    o_ext.add_argument("--file", help="Specific dump file path")
    o_ext.add_argument("--owners", help="Comma-separated owners to filter")
    o_ext.add_argument("--bg", action="store_true", help="Run in background")

    o_inv = o_sub.add_parser("inventory", help="Step 02: S3 Discovery & Small-to-Large Ordering")
    o_inv.add_argument("--s3", help="Target S3 prefix")
    o_inv.add_argument("--owners", help="Comma-separated owners")
    o_inv.add_argument("--tables", help="Comma-separated tables")
    o_inv.add_argument("--bg", action="store_true", help="Run in background")

    o_gate = o_sub.add_parser("gate", help="Step 03: Pre-flight Data Type Gate")
    o_gate.add_argument("--rows", type=int, default=1000, help="Rows to sample per file")
    o_gate.add_argument("--tables", help="Comma-separated tables")
    o_gate.add_argument("--max-tables", type=int, help="Limit number of tables tested")
    o_gate.add_argument("--bg", action="store_true", help="Run in background")

    o_ddl = o_sub.add_parser("gen-ddl", aliases=["ddl"], help="Step 04: Generate Bare Tables & Partitions")
    o_ddl.add_argument("--owners", help="Comma-separated owners")
    o_ddl.add_argument("--tables", help="Comma-separated tables")
    o_ddl.add_argument("--bg", action="store_true", help="Run in background")

    o_apply = o_sub.add_parser("apply-ddl", help="Step 05: Apply DDL to PostgreSQL")
    o_apply.add_argument("--schemas", help="Comma-separated schemas")
    o_apply.add_argument("--skip-meta", action="store_true", help="Skip re-deploying sweep_meta.sql")
    o_apply.add_argument("--bg", action="store_true", help="Run in background")

    o_load = o_sub.add_parser("load-data", aliases=["load"], help="Step 06: S3 Streaming Ingestion")
    o_load.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    o_load.add_argument("--tables", help="Comma-separated tables")
    o_load.add_argument("--exclude-tables", help="Comma-separated tables to exclude")
    o_load.add_argument("--file", help="Specific S3 URI to load")
    o_load.add_argument("--filename", help="Specific S3 filename/basename to load")
    o_load.add_argument("--gate-pass", action="store_true", help="Load only passed tables")
    o_load.add_argument("--limit-files", type=int, help="Limit files loaded")
    o_load.add_argument("--max-size-mb", type=float, help="Filter files with size <= max_size_mb")
    o_load.add_argument("--min-size-mb", type=float, help="Filter files with size >= min_size_mb")
    o_load.add_argument("--dry-run", action="store_true", help="Simulate load")
    o_load.add_argument("--reload", action="store_true", help="Force reloading/upserting files even if previously loaded")
    o_load.add_argument("--max-rejects", type=int, default=None, help="Fail a file when this many rows are rejected (default 0 = never fail; N = fail the file at N rejects)")
    o_load.add_argument("--fast-bulk", action="store_true", help="Direct COPY into empty constraint-free tables (no bad-row isolation)")
    o_load.add_argument("--chunk-rows", type=int, default=None, help="Rows per chunk for staged loads of big files (default 2,000,000; 0 = never chunk)")
    o_load.add_argument("--chunk-min-mb", type=float, default=None, help="Only files at least this big (compressed MB) are chunked (default 256)")
    o_load.add_argument("--trigger-mode", choices=["auto", "replica", "disable"], default=None, help="How triggers are switched off while loading")
    o_load.add_argument("--bg", action="store_true", help="Run in background daemon")

    o_obj = o_sub.add_parser("load-obj", aliases=["post-load"], help="Step 07: Post-Load Keys, Indexes, Views, Triggers")
    o_obj.add_argument("--stage", choices=["all", "pk", "alter", "index", "fk", "validate", "views", "triggers"], default="all")
    o_obj.add_argument("--schemas", help="Comma-separated schemas")
    o_obj.add_argument("--only-loaded", action="store_true", help="Only load objects for tables with loaded data (default: all tables)")
    o_obj.add_argument("--bg", action="store_true", help="Run in background")

    o_val = o_sub.add_parser("validate", aliases=["verify"], help="Step 08: Reconciliation & Audit Tally")
    o_val.add_argument("--exact-counts", action="store_true", help="Perform exact count(*) queries")
    o_val.add_argument("--tables", help="Comma-separated tables")
    o_val.add_argument("--schemas", help="Comma-separated schemas")
    o_val.add_argument("--file", help="Specific master Oracle SQL dump file path")
    o_val.add_argument("--bg", action="store_true", help="Run in background")

    o_run_all = o_sub.add_parser("run-all", help="Execute complete Oracle pipeline end-to-end")
    o_run_all.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    o_run_all.add_argument("--bg", action="store_true", help="Run full Oracle pipeline in background daemon")

    # -------------------------------------------------------------------------
    # Track: VSAM (7-Step Pipeline)
    # -------------------------------------------------------------------------
    p_vsam = subparsers.add_parser("vsam", help="VSAM to PostgreSQL migration pipeline")
    v_sub = p_vsam.add_subparsers(dest="vsam_cmd", help="VSAM subcommands")

    v_ext = v_sub.add_parser("extract", aliases=["extract-copybooks"], help="Step 01: Extract copybook metadata (fields, types, keys)")
    v_ext.add_argument("--bg", action="store_true", help="Run in background")

    v_inv = v_sub.add_parser("inventory", help="Step 02: S3 Discovery & Source-to-Target Mapping")
    v_inv.add_argument("--s3", help="Target S3 prefix")
    v_inv.add_argument("--company", help="Comma-separated companies")
    v_inv.add_argument("--tbl-col", choices=["headers", "copybook"], help="Table column names: S3 file headers (default) or mapped copybook column names")
    v_inv.add_argument("--bg", action="store_true", help="Run in background")

    v_gate = v_sub.add_parser("type-gate", aliases=["gate"], help="Step 03: PK uniqueness verification & schema column detection")
    v_gate.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    v_gate.add_argument("--company", help="Comma-separated company tokens to filter")
    v_gate.add_argument("--bg", action="store_true", help="Run in background")

    v_ddl = v_sub.add_parser("gen-ddl", aliases=["ddl"], help="Step 04: Generate DDL (tables + PK in gen_ddl)")
    v_ddl.add_argument("--company", help="Comma-separated companies")
    v_ddl.add_argument("--execute", action="store_true", help="Execute generated pre-load DDL")
    v_ddl.add_argument("--bg", action="store_true", help="Run in background")

    v_apply = v_sub.add_parser("apply-ddl", help="Step 05: Apply table DDL (tables only, no PK) to PostgreSQL")
    v_apply.add_argument("--skip-meta", action="store_true", help="Skip sweep_meta.sql")
    v_apply.add_argument("--bg", action="store_true", help="Run in background")

    v_load = v_sub.add_parser("load-data", aliases=["load"], help="Step 06: Schema-segregated S3 streaming ingestion & dynamic upsert")
    v_load.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    v_load.add_argument("--company", help="Comma-separated companies")
    v_load.add_argument("--tables", help="Comma-separated tables")
    v_load.add_argument("--limit-files", type=int, help="Limit files loaded")
    v_load.add_argument("--force-truncate", action="store_true", help="Allow replacing a table that already has rows and no PK in a PARTIAL run (--company/--tables/--limit-files); full runs replace such tables automatically")
    v_load.add_argument("--dry-run", action="store_true", help="List the files that would be loaded without writing to the database")
    v_load.add_argument("--reload", action="store_true", help="Load files again even when their S3 LastModified/ETag/size are unchanged")
    v_load.add_argument("--max-rejects", type=int, default=None, help="Fail a file when this many rows are rejected (default 0 = never fail; N = fail the file at N rejects)")
    v_load.add_argument("--bg", action="store_true", help="Run in background daemon")

    v_lobj = v_sub.add_parser("load-obj", aliases=["load-objects", "post-load"], help="Legacy alias: Merged into Step 06 (load-data)")
    v_lobj.add_argument("--schema", help="Apply PK only for this schema")
    v_lobj.add_argument("--bg", action="store_true", help="Run in background")

    v_val = v_sub.add_parser("validate", aliases=["verify"], help="Step 07: Row counts, column, type, PK & reconciliation audit")
    v_val.add_argument("--exact-counts", action="store_true", help="Exact row count queries")
    v_val.add_argument("--tables", help="Comma-separated tables")
    v_val.add_argument("--schema", help="Validate only this schema")
    v_val.add_argument("--bg", action="store_true", help="Run in background")

    v_runall = v_sub.add_parser("run-all", help="Execute complete VSAM 7-step pipeline end-to-end")
    v_runall.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    v_runall.add_argument("--force-truncate", action="store_true", help="Pass --force-truncate to the VSAM load step")
    v_runall.add_argument("--tbl-col", choices=["headers", "copybook"], help="Table column names: S3 file headers (default) or copybook column names")
    v_runall.add_argument("--bg", action="store_true", help="Run full pipeline in background")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    if args.command == "check":
        sys.exit(0 if cmd_check(args) else 1)
    elif args.command == "oracle":
        handle_oracle(args)
    elif args.command == "vsam":
        handle_vsam(args)
    elif args.command == "run-all":
        cmd_run_all(args)
    elif args.command == "status":
        sweep_bg.print_status()
    elif args.command == "monitor":
        sweep_bg.monitor_live(
            interval=args.interval,
            watch=args.watch,
            use_color=not getattr(args, "no_color", False),
            track=getattr(args, "track", "all")
        )
    elif args.command in ("logs", "log"):
        sweep_bg.tail_logs(lines=args.lines, follow=args.follow, log_file=args.file, track=getattr(args, "track", None))
    elif args.command == "stop":
        sweep_bg.stop_jobs(job_name=args.name, kill_pg_backends=args.kill_db)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
