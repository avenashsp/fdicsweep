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
from sweep_targeting import add_targeting_args
from sweep_layers import add_layer_arg

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


TARGET_FLAGS = ("tables", "schemas", "exclude_tables", "file", "filename", "company", "copybook")


def targeting(args, *, skip=()):
    """Command line flags for the uniform targeting options the user gave (--tables, --schemas, ... --dry-run)."""
    out = []
    schemas = getattr(args, "schemas", None) or getattr(args, "owners", None) or getattr(args, "schema", None)
    values = {name: getattr(args, name, None) for name in TARGET_FLAGS}
    values["schemas"] = schemas
    for name in TARGET_FLAGS:
        if name in skip or not values[name]:
            continue
        out.extend([f"--{name.replace('_', '-')}", str(values[name])])
    if getattr(args, "dry_run", False):
        out.append("--dry-run")
    return out


# flags each step accepts when run-all passes the targeting options on (--file is never passed: it is ambiguous across steps)
_TBL = ("tables", "schemas", "exclude_tables")
RUNALL_FLAGS = {
    "vsam_01_extract_copybooks": ("copybook",),
    "vsam_02_inventory": _TBL + ("company",), "vsam_03_type_gate": _TBL + ("company",), "vsam_04_generate_ddl": _TBL + ("company",),
    "vsam_05_apply_ddl": _TBL, "vsam_06_load_data": _TBL + ("company",), "vsam_07_validate": _TBL + ("company",),
}


DB_STEPS = {"oracle_04_generate_ddl", "oracle_05_apply_ddl", "oracle_06_load_data", "oracle_07_load_objects", "oracle_08_validate",
            "vsam_04_generate_ddl", "vsam_05_apply_ddl", "vsam_06_load_data", "vsam_07_validate"}


def runall_targeting(args, module):
    """Targeting flags (and --layer for the steps that touch a database) for one step of run-all."""
    if module == "vsam_03_type_gate" and getattr(args, "rows", None):
        return ["--rows", str(args.rows)] + _runall_targeting(args, module)
    return _runall_targeting(args, module)


def _runall_targeting(args, module):
    allowed = RUNALL_FLAGS.get(module, _TBL)
    out = [f for f in targeting(args, skip=tuple(n for n in TARGET_FLAGS if n not in allowed)) if f != "--dry-run"]
    if getattr(args, "dry_run", False):
        out.append("--dry-run")
    if module in DB_STEPS:
        out.extend(layer_flags(args))
    return out


def layer_flags(args):
    """--layer / --allow-ddl for the steps that touch a database (the layer is passed down so every step guards its database)."""
    out = []
    if getattr(args, "layer", None):
        out.extend(["--layer", args.layer])
    if getattr(args, "allow_ddl", False):
        out.append("--allow-ddl")
    return out


def selectors_given(args):
    return bool(targeting(args, skip=("file", "filename"))) or bool(getattr(args, "file", None) or getattr(args, "filename", None))


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
        if getattr(args, "file", None): c.extend(["--file", args.file])      # the dump file, not an S3 file
        c.extend(targeting(args, skip=("file", "filename")))
        run_or_bg("oracle_01_extract_mtd", c, getattr(args, "bg", False))

    elif cmd == "inventory":
        c = step("oracle", "oracle_02_inventory")
        if getattr(args, "s3", None): c.extend(["--s3", args.s3])
        c.extend(targeting(args))
        run_or_bg("oracle_02_inventory", c, getattr(args, "bg", False))

    elif cmd == "gate":
        c = step("oracle", "oracle_03_type_gate", "--rows", str(getattr(args, "rows", 1000)))
        c.extend(targeting(args))
        if getattr(args, "max_tables", None): c.extend(["--max-tables", str(args.max_tables)])
        run_or_bg("oracle_03_type_gate", c, getattr(args, "bg", False))

    elif cmd in ("gen-ddl", "ddl"):
        c = step("oracle", "oracle_04_generate_ddl")
        c.extend(targeting(args))
        c.extend(layer_flags(args))
        run_or_bg("oracle_04_generate_ddl", c, getattr(args, "bg", False))

    elif cmd == "apply-ddl":
        c = step("oracle", "oracle_05_apply_ddl")
        c.extend(targeting(args))
        c.extend(layer_flags(args))
        if getattr(args, "skip_meta", False): c.append("--skip-meta")
        run_or_bg("oracle_05_apply_ddl", c, getattr(args, "bg", False))

    elif cmd in ("load-data", "load"):
        c = step("oracle", "oracle_06_load_data", "--workers", str(getattr(args, "workers", DEFAULT_WORKERS)))
        c.extend(targeting(args))
        c.extend(layer_flags(args))
        if getattr(args, "full", False): c.append("--full")
        if getattr(args, "gate_pass", False): c.append("--gate-pass")
        if getattr(args, "limit_files", None): c.extend(["--limit-files", str(args.limit_files)])
        if getattr(args, "max_size_mb", None) is not None: c.extend(["--max-size-mb", str(args.max_size_mb)])
        if getattr(args, "min_size_mb", None) is not None: c.extend(["--min-size-mb", str(args.min_size_mb)])
        if getattr(args, "reload", False): c.append("--reload")
        if getattr(args, "max_rejects", None) is not None: c.extend(["--max-rejects", str(args.max_rejects)])
        if getattr(args, "fast_bulk", False): c.append("--fast-bulk")
        if getattr(args, "trigger_mode", None): c.extend(["--trigger-mode", args.trigger_mode])
        run_or_bg("oracle_06_load_data", c, getattr(args, "bg", False))

    elif cmd in ("load-obj", "post-load"):
        c = step("oracle", "oracle_07_load_objects", "--stage", getattr(args, "stage", "all"))
        c.extend(targeting(args))
        c.extend(layer_flags(args))
        if getattr(args, "only_loaded", False): c.append("--only-loaded")
        run_or_bg("oracle_07_load_objects", c, getattr(args, "bg", False))

    elif cmd in ("validate", "verify"):
        c = step("oracle", "oracle_08_validate")
        if getattr(args, "exact_counts", False): c.append("--exact-counts")
        if getattr(args, "file", None): c.extend(["--file", args.file])      # the dump file
        c.extend(targeting(args, skip=("file", "filename")))
        c.extend(layer_flags(args))
        run_or_bg("oracle_08_validate", c, getattr(args, "bg", False))

    elif cmd == "run-all":
        workers = getattr(args, "workers", DEFAULT_WORKERS)
        commands = [step("oracle", m, *(["--workers", workers] if m == "oracle_06_load_data" else []), *runall_targeting(args, m)) for m in ORACLE_STEPS]
        bg_cmd = [sys.executable, str(BASE_DIR / "sweep.py"), "oracle", "run-all", "--workers", str(workers), *targeting(args, skip=("file", "filename", "company", "copybook"))]
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
        c.extend(targeting(args))
        run_or_bg("vsam_01_extract_copybooks", c, getattr(args, "bg", False))

    # Step 02: Inventory & S3 mapping
    elif cmd == "inventory":
        c = step("vsam", "vsam_02_inventory")
        if getattr(args, "s3", None): c.extend(["--s3", args.s3])
        c.extend(targeting(args))
        if getattr(args, "tbl_col", None): c.extend(["--tbl-col", args.tbl_col])
        run_or_bg("vsam_02_inventory", c, getattr(args, "bg", False))

    # Step 03: Type Gate - PK verification & schema col detection
    elif cmd in ("type-gate", "gate"):
        c = step("vsam", "vsam_03_type_gate")
        if getattr(args, "workers", None): c.extend(["--workers", str(args.workers)])
        if getattr(args, "rows", None): c.extend(["--rows", str(args.rows)])
        c.extend(targeting(args))
        run_or_bg("vsam_03_type_gate", c, getattr(args, "bg", False))

    # Step 04: Generate DDL (pre_load + post_load)
    elif cmd in ("gen-ddl", "ddl"):
        c = step("vsam", "vsam_04_generate_ddl")
        c.extend(targeting(args, skip=("file", "filename")))
        c.extend(layer_flags(args))
        if getattr(args, "execute", False): c.append("--execute")
        run_or_bg("vsam_04_generate_ddl", c, getattr(args, "bg", False))

    # Step 05: Apply pre-load DDL (tables only, no PK)
    elif cmd == "apply-ddl":
        c = step("vsam", "vsam_05_apply_ddl")
        c.extend(targeting(args, skip=("file", "filename", "company")))
        c.extend(layer_flags(args))
        if getattr(args, "skip_meta", False): c.append("--skip-meta")
        run_or_bg("vsam_05_apply_ddl", c, getattr(args, "bg", False))

    # Step 06: Load data with dynamic upsert and schema segregation (includes PK application in default mode)
    elif cmd in ("load-data", "load", "load-obj", "load-objects", "post-load"):
        c = step("vsam", "vsam_06_load_data", "--workers", str(getattr(args, "workers", DEFAULT_WORKERS)))
        c.extend(targeting(args))
        c.extend(layer_flags(args))
        if getattr(args, "full", False): c.append("--full")
        if getattr(args, "limit_files", None): c.extend(["--limit-files", str(args.limit_files)])
        if getattr(args, "force_truncate", False): c.append("--force-truncate")
        if getattr(args, "reload", False): c.append("--reload")
        if getattr(args, "max_rejects", None) is not None: c.extend(["--max-rejects", str(args.max_rejects)])
        run_or_bg("vsam_06_load_data", c, getattr(args, "bg", False))

    # Step 07: Validate
    elif cmd in ("validate", "verify"):
        c = step("vsam", "vsam_07_validate")
        if getattr(args, "exact_counts", False): c.append("--exact-counts")
        c.extend(targeting(args, skip=("file", "filename")))
        c.extend(layer_flags(args))
        run_or_bg("vsam_07_validate", c, getattr(args, "bg", False))

    elif cmd == "run-all":
        workers = getattr(args, "workers", DEFAULT_WORKERS)
        extra = ["--force-truncate"] if getattr(args, "force_truncate", False) else []
        tbl = ["--tbl-col", args.tbl_col] if getattr(args, "tbl_col", None) else []
        commands = [step("vsam", m, *(["--workers", workers, *extra] if m == "vsam_06_load_data" else tbl if m == "vsam_02_inventory" else []), *runall_targeting(args, m)) for m in VSAM_STEPS]
        bg_cmd = [sys.executable, str(BASE_DIR / "sweep.py"), "vsam", "run-all", "--workers", str(workers), *extra, *tbl, *targeting(args, skip=("file", "filename")), *(["--rows", str(args.rows)] if getattr(args, "rows", None) else [])]
        pipeline_or_bg("vsam_full_pipeline", "VSAM Full", commands, bg_cmd, getattr(args, "bg", False))


# -----------------------------------------------------------------------------
# Layer commands: promote, chain, migrate, layers
# -----------------------------------------------------------------------------
def _layer_error(e):
    print(f"Error: {e}", file=sys.stderr)
    return 2


def cmd_promote(args):
    import sweep_promote as sp
    from sweep_layers import LayerError, get_layer
    try:
        tables = [t for t in (args.tables or "").split(",") if t.strip()]
        if args.status:
            rows, layers = sp.promote_status(tables=tables or None, limit=args.limit)
            print(sp.format_status(rows, layers))
            return 0
        if args.purge:
            purged = sp.purge_deltas(dry_run=args.dry_run)
            print(f"{len(purged)} delta(s) " + ("would be purged" if args.dry_run else "purged"))
            return 0
        if not args.from_layer or not args.to_layer:
            return _layer_error(LayerError("--from and --to are required (promote moves data one hop, e.g. --from bronze --to silver)"))
        results, code = sp.promote(args.from_layer, args.to_layer, tables=tables or None, full=args.full, dry_run=args.dry_run,
                                   workers=args.workers, transport=args.transport, ignore_pending_fwd=getattr(args, "ignore_pending_fwd", False))
        for r in results:
            print(f"{r['status']:<10} {r['table']:<50} inserted {r['inserted']:>9} updated {r['updated']:>9} unchanged {r['unchanged']:>9}"
                  + (f"  {r['message']}" if r["message"] else ""))
        if results:
            sp.write_promote_report(results, get_layer(args.from_layer), get_layer(args.to_layer), args.dry_run)
        return code
    except LayerError as e:
        return _layer_error(e)


def cmd_fwd_ddl(args):
    import sweep_fwd_ddl as fd
    from sweep_layers import LayerError
    try:
        if args.fwd_cmd == "status":
            return fd.cmd_status(args)
        if args.fwd_cmd == "check":
            return fd.cmd_check(args)
        if args.fwd_cmd == "apply":
            return fd.cmd_apply(args)
        if args.fwd_cmd == "promote":
            return fd.cmd_promote(args)
        print("Usage: python3 sweep.py fwd-ddl {status,apply,promote,check} ...")
        return 0
    except LayerError as e:
        return _layer_error(e)


def cmd_chain(args):
    import sweep_promote as sp
    from sweep_layers import LayerError
    load = []
    if not args.skip_load and not args.dry_run:
        for track in [t.strip() for t in args.tracks.split(",") if t.strip()]:
            if track not in ("oracle", "vsam"):
                return _layer_error(LayerError(f"unknown track {track!r}: oracle, vsam"))
            load.append([sys.executable, str(BASE_DIR / "sweep.py"), track, "load-data", "--layer", "bronze", "--workers", str(args.workers)])
    try:
        return sp.run_chain(until=args.until, load_cmds=load, workers=args.workers, transport=args.transport,
                            tables=[t for t in (args.tables or "").split(",") if t.strip()] or None, dry_run=args.dry_run,
                            skip_models=args.skip_models, allow_ddl=args.allow_ddl)
    except LayerError as e:
        return _layer_error(e)


def cmd_migrate(args):
    from sweep_layers import LayerError, chain as layer_chain, get_layer, use_layer, verify_database
    import sweep_migrate
    try:
        if args.all_layers:
            layers = layer_chain()
        elif args.layer:
            layers = [get_layer(args.layer)]
        else:
            return _layer_error(LayerError("give --layer <name> or --all-layers"))
        for lay in layers:
            with use_layer(lay):
                verify_database(lay)
                applied = sweep_migrate.migrate_current_database(dry_run=args.dry_run)
                what = (("would apply " if args.dry_run else "applied ") + ", ".join(map(str, applied))) if applied else "up to date"
                print(f"{lay.name} ({lay.database}): {what}")
        return 0
    except (LayerError, RuntimeError) as e:
        return _layer_error(e)


def cmd_layers(args):
    from sweep_layers import LayerError
    import sweep_layer_cmds as lc
    try:
        if args.layers_cmd == "init-manifests":
            lc.init_manifests(force=args.force)
            return 0
        if args.layers_cmd == "scan-models":
            lc.scan_models(args.models_dir)
            return 0
        if args.layers_cmd == "reconcile":
            import sweep_reconcile as rc
            return rc.reconcile_cmd(args.from_layer, args.to_layer, [t for t in (args.tables or "").split(",") if t.strip()] or None, args.exact)
        if args.layers_cmd == "check":
            import sweep_reconcile as rc
            return rc.check_cmd(args.layer)
        print("Usage: python3 sweep.py layers {init-manifests,scan-models,reconcile,check} ...")
        return 0
    except LayerError as e:
        return _layer_error(e)


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
        if selectors_given(args):
            print("Selectors (--tables, --schemas, --company ...) name tables of ONE track: use `run-all --track oracle` or "
                  "`--track vsam` when targeting.", file=sys.stderr)
            sys.exit(2)
        logger.info("Executing End-to-End Migration for BOTH Oracle and VSAM tracks...")
        extra = ["--force-truncate"] if getattr(args, "force_truncate", False) else []
        tbl = ["--tbl-col", args.tbl_col] if getattr(args, "tbl_col", None) else []
        oracle_cmds = [step("oracle", m, *(["--workers", workers] if m == "oracle_06_load_data" else [])) for m in ORACLE_STEPS]
        vsam_cmds = [step("vsam", m, *(["--workers", workers, *extra] if m == "vsam_06_load_data" else tbl if m == "vsam_02_inventory" else [])) for m in VSAM_STEPS]
        bg_cmd = [sys.executable, str(BASE_DIR / "sweep.py"), "run-all", "--track", "all", "--workers", str(workers), *extra, *tbl, *(["--rows", str(args.rows)] if getattr(args, "rows", None) else [])]
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
    p_all.add_argument("--rows", metavar="N|all", help="Rows per file for the VSAM type gate (Step 03): a number or 'all'")
    p_all.add_argument("--bg", action="store_true", help="Run full pipeline as detached background daemon")
    add_targeting_args(p_all, company=True)
    add_layer_arg(p_all)

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
    # Layers: promote / chain / migrate / layers
    # -------------------------------------------------------------------------
    p_prom = subparsers.add_parser("promote", help="Move data ONE hop along bronze -> silver -> gold_backup -> gold")
    p_prom.add_argument("--from", dest="from_layer", help="Source layer (the layer before --to)")
    p_prom.add_argument("--to", dest="to_layer", help="Target layer")
    p_prom.add_argument("--tables", help="Comma-separated tables (SCHEMA.TABLE or TABLE)")
    p_prom.add_argument("--full", action="store_true", help="Copy the whole source table instead of the captured deltas (baseline / resync)")
    p_prom.add_argument("--dry-run", action="store_true", help="Show how many rows would be inserted, updated and skipped per table; write nothing")
    p_prom.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel tables inside one dependency level")
    p_prom.add_argument("--transport", choices=["auto", "program", "stream"], default=None,
                        help="Cross-database transport: psql copy FROM PROGRAM (program), a pipe made by Python (stream), or auto (default)")
    p_prom.add_argument("--status", action="store_true", help="Print the delta x layer status matrix and exit")
    p_prom.add_argument("--purge", action="store_true", help="Remove delta tables every layer applied (plus SWEEP_DELTA_RETENTION_DAYS) and exit")
    p_prom.add_argument("--ignore-pending-fwd", action="store_true", help="Promote data even when the target layer has pending FE (fwd-ddl) files")
    p_prom.add_argument("--limit", type=int, default=10, help="Deltas per table shown by --status (default 10)")

    p_chain = subparsers.add_parser("chain", help="Load bronze from S3, then promote silver -> gold_backup -> gold (stops at the first failure)")
    p_chain.add_argument("--until", help="Stop after this stage: bronze, silver, models, gold_backup or gold (e.g. gold_backup, to review before gold)")
    p_chain.add_argument("--skip-models", action="store_true", help="Promote without running the data models")
    p_chain.add_argument("--allow-ddl", action="store_true", help="Also apply pending FE (fwd-ddl) files to gold_backup / gold")
    p_chain.add_argument("--tracks", default="oracle,vsam", help="Tracks to load into bronze: oracle, vsam or both (default both)")
    p_chain.add_argument("--skip-load", action="store_true", help="Do not load bronze (promote what bronze already holds)")
    p_chain.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    p_chain.add_argument("--transport", choices=["auto", "program", "stream"], default=None)
    p_chain.add_argument("--tables", help="Comma-separated tables to promote")
    p_chain.add_argument("--dry-run", action="store_true", help="Dry-run the promote hops (the bronze load is skipped)")

    p_mig = subparsers.add_parser("migrate", help="Apply the versioned sweep_ctl migrations (idempotent, transactional)")
    p_mig.add_argument("--layer", help="Layer to migrate")
    p_mig.add_argument("--all-layers", action="store_true", help="Migrate every layer, in chain order")
    p_mig.add_argument("--dry-run", action="store_true", help="Show the pending migrations; change nothing")

    p_fwd = subparsers.add_parser("fwd-ddl", help="Forward-engineering (FE) SQL files: status / apply / promote / check across the layers")
    f_sub = p_fwd.add_subparsers(dest="fwd_cmd", help="fwd-ddl subcommands")
    def _fwd_common(sp, layer_required=False):
        add_targeting_args(sp, file=True)
        add_layer_arg(sp)
        sp.add_argument("--allow-data", action="store_true", help="Allow statements that insert or change data (FE files are structure only)")
        sp.add_argument("--force", action="store_true", help="Apply even when the compatibility check reports a BLOCK")
    f_st = f_sub.add_parser("status", help="Pending / applied / failed FE files per layer (read-only; all layers without --layer)")
    _fwd_common(f_st)
    f_ap = f_sub.add_parser("apply", help="Apply the pending FE files of a layer in order (one transaction per file)")
    _fwd_common(f_ap)
    f_pr = f_sub.add_parser("promote", help="Apply to the next layer the files the previous layer applied")
    _fwd_common(f_pr)
    f_pr.add_argument("--from", dest="from_layer", required=True)
    f_pr.add_argument("--to", dest="to_layer", required=True)
    f_ck = f_sub.add_parser("check", help="Dry-run every pending file with the compatibility check; nothing is written (read-only)")
    _fwd_common(f_ck)

    p_lay = subparsers.add_parser("layers", help="Layer manifests, model scan, reconcile and checks")
    l_sub = p_lay.add_subparsers(dest="layers_cmd", help="layers subcommands")
    l_init = l_sub.add_parser("init-manifests", help="Draft layers/<layer>_tables.csv from the inventory (existing manifests are kept)")
    l_init.add_argument("--force", action="store_true", help="Replace existing manifests (a .bak is kept)")
    l_scan = l_sub.add_parser("scan-models", help="Scan the data-model scripts and DRAFT the silver VSAM list and the output tables for review")
    l_scan.add_argument("--models-dir", required=True, help="Folder with the data-model Python scripts (they are only read)")
    l_rec = l_sub.add_parser("reconcile", help="Row counts + key hash of the tables of two layers (Excel report, exit code)")
    l_rec.add_argument("--from", dest="from_layer", required=True)
    l_rec.add_argument("--to", dest="to_layer", required=True)
    l_rec.add_argument("--tables", help="Comma-separated tables")
    l_rec.add_argument("--exact", action="store_true", help="Hash every row (default: a 1 percent sample of the keys)")
    l_chk = l_sub.add_parser("check", help="Database reachable, PostgreSQL >= 16, privileges, sweep_ctl migrated, drift, sequences")
    l_chk.add_argument("--layer", help="Check one layer (default: all)")

    # -------------------------------------------------------------------------
    # Track: ORACLE
    # -------------------------------------------------------------------------
    p_oracle = subparsers.add_parser("oracle", help="Oracle to PostgreSQL migration pipeline")
    o_sub = p_oracle.add_subparsers(dest="oracle_cmd", help="Oracle subcommands")

    o_ext = o_sub.add_parser("extract", help="Step 01: Extract metadata from master Oracle SQL dump")
    o_ext.add_argument("--file", help="Specific dump file path")
    o_ext.add_argument("--owners", help="Comma-separated owners to filter")
    add_targeting_args(o_ext, schemas="--owners")
    o_ext.add_argument("--bg", action="store_true", help="Run in background")

    o_inv = o_sub.add_parser("inventory", help="Step 02: S3 Discovery & Small-to-Large Ordering")
    o_inv.add_argument("--s3", help="Target S3 prefix")
    o_inv.add_argument("--owners", help="Comma-separated owners")
    o_inv.add_argument("--tables", help="Comma-separated tables")
    add_targeting_args(o_inv, schemas="--owners", file=True)
    o_inv.add_argument("--bg", action="store_true", help="Run in background")

    o_gate = o_sub.add_parser("gate", help="Step 03: Pre-flight Data Type Gate")
    o_gate.add_argument("--rows", type=int, default=1000, help="Rows to sample per file")
    o_gate.add_argument("--tables", help="Comma-separated tables")
    o_gate.add_argument("--max-tables", type=int, help="Limit number of tables tested")
    add_targeting_args(o_gate, schemas="--owners", file=True)
    o_gate.add_argument("--bg", action="store_true", help="Run in background")

    o_ddl = o_sub.add_parser("gen-ddl", aliases=["ddl"], help="Step 04: Generate Bare Tables & Partitions")
    o_ddl.add_argument("--owners", help="Comma-separated owners")
    o_ddl.add_argument("--tables", help="Comma-separated tables")
    add_targeting_args(o_ddl, schemas="--owners")
    add_layer_arg(o_ddl)
    o_ddl.add_argument("--bg", action="store_true", help="Run in background")

    o_apply = o_sub.add_parser("apply-ddl", help="Step 05: Apply DDL to PostgreSQL")
    o_apply.add_argument("--schemas", help="Comma-separated schemas")
    o_apply.add_argument("--skip-meta", action="store_true", help="Skip re-deploying sweep_meta.sql")
    add_targeting_args(o_apply)
    add_layer_arg(o_apply)
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
    o_load.add_argument("--full", action="store_true", help="Do not capture the applied delta: the promote hop copies the whole table")
    o_load.add_argument("--trigger-mode", choices=["auto", "replica", "disable"], default=None, help="How triggers are switched off while loading")
    add_targeting_args(o_load, schemas="--owners", file=True)
    add_layer_arg(o_load)
    o_load.add_argument("--bg", action="store_true", help="Run in background daemon")

    o_obj = o_sub.add_parser("load-obj", aliases=["post-load"], help="Step 07: Post-Load Keys, Indexes, Views, Triggers")
    o_obj.add_argument("--stage", choices=["all", "pk", "alter", "index", "fk", "validate", "views", "triggers"], default="all")
    o_obj.add_argument("--schemas", help="Comma-separated schemas")
    o_obj.add_argument("--only-loaded", action="store_true", help="Only load objects for tables with loaded data (default: all tables)")
    add_targeting_args(o_obj)
    add_layer_arg(o_obj)
    o_obj.add_argument("--bg", action="store_true", help="Run in background")

    o_val = o_sub.add_parser("validate", aliases=["verify"], help="Step 08: Reconciliation & Audit Tally")
    o_val.add_argument("--exact-counts", action="store_true", help="Perform exact count(*) queries")
    o_val.add_argument("--tables", help="Comma-separated tables")
    o_val.add_argument("--schemas", help="Comma-separated schemas")
    o_val.add_argument("--file", help="Specific master Oracle SQL dump file path")
    add_targeting_args(o_val)
    add_layer_arg(o_val)
    o_val.add_argument("--bg", action="store_true", help="Run in background")

    o_run_all = o_sub.add_parser("run-all", help="Execute complete Oracle pipeline end-to-end")
    o_run_all.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    o_run_all.add_argument("--bg", action="store_true", help="Run full Oracle pipeline in background daemon")
    add_targeting_args(o_run_all, schemas="--owners")
    add_layer_arg(o_run_all)

    # -------------------------------------------------------------------------
    # Track: VSAM (7-Step Pipeline)
    # -------------------------------------------------------------------------
    p_vsam = subparsers.add_parser("vsam", help="VSAM to PostgreSQL migration pipeline")
    v_sub = p_vsam.add_subparsers(dest="vsam_cmd", help="VSAM subcommands")

    v_ext = v_sub.add_parser("extract", aliases=["extract-copybooks"], help="Step 01: Extract copybook metadata (fields, types, keys)")
    v_ext.add_argument("--bg", action="store_true", help="Run in background")
    add_targeting_args(v_ext, tables=False, schemas=None, exclude=False, copybook=True)

    v_inv = v_sub.add_parser("inventory", help="Step 02: S3 Discovery & Source-to-Target Mapping")
    v_inv.add_argument("--s3", help="Target S3 prefix")
    v_inv.add_argument("--company", help="Comma-separated companies")
    v_inv.add_argument("--tbl-col", choices=["headers", "copybook"], help="Table column names: S3 file headers (default) or mapped copybook column names")
    add_targeting_args(v_inv, company=True, file=True)
    v_inv.add_argument("--bg", action="store_true", help="Run in background")

    v_gate = v_sub.add_parser("type-gate", aliases=["gate"], help="Step 03: PK uniqueness verification & schema column detection")
    v_gate.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    v_gate.add_argument("--company", help="Comma-separated company tokens to filter")
    add_targeting_args(v_gate, company=True, file=True)
    v_gate.add_argument("--rows", metavar="N|all", help="Rows to read per file: a number or 'all' (default: VSAM_ANALYZE_MAX_ROWS, else all)")
    v_gate.add_argument("--bg", action="store_true", help="Run in background")

    v_ddl = v_sub.add_parser("gen-ddl", aliases=["ddl"], help="Step 04: Generate DDL (tables + PK in gen_ddl)")
    v_ddl.add_argument("--company", help="Comma-separated companies")
    v_ddl.add_argument("--execute", action="store_true", help="Execute generated pre-load DDL")
    add_targeting_args(v_ddl, company=True)
    add_layer_arg(v_ddl)
    v_ddl.add_argument("--bg", action="store_true", help="Run in background")

    v_apply = v_sub.add_parser("apply-ddl", help="Step 05: Apply table DDL (tables only, no PK) to PostgreSQL")
    v_apply.add_argument("--skip-meta", action="store_true", help="Skip sweep_meta.sql")
    add_targeting_args(v_apply)
    add_layer_arg(v_apply)
    v_apply.add_argument("--bg", action="store_true", help="Run in background")

    v_load = v_sub.add_parser("load-data", aliases=["load"], help="Step 06: Schema-segregated S3 streaming ingestion & dynamic upsert")
    v_load.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    v_load.add_argument("--company", help="Comma-separated companies")
    v_load.add_argument("--tables", help="Comma-separated tables")
    v_load.add_argument("--limit-files", type=int, help="Limit files loaded")
    v_load.add_argument("--force-truncate", action="store_true", help="Allow replacing a table that already has rows and no PK in a PARTIAL run (--company/--tables/--limit-files); full runs replace such tables automatically")
    add_targeting_args(v_load, company=True, file=True)
    add_layer_arg(v_load)
    v_load.add_argument("--reload", action="store_true", help="Load files again even when their S3 LastModified/ETag/size are unchanged")
    v_load.add_argument("--full", action="store_true", help="Do not capture the applied delta: the promote hop copies the whole table")
    v_load.add_argument("--max-rejects", type=int, default=None, help="Fail a file when this many rows are rejected (default 0 = never fail; N = fail the file at N rejects)")
    v_load.add_argument("--bg", action="store_true", help="Run in background daemon")

    v_lobj = v_sub.add_parser("load-obj", aliases=["load-objects", "post-load"], help="Legacy alias: Merged into Step 06 (load-data)")
    v_lobj.add_argument("--schema", help="Apply PK only for this schema")
    add_targeting_args(v_lobj, company=True, file=True)
    add_layer_arg(v_lobj)
    v_lobj.add_argument("--bg", action="store_true", help="Run in background")

    v_val = v_sub.add_parser("validate", aliases=["verify"], help="Step 07: Row counts, column, type, PK & reconciliation audit")
    v_val.add_argument("--exact-counts", action="store_true", help="Exact row count queries")
    v_val.add_argument("--tables", help="Comma-separated tables")
    v_val.add_argument("--schema", help="Validate only this schema")
    add_targeting_args(v_val, schemas="--schema", company=True)
    add_layer_arg(v_val)
    v_val.add_argument("--bg", action="store_true", help="Run in background")

    v_runall = v_sub.add_parser("run-all", help="Execute complete VSAM 7-step pipeline end-to-end")
    v_runall.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers")
    v_runall.add_argument("--force-truncate", action="store_true", help="Pass --force-truncate to the VSAM load step")
    v_runall.add_argument("--tbl-col", choices=["headers", "copybook"], help="Table column names: S3 file headers (default) or copybook column names")
    v_runall.add_argument("--rows", metavar="N|all", help="Rows per file for the VSAM type gate (Step 03): a number or 'all'")
    v_runall.add_argument("--bg", action="store_true", help="Run full pipeline in background")
    add_targeting_args(v_runall, company=True)
    add_layer_arg(v_runall)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    if args.command == "check":
        sys.exit(0 if cmd_check(args) else 1)
    elif args.command == "fwd-ddl":
        sys.exit(cmd_fwd_ddl(args))
    elif args.command == "promote":
        sys.exit(cmd_promote(args))
    elif args.command == "chain":
        sys.exit(cmd_chain(args))
    elif args.command == "migrate":
        sys.exit(cmd_migrate(args))
    elif args.command == "layers":
        sys.exit(cmd_layers(args))
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
