#!/usr/bin/env python3
"""
=============================================================================
sweep_common.py - Core Configuration, Database Helpers, Logging & Reporting
for the Unified SWEEP Data Migration Framework (Oracle & VSAM to PostgreSQL).
=============================================================================
"""
import os
import re
import sys
import csv
import io
import logging
import subprocess
import time
import zipfile
from datetime import datetime
from pathlib import Path

from sweep_platform import make_symlink, group_popen_kwargs, kill_process_group

__version__ = "5.2"

def get_next_version(current_ver: str) -> str:
    """
    Increments version string:
    1.1 -> 1.2 ... -> 1.9 -> 2.0 ... -> 2.9 -> 3.0
    """
    v = current_ver.lstrip("v")
    major, minor = v.split(".")
    maj = int(major)
    minr = int(minor)
    if minr >= 9:
        maj += 1
        minr = 0
    else:
        minr += 1
    return f"{maj}.{minr}"

# -----------------------------------------------------------------------------
# Exception groups used instead of blanket `except Exception`
# -----------------------------------------------------------------------------
PARSE_ERRORS = (ValueError, TypeError)                 # int()/float()/datetime parsing of untrusted text
DB_ERRORS = (RuntimeError, ValueError, KeyError, IndexError)   # run_psql/query_db_* failure + row parsing
FILE_READ_ERRORS = (OSError, ValueError, KeyError, UnicodeError, zipfile.BadZipFile)  # files, CSV, Excel via pandas
try:  # openpyxl signals an unreadable workbook with its own exception type
    from openpyxl.utils.exceptions import InvalidFileException as _InvalidXlsx
    FILE_READ_ERRORS = FILE_READ_ERRORS + (_InvalidXlsx,)
except ImportError:
    pass
AWS_ERRORS = (ImportError, OSError, ValueError, KeyError, EOFError)  # boto3 / S3 access
try:
    from botocore.exceptions import BotoCoreError as _BotoCoreError, ClientError as _ClientError
    AWS_ERRORS = AWS_ERRORS + (_BotoCoreError, _ClientError)
except ImportError:
    pass

# -----------------------------------------------------------------------------
# Base Directories & Environment Configuration
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = BASE_DIR
INPUTS_DIR = BASE_DIR / "inputs"
OUTPUTS_DIR = BASE_DIR / "outputs"
REPORTS_DIR = BASE_DIR / "reports"
LOGS_DIR = BASE_DIR / "logs"

# Track-Specific Directories: Oracle (oracle/{inputs,outputs,reports,logs})
ORACLE_DIR = BASE_DIR / "oracle"
ORACLE_INPUTS_DIR = ORACLE_DIR / "inputs"
ORACLE_OUTPUTS_DIR = ORACLE_DIR / "outputs"
ORACLE_REPORTS_DIR = ORACLE_DIR / "reports"
ORACLE_LOGS_DIR = ORACLE_DIR / "logs"

ORACLE_META_DIR = ORACLE_OUTPUTS_DIR / "oracle_metadata"
ORACLE_PRE_LOAD_DIR = ORACLE_OUTPUTS_DIR / "pre_load"
ORACLE_POST_LOAD_DIR = ORACLE_OUTPUTS_DIR / "post_load"
ORACLE_DUMP_DIR = ORACLE_INPUTS_DIR / "oracle_dump" if (ORACLE_INPUTS_DIR / "oracle_dump").exists() else ORACLE_INPUTS_DIR

# Track-Specific Directories: VSAM (vsam/{inputs,outputs,reports,logs})
VSAM_DIR = BASE_DIR / "vsam"
VSAM_INPUTS_DIR = VSAM_DIR / "inputs"
VSAM_OUTPUTS_DIR = VSAM_DIR / "outputs"
VSAM_REPORTS_DIR = VSAM_DIR / "reports"
VSAM_LOGS_DIR = VSAM_DIR / "logs"

VSAM_COPYBOOK_DIR = VSAM_INPUTS_DIR / "copybooks"
VSAM_GEN_DDL_DIR = VSAM_OUTPUTS_DIR / "gen_ddl"
VSAM_PRE_LOAD_DIR = VSAM_GEN_DDL_DIR
VSAM_APPROVALS_DIR = VSAM_OUTPUTS_DIR / "approvals"

# Shared Aliases
INPUTS_DIR = ORACLE_INPUTS_DIR
OUTPUTS_DIR = BASE_DIR / "outputs"
REPORTS_DIR = ORACLE_REPORTS_DIR
LOGS_DIR = ORACLE_LOGS_DIR

PRE_LOAD_DIR = ORACLE_PRE_LOAD_DIR
POST_LOAD_DIR = ORACLE_POST_LOAD_DIR

SWEEP_META_SQL = (
    ORACLE_INPUTS_DIR / "sweep_meta.sql"
    if (ORACLE_INPUTS_DIR / "sweep_meta.sql").exists()
    else VSAM_INPUTS_DIR / "sweep_meta.sql"
    if (VSAM_INPUTS_DIR / "sweep_meta.sql").exists()
    else BASE_DIR / "sweep_meta.sql"
)

# Standard Metadata CSV File Paths for Oracle
META_TABLES_CSV = ORACLE_META_DIR / "oracle_01_metadata_tables.csv"
META_CONSTRAINTS_CSV = ORACLE_META_DIR / "oracle_01_metadata_constraints.csv"
META_INDEXES_CSV = ORACLE_META_DIR / "oracle_01_metadata_indexes.csv"
META_PARTITIONS_CSV = ORACLE_META_DIR / "oracle_01_metadata_partitions.csv"
META_TABLE_STATS_CSV = ORACLE_META_DIR / "oracle_01_metadata_table_stats.csv"
META_SEQUENCES_CSV = ORACLE_META_DIR / "oracle_01_metadata_sequences.csv"
META_OBJECTS_CSV = ORACLE_META_DIR / "oracle_01_metadata_objects.csv"

def resolve_meta_csv(path: Path) -> Path:
    """Returns path if it exists, or falls back to legacy un-prefixed name if present."""
    if path.exists():
        return path
    legacy_name = path.name.replace("oracle_01_", "")
    legacy_path = path.parent / legacy_name
    if legacy_path.exists():
        return legacy_path
    return path

# Ensure all essential directories exist
ALL_DIRS = [
    ORACLE_DIR, ORACLE_INPUTS_DIR, ORACLE_OUTPUTS_DIR, ORACLE_META_DIR,
    ORACLE_PRE_LOAD_DIR, ORACLE_POST_LOAD_DIR, ORACLE_REPORTS_DIR, ORACLE_LOGS_DIR,
    VSAM_DIR, VSAM_INPUTS_DIR, VSAM_COPYBOOK_DIR, VSAM_OUTPUTS_DIR,
    VSAM_PRE_LOAD_DIR, VSAM_APPROVALS_DIR, VSAM_REPORTS_DIR, VSAM_LOGS_DIR
]
for d in ALL_DIRS:
    d.mkdir(parents=True, exist_ok=True)


# Environment-file names probed (in order) under BASE_DIR. The first one found wins.
ENV_FILE_NAMES = ("sweep.env", "sweep.env.txt", "sweep_env.sh", "qa.env", ".env")


def parse_env_line(line):
    """
    Parses one `[export ]KEY=VALUE` line. Returns (key, value) or None for blanks, comments
    and lines without '='. Surrounding single/double quotes around VALUE are removed.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):
        line = line[7:].strip()
    if "=" not in line:
        return None
    key, val = line.split("=", 1)
    key, val = key.strip(), val.strip()
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        val = val[1:-1]
    return (key, val) if key else None


def load_env_file(filepath=None):
    """
    Loads KEY=VALUE pairs from sweep.env (or sweep_env.sh, qa.env, .env) into os.environ.
    Variables ALREADY present in the real process environment win over the file, so an operator
    can always override a setting with `export VAR=...` or `VAR=... python3 sweep.py ...`.
    """
    paths_to_check = [filepath] + [BASE_DIR / n for n in ENV_FILE_NAMES]
    for p in paths_to_check:
        if p and Path(p).exists():
            with open(p, "r", encoding="utf-8") as f:
                for raw in f:
                    kv = parse_env_line(raw)
                    if kv:
                        os.environ.setdefault(kv[0], kv[1])
            break
    refresh_db_settings()


def refresh_db_settings():
    """(Re)reads the PostgreSQL connection settings from os.environ into module globals."""
    global PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
    PGHOST = os.getenv("PGHOST", "localhost")
    PGPORT = os.getenv("PGPORT", "5432")
    PGDATABASE = os.getenv("PGDATABASE", "sweep")
    PGUSER = os.getenv("PGUSER", "postgres")
    PGPASSWORD = os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD") or os.getenv("DB_PASSWORD") or ""
    PGSSLMODE = os.getenv("PGSSLMODE", "prefer")
    if PGPASSWORD:
        os.environ["PGPASSWORD"] = PGPASSWORD


# Initial load (also defines the PG* module globals)
PGHOST = PGPORT = PGDATABASE = PGUSER = PGPASSWORD = PGSSLMODE = None
load_env_file()


def get_db_name():
    """Returns the database name (lowercased) configured in sweep.env or environment."""
    db = os.getenv("PGDATABASE") or PGDATABASE or "sweep"
    return db.strip().lower()


def has_db_auth():
    """
    True when psql can authenticate non-interactively: a password is configured, or a
    ~/.pgpass / PGPASSFILE / PGSERVICE is available.
    Set SWEEP_ALLOW_NO_PASSWORD=1 for trust/peer authentication setups.
    """
    if PGPASSWORD:
        return True
    if os.getenv("SWEEP_ALLOW_NO_PASSWORD") == "1" or os.getenv("PGSERVICE"):
        return True
    passfile = os.getenv("PGPASSFILE") or str(Path.home() / ".pgpass")
    return Path(passfile).is_file()


AUTH_HELP = (
    "No database credentials found. Set PGPASSWORD in sweep.env (export PGPASSWORD=...), "
    "or provide ~/.pgpass / PGPASSFILE / PGSERVICE, or set SWEEP_ALLOW_NO_PASSWORD=1 for trust auth."
)


def check_db_credentials(logger=None):
    """Returns True if non-interactive DB authentication is configured, else logs and raises RuntimeError."""
    if not has_db_auth():
        if logger:
            logger.error(AUTH_HELP)
        raise RuntimeError(AUTH_HELP)
    return True


AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-2"))
ORACLE_S3_BUCKET = os.getenv("ORACLE_S3_BUCKET", os.getenv("SWEEP_S3_BUCKET", ""))
S3_BUCKET = ORACLE_S3_BUCKET or os.getenv("VSAM_S3_BUCKET", "")
VSAM_S3_BUCKET = os.getenv("VSAM_S3_BUCKET", S3_BUCKET)
DEFAULT_WORKERS = int(os.getenv("ORACLE_WORKERS", os.getenv("SWEEP_WORKERS", os.getenv("VSAM_WORKERS", "4"))))
PSQL_BIN = os.getenv("PSQL", "psql")
STREAM_SCRIPT = BASE_DIR / "sweep_stream.sh"

CTL_SCHEMA = "sweep_ctl"
META_SCHEMA = "sweep_meta"
LOAD_LOG_TABLE = f"{CTL_SCHEMA}.load_log"
LOAD_AUDIT_TABLE = f"{CTL_SCHEMA}.load_audit"

# -----------------------------------------------------------------------------
# Logging Utilities
# -----------------------------------------------------------------------------
def get_timestamp():
    """Returns standard timestamp string for file naming: YYYYMMDD_HHMMSS."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_logger(name, step_name=None, to_file=None, track=None):
    """Configures and returns a dual console/file logger with track segregation and duplicate suppression."""
    logger = logging.getLogger(name)
    logger.propagate = False

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # Determine execution context (daemon runner vs interactive terminal)
    is_daemon = os.getenv("SWEEP_DAEMON") == "1"
    daemon_log_file = os.getenv("SWEEP_LOG_FILE")

    # Only attach console StreamHandler when running interactively (not under daemon supervisor)
    # When running as a background daemon, stdout is already redirected to the log file by the supervisor;
    # attaching StreamHandler in daemon mode would duplicate every log entry in the log file.
    if not is_daemon:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    if to_file is None:
        to_file = bool(step_name) or is_daemon

    # File Handler
    if to_file:
        if daemon_log_file:
            log_file = Path(daemon_log_file)
            target_log_dir = log_file.parent
        else:
            ts = get_timestamp()
            db = get_db_name()
            db_prefix = f"{db}_" if db else ""
            clean_step = (step_name or "").strip()
            if db and clean_step.lower().startswith(f"{db}_"):
                prefix = f"{clean_step}_"
            elif clean_step:
                prefix = f"{db_prefix}{clean_step}_"
            else:
                prefix = db_prefix

            # Determine target track directory (logs/oracle vs logs/vsam vs logs/)
            target_log_dir = LOGS_DIR
            determined_track = track
            if not determined_track:
                try:
                    frame = sys._getframe(1)
                    caller_file = frame.f_code.co_filename.replace(os.sep, '/').lower()
                    if '/oracle/' in caller_file or 'oracle_' in caller_file:
                        determined_track = 'oracle'
                    elif '/vsam/' in caller_file or 'vsam_' in caller_file:
                        determined_track = 'vsam'
                except (ValueError, AttributeError):
                    pass

            if not determined_track:
                check_str = f"{name} {step_name or ''}".lower()
                if "vsam" in check_str:
                    determined_track = "vsam"
                elif "oracle" in check_str or any(x in check_str for x in [
                    "01_extract", "02_s3", "03_type", "04_gen", "05_apply", "06_load", "07_load", "08_val",
                    "extractmetadata", "s3inventory", "typegateway", "generateddl", "applyddl", "loaddata", "loadobjects"
                ]):
                    determined_track = "oracle"

            if determined_track == "oracle":
                target_log_dir = ORACLE_LOGS_DIR
            elif determined_track == "vsam":
                target_log_dir = VSAM_LOGS_DIR

            target_log_dir.mkdir(parents=True, exist_ok=True)
            log_file = target_log_dir / f"{prefix}{ts}.log"

        target_log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        # Maintain latest-log pointers on POSIX
        for link in (target_log_dir / "latest.log", BASE_DIR / "sweep_latest.log"):
            make_symlink(link, log_file)

    return logger

# -----------------------------------------------------------------------------
# Excel Reporting (openpyxl)
# -----------------------------------------------------------------------------
def generate_excel_report(report_path, sheets_data):
    """
    Generates a beautifully styled multi-tab Excel report.
    Automatically prepends database name prefix if missing.
    sheets_data: dict of { sheet_title: [list of dicts or list of row lists] }
    """
    p = Path(report_path)
    db = get_db_name()
    if db and not p.name.lower().startswith(f"{db}_"):
        report_path = p.parent / f"{db}_{p.name}"
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter

        wb = openpyxl.Workbook()
        wb.remove(wb.active)  # Remove default blank sheet

        header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        zebra_fill = PatternFill(start_color="F9FAFB", end_color="F9FAFB", fill_type="solid")
        thin_border = Border(
            left=Side(style="thin", color="D9D9D9"),
            right=Side(style="thin", color="D9D9D9"),
            top=Side(style="thin", color="D9D9D9"),
            bottom=Side(style="thin", color="D9D9D9")
        )

        for title, rows in sheets_data.items():
            safe_title = str(title)[:31].replace(":", "_").replace("/", "_").replace("\\", "_")
            ws = wb.create_sheet(title=safe_title)
            ws.views.sheetView[0].showGridLines = True

            if not rows:
                ws.append(["No data recorded"])
                continue

            if isinstance(rows[0], dict):
                headers = list(rows[0].keys())
                ws.append(headers)
                for r in rows:
                    ws.append([r.get(h, "") for h in headers])
            else:
                for r in rows:
                    ws.append(list(r))

            # Style header row
            for col_num in range(1, ws.max_column + 1):
                cell = ws.cell(row=1, column=col_num)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

            ws.freeze_panes = "A2"

            # Style data cells and zebra rows
            for row_num in range(2, ws.max_row + 1):
                is_even = (row_num % 2 == 0)
                for col_num in range(1, ws.max_column + 1):
                    cell = ws.cell(row=row_num, column=col_num)
                    cell.border = thin_border
                    if is_even:
                        cell.fill = zebra_fill
                    if isinstance(cell.value, (int, float)):
                        cell.alignment = Alignment(horizontal="right")

            # Auto adjust column widths
            for col in ws.columns:
                max_len = 0
                col_letter = get_column_letter(col[0].column)
                for cell in col:
                    val_str = str(cell.value or "")
                    if "\n" in val_str:
                        val_str = max(val_str.split("\n"), key=len)
                    max_len = max(max_len, len(val_str))
                ws.column_dimensions[col_letter].width = min(max(max_len + 3, 12), 60)

        wb.save(report_path)
        return str(report_path)
    except (ImportError, OSError, ValueError, TypeError, KeyError) as e:
        print(f"Warning: Failed to create Excel report via openpyxl ({e}). Writing CSV fallback.")
        csv_path = Path(report_path).with_suffix(".csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for title, rows in sheets_data.items():
                w.writerow([f"=== {title} ==="])
                if rows and isinstance(rows[0], dict):
                    w.writerow(list(rows[0].keys()))
                    for r in rows:
                        w.writerow(list(r.values()))
                elif rows:
                    w.writerows(rows)
                w.writerow([])
        return str(csv_path)

# -----------------------------------------------------------------------------
# Database Utilities
# -----------------------------------------------------------------------------
PSQL_APP_NAME = "sweep"
# Applied to every psql session: literals are escaped the standard way (backslash is a plain
# character), which the hand-built SQL in the loaders relies on.
BASE_PGOPTIONS = "-c standard_conforming_strings=on"
DEFAULT_PSQL_TIMEOUT = int(os.getenv("SWEEP_PSQL_TIMEOUT", "7200"))
CONNECT_RETRIES = int(os.getenv("SWEEP_CONNECT_RETRIES", "3"))

# psql failures where the statement provably never ran, so retrying is always safe.
_TRANSIENT_CONNECT_ERRORS = (
    "could not connect to server",
    "could not translate host name",
    "connection refused",
    "timeout expired",
    "the database system is starting up",
    "too many connections",
    "remaining connection slots are reserved",
    "connection timed out",
)


def psql_env(extra_env=None, options=None):
    """
    Environment for a psql child process: password (if any), application_name for session
    tagging, and PGOPTIONS merged from BASE_PGOPTIONS + `options` + extra_env['PGOPTIONS'].
    """
    env = os.environ.copy()
    if PGPASSWORD:
        env["PGPASSWORD"] = PGPASSWORD
    env.setdefault("PGAPPNAME", PSQL_APP_NAME)
    if PGSSLMODE:
        env.setdefault("PGSSLMODE", PGSSLMODE)
    opts = [BASE_PGOPTIONS]
    if options:
        opts.append(options)
    if extra_env:
        extra = dict(extra_env)
        if extra.get("PGOPTIONS"):
            opts.append(extra.pop("PGOPTIONS"))
        env.update(extra)
    env["PGOPTIONS"] = " ".join(opts)
    return env


def psql_base_args(dbname=None):
    return [PSQL_BIN, "-X", "-w", "-h", PGHOST, "-p", str(PGPORT), "-U", PGUSER, "-d", dbname or PGDATABASE]


def _is_transient_connect_error(text):
    low = (text or "").lower()
    return any(m in low for m in _TRANSIENT_CONNECT_ERRORS)


def run_subprocess_checked(args, *, input_text=None, env=None, capture=True, timeout=DEFAULT_PSQL_TIMEOUT,
                           retries=CONNECT_RETRIES, what="psql"):
    """
    Runs `args` with a timeout. Connection-establishment failures (statement never executed) are
    retried with exponential backoff; everything else fails immediately.
    Returns the CompletedProcess; raises RuntimeError on non-zero exit or timeout.
    """
    attempt = 0
    while True:
        try:
            res = subprocess.run(args, input=input_text, capture_output=capture, text=True,
                                 env=env, timeout=timeout or None)
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"{what} timed out after {timeout}s")
        if res.returncode == 0:
            return res
        err = (res.stderr or res.stdout or "").strip()
        if attempt < retries and _is_transient_connect_error(err):
            time.sleep(min(2 ** attempt, 30))
            attempt += 1
            continue
        raise RuntimeError(f"{what} execution failed (code {res.returncode}): {err}")


LOAD_TIMEOUT = int(os.getenv("SWEEP_LOAD_TIMEOUT", "21600"))  # 6h per file; 0 disables


def run_psql_script(script_text, *, options=None, timeout=None, retries=CONNECT_RETRIES, dbname=None):
    """
    Runs a multi-statement psql script from stdin (used for transactional file loads whose
    `\\copy ... FROM PROGRAM` spawns child processes). The psql process is started in its own
    process group so a timeout kills psql AND the S3/gzip pipeline it launched.

    Returns subprocess.CompletedProcess (stdout/stderr text). Never raises for a non-zero exit;
    raises RuntimeError only on timeout. Connection-establishment failures are retried.
    """
    if not has_db_auth():
        raise RuntimeError(AUTH_HELP)
    timeout = LOAD_TIMEOUT if timeout is None else timeout
    args = psql_base_args(dbname) + ["-v", "ON_ERROR_STOP=1", "-f", "-"]
    env = psql_env(options=options)
    attempt = 0
    while True:
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, env=env, **group_popen_kwargs())
        try:
            out, err = proc.communicate(script_text, timeout=timeout or None)
        except subprocess.TimeoutExpired:
            kill_process_group(proc)
            proc.communicate()
            raise RuntimeError(f"psql load timed out after {timeout}s")
        if proc.returncode != 0 and attempt < retries and _is_transient_connect_error(err or out):
            time.sleep(min(2 ** attempt, 30))
            attempt += 1
            continue
        return subprocess.CompletedProcess(args, proc.returncode, out, err)


def run_psql(cmd_or_sql, is_file=False, capture=True, extra_env=None, tuples_only=False, unaligned=False,
             dbname=None, timeout=None):
    """Executes SQL (or a .sql file when is_file=True) via the psql CLI and returns stripped stdout."""
    if not has_db_auth():
        raise RuntimeError(AUTH_HELP)
    env = psql_env(extra_env)
    args = psql_base_args(dbname)
    if tuples_only:
        args.append("-t")
    if unaligned:
        args.append("-A")

    input_text = None
    if is_file:
        args.extend(["-v", "ON_ERROR_STOP=1", "-f", str(cmd_or_sql)])
    elif "\n" in cmd_or_sql or cmd_or_sql.strip().startswith("\\"):
        # Multi-line statements or scripts containing backslash directives go through stdin
        args.extend(["-v", "ON_ERROR_STOP=1", "-f", "-"])
        input_text = cmd_or_sql
    else:
        args.extend(["-v", "ON_ERROR_STOP=1", "-c", cmd_or_sql])

    res = run_subprocess_checked(args, input_text=input_text, env=env, capture=capture,
                                 timeout=DEFAULT_PSQL_TIMEOUT if timeout is None else timeout)
    return (res.stdout or "").strip()


def query_db_rows(sql, dbname=None):
    """Executes SQL query and returns list of clean rows without headers or row count footer."""
    out = run_psql(sql, tuples_only=True, unaligned=True, extra_env={"PGOPTIONS": "-c statement_timeout=0"}, dbname=dbname)
    return [l.strip() for l in out.splitlines() if l.strip()]


def query_db_dicts(sql, dbname=None):
    """
    Runs a SELECT and returns its rows as a list of dicts (column name -> text).
    Uses `COPY (<query>) TO STDOUT WITH CSV HEADER`, so multi-line queries, `--` comments and
    embedded newlines in values are all preserved.
    """
    if not has_db_auth():
        raise RuntimeError(AUTH_HELP)
    inner = sql.strip().rstrip(";").rstrip()
    copy_sql = f"COPY (\n{inner}\n) TO STDOUT WITH (FORMAT csv, HEADER true)"
    args = psql_base_args(dbname) + ["-v", "ON_ERROR_STOP=1", "-c", copy_sql]
    res = run_subprocess_checked(args, env=psql_env(), what="psql query")
    return list(csv.DictReader(io.StringIO(res.stdout, newline="")))

# -----------------------------------------------------------------------------
# String, Type, and Identifier Mappers
# -----------------------------------------------------------------------------
def norm_col(name):
    """Lowercases and cleans identifier names."""
    if not name:
        return ""
    cleaned = re.sub(r"[^A-Za-z0-9_#$]+", "_", str(name).strip().lower()).strip("_")
    if cleaned and cleaned[0].isdigit():
        cleaned = "c_" + cleaned
    return cleaned[:63]


def q(identifier):
    """Quotes PostgreSQL identifier."""
    clean = str(identifier).replace('"', '""').strip()
    return f'"{clean}"'


def lit(val):
    """Escapes and formats SQL string literal."""
    if val is None:
        return "NULL"
    # NUL cannot be stored in PostgreSQL text; backslashes are literal because every SWEEP psql
    # session sets standard_conforming_strings=on (see BASE_PGOPTIONS).
    escaped = str(val).replace("\x00", "").replace("'", "''")
    return f"'{escaped}'"


def oracle_to_pg(dt, length, prec, scale):
    """Translates Oracle data type to PostgreSQL equivalent."""
    d = re.sub(r"\(.*\)", "", str(dt).upper()).strip()
    length = int(length) if str(length).strip() not in ("", "nan", "None") else None
    prec = int(float(prec)) if str(prec).strip() not in ("", "nan", "None") else None
    scale = int(float(scale)) if str(scale).strip() not in ("", "nan", "None") else None

    if d in ("VARCHAR2", "NVARCHAR2", "VARCHAR"):
        return f"varchar({length})" if length and length <= 10485760 else "text"
    if d in ("CHAR", "NCHAR"):
        return f"char({length})" if length else "char(1)"
    if d in ("NUMBER", "NUMERIC", "DECIMAL"):
        if prec is None or (scale is not None and (scale < 0 or scale > prec)):
            return "numeric"
        if not scale or scale == 0:
            if prec <= 4:
                return "smallint"
            if prec <= 9:
                return "integer"
            if prec <= 18:
                return "bigint"
            return f"numeric({prec},0)"
        return f"numeric({prec},{scale})"
    if d == "DATE":
        return "timestamp(0)"
    if d.startswith("TIMESTAMP"):
        return "timestamptz" if "TIME ZONE" in str(dt).upper() else "timestamp"
    if d.startswith("INTERVAL"):
        return "interval"
    if d in ("FLOAT", "BINARY_DOUBLE"):
        return "double precision"
    if d == "BINARY_FLOAT":
        return "real"
    if d in ("CLOB", "NCLOB", "LONG", "BFILE"):
        return "text"
    if d in ("BLOB", "RAW", "LONG RAW"):
        return "bytea"
    if d == "XMLTYPE":
        return "xml"
    if d in ("ROWID", "UROWID"):
        return "varchar(4000)"
    return "text"


def bound_pg(txt):
    """Converts Oracle partition bound expression to clean PostgreSQL literal."""
    t = str(txt).strip()
    if t.upper() in ("MAXVALUE", "DEFAULT", "MINVALUE"):
        return t.upper()
    m = re.match(r"(?:TO_DATE|TO_TIMESTAMP)\s*\(\s*'\s*([^']*?)\s*'", t, re.I) or \
        re.match(r"(?:TIMESTAMP|DATE)\s*'\s*([^']*?)\s*'", t, re.I)
    if m:
        v = m.group(1).strip()
        if v.endswith(" 00:00:00"):
            v = v[:-9]
        return f"'{v}'"
    return t


def split_bounds(txt):
    """Splits multi-column partition bound strings without breaking commas inside parens or quotes."""
    out, depth, cur, in_quote = [], 0, [], False
    for ch in txt:
        if ch == "'":
            in_quote = not in_quote
        if not in_quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                out.append("".join(cur).strip())
                cur = []
                continue
        cur.append(ch)
    if cur:
        out.append("".join(cur).strip())
    return [x for x in out if x]


def child_name(table_name, part_name):
    """Generates a PostgreSQL-compliant child partition table name."""
    t = norm_col(table_name)
    p = norm_col(part_name)
    return f"{t}_{p}"[:63]

def format_size_actual(mb_val):
    """
    Converts size in MB to actual human-readable size units:
      < 1 MB         -> X KB
      1 MB - 1023 MB -> X MB  (e.g., 999MB, 78.3MB)
      1024 MB - 1 TB -> X GB  (e.g., 1500MB -> 1.5GB, 3365.7MB -> 3.3GB)
      >= 1 TB        -> X TB  (e.g., 1500GB -> 1.5TB)
    """
    if mb_val is None:
        return '0MB'
    try:
        mb = float(mb_val)
    except PARSE_ERRORS:
        return str(mb_val)
    if mb < 0:
        return f'-{format_size_actual(-mb)}'
    if mb >= 1024 * 1024:
        tb = mb / (1024.0 * 1024.0)
        s = f'{tb:.1f}'.rstrip('0').rstrip('.')
        return f'{s}TB'
    elif mb >= 1024:
        gb = mb / 1024.0
        s = f'{gb:.1f}'.rstrip('0').rstrip('.')
        return f'{s}GB'
    elif mb >= 1:
        s = f'{mb:.1f}'.rstrip('0').rstrip('.')
        return f'{s}MB'
    elif mb > 0:
        kb = mb * 1024.0
        return f'{kb:.0f}KB'
    return '0MB'
