#!/usr/bin/env python3
"""
vsam/common_config.py - VSAM paths, environment settings, folder discovery and safe Excel reads.
"""
import io
import os
import re
from pathlib import Path
import pandas as pd
from sweep_platform import read_file_bytes_shared
from sweep_common import (
    AWS_ERRORS,
    AWS_REGION,
    BASE_DIR,
    CTL_SCHEMA,
    get_logger,
    META_SCHEMA,
    VSAM_COPYBOOK_DIR,
    VSAM_INPUTS_DIR,
    VSAM_OUTPUTS_DIR,
)


logger = get_logger("vsam_common", "vsam_common")


def read_file_bytes_safe(path) -> bytes:
    """
    Reads all bytes of a file. On Windows uses full sharing flags so Excel/CSV files can be read
    even while open in Microsoft Excel (implementation lives in sweep_platform).
    """
    return read_file_bytes_shared(path)


def read_excel_safe(path, sheet_name=0, **kwargs):
    """
    Reads an Excel sheet into a pandas DataFrame safely, even if locked by Microsoft Excel.
    """
    raw_bytes = read_file_bytes_safe(path)
    return pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet_name, **kwargs)


BASE = Path(os.getenv("VSAM_BASE", str(BASE_DIR)))


COPYBOOK_DIR = Path(os.getenv("VSAM_COPYBOOK_DIR", str(VSAM_COPYBOOK_DIR)))


MASTER_MAPPING_FILE = Path(os.getenv("VSAM_MASTER_MAPPING", str(VSAM_INPUTS_DIR / "vsam_master_mapping.xlsx")))


MASTER_OVERRIDES_FILE = Path(os.getenv("VSAM_MASTER_OVERRIDES", str(VSAM_INPUTS_DIR / "vsam_master_overrides.xlsx")))


MASTER_REF = MASTER_MAPPING_FILE


OUT = VSAM_OUTPUTS_DIR


VSAM_GEN_DDL_DIR = OUT / "gen_ddl"


VSAM_PRE_LOAD_DIR = VSAM_GEN_DDL_DIR


VSAM_POST_LOAD_DIR = VSAM_GEN_DDL_DIR


CTL = CTL_SCHEMA


META = META_SCHEMA


DELIM = os.getenv("VSAM_DELIM", "auto")


PG_ENCODING = os.getenv("VSAM_ENCODING", "UTF8")


MAX_ROWS = int(os.getenv("VSAM_ANALYZE_MAX_ROWS", "0")) or None


COMMON_SCHEMA_CASE = os.getenv("VSAM_COMMON_SCHEMA_CASE", "lower")


VERSION_MODE = os.getenv("VSAM_VERSION_MODE", "upsert")


def get_vsam_folders(s3_uri=None):
    """
    Dynamically discovers folders / sub-prefixes to scan for VSAM files.
    1. If VSAM_FOLDERS env var is set, uses comma-separated list.
    2. If S3 URI provided, discovers common sub-prefixes under that URI via S3 API.
    3. If local data directory has subdirectories, lists all non-hidden directories.
    4. Defaults to [''] (scans the root prefix directly).
    """
    env_f = os.getenv("VSAM_FOLDERS")
    if env_f:
        return [f.strip() for f in env_f.split(",") if f.strip()]

    folders = []
    if s3_uri and s3_uri.startswith("s3://"):
        try:
            import boto3
            bucket_name = s3_uri[5:].split("/")[0]
            prefix = s3_uri[5:].split("/", 1)[1] if "/" in s3_uri[5:] else ""
            prefix = prefix.rstrip("/") + "/" if prefix else ""
            s3 = boto3.client("s3", region_name=AWS_REGION)
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    fld = cp["Prefix"][len(prefix):].strip("/")
                    if fld:
                        folders.append(fld)
        except AWS_ERRORS as exc:
            logger.warning(f"S3 folder discovery failed for {s3_uri}: {exc}")

    if not folders and BASE.exists():
        folders = [d.name for d in BASE.iterdir() if d.is_dir() and not d.name.startswith((".", "__")) and d.name not in ("logs", "outputs", "reports", "inputs")]

    return folders if folders else [""]


# Dynamically initialize FOLDERS
FOLDERS = get_vsam_folders()


def common_schema_name(name: str) -> str:
    """Normalizes common target schema name based on VSAM_COMMON_SCHEMA_CASE ('lower' or 'upper')."""
    if not name:
        return name
    return name.lower() if COMMON_SCHEMA_CASE == "lower" else name.upper()


FN = re.compile(r"^FDICSWP_([A-Z0-9]+)_(.+?)_FLAT(?:_(\d{8}))?\.csv(?:\.gz)?$", re.I)


IDENT = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]*$")


USAGE = re.compile(r"\b(COMPUTATIONAL-[1-5]|COMPUTATIONAL|COMP-[1-5]|COMP|BINARY|PACKED-DECIMAL)\b", re.I)


def get_pipeline_mode(arg_mode: str = None) -> str:
    """
    Returns active pipeline mode:
      - 'data': Raw data priority, NO PK constraints, TRUNCATE & LOAD, all rows loaded (incl. NULL/sentinel).
      - 'quality': Strict data quality, active PK constraints, DYNAMIC UPSERT, purges NULL/sentinel rows.
    Resolution: CLI argument -> VSAM_MODE env var -> default 'quality'.
    """
    mode = (arg_mode or os.getenv("VSAM_MODE", "quality")).strip().lower()
    if mode in ("data", "data-priority", "raw", "truncate", "no-pk"):
        return "data"
    return "quality"
