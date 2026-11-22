#!/usr/bin/env python3
"""
sweep_merge.py - merge-safe, atomic output writers.

Before 6.0.0 every step overwrote its whole output. A targeted run (--tables, --schemas, --company, --file ...) must
change ONLY its own entries and leave everything else byte-identical, so the CSV/JSON writers here

  * REPLACE the whole file for a full run (a run without filters) and MERGE for a targeted run:
    rows the run is responsible for (`scope`) are removed, the new rows are added, all other rows are kept as they are;
  * lock the file (file_lock), write a temp file and os.replace it (a half-written file never exists);
  * keep one `<name>.bak` of the previous version before every write;
  * stamp provenance: every NEW CSV row gets `_run_id` and `_updated_at`, every NEW JSON entry a `_meta` block;
  * log "targeted run: merged N entries, kept M".
"""
import contextlib
import csv
import io
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from sweep_common import get_logger
from sweep_platform import atomic_write_text, file_lock

logger = get_logger("sweep_merge", to_file=False)

PROVENANCE_COLS = ("_run_id", "_updated_at")
LOCK_TIMEOUT = float(os.getenv("SWEEP_MERGE_LOCK_TIMEOUT", "60"))


def current_run_id():
    """Run id shared by all writes of one process (SWEEP_RUN_ID when the CLI set one, else a timestamp)."""
    rid = os.environ.get("SWEEP_RUN_ID")
    if not rid:
        rid = datetime.now().strftime("%Y%m%d_%H%M%S") + f"_{os.getpid()}"
        os.environ["SWEEP_RUN_ID"] = rid
    return rid


def _now():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def backup_path(path):
    path = Path(path)
    return path.with_name(path.name + ".bak")


def _keep_backup(path):
    """One .bak of the previous version (overwritten every time)."""
    path = Path(path)
    if path.exists():
        shutil.copy2(path, backup_path(path))


# -----------------------------------------------------------------------------
# CSV
# -----------------------------------------------------------------------------
def read_csv_rows(path):
    """(fieldnames, [row dicts]) of a CSV file; ([], []) when it is missing or empty."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    with open(path, "r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


def atomic_write_csv(path, rows, fieldnames):
    """Writes rows (list of dicts) as CSV through a temp file + os.replace. Header only when there are no rows."""
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=list(fieldnames), extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            fh.write(buf.getvalue())
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def merge_csv(path, new_rows, *, scope=None, full=False, fieldnames=None, post=None, label=None):
    """
    Writes `new_rows` into the CSV at `path`.
      full=True (or scope=None): the file is replaced by `new_rows`.
      otherwise: existing rows with scope(row) True are dropped, `new_rows` are added, other rows are kept untouched.
    `fieldnames`: header used when the file is new / replaced; merged files keep their columns and gain new ones.
    `post(rows) -> rows` runs on the final list (e.g. re-numbering a rank column).
    Returns (written, kept) row counts.
    """
    path = Path(path)
    stamp = {"_run_id": current_run_id(), "_updated_at": _now()}
    new_rows = [{**r, **stamp} for r in new_rows]
    with file_lock(path, timeout=LOCK_TIMEOUT):
        old_fields, old_rows = read_csv_rows(path)
        _keep_backup(path)
        if full or scope is None:
            kept = []
        else:
            kept = [r for r in old_rows if not scope(r)]
        merged = kept + new_rows
        if post:
            merged = post(merged)
        header = list(fieldnames or (list(new_rows[0].keys()) if new_rows else old_fields))
        if not (full or scope is None):
            for col in old_fields + [k for r in new_rows for k in r.keys()]:
                if col not in header:
                    header.append(col)
        for col in PROVENANCE_COLS:
            if col not in header:
                header.append(col)
        atomic_write_csv(path, merged, header)
    if not (full or scope is None):
        logger.info(f"targeted run: merged {len(new_rows)} entries, kept {len(kept)} in {path.name}"
                    + (f" ({label})" if label else ""))
    return len(new_rows), len(kept)


# -----------------------------------------------------------------------------
# JSON ({key: entry} documents)
# -----------------------------------------------------------------------------
def merge_json(path, new_entries, *, full=False, stamp=True, label=None):
    """
    Merges a {key: entry} mapping into the JSON document at `path`: the run's keys are replaced / added, other keys are
    kept untouched. full=True replaces the whole document. Dict entries get a `_meta` block {run_id, timestamp}.
    Returns (written, kept).
    """
    path = Path(path)
    meta = {"run_id": current_run_id(), "timestamp": _now()}
    entries = {}
    for k, v in new_entries.items():
        entries[k] = {**v, "_meta": meta} if (stamp and isinstance(v, dict)) else v
    with file_lock(path, timeout=LOCK_TIMEOUT):
        old = {}
        if path.exists() and not full:
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                old = loaded if isinstance(loaded, dict) else {}
            except (OSError, ValueError):
                logger.warning(f"{path.name} is not valid JSON; it is replaced by this run's entries")
        _keep_backup(path)
        kept = {k: v for k, v in old.items() if k not in entries}
        atomic_write_text(path, json.dumps({**kept, **entries}, indent=2, default=str))
    if not full:
        logger.info(f"targeted run: merged {len(entries)} entries, kept {len(kept)} in {path.name}"
                    + (f" ({label})" if label else ""))
    return len(entries), len(kept)


def write_text_file(path, text, *, keep_backup=True):
    """Atomic text write with a .bak of the previous version (for generated artefacts that are rewritten whole)."""
    path = Path(path)
    with file_lock(path, timeout=LOCK_TIMEOUT):
        if keep_backup:
            _keep_backup(path)
        atomic_write_text(path, text)

