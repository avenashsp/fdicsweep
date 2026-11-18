#!/usr/bin/env python3
"""
vsam/common_colmap.py - Maps S3 file headers to copybook columns (VSAM Step 02) and serves the result to
every later step.

Why: VSAM files always have a header row, but the header names and their order do not match the copybook.
Table columns are therefore named after the S3 headers (default) while each column's datatype comes from the
copybook column the header was matched to. `--tbl-col copybook` names the table columns after the copybook
instead.

Matching, in order of precedence per header:
  1. OVERRIDE   rows of the `column_overrides` sheet in vsam_master_overrides.xlsx (human decision always wins)
  2. STICKY     the mapping of the previous run for headers that are still present (keeps delta files stable)
  3. EXACT_NAME header equals a copybook column after normalisation
  4. ALGORITHM  multi-factor name similarity + optimal one-to-one assignment (Kuhn-Munkres / Hungarian) over the
                headers and copybook columns that are still unmatched

Adapted from the VSAM column mapper prototype (name segmentation, mainframe abbreviations, acronym and
modifier guards). Differences: cached canonical forms, exact/override/sticky passes before the global
assignment, a numpy Hungarian solver, a stricter acronym rule, and confidence levels that gate key columns.
"""
import csv
import difflib
import io
import os
import re
from collections import Counter

import numpy as np
import pandas as pd

from sweep_common import FILE_READ_ERRORS, norm_col
from vsam.common_config import MASTER_OVERRIDES_FILE, OUT, logger, read_excel_safe
from vsam.common_copybook import dedupe

TBL_COL_CHOICES = ("headers", "copybook")
COLMAP_FILE = OUT / "vsam_02_column_map.csv"
ACCEPT_SCORE = float(os.getenv("VSAM_MAP_ACCEPT", "0.70"))   # >= : ACCEPTED (HIGH / EXACT)
MIN_SCORE = float(os.getenv("VSAM_MAP_MIN", "0.35"))         # >= : mapped but REVIEW (MEDIUM); below: UNMAPPED
EXACT_SCORE = 0.88

COLMAP_FIELDS = [
    "source_company", "source_filename", "latest_filename", "copybook_name", "header_position", "header",
    "table_column", "copybook_field", "copybook_col", "pic", "usage", "copybook_type", "pg_type", "is_key",
    "score", "confidence", "status", "method", "tbl_col_mode",
]


def resolve_tbl_col(value=None):
    """CLI value -> VSAM_TBL_COL environment variable -> 'headers'."""
    v = (value or os.getenv("VSAM_TBL_COL") or "headers").strip().lower()
    if v not in TBL_COL_CHOICES:
        raise ValueError(f"--tbl-col must be one of {', '.join(TBL_COL_CHOICES)} (got {v!r})")
    return v


# =============================================================================
# Mainframe lexicon
# =============================================================================
KNOWN_SEGMENTS = sorted(set([
    "RESPONSIBLE", "ACCNTING", "ACCOUNTING", "UNIT", "ORGANIZATION", "PRODUCT", "BRANCH", "OFFICER",
    "MAINTENANCE", "MAINT", "REALIGNMENT", "REALIGN", "PRIORITY", "PRI", "CLOSING", "CLOSED", "CLOSE", "NOTICE",
    "NOTC", "SWEEP", "SWP", "TRANSFER", "TRNSFR", "INVESTMENT", "INVST", "INV", "REDEMPTION", "REDMTN", "REDEMP",
    "RED", "PURCHASE", "PURCH", "PUR", "TARGET", "TRGT", "TGT", "CEILING", "CLNG", "CLG", "FLOOR", "FLR", "FAMILY",
    "STATEMENT", "STMT", "STM", "STATE", "STATUS", "STAT", "SHORT", "SPECIAL", "SPEC", "OPTION", "OPT", "PARAM",
    "PARM", "BASIS", "POINT", "BP", "MONTH", "MNTHD", "MNTH", "MON", "YEAR", "BALANCE", "BAL", "AMOUNT", "AMT",
    "INCREMENT", "INCR", "EMAIL", "MEDIA", "FROZEN", "CLASS", "AGGREGATE", "AGR", "PAID", "FEDERAL", "FED",
    "WITHHELD", "WTHLD", "WH", "FREQUENCY", "FREQ", "TERM", "DAYS", "DAY", "NEXT", "NXT", "LAST", "LST",
    "PREVIOUS", "PREV", "PRV", "TODAY", "OPEN", "COST", "CNTR", "CTR", "DDA", "DIV", "FEE", "FEES", "TAX",
    "ACCOUNT", "ACCT", "ORG", "DATE", "DT", "DAT", "DTE", "CODE", "CD", "IDENTIFIER", "ID", "FLAG", "FLG", "FL",
    "IND", "INDICATOR", "NUMBER", "NBR", "NUM", "NO", "YTD", "LYR", "MTD", "EOY", "RAU", "BLK", "LM", "AVG",
    "PCT", "KEY", "USER", "FUND", "MIN", "MAX", "CUSTOMER", "CUST", "FIRST", "FST", "FN", "MIDDLE", "MID",
    "INITIAL", "INIT", "SURNAME", "NAME", "NM", "PRICE", "PRC", "DISCOUNT", "DISC", "SHIPPING", "SHIP",
    "QUANTITY", "QTY", "CREATION", "CREATED", "UPDATE", "UPDT", "TIME", "TIMESTAMP", "TS", "TMS", "ACTIVE",
    "ACTV", "SIGNUP", "ENROLL", "ENROLLMENT", "EFF", "EFFECTIVE", "EXP", "EXPIR", "EXPIRATION", "K", "M", "T", "B", "L", "WS",
]), key=len, reverse=True)

CANONICAL_MAP = {
    "DDA": "ACCOUNT", "KACCOUNT": "ACCOUNT", "KORG": "ORGANIZATION", "ORG": "ORGANIZATION",
    "NBR": "NUMBER", "NUM": "NUMBER", "NO": "NUMBER", "ACCT": "ACCOUNT", "ACCNTING": "ACCOUNTING",
    "RAU": "RESPONSIBLE_ACCOUNTING_UNIT", "BP": "BASIS_POINT", "EOY": "END_OF_YEAR", "YTD": "YEAR_TO_DATE",
    "LYR": "LAST_YEAR", "LM": "LAST_MONTH", "MTD": "MONTH_TO_DATE", "SWP": "SWEEP", "BLK": "BLOCK",
    "NOTC": "NOTICE", "STM": "STATEMENT", "STMT": "STATEMENT", "FL": "FLAG", "FLG": "FLAG", "IND": "FLAG",
    "INDICATOR": "FLAG", "DT": "DATE", "DAT": "DATE", "DTE": "DATE", "CD": "CODE", "CNTR": "CENTER",
    "PRV": "PREVIOUS", "PREV": "PREVIOUS", "TRNSFR": "TRANSFER", "INVST": "INVESTMENT", "INV": "INVESTMENT",
    "REDMTN": "REDEMPTION", "REDEMP": "REDEMPTION", "RED": "REDEMPTION", "PURCH": "PURCHASE", "PUR": "PURCHASE",
    "TRGT": "TARGET", "TGT": "TARGET", "CLNG": "CEILING", "CLG": "CEILING", "FLR": "FLOOR", "BAL": "BALANCE",
    "AMT": "AMOUNT", "INCR": "INCREMENT", "STAT": "STATUS", "MAINT": "MAINTENANCE", "REALIGN": "REALIGNMENT",
    "MNTH": "MONTH", "MNTHD": "MONTH_CODE", "MON": "MONTH", "PARM": "PARAMETER", "WTHLD": "WITHHELD",
    "WH": "WITHHELD", "AVG": "AVERAGE", "AGR": "AGGREGATE", "NXT": "NEXT", "PRI": "PRIORITY", "OPT": "OPTION",
    "FREQ": "FREQUENCY", "CUST": "CUSTOMER", "CLI": "CUSTOMER", "CLIENT": "CUSTOMER", "FST": "FIRST",
    "FN": "FIRST", "FNAME": "FIRST_NAME", "LST": "LAST", "LN": "LAST", "LNAME": "LAST_NAME",
    "SURNAME": "LAST_NAME", "MID": "MIDDLE", "MDL": "MIDDLE", "MN": "MIDDLE", "MNAME": "MIDDLE_NAME",
    "INIT": "INITIAL", "INITIALS": "INITIAL", "PRC": "PRICE", "COST": "PRICE", "DISC": "DISCOUNT",
    "SHIP": "SHIPPING", "QTY": "QUANTITY", "CNT": "COUNT", "CTR": "COUNTER", "CRTD": "CREATE", "CREAT": "CREATE",
    "UPDT": "UPDATE", "MOD": "UPDATE", "MODIFIED": "UPDATE", "TS": "TIMESTAMP", "TMS": "TIMESTAMP", "TM": "TIME",
    "ADDR": "ADDRESS", "ADR": "ADDRESS", "STR": "STREET", "STRT": "STREET", "ST": "STREET", "CTY": "CITY",
    "STE": "STATE", "ZIP": "POSTAL", "PST": "POSTAL", "POSTAL": "POSTAL", "PHN": "PHONE", "TEL": "PHONE",
    "TELE": "PHONE", "MOB": "PHONE", "EML": "EMAIL", "MAIL": "EMAIL", "DOB": "BIRTH_DATE", "CR": "CREDIT",
    "CRD": "CREDIT", "DR": "DEBIT", "LMT": "LIMIT", "ACTV": "ACTIVE", "INACTV": "INACTIVE",
    "SIGNUP": "REGISTRATION", "ENROLL": "REGISTRATION", "ENROLLMENT": "REGISTRATION", "REG": "REGISTRATION",
    "FEE": "FEE", "FEES": "FEE", "DIV": "DIVIDEND", "TERM": "TERM", "SHORT": "SHORT",
    "EFF": "EFFECTIVE", "EXP": "EXPIRATION", "EXPIR": "EXPIRATION",
}
SUFFIX_WORDS = {"CD", "DT", "FLG", "FL", "ID", "NBR", "NUM", "NO", "AMT", "OPT", "PARM"}
PREFIX_WORDS = {"PREV", "PRV", "LAST", "LST", "TODAY", "OPEN", "CLOSE"}
_PROTECTED = set(KNOWN_SEGMENTS[:20])


# =============================================================================
# Name canonicalisation + similarity (cached)
# =============================================================================
def segment_compound_name(name):
    """Splits an unbroken name (MFEEACCT) into known segments with a shortest-path DP."""
    clean = re.sub(r"[^A-Z0-9]", "", name.upper())
    n = len(clean)
    if n == 0:
        return []
    dp = {0: []}
    for i in range(n):
        if i not in dp:
            continue
        digits = re.match(r"^(\d+)", clean[i:])
        if digits:
            nxt = i + len(digits.group(1))
            if nxt not in dp or len(dp[i]) + 1 < len(dp[nxt]):
                dp[nxt] = dp[i] + [digits.group(1)]
        for w in KNOWN_SEGMENTS:
            if clean.startswith(w, i):
                nxt = i + len(w)
                if nxt not in dp or len(dp[i]) + 1 < len(dp[nxt]):
                    dp[nxt] = dp[i] + [w]
    return dp[n] if n in dp else [clean]


def extract_base_and_index(name):
    """ADDR_LINE_1 -> ('ADDR_LINE', 1); PHONE2 -> ('PHONE', 2)."""
    m = re.search(r"[_\-\[\(](\d+)[\)\]]?$", name)
    if m:
        return name[:m.start()].rstrip("_-"), int(m.group(1))
    m2 = re.search(r"([A-Za-z]+)(\d)$", name)
    if m2 and len(m2.group(1)) >= 3:
        return m2.group(1), int(m2.group(2))
    return name, None


def detect_common_prefix(names):
    """A table prefix (e.g. 'MXSOPTO', 'USA090') shared by at least 35% of the copybook columns."""
    prefixes = [re.split(r"[-_]", n)[0].upper() for n in names if len(re.split(r"[-_]", n)) > 1]
    if not prefixes:
        return ""
    most, freq = Counter(prefixes).most_common(1)[0]
    return most if freq >= max(2, len(names) * 0.35) else ""


_CANON_CACHE = {}


def canonicalize(name, table_prefix="", is_copybook=False):
    key = (name, table_prefix, is_copybook)
    hit = _CANON_CACHE.get(key)
    if hit is not None:
        return hit
    base, idx = extract_base_and_index(name)
    clean = base.upper()
    if table_prefix:
        tp = table_prefix.upper()
        if clean.startswith(tp + "-") or clean.startswith(tp + "_"):
            clean = clean[len(tp) + 1:]
        elif clean.startswith(tp):
            clean = clean[len(tp):].lstrip("_-")
    clean = re.sub(r"_+", "_", re.sub(r"[^A-Z0-9]", "_", clean)).strip("_")

    raw = []
    if "_" in clean:
        for part in clean.split("_"):
            if part:
                if len(part) >= 6 and part not in _PROTECTED:
                    raw.extend(segment_compound_name(part))
                else:
                    raw.append(part)
    else:
        raw = segment_compound_name(clean)

    # Mainframe single-letter prefixes (M master, K key, T transfer, B bank/block, L last) are optional on BOTH
    # sides: header `b_korg` must match copybook column `KORG`.
    raw_nopfx = raw
    if len(raw) > 1 and raw[0] in ("M", "K", "T", "B", "L", "WS"):
        raw_nopfx = raw[1:]

    def expand(tokens):
        out = []
        for t in tokens:
            out.extend(CANONICAL_MAP.get(t, t).split("_"))
        return out

    expanded, expanded_nopfx = expand(raw), expand(raw_nopfx)
    core = [t for t in raw if t not in SUFFIX_WORDS and t not in PREFIX_WORDS]
    acronyms = set()
    if len(core) >= 2:
        acr = "".join(w[0] for w in core if w)
        if len(acr) >= 2:
            acronyms.add(acr)
    info = {
        "raw": raw, "raw_nopfx": raw_nopfx, "expanded": expanded, "expanded_nopfx": expanded_nopfx,
        "set": frozenset(expanded), "set_nopfx": frozenset(expanded_nopfx), "acronyms": acronyms,
        "joined": "_".join(expanded), "joined_nopfx": "_".join(expanded_nopfx), "idx": idx,
        "has_prev": any(t in ("PREV", "PRV") for t in raw), "has_today": "TODAY" in raw,
        "has_fee": any(t in ("FEE", "FEES") for t in raw), "has_div": "DIV" in raw,
        "has_stm": any(t in ("STM", "STMT") for t in raw),
    }
    _CANON_CACHE[key] = info
    return info


def similarity(s3, cb):
    """Score 0..1 between canonical forms of an S3 header and a copybook column."""
    idx_bonus = 0.0
    if s3["idx"] is not None and cb["idx"] is not None:
        if s3["idx"] != cb["idx"]:
            return 0.0                            # PHONE_1 vs PHONE_2: conflicting occurrence
        idx_bonus = 0.15
    elif s3["idx"] is not None or cb["idx"] is not None:
        idx_bonus = -0.10

    for flag in ("has_prev", "has_today"):
        if s3[flag] != cb[flag]:
            return 0.05
    for flag in ("has_fee", "has_div", "has_stm"):
        if s3[flag] != cb[flag]:
            return 0.05

    if {s3["joined"], s3["joined_nopfx"]} & {cb["joined"], cb["joined_nopfx"]}:
        return min(1.0, 1.0 + idx_bonus)

    # Acronym rule (stricter than the prototype): only when one side IS the acronym (a single token),
    # so FIRST_NAME does not match 'FN-ADDR'.
    if len(cb["raw_nopfx"]) == 1 and cb["raw_nopfx"][0] in s3["acronyms"]:
        return min(1.0, 0.98 + idx_bonus)
    if len(s3["raw"]) == 1 and s3["raw"][0] in cb["acronyms"]:
        return min(1.0, 0.98 + idx_bonus)

    a, b = s3["set_nopfx"], cb["set_nopfx"]
    inter, union = len(a & b), len(a | b)
    jaccard = inter / union if union else 0.0
    subset_bonus = 0.0
    if a and b:
        if a == b:
            return min(1.0, 0.98 + idx_bonus)
        if a <= b or b <= a:
            subset_bonus = 0.25

    seq = 0.0
    for other in (cb["joined"], cb["joined_nopfx"]):
        sm = difflib.SequenceMatcher(None, s3["joined_nopfx"], other)
        if sm.real_quick_ratio() >= 0.3 and sm.quick_ratio() >= 0.3:
            seq = max(seq, sm.ratio())
    return max(0.0, min(1.0, 0.50 * jaccard + 0.30 * seq + subset_bonus + idx_bonus))


# =============================================================================
# Optimal assignment (Kuhn-Munkres), numpy-vectorised inner loop
# =============================================================================
def hungarian_max(score):
    """
    Maximum-weight one-to-one assignment. `score` is an n x m array; returns a list of length n holding the
    assigned column index per row, or -1 for rows that received only padding.
    """
    score = np.asarray(score, dtype=float)
    n, m = score.shape if score.ndim == 2 else (0, 0)
    if n == 0 or m == 0:
        return [-1] * n
    k = max(n, m)
    cost = np.zeros((k, k))
    cost[:n, :m] = score.max() - score
    cost[n:, :] = score.max()
    cost[:, m:] = score.max()
    u = np.zeros(k + 1)
    v = np.zeros(k + 1)
    p = np.zeros(k + 1, dtype=int)
    way = np.zeros(k + 1, dtype=int)
    for i in range(1, k + 1):
        p[0] = i
        j0 = 0
        minv = np.full(k + 1, np.inf)
        used = np.zeros(k + 1, dtype=bool)
        while True:
            used[j0] = True
            i0 = p[j0]
            cur = cost[i0 - 1, :] - u[i0] - v[1:]
            free = ~used[1:]
            better = free & (cur < minv[1:])
            minv[1:][better] = cur[better]
            way[1:][better] = j0
            masked = np.where(free, minv[1:], np.inf)
            j1 = int(masked.argmin()) + 1
            delta = masked[j1 - 1]
            used_idx = np.nonzero(used)[0]
            u[p[used_idx]] += delta
            v[used_idx] -= delta
            minv[1:][free] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break
    ans = [-1] * n
    for j in range(1, k + 1):
        row = p[j] - 1
        if 0 <= row < n and j - 1 < m:
            ans[row] = j - 1
    return ans


# =============================================================================
# Mapping
# =============================================================================
def _confidence(score, method):
    if method in ("OVERRIDE", "EXACT_NAME", "STICKY"):
        return "EXACT" if method != "STICKY" else "STICKY"
    return "EXACT" if score >= EXACT_SCORE else "HIGH" if score >= ACCEPT_SCORE else "MEDIUM"


def _entry(position, header, field, method, score, status, mode=None):
    pg = (field or {}).get("sql_type") or ""
    return {
        "header_position": position, "header": header,
        "copybook_field": (field or {}).get("field_name", ""),
        "copybook_col": (field or {}).get("col_name", ""),
        "pic": (field or {}).get("pic", "") or "", "usage": (field or {}).get("usage", "") or "",
        "copybook_type": pg, "pg_type": pg or "text",
        "is_key": "YES" if (field or {}).get("is_key") in (True, "YES") else "NO",
        "score": round(float(score), 3), "method": method, "status": status,
        "confidence": _confidence(score, method) if status != "UNMAPPED" else "UNMAPPED",
    }


def map_columns(headers, fields, overrides=None, previous=None, threshold=None):
    """
    headers   normalised S3 header names in file order
    fields    copybook field dicts (col_name, field_name, pic, usage, sql_type, is_key, is_filler)
    overrides {header_norm: copybook_col or ''}  ('' forces UNMAPPED)
    previous  {header_norm: copybook_col} from the last run (sticky)
    Returns one entry per header (file order) followed by COPYBOOK_ONLY entries for unused copybook columns.
    """
    min_score = MIN_SCORE if threshold is None else threshold
    overrides, previous = overrides or {}, previous or {}
    data_fields = [f for f in fields if f.get("is_filler") not in (True, "YES", "yes", "True")]
    by_col = {}
    for f in data_fields:
        by_col.setdefault(f["col_name"], f)
        by_col.setdefault(norm_col(f.get("field_name", "")), f)

    result = [None] * len(headers)
    used = set()

    def take(i, field, method, score, status):
        result[i] = _entry(i + 1, headers[i], field, method, score, status)
        used.add(field["col_name"])

    # 1. overrides
    for i, h in enumerate(headers):
        if h in overrides:
            target = overrides[h]
            if not target or target.upper() == "UNMAPPED":
                result[i] = _entry(i + 1, h, None, "OVERRIDE", 0.0, "UNMAPPED")
                result[i]["confidence"] = "OVERRIDE"
            elif norm_col(target) in by_col and by_col[norm_col(target)]["col_name"] not in used:
                take(i, by_col[norm_col(target)], "OVERRIDE", 1.0, "ACCEPTED")
            else:
                logger.warning(f"column_overrides: '{h}' -> '{target}' ignored (copybook column unknown or already used)")
    # 2. sticky (previous run)
    for i, h in enumerate(headers):
        if result[i] is None and previous.get(h) and previous[h] in by_col and by_col[previous[h]]["col_name"] not in used:
            take(i, by_col[previous[h]], "STICKY", 1.0, "ACCEPTED")
    # 3. exact names
    for i, h in enumerate(headers):
        if result[i] is None and h in by_col and by_col[h]["col_name"] not in used:
            take(i, by_col[h], "EXACT_NAME", 1.0, "ACCEPTED")

    # 4. similarity + optimal assignment over what is left
    rest_h = [i for i, r in enumerate(result) if r is None]
    rest_f = [f for f in data_fields if f["col_name"] not in used]
    if rest_h and rest_f:
        prefix = detect_common_prefix([f.get("field_name", f["col_name"]) for f in data_fields])
        s3c = [canonicalize(headers[i], prefix, False) for i in rest_h]
        cbc = [canonicalize(f.get("field_name") or f["col_name"], prefix, True) for f in rest_f]
        matrix = np.zeros((len(rest_h), len(rest_f)))
        for a, s in enumerate(s3c):
            for b, c in enumerate(cbc):
                matrix[a, b] = similarity(s, c)
        rows = [a for a in range(len(rest_h)) if matrix[a].max() >= min_score]
        cols = [b for b in range(len(rest_f)) if matrix[:, b].max() >= min_score]
        if rows and cols:
            sub = matrix[np.ix_(rows, cols)]
            for ra, cb_ in enumerate(hungarian_max(sub)):
                if cb_ < 0:
                    continue
                a, b = rows[ra], cols[cb_]
                sc = matrix[a, b]
                if sc >= min_score:
                    i = rest_h[a]
                    take(i, rest_f[b], "ALGORITHM", sc, "ACCEPTED" if sc >= ACCEPT_SCORE else "REVIEW")

    for i, h in enumerate(headers):
        if result[i] is None:
            result[i] = _entry(i + 1, h, None, "NONE", 0.0, "UNMAPPED")

    for f in data_fields:                     # copybook columns that never appear in the file
        if f["col_name"] not in used:
            e = _entry(0, "", f, "NONE", 0.0, "COPYBOOK_ONLY")
            e["confidence"] = "-"
            e["pg_type"] = f.get("sql_type") or ""
            result.append(e)
    return result


def table_column_names(entries, mode):
    """Final table column per header entry (de-duplicated)."""
    names = []
    for e in entries:
        if e["status"] == "COPYBOOK_ONLY":
            continue
        if mode == "copybook" and e["copybook_col"] and e["status"] in ("ACCEPTED", "REVIEW"):
            names.append(e["copybook_col"])
        else:
            names.append(e["header"])
    return dedupe(names)


def finalize_entries(entries, mode, company, source, latest_filename, copybook_name):
    """Adds table_column + identity fields, returns rows ready for CSV / Excel."""
    tbl = table_column_names(entries, mode)
    out, k = [], 0
    for e in entries:
        row = dict(e)
        row.update({"source_company": company, "source_filename": source, "latest_filename": latest_filename,
                    "copybook_name": copybook_name, "tbl_col_mode": mode})
        if e["status"] == "COPYBOOK_ONLY":
            row["table_column"] = ""
        else:
            row["table_column"] = tbl[k]
            k += 1
        out.append(row)
    return out


def no_copybook_entries(headers):
    """Files without a copybook: every header is its own column (types are inferred from data in Step 03)."""
    entries = []
    for i, h in enumerate(headers):
        e = _entry(i + 1, h, None, "NONE", 0.0, "UNMAPPED")
        e["confidence"] = "NO_COPYBOOK"
        entries.append(e)
    return entries


def translate_key_columns(key_cols, entries, mode):
    """
    Copybook key columns -> table column names. Only ACCEPTED matches may carry a key; if any key column is
    missing or unreliable the whole key is dropped. Returns (table_key_columns, problems).
    """
    if not key_cols:
        return [], []
    tbl = {}
    names = table_column_names(entries, mode)
    k = 0
    for e in entries:
        if e["status"] == "COPYBOOK_ONLY":
            continue
        tbl[(e["copybook_col"] or "").lower()] = (names[k], e["status"])
        k += 1
    out, problems = [], []
    for c in key_cols:
        hit = tbl.get(c.lower())
        if not hit:
            problems.append(f"KEY_COLUMN_NOT_IN_FILE:{c}")
        elif hit[1] != "ACCEPTED":
            problems.append(f"KEY_COLUMN_MAPPING_{hit[1]}:{c}")
        else:
            out.append(hit[0])
    return ([] if problems else out), problems


def translate_key_columns_from_rows(key_cols, rows):
    """Same as `translate_key_columns`, but on finalized map rows (after shared-table unification)."""
    if not key_cols:
        return [], []
    tbl = {(r["copybook_col"] or "").lower(): (r["table_column"], r["status"])
           for r in rows if r["status"] != "COPYBOOK_ONLY" and r["copybook_col"]}
    out, problems = [], []
    for c in key_cols:
        hit = tbl.get(c.lower())
        if not hit:
            problems.append(f"KEY_COLUMN_NOT_IN_FILE:{c}")
        elif hit[1] != "ACCEPTED":
            problems.append(f"KEY_COLUMN_MAPPING_{hit[1]}:{c}")
        else:
            out.append(hit[0])
    return ([] if problems else out), problems


# =============================================================================
# Overrides sheet / persistence / consumers
# =============================================================================
def load_column_overrides():
    """{(COMPANY|'' , SOURCE_OR_FILE): {header_norm: copybook_col}} from sheet `column_overrides`."""
    out = {}
    if not MASTER_OVERRIDES_FILE.exists():
        return out
    try:
        df = read_excel_safe(MASTER_OVERRIDES_FILE, sheet_name="column_overrides", dtype=str).fillna("")
    except FILE_READ_ERRORS as exc:
        logger.debug(f"No column_overrides sheet: {exc}")
        return out
    for r in df.to_dict(orient="records"):
        header = norm_col(str(r.get("header", "")))
        fname = str(r.get("filename", "")).strip().upper()
        if not header or not fname:
            continue
        key = (str(r.get("sourcecompany", "")).strip().upper(), os.path.splitext(fname)[0])
        out.setdefault(key, {})[header] = norm_col(str(r.get("copybook_column", ""))) if str(r.get("copybook_column", "")).strip() else ""
    return out


def overrides_for(all_overrides, company, source, s3_filename=""):
    merged = {}
    stem = os.path.splitext(os.path.basename(s3_filename or "").upper())[0]
    for key in ((("", source.upper())), ((company or "").upper(), source.upper()), ("", stem), ((company or "").upper(), stem)):
        merged.update(all_overrides.get(key, {}))
    return merged


def load_previous_map(path=None):
    """{(COMPANY, SOURCE): {header_norm: copybook_col}} from the previous run (used for sticky mapping)."""
    path = path or COLMAP_FILE
    prev = {}
    if not path.exists():
        return prev
    try:
        for r in csv.DictReader(io.StringIO(path.read_text(encoding="utf-8"), newline="")):
            if r.get("status") in ("ACCEPTED", "REVIEW") and r.get("copybook_col"):
                prev.setdefault((r["source_company"].upper(), r["source_filename"].upper()), {})[r["header"]] = r["copybook_col"]
    except (OSError, csv.Error, KeyError) as exc:
        logger.warning(f"Could not read previous column map {path}: {exc}")
    return prev


def write_column_map(rows, path=None):
    path = path or COLMAP_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=COLMAP_FIELDS).to_csv(path, index=False)
    _MAP_CACHE.clear()
    return path


_MAP_CACHE = {}


def load_column_map(path=None):
    """{(COMPANY, SOURCE): [row dicts with header entries only, file order]} (cached)."""
    path = path or COLMAP_FILE
    key = str(path)
    if key in _MAP_CACHE and _MAP_CACHE[key][0] == (path.stat().st_mtime if path.exists() else None):
        return _MAP_CACHE[key][1]
    out = {}
    if path.exists():
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
        except FILE_READ_ERRORS as exc:
            logger.warning(f"Could not read column map {path}: {exc}")
            df = pd.DataFrame()
        for r in df.to_dict(orient="records"):
            if r.get("status") == "COPYBOOK_ONLY":
                continue
            out.setdefault((r["source_company"].upper(), r["source_filename"].upper()), []).append(r)
        for rows in out.values():
            rows.sort(key=lambda r: int(r.get("header_position") or 0))
    _MAP_CACHE[key] = (path.stat().st_mtime if path.exists() else None, out)
    return out


def effective_columns_meta(company, source, fallback_meta=None, path=None):
    """
    Column metadata for a source as the later steps need it.
      * With a column map: one entry per S3 header. `col_name` is the TABLE column (header or copybook name per
        --tbl-col), `header` is the name in the file, `sql_type` comes from the mapped copybook column.
      * Without one (no Step 02 map yet, or a source without a copybook): `fallback_meta` unchanged.
    """
    rows = load_column_map(path).get((str(company).upper(), str(source).upper()))
    if not rows or not any(r["status"] in ("ACCEPTED", "REVIEW") for r in rows):
        return fallback_meta if fallback_meta is not None else []
    meta = []
    for r in rows:
        mapped = r["status"] in ("ACCEPTED", "REVIEW")
        meta.append({
            "col_name": r["table_column"], "field_name": r["copybook_field"] or r["header"], "header": r["header"],
            "pic": r["pic"], "usage": r["usage"], "sql_type": (r["pg_type"] if mapped and r["pg_type"] else "text"),
            "len_display": "", "len_storage": "", "is_key": r["is_key"] if mapped else "NO", "is_filler": "NO",
            "map_status": r["status"], "map_score": r["score"],
        })
    return meta


def header_to_table_map(meta):
    """{header_norm: table_column} for a meta list produced by `effective_columns_meta` (identity otherwise)."""
    return {m.get("header") or m["col_name"]: m["col_name"] for m in (meta or [])}
