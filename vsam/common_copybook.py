#!/usr/bin/env python3
"""
vsam/common_copybook.py - COBOL copybook parsing, SQL type derivation and column-profile merging.
"""
import re
from datetime import datetime
from pathlib import Path
import pandas as pd
from sweep_common import norm_col
from vsam.common_config import FN, USAGE


def norm_usage(u):
    """COMPUTATIONAL-3 / PACKED-DECIMAL -> COMP-3, COMPUTATIONAL / BINARY -> COMP."""
    u = (u or "").upper().replace("COMPUTATIONAL", "COMP")
    return {"BINARY": "COMP", "PACKED-DECIMAL": "COMP-3"}.get(u, u)


def dedupe(names):
    seen, out = {}, []
    for n in names:
        if n in seen:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            out.append(n)
    return out


def parse_filename(name):
    """FDICSWP_<COMPANY>_<SOURCE>_FLAT[_MMDDYYYY].csv[.gz] -> (company_token, source, date|None)"""
    m = FN.match(name)
    if not m:
        return None
    co, src, d = m.groups()
    try:
        dt = datetime.strptime(d, "%m%d%Y").date() if d else None
    except ValueError:
        dt = None
    return co.upper(), src.upper(), dt


def infer_columns_from_df(df: pd.DataFrame) -> list:
    """
    Infers column names and PostgreSQL datatypes directly from a DataFrame's header and data.
    Used for AGILETIC source company or when copybook is missing.
    """
    inferred_cols = []
    if df is None or df.empty:
        return inferred_cols

    for col_name in df.columns:
        c_norm = norm_col(col_name)
        val = df[col_name]
        col_series = val.iloc[:, 0] if isinstance(val, pd.DataFrame) else val
        s_clean = col_series.dropna().astype(str).str.strip()
        s_non_empty = s_clean[s_clean != ""]
        if s_non_empty.empty:
            sql_type = "text"
        elif (s_non_empty.str.fullmatch(r"^\d{4}-\d{2}-\d{2}$").sum() / len(s_non_empty)) >= 0.8:
            sql_type = "date"
        elif (s_non_empty.str.fullmatch(r"^\d{4}_\d{2}_\d{2}$").sum() / len(s_non_empty)) >= 0.8:
            sql_type = "varchar(10)"
        elif (s_non_empty.str.fullmatch(r"^-?\d+$").sum() / len(s_non_empty)) == 1.0:
            max_len = int(s_non_empty.str.len().max())
            sql_type = "integer" if max_len <= 9 else "bigint"
        elif (s_non_empty.str.fullmatch(r"^-?\d+(\.\d+)?$").sum() / len(s_non_empty)) >= 0.9:
            sql_type = "numeric"
        else:
            max_len = int(s_non_empty.str.len().max())
            sql_type = f"varchar({max(max_len + 4, 10)})" if max_len <= 255 else "text"

        inferred_cols.append({
            "col_name": c_norm,
            "sql_type": sql_type,
            "is_filler": "NO"
        })
    return inferred_cols


# -----------------------------------------------------------------------------
# Copybook Parser
# -----------------------------------------------------------------------------
def sql_type(pic, usage):
    u = norm_usage(usage)
    if u == "COMP-1":
        return "real"
    if u == "COMP-2":
        return "double precision"
    p = re.sub(r"([A-Za-z9])\((\d+)\)", lambda m: m.group(1) * int(m.group(2)), (pic or "").upper())
    if p and set(p) <= set("S9V"):
        ip, _, dp = p.replace("S", "").partition("V")
        tot = len(ip) + len(dp)
        if not dp:
            return "integer" if tot <= 9 else "bigint" if tot <= 18 else f"numeric({tot},0)"
        return f"numeric({tot},{len(dp)})"
    if p and set(p) <= set("XA9") and ("X" in p or "A" in p):
        return f"varchar({len(p)})"
    return "text"


def field_lengths(pic, usage):
    u = norm_usage(usage)
    if u == "COMP-1":
        return 4, 4
    if u == "COMP-2":
        return 8, 8
    p = re.sub(r"([A-Za-z9])\((\d+)\)", lambda m: m.group(1) * int(m.group(2)), (pic or "").upper())
    if "X" in p or "A" in p:
        return len(p), len(p)
    digits = p.count("9")
    if u in ("COMP", "COMP-4", "COMP-5"):
        return digits, 2 if digits <= 4 else 4 if digits <= 9 else 8
    if u == "COMP-3":
        return digits, digits // 2 + 1
    return digits, digits


def parse_copybook(path):
    lines = []
    for ln in Path(path).read_text(errors="ignore").splitlines():
        ln = re.sub(r"^\d{5,8}\s*", "", ln)
        if ln[:1] == "*" or ln.strip().startswith(("*", "/")) or (len(ln) > 0 and ln[0] in "*/"):
            continue
        lines.append(ln.strip())
    stmts = [s.strip() for s in re.split(r"\.(?=\s|$)", " ".join(lines)) if s.strip()]
    root = {"level": 0, "children": []}
    stack = [root]
    for s in stmts:
        m = re.match(r"(\d{1,2})\s+([A-Za-z0-9_\-]+)\s*(.*)$", s, re.S)
        if not m:
            continue
        lvl, name, rest = int(m.group(1)), m.group(2), m.group(3)
        if lvl in (66, 88):
            continue
        pic = re.search(r"\bPIC(?:TURE)?\s+(?:IS\s+)?(\S+)", rest, re.I)
        occ = re.search(r"\bOCCURS\s+(\d+)", rest, re.I)
        node = {
            "level": lvl, "name": name,
            "redef": bool(re.search(r"\bREDEFINES\b", rest, re.I)),
            "pic": pic.group(1) if pic else None,
            "usage": (norm_usage(USAGE.search(rest).group(1)) if USAGE.search(rest) else None),
            "occurs": int(occ.group(1)) if occ else None, "children": []
        }
        while len(stack) > 1 and stack[-1]["level"] >= lvl:
            stack.pop()
        stack[-1]["children"].append(node)
        stack.append(node)

    def flat(nodes, sfx=""):
        for n in nodes:
            if n["redef"]:
                continue
            for i in range(1, (n["occurs"] or 1) + 1):
                s2 = sfx + (f"_{i}" if n["occurs"] else "")
                if n["children"]:
                    yield from flat(n["children"], s2)
                elif n["pic"] or n["usage"] in ("COMP-1", "COMP-2"):
                    ld, ls = field_lengths(n["pic"], n["usage"])
                    yield {
                        "name": n["name"] + s2, "pic": n["pic"], "usage": n["usage"],
                        "filler": n["name"].upper() == "FILLER", "len_display": ld, "len_storage": ls,
                        "sql_type": sql_type(n["pic"], n["usage"])
                    }
    return list(flat(root["children"]))


def copybook_types(fields):
    names = dedupe([norm_col(f["name"]) for f in fields])
    return dict(zip(names, [f["sql_type"] for f in fields]))


# -----------------------------------------------------------------------------
# Target Profiles Merger for Multi-Company Shared Tables
# -----------------------------------------------------------------------------
def _num_info(t):
    t = t.lower()
    if t in ("smallint", "integer", "bigint"):
        return {"smallint": (5, 0), "integer": (10, 0), "bigint": (19, 0)}[t]
    m = re.fullmatch(r"numeric\((\d+),(\d+)\)", t)
    return (int(m.group(1)), int(m.group(2))) if m else ("any", 0) if t == "numeric" else None


def merge_type(a, b):
    if a == b:
        return a
    ta, tb = a.lower(), b.lower()
    va, vb = re.fullmatch(r"varchar\((\d+)\)", ta), re.fullmatch(r"varchar\((\d+)\)", tb)
    if va and vb:
        return f"varchar({max(int(va.group(1)), int(vb.group(1)))})"
    na, nb = _num_info(ta), _num_info(tb)
    if na and nb:
        if na[0] == "any" or nb[0] == "any":
            return "numeric"
        scale = max(na[1], nb[1])
        ints = max(na[0] - na[1], nb[0] - nb[1])
        if na[1] == 0 and nb[1] == 0:
            return "bigint" if max(na[0], nb[0]) > 10 else "integer"
        return f"numeric({ints + scale},{scale})"
    if {ta, tb} <= {"date", "timestamp"}:
        return "timestamp"
    return "text"


def merge_profiles(plist):
    import copy
    base = copy.deepcopy(plist[0])
    byname = {c["name"]: c for c in base["columns"]}
    for p in plist[1:]:
        for c in p["columns"]:
            if c["name"] in byname:
                byname[c["name"]]["sql_type"] = merge_type(byname[c["name"]]["sql_type"], c["sql_type"])
            else:
                nc = copy.deepcopy(c)
                base["columns"].append(nc)
                byname[nc["name"]] = nc
    keys = [list(p.get("key") or []) for p in plist]
    common = [k for k in keys[0] if all(k in kk for kk in keys)]
    if all(kk == keys[0] for kk in keys):
        base["key"] = keys[0]
    else:
        base["key"] = common
        base["key_source"] = "SHARED_COMMON_COLUMNS" if common else "NONE"
    base["shared_by"] = sorted({f"{p.get('company')}|{p.get('source_file')}" for p in plist})
    base["issues"] = list(base.get("issues", [])) + [
        f"SHARED_TABLE_MULTI_COMPANY:{len(base['shared_by'])} sources, key {base['key']} verified per company only"
    ]
    return base


def target_profiles(lm, prof):
    out = {}
    for (s, t), g in lm.groupby(["target_schema", "target_table"]):
        ps = []
        for co, src in dict.fromkeys(zip(g["source_company"], g["source_file"])):
            p = prof.get(f"{co}|{src}")
            if p and p.get("status") == "OK":
                ps.append(p)
        if ps:
            out[(s, t)] = ps[0] if len(ps) == 1 else merge_profiles(ps)
    return out
