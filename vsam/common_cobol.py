#!/usr/bin/env python3
"""
vsam/common_cobol.py - Robust COBOL copybook reader (text -> field tree).

Handles the layouts found in real mainframe copybooks:
  * fixed-format sequence areas: columns 1-6 (line / change numbers such as ``106618``) and columns 73-80
    (``00040001``), including numbers glued to the closing period (``VALUE SPACE.00050001``)
  * comment lines (``*`` or ``/`` in column 7, ``*>`` inline comments) and banner lines (``=====``)
  * COPY ... REPLACING pseudo-text names such as ``:USA090:-KORG`` or ``:NEXT-MODEL:-KEY``
  * statements split over several lines, quoted literals containing periods, VALUE / OCCURS / REDEFINES
  * field names that start with digits (``06BALCUR``)

The module is pure text processing (no SQL types, no I/O beyond reading the file) so both the Step 01
extractor and the legacy `parse_copybook` helper build on it.
"""
import re
from pathlib import Path

USAGE_RX = re.compile(
    r"\b(COMPUTATIONAL-[1-5]|COMPUTATIONAL|COMP-[1-5]|COMP|BINARY|PACKED-DECIMAL)\b", re.I)
_SEPARATOR_RX = re.compile(r"^[\s=\-*_#~.]+$")
_PSEUDO_TEXT_RX = re.compile(r":([A-Za-z0-9][A-Za-z0-9_\-]*):")
_QUOTED_RX = re.compile(r"""("[^"]*"|'[^']*')""")
_TRAILING_SEQ_RX = re.compile(r"(\.)\d{6,8}\s*$|(?<=\s)\d{8}\s*$")


def _is_fixed_format(line):
    """True when columns 1-6 hold only a sequence/change number (or blanks) and column 7 is an indicator."""
    if len(line) < 7:
        return False
    area, indicator = line[:6], line[6]
    return bool(re.fullmatch(r"[0-9 ]{6}", area)) and indicator in " *-/D"


def clean_copybook_lines(text):
    """Returns the meaningful source lines of a copybook, stripped of sequence areas and comments."""
    out = []
    for raw in text.replace("\t", "    ").splitlines():
        line = raw.rstrip("\r\n")
        if not line.strip() or _SEPARATOR_RX.match(line):
            continue
        if _is_fixed_format(line):
            if line[6] in "*/":
                continue                      # comment line (indicator column)
            line = line[7:72]                 # drop columns 1-7 and the 73-80 identification area
            line = _TRAILING_SEQ_RX.sub(lambda m: m.group(1) or "", line)   # short lines: number right after the period
        else:
            stripped = line.strip()
            if stripped.startswith(("*", "/")):
                continue
            line = _TRAILING_SEQ_RX.sub(lambda m: m.group(1) or "", line)
            line = re.sub(r"^\d{5,8}\s+(?=\d{1,2}\s)", "", line)   # leading sequence number before a level number
        if "*>" in line:
            line = line[:line.index("*>")]
        if line.strip():
            out.append(line.strip())
    return out


def split_statements(lines):
    """Joins lines and splits on sentence-ending periods, ignoring periods inside quoted literals."""
    joined = _QUOTED_RX.sub(lambda m: m.group(0).replace(".", "\x00"), " ".join(lines))
    parts = re.split(r"\.(?=\s|$)", joined)
    return [p.replace("\x00", ".").strip() for p in parts if p.strip()]


def unwrap_pseudo_text(text):
    """``:USA090:-KORG`` -> ``USA090-KORG`` (COPY ... REPLACING placeholders)."""
    return _PSEUDO_TEXT_RX.sub(r"\1", text)


def parse_copybook_tree(text):
    """
    Parses copybook text into a tree. Each node is a dict:
      level, name, redef, pic, usage_raw, occurs, children
    Returns the synthetic root node (level 0).
    """
    statements = split_statements([unwrap_pseudo_text(l) for l in clean_copybook_lines(text)])
    root = {"level": 0, "name": "__ROOT__", "children": []}
    stack = [root]
    for stmt in statements:
        m = re.match(r"(\d{1,2})\s+([A-Za-z0-9_\-]+)\s*(.*)$", stmt, re.S)
        if not m:
            continue
        level, name, rest = int(m.group(1)), m.group(2), m.group(3)
        if level in (66, 88):
            continue
        pic = re.search(r"\bPIC(?:TURE)?\s+(?:IS\s+)?(\S+)", rest, re.I)
        occ = re.search(r"\bOCCURS\s+(\d+)", rest, re.I)
        usage = USAGE_RX.search(rest)
        node = {
            "level": level,
            "name": name,
            "redef": bool(re.search(r"\bREDEFINES\b", rest, re.I)),
            "pic": pic.group(1) if pic else None,
            "usage_raw": usage.group(1) if usage else None,
            "occurs": int(occ.group(1)) if occ else None,
            "children": [],
        }
        while len(stack) > 1 and stack[-1]["level"] >= level:
            stack.pop()
        stack[-1]["children"].append(node)
        stack.append(node)
    return root


def read_copybook_tree(path):
    return parse_copybook_tree(Path(path).read_text(errors="ignore"))
