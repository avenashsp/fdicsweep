#!/usr/bin/env python3
"""
sweep_ddlfrag.py - generated DDL is written PER TABLE and the per-schema scripts are assembled from the fragments.

Step 04 of both tracks used to rewrite (and clean) a whole output folder, so generating one table destroyed the DDL of
all the others. Now

    <root>/fragments/<schema>/<table>.<kind>.sql      one fragment per table and script kind
    <root>/fragments/<schema>/_schema_.<kind>.sql     schema-level statements (sequences, views, triggers)
    <root>/manifest.json                              {"schema.table": {"kinds": [...], "_meta": {...}}}

A targeted run rewrites only its own fragments (and the manifest entries of its tables) and then re-assembles the
per-schema scripts of the schemas it touched from ALL fragments of the schema. A full run writes everything and then
prunes the fragments of tables that no longer exist. Every write is atomic and keeps one .bak (sweep_merge).
"""
import contextlib
import re
from pathlib import Path

from sweep_merge import merge_json, write_text_file

SCHEMA_LEVEL = "_schema_"
PREAMBLE = "_preamble_"          # constant script header of a schema, kept so a single table can be applied on its own
_SAFE = re.compile(r"[^A-Za-z0-9_#$.\-]")


def _safe(name):
    return _SAFE.sub("_", str(name))


class FragmentStore:
    def __init__(self, root):
        self.root = Path(root)
        self.frag_dir = self.root / "fragments"
        self.manifest = self.root / "manifest.json"

    # ---- fragments ---------------------------------------------------------------------------------------------
    def _path(self, schema, table, kind):
        return self.frag_dir / _safe(schema) / f"{_safe(table)}.{kind}.sql"

    def write(self, schema, table, kind, text):
        """Writes one fragment; an empty text removes a stale fragment of an earlier run."""
        path = self._path(schema, table, kind)
        if text and text.strip():
            write_text_file(path, text, keep_backup=False)
            return True
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        return False

    def tables(self, schema):
        """Table names that have fragments in `schema` (schema-level fragments excluded), sorted."""
        d = self.frag_dir / _safe(schema)
        if not d.is_dir():
            return []
        return sorted({p.name.rsplit(".", 2)[0] for p in d.glob("*.sql")} - {SCHEMA_LEVEL, PREAMBLE})

    def schemas(self):
        return sorted(p.name for p in self.frag_dir.iterdir() if p.is_dir()) if self.frag_dir.is_dir() else []

    def read(self, schema, table, kind):
        path = self._path(schema, table, kind)
        return path.read_text(encoding="utf-8") if path.exists() else ""

    def prune(self, keep):
        """Full run: drop the fragments of tables not in `keep` {(schema, table)}. Returns how many were removed."""
        removed = 0
        for schema in self.schemas():
            for table in self.tables(schema):
                if (schema, table) not in keep:
                    for p in (self.frag_dir / _safe(schema)).glob(f"{_safe(table)}.*.sql"):
                        p.unlink()
                        removed += 1
        return removed

    # ---- manifest ----------------------------------------------------------------------------------------------
    def record(self, entries, full):
        """Merges {"schema.table": {...}} into manifest.json (a full run replaces it)."""
        if full:
            # entries of tables that no longer exist disappear with the replacement
            return merge_json(self.manifest, entries, full=True)
        return merge_json(self.manifest, entries, full=False)

    # ---- assembly ----------------------------------------------------------------------------------------------
    def assemble(self, schema, kind, preamble, target):
        """Writes `target` = preamble + schema-level fragment + every table fragment of `kind`, tables sorted."""
        write_text_file(self._path(schema, PREAMBLE, kind), preamble, keep_backup=False)
        parts = [preamble]
        schema_level = self.read(schema, SCHEMA_LEVEL, kind)
        if schema_level:
            parts.append(schema_level if schema_level.endswith("\n") else schema_level + "\n")
        for table in self.tables(schema):
            text = self.read(schema, table, kind)
            if text:
                parts.append(text if text.endswith("\n") else text + "\n")
        write_text_file(target, "".join(parts))
        return target

    def script_for(self, schema, kind, tables):
        """Preamble + the fragments of the given tables (used to apply single tables without touching the others)."""
        parts = [self.read(schema, PREAMBLE, kind)]
        for table in tables:
            text = self.read(schema, table, kind)
            if text:
                parts.append(text if text.endswith("\n") else text + "\n")
        return "".join(parts)
