#!/usr/bin/env python3
"""
sweep_targeting.py - the targeting flags every step understands, implemented once.

    --tables a,b,...        SCHEMA.TABLE or bare table name, case-insensitive
    --schemas s1,s2         (aliases: --owners for Oracle steps, --schema for VSAM Step 07)
    --exclude-tables ...    same syntax as --tables
    --file / --filename     S3 file (full URI, key suffix or basename) for steps that work on files
    --company               VSAM company tokens
    --dry-run               print what would be touched, write nothing

A step builds a `Selector` from its parsed arguments, filters its own candidates with `selector.matches(schema, table)`
and validates the request against what exists with `selector.validate_tables(known)` BEFORE doing any work: an unknown
table, a file that matches nothing or an unparseable name is a clear message and exit code 2. A run without any
selector is a FULL run (`selector.filtered` is False); targeted runs merge their outputs instead of replacing them
(see sweep_merge.py).
"""
import difflib
import sys
from dataclasses import dataclass, field

EXIT_BAD_SELECTOR = 2


class SelectorError(ValueError):
    """A selector that cannot be satisfied (unknown table, no matching file, unparseable value)."""


def split_list(value):
    """'a, b;c' -> ['a', 'b', 'c'] (empty items dropped). Accepts None, a string or an iterable of strings."""
    if not value:
        return []
    if isinstance(value, str):
        value = value.replace(";", ",").split(",")
    return [v.strip() for v in value if v and v.strip()]


def parse_table_ref(text):
    """'BSS01001.BROKER' -> ('bss01001', 'broker'); 'broker' -> (None, 'broker'). Raises SelectorError when unparseable."""
    raw = str(text).strip().strip('"')
    if not raw:
        raise SelectorError("empty table name in --tables / --exclude-tables")
    parts = [p.strip().strip('"') for p in raw.split(".")]
    if len(parts) > 2 or any(not p for p in parts):
        raise SelectorError(f"cannot parse table name {text!r}: use SCHEMA.TABLE or TABLE")
    if len(parts) == 1:
        return None, parts[0].lower()
    return parts[0].lower(), parts[1].lower()


def _same_name(a, b):
    return str(a).strip().lower() == str(b).strip().lower()


@dataclass
class Selector:
    tables: list = field(default_factory=list)       # [(schema | None, table)]
    schemas: set = field(default_factory=set)
    exclude: list = field(default_factory=list)
    files: list = field(default_factory=list)
    companies: set = field(default_factory=set)
    copybooks: set = field(default_factory=set)      # copybook file names / stems, lower case
    dry_run: bool = False

    # ---- construction ------------------------------------------------------------------------------------------
    @classmethod
    def from_args(cls, args, use_files=True):
        """Builds the selector from a parsed argparse namespace (every attribute is optional)."""
        g = lambda name: getattr(args, name, None)
        schemas = set()
        for name in ("schemas", "owners", "schema"):
            schemas |= {s.lower() for s in split_list(g(name))}
        files = (split_list(g("file")) + split_list(g("filename"))) if use_files else []
        return cls(
            tables=[parse_table_ref(t) for t in split_list(g("tables"))],
            schemas=schemas,
            exclude=[parse_table_ref(t) for t in split_list(g("exclude_tables"))],
            files=files,
            companies={c.upper() for c in split_list(g("company"))},
            copybooks={c.lower() for c in split_list(g("copybook"))},
            dry_run=bool(g("dry_run")),
        )

    # ---- properties --------------------------------------------------------------------------------------------
    @property
    def filtered(self):
        """True when the run is targeted (any selector given). Only a run WITHOUT filters is a full run."""
        return bool(self.tables or self.schemas or self.exclude or self.files or self.companies or self.copybooks)

    @property
    def table_selectors(self):
        """True when the table set is narrowed (tables / schemas / excludes), as opposed to file or company selectors."""
        return bool(self.tables or self.schemas or self.exclude)

    # ---- matching ----------------------------------------------------------------------------------------------
    @staticmethod
    def _ref_matches(ref, schema, table):
        ref_schema, ref_table = ref
        return _same_name(ref_table, table) and (ref_schema is None or _same_name(ref_schema, schema or ""))

    def matches(self, schema, table):
        """True when schema.table is selected (no selectors = everything). Excludes always win."""
        if any(self._ref_matches(r, schema, table) for r in self.exclude):
            return False
        if self.schemas and str(schema or "").lower() not in self.schemas:
            return False
        if self.tables and not any(self._ref_matches(r, schema, table) for r in self.tables):
            return False
        return True

    def matches_table_name(self, table):
        """Name-only test (the schema is not known yet): used to pre-select work before targets are resolved."""
        t = str(table).strip().lower()
        if any(r_t == t and r_s is None for r_s, r_t in self.exclude):
            return False
        return not self.tables or any(r_t == t for _, r_t in self.tables)

    def matches_schema(self, schema):
        """Schema-level test (whole-schema scripts): honours --schemas only; table selectors do not narrow it."""
        return not self.schemas or str(schema).lower() in self.schemas

    def matches_file(self, uri):
        """True when `uri` is selected by --file/--filename (full URI, trailing path or basename; no selector = all)."""
        if not self.files:
            return True
        return any(self.matches_file_single(f, uri) for f in self.files)

    def matches_copybook(self, name):
        """True when the copybook file `name` is selected by --copybook (with or without extension; no selector = all)."""
        if not self.copybooks:
            return True
        n = str(name).strip().lower()
        return n in self.copybooks or n.rsplit(".", 1)[0] in {c.rsplit(".", 1)[0] for c in self.copybooks}

    def validate_copybooks(self, known, what="the copybook folder"):
        known = [str(k) for k in known]
        missing = [c for c in sorted(self.copybooks) if not any(
            k.lower() == c or k.lower().rsplit(".", 1)[0] == c.rsplit(".", 1)[0] for k in known)]
        if missing:
            near = difflib.get_close_matches(missing[0], [k.lower() for k in known], n=3, cutoff=0.5)
            raise SelectorError(f"unknown copybook {', '.join(missing)} in {what}" + (f" (did you mean: {', '.join(near)}?)" if near else ""))

    def matches_company(self, company):
        return not self.companies or str(company).strip().upper() in self.companies

    # ---- validation (before anything runs) --------------------------------------------------------------------
    def validate_tables(self, known, what="the inventory"):
        """Every requested table, schema and excluded table must exist in `known` [(schema, table)]."""
        known = [(str(s or "").lower(), str(t).lower()) for s, t in known]
        problems = []
        for sch, tbl in self.tables + self.exclude:
            if not any(self._ref_matches((sch, tbl), s, t) for s, t in known):
                label = f"{sch}.{tbl}" if sch else tbl
                near = difflib.get_close_matches(tbl, sorted({t for _, t in known}), n=3, cutoff=0.6)
                problems.append(f"unknown table '{label}' in {what}" + (f" (did you mean: {', '.join(near)}?)" if near else ""))
        known_schemas = {s for s, _ in known}
        for sch in sorted(self.schemas):
            if sch not in known_schemas:
                near = difflib.get_close_matches(sch, sorted(known_schemas), n=3, cutoff=0.6)
                problems.append(f"unknown schema '{sch}' in {what}" + (f" (did you mean: {', '.join(near)}?)" if near else ""))
        if problems:
            raise SelectorError("; ".join(problems))
        if self.table_selectors and not any(self.matches(s, t) for s, t in known):
            raise SelectorError(f"the selectors match no table in {what}")

    def validate_files(self, known_uris, what="the file map"):
        """Every --file/--filename must match at least one known S3 file."""
        known_uris = list(known_uris)
        missing = [f for f in self.files if not any(self.matches_file_single(f, u) for u in known_uris)]
        if missing:
            base = sorted({str(u).rsplit("/", 1)[-1] for u in known_uris})
            hints = []
            for m in missing:
                near = difflib.get_close_matches(m.rsplit("/", 1)[-1], base, n=3, cutoff=0.6)
                hints.append(f"no file matches '{m}' in {what}" + (f" (did you mean: {', '.join(near)}?)" if near else ""))
            raise SelectorError("; ".join(hints))

    @staticmethod
    def matches_file_single(selector_value, uri):
        u, f = str(uri).lower(), str(selector_value).strip().lower()
        return u == f or u.rsplit("/", 1)[-1] == f or u.endswith("/" + f.lstrip("/"))

    def validate_companies(self, known, what="the inventory"):
        known = {str(c).strip().upper() for c in known}
        missing = sorted(c for c in self.companies if c not in known)
        if missing:
            near = difflib.get_close_matches(missing[0], sorted(known), n=3, cutoff=0.5)
            raise SelectorError(f"unknown company {', '.join(missing)} in {what}"
                                + (f" (known: {', '.join(near)})" if near else ""))

    # ---- merge scope -------------------------------------------------------------------------------------------
    def table_scope(self, schema_col, table_col):
        """Predicate over a CSV row: does it belong to the tables this run is responsible for?"""
        return lambda row: self.matches(row.get(schema_col, ""), row.get(table_col, ""))

    def describe(self):
        bits = []
        if self.tables:
            bits.append("tables=" + ",".join(f"{s}.{t}" if s else t for s, t in self.tables))
        if self.schemas:
            bits.append("schemas=" + ",".join(sorted(self.schemas)))
        if self.exclude:
            bits.append("exclude=" + ",".join(f"{s}.{t}" if s else t for s, t in self.exclude))
        if self.files:
            bits.append("files=" + ",".join(self.files))
        if self.copybooks:
            bits.append("copybook=" + ",".join(sorted(self.copybooks)))
        if self.companies:
            bits.append("company=" + ",".join(sorted(self.companies)))
        return " ".join(bits) or "no filters (full run)"


# -----------------------------------------------------------------------------
# argparse helpers
# -----------------------------------------------------------------------------
def add_targeting_args(parser, *, tables=True, schemas="--schemas", exclude=True, file=False, company=False, copybook=False, dry_run=True):
    """
    Adds the uniform targeting flags to `parser`. Flags a parser already defines are skipped, so a step can keep a
    specific help text. `schemas` is the primary flag name (None = no schema flag); the aliases are added for it.
    """
    existing = {opt for a in parser._actions for opt in a.option_strings}

    def add(names, **kw):
        names = [n for n in names if n not in existing]
        if names:
            parser.add_argument(*names, **kw)
            existing.update(names)

    if tables:
        add(["--tables"], help="Comma-separated tables: SCHEMA.TABLE or TABLE (case-insensitive)")
    if schemas:
        aliases = {"--schemas": ["--schemas", "--owners"], "--owners": ["--owners", "--schemas"],
                   "--schema": ["--schema", "--schemas"]}.get(schemas, [schemas])
        add(aliases, dest="schemas" if schemas != "--schema" else "schema",
            help="Comma-separated schemas / owners (case-insensitive)")
    if exclude:
        add(["--exclude-tables"], help="Comma-separated tables to leave out (same syntax as --tables)")
    if file:
        add(["--file"], help="Specific S3 file (full URI, key suffix or file name)")
        add(["--filename"], help="Specific S3 file name (basename)")
    if company:
        add(["--company"], help="Comma-separated company tokens")
    if copybook:
        add(["--copybook"], help="Comma-separated copybook files (name with or without extension)")
    if dry_run:
        add(["--dry-run"], action="store_true", help="Print what would be touched; write nothing")


def selector_or_exit(args, logger=None, use_files=True):
    """Selector.from_args with a clear message and exit code 2 when a value cannot be parsed."""
    try:
        return Selector.from_args(args, use_files=use_files)
    except SelectorError as e:
        _fail(str(e), logger)


def validate_or_exit(fn, *a, logger=None, **kw):
    """Runs one of the Selector.validate_* methods; a SelectorError ends the process with exit code 2."""
    try:
        return fn(*a, **kw)
    except SelectorError as e:
        _fail(str(e), logger)


def exit_bad_selection(message, logger=None):
    """Prints 'Invalid selection: ...' and ends the process with exit code 2."""
    _fail(message, logger)


def _fail(message, logger=None):
    text = f"Invalid selection: {message}"
    if logger:
        logger.error(text)
    else:
        print(text, file=sys.stderr)
    sys.exit(EXIT_BAD_SELECTOR)
