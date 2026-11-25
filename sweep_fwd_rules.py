#!/usr/bin/env python3
"""
sweep_fwd_rules.py - the loader rules registered by FE files (`-- sweep:ignore-column | rename-column | default-column | skip-table`).

FE files are applied in silver (never in bronze). `promote` reads the rules of the table from sweep_ctl.fwd_ddl_rules of the TARGET layer
and applies them while staging the delta of the previous layer (the Step 06 loaders load bronze and have no rules):
  ignore-column   the file still carries the column, the table no longer has it: it stays in the raw staging table and is left out of the typed one
  rename-column   the file column old_name loads into the table column new_name
  default-column  the file has no such column: the typed staging table gets the SQL expression (existing rows are never overwritten with it)
  skip-table      the table was dropped: its files are reported SKIPPED, not failed
A source column that is not in the table and has no rule still FAILS (an unexpected source change is never hidden). Delta capture uses the
TABLE's column names, so the deltas from silver onwards already match the tables of the next layers.
"""
from dataclasses import dataclass, field

from sweep_common import CTL_SCHEMA, query_db_dicts

FWD_HELP = ("bronze mirrors the source: fix the source file or the table; a deliberate change is made in silver with an FE file "
            "(`-- sweep:ignore-column` / `rename-column`, see docs/fwd_ddl.md), which `promote` applies")


def load_rules():
    """[(rule, schema, table, column, new_name, expression)] of the connected database ([] when the table does not exist yet)."""
    try:
        rows = query_db_dicts(f"SELECT rule, schema_name, table_name, column_name, new_name, expression FROM {CTL_SCHEMA}.fwd_ddl_rules")
    except RuntimeError:
        return []
    return [(r["rule"], r["schema_name"].lower(), r["table_name"].lower(), (r["column_name"] or "").lower(), (r["new_name"] or "").lower(),
             r["expression"] or "") for r in rows]


@dataclass
class TableRules:
    skip: bool = False
    ignore: set = field(default_factory=set)
    rename: dict = field(default_factory=dict)          # old file column -> table column
    default: dict = field(default_factory=dict)         # table column -> SQL expression

    def __bool__(self):
        return bool(self.skip or self.ignore or self.rename or self.default)


def rules_for(rules, schemas, table):
    """Rules of one table; `schemas` = the schema(s) the table is loaded into (a rule with schema '*' matches every schema)."""
    schemas = {s.lower() for s in ([schemas] if isinstance(schemas, str) else schemas)} | {"*"}
    out = TableRules()
    for rule, schema, t, col, new, expr in rules:
        if t != table.lower() or schema not in schemas:
            continue
        if rule == "skip-table":
            out.skip = True
        elif rule == "ignore-column":
            out.ignore.add(col)
        elif rule == "rename-column":
            out.rename[col] = new
        elif rule == "default-column":
            out.default[col] = expr
    return out


@dataclass
class Resolution:
    table_cols: list = field(default_factory=list)      # table columns the typed staging table holds (file order, then defaults)
    col_map: dict = field(default_factory=dict)         # table column -> raw (file) column
    ignored: list = field(default_factory=list)
    defaults: dict = field(default_factory=dict)        # table column -> expression
    problem: str = ""


def resolve_columns(file_cols, table_cols, rules):
    """Maps the columns of a file onto the table, applying the rules. `problem` is set when a column cannot be placed."""
    res = Resolution()
    unknown, bad_rename = [], []
    for fc in file_cols:
        if fc in rules.ignore:
            res.ignored.append(fc)
            continue
        target = rules.rename.get(fc, fc)
        if target not in table_cols:
            (bad_rename if fc in rules.rename else unknown).append(fc if fc not in rules.rename else f"{fc} -> {target}")
            continue
        if target in res.col_map:
            res.problem = f"Columns {res.col_map[target]} and {fc} of the file both load into {target}"
            return res
        res.table_cols.append(target)
        res.col_map[target] = fc
    if unknown:
        res.problem = (f"Columns in S3 file not found in table: {', '.join(unknown[:5])}"
                       f" (an unexpected source change is never hidden: {FWD_HELP})")
    elif bad_rename:
        res.problem = f"rename-column target does not exist in the table: {', '.join(bad_rename[:5])}"
    for col, expr in rules.default.items():
        if col in table_cols and col not in res.col_map:
            res.table_cols.append(col)
            res.defaults[col] = expr
    return res
