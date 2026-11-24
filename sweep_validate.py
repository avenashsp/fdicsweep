#!/usr/bin/env python3
"""
sweep_validate.py - the checks shared by Oracle Step 08, VSAM Step 07, `layers reconcile` / `layers check` and `sweep.py validate`.

  * ledger check (I2): count(table) == rows_before + sum(rows_inserted) of sweep_ctl.load_history, so a row that was deleted or
    added outside the loaders shows up. A keyless VSAM table is replaced by every run: its ledger is the rows of the latest run.
  * FE overlay (I3): a difference that an applied FE file explains is classified FE_ADDED / FE_CHANGED / FE_DROPPED, counts as PASS
    and is listed in the `FE_Changes` sheet. Nothing else is ever hidden.
"""
from collections import defaultdict

from sweep_common import CTL_SCHEMA, lit, q, query_db_dicts

LEDGER_MATCH, LEDGER_MISMATCH = "LEDGER_MATCH", "LEDGER_MISMATCH"
FE_STATUSES = ("FE_ADDED", "FE_CHANGED", "FE_DROPPED")


# -----------------------------------------------------------------------------
# I2: ledger
# -----------------------------------------------------------------------------
def _in_filter(schemas, tables):
    def ok(s, t):
        if schemas and s not in schemas:
            return False
        return not (tables and not tables.matches(s, t))
    return ok


def ledger_expected(dbname=None):
    """{(schema, table): {"expected", "track", "files", "last_run"}} from load_history (SUCCESS rows only)."""
    try:
        rows = query_db_dicts(f"""
            WITH ok AS (SELECT * FROM {CTL_SCHEMA}.load_history WHERE status = 'SUCCESS'),
            base AS (SELECT DISTINCT ON (target_schema, target_table) target_schema, target_table, id, COALESCE(rows_before, 0) AS b
                     FROM ok WHERE rows_before IS NOT NULL ORDER BY target_schema, target_table, id DESC),
            last_run AS (SELECT DISTINCT ON (target_schema, target_table) target_schema, target_table, run_id, track
                         FROM ok ORDER BY target_schema, target_table, id DESC)
            SELECT o.target_schema AS s, o.target_table AS t, l.track,
                   (b.b + sum(o.rows_inserted))::text AS since_base,
                   sum(o.rows_inserted) FILTER (WHERE o.run_id = l.run_id)::text AS last_run_rows,
                   count(*)::text AS loads
            FROM ok o JOIN base b ON (b.target_schema, b.target_table) = (o.target_schema, o.target_table) AND o.id >= b.id
                      JOIN last_run l ON (l.target_schema, l.target_table) = (o.target_schema, o.target_table)
            GROUP BY o.target_schema, o.target_table, l.track, b.b""", dbname=dbname)
    except RuntimeError:
        return {}
    return {(r["s"].lower(), r["t"].lower()): {"since_base": int(r["since_base"] or 0), "last_run_rows": int(r["last_run_rows"] or 0),
                                              "track": r["track"], "loads": int(r["loads"] or 0)} for r in rows}


def _keyed_tables(dbname=None):
    rows = query_db_dicts("SELECT DISTINCT n.nspname AS s, c.relname AS t FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid "
                          "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE i.indisunique AND i.indisvalid", dbname=dbname)
    return {(r["s"].lower(), r["t"].lower()) for r in rows}


def _existing_tables(dbname=None):
    rows = query_db_dicts("SELECT table_schema AS s, table_name AS t FROM information_schema.tables "
                          "WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')", dbname=dbname)
    return {(r["s"].lower(), r["t"].lower()) for r in rows}


def exact_counts(pairs, dbname=None, chunk=40):
    out = {}
    pairs = sorted(pairs)
    for i in range(0, len(pairs), chunk):
        part = pairs[i:i + chunk]
        sql = " UNION ALL ".join(f"SELECT {lit(s)} AS s, {lit(t)} AS t, count(*) AS n FROM {q(s)}.{q(t)}" for s, t in part)
        for r in query_db_dicts(sql, dbname=dbname):
            out[(r["s"], r["t"])] = int(r["n"])
    return out


def ledger_check(schemas=None, tables=None, fe=None, dbname=None):
    """
    One row per table that has load_history rows: expected (ledger) vs actual (exact count) and LEDGER_MATCH / LEDGER_MISMATCH.
    A table an FE file dropped is reported FE_DROPPED (not a mismatch). Returns a list of dicts.
    """
    ok = _in_filter(schemas, tables)
    expected = {k: v for k, v in ledger_expected(dbname).items() if ok(*k)}
    if not expected:
        return []
    existing = _existing_tables(dbname)
    keyed = _keyed_tables(dbname)
    counts = exact_counts([k for k in expected if k in existing], dbname)
    out = []
    for (s, t), e in sorted(expected.items()):
        snapshot = e["track"] == "vsam" and (s, t) not in keyed
        want = e["last_run_rows"] if snapshot else e["since_base"]
        row = {"target_schema": s, "target_table": t, "track": e["track"], "ledger_rule": "latest snapshot" if snapshot else "rows before + inserted",
               "ledger_loads": e["loads"], "ledger_rows": want, "actual_rows": counts.get((s, t), ""), "ledger_status": "", "note": ""}
        if (s, t) not in existing:
            if fe is not None and fe.table_dropped(s, t):
                row.update(ledger_status="FE_DROPPED", note=f"table dropped by {fe.dropped_tables[(s, t)]}")
            else:
                row.update(ledger_status=LEDGER_MISMATCH, note="table does not exist")
        elif counts[(s, t)] == want:
            row["ledger_status"] = LEDGER_MATCH
        else:
            diff = counts[(s, t)] - want
            row.update(ledger_status=LEDGER_MISMATCH, note=f"{diff:+d} row(s) not explained by the load history"
                       + (f" (table touched by {fe.changed_tables[(s, t)]})" if fe is not None and fe.table_touched(s, t) else ""))
        out.append(row)
    return out


# -----------------------------------------------------------------------------
# I3: FE overlay
# -----------------------------------------------------------------------------
class FeLog:
    """Collects the differences that applied FE files explain; becomes the FE_Changes sheet."""

    def __init__(self, fe):
        self.fe, self.rows = fe, []

    def add(self, status, schema, table, column="", file="", was="", detail=""):
        if any((r["fe_status"], r["target_schema"], r["target_table"], r["column_name"], r["fe_file"]) == (status, schema, table, column, file)
               for r in self.rows):
            return                      # the same explained difference seen by two checks is listed once
        self.rows.append({"fe_status": status, "target_schema": schema, "target_table": table, "column_name": column,
                          "fe_file": file, "was": was, "detail": detail})

    def table(self, schema, table, status):
        """FE status explaining a table level difference ('' = not explained)."""
        fe = self.fe
        if status in ("MISSING_IN_PG", "MISSING", "MISSING_IN_TARGET") and fe.table_dropped(schema, table):
            f = fe.dropped_tables[(schema, table)]
            self.add("FE_DROPPED", schema, table, file=f, was=status, detail="table dropped by an applied FE file")
            return "FE_DROPPED"
        if status in ("EXTRA_IN_PG", "EXTRA", "EXTRA_IN_TARGET") and (schema, table) in fe.added_tables:
            self.add("FE_ADDED", schema, table, file=fe.added_tables[(schema, table)], was=status, detail="table added by an applied FE file")
            return "FE_ADDED"
        return ""

    def column(self, schema, table, column, status):
        fe = self.fe
        key = (schema, table)
        if status in ("MISSING_IN_PG", "MISSING", "MISSING_IN_TARGET"):
            if fe.table_dropped(schema, table):
                f = fe.dropped_tables[key]
            elif column in fe.dropped_columns.get(key, {}):
                f = fe.dropped_columns[key][column]
            else:
                return ""
            self.add("FE_DROPPED", schema, table, column, f, status, "column dropped (or its table) by an applied FE file")
            return "FE_DROPPED"
        if status in ("EXTRA_IN_PG", "EXTRA", "EXTRA_IN_TARGET") and column in fe.added_columns.get(key, {}):
            self.add("FE_ADDED", schema, table, column, fe.added_columns[key][column], status, "column added by an applied FE file")
            return "FE_ADDED"
        if status in ("TYPE_MISMATCH", "NULLABILITY_MISMATCH", "TYPE_DIFFERS", "DEFAULT_MISMATCH") and column in fe.changed_columns.get(key, {}):
            self.add("FE_CHANGED", schema, table, column, fe.changed_columns[key][column], status, "column changed by an applied FE file")
            return "FE_CHANGED"
        return ""

    def sheet(self):
        if self.rows:
            return self.rows
        applied = sorted({r["file_name"] for r in self.fe.rows})
        return [{"fe_status": "NONE", "detail": "no difference is explained by FE files"
                 + (f" (applied FE files: {', '.join(applied)})" if applied else " (no FE file applied)")}]

    def counts(self):
        c = defaultdict(int)
        for r in self.rows:
            c[r["fe_status"]] += 1
        return dict(c)


def drop_discrepancies(discrepancies, schema, table, column=None, categories=None):
    """Removes explained entries from the discrepancy list (in place)."""
    keep = [d for d in discrepancies
            if not (d.get("Target_Schema") == schema and d.get("Target_Table") == table
                    and (column is None or d.get("Object_Name") == column)
                    and (categories is None or d.get("Object_Category") in categories))]
    discrepancies[:] = keep
