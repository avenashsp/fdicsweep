#!/usr/bin/env python3
"""
oracle_06_order.py - Parent-first load order for Oracle Step 06.

Tables are loaded level by level: level 0 holds tables that reference nobody (parents / lookup tables), level 1
tables that reference level 0, and so on. All files of a level finish before the next level starts, so child rows
never arrive before their parents and the FK_MISSING check does not see false orphans. Foreign keys stay enabled.

Dependencies come from
  1. the foreign keys extracted from the Oracle DDL in Step 01 (oracle_01_metadata_constraints.csv, ctype R) - on
     Day-1 the target tables have no foreign keys yet, so this is the main source;
  2. the live foreign keys in PostgreSQL (pg_constraint), when they exist.
Self-references are ignored; tables in a reference cycle share one level.
"""
import csv
from collections import defaultdict

from sweep_common import META_CONSTRAINTS_CSV, get_logger, norm_col, query_db_dicts

logger = get_logger("oracle_06_order", "oracle_06_load_data")


def key_of(schema, table):
    return (str(schema).strip().lower(), norm_col(str(table).strip()))


def extracted_edges(csv_path=META_CONSTRAINTS_CSV):
    """{(child schema, child table): {(parent schema, parent table)}} from the Step 01 constraints CSV."""
    edges = defaultdict(set)
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh):
                if (r.get("ctype") or "").strip().upper() != "R":
                    continue
                owner, table = (r.get("owner") or "").strip(), (r.get("table_name") or "").strip()
                ref_owner = (r.get("ref_owner") or "").strip() or owner
                ref_table = (r.get("ref_table") or "").strip()
                if owner and table and ref_table:
                    edges[key_of(owner, table)].add(key_of(ref_owner, ref_table))
    except OSError as e:
        logger.warning(f"Foreign-key metadata {csv_path} not readable ({e}); load order relies on live foreign keys only")
    return edges


def live_edges():
    """Foreign keys that already exist in PostgreSQL."""
    edges = defaultdict(set)
    try:
        rows = query_db_dicts(
            "SELECT cn.nspname AS child_schema, c.relname AS child_table, pn.nspname AS parent_schema, "
            "p.relname AS parent_table FROM pg_constraint k "
            "JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace cn ON cn.oid = c.relnamespace "
            "JOIN pg_class p ON p.oid = k.confrelid JOIN pg_namespace pn ON pn.oid = p.relnamespace "
            "WHERE k.contype = 'f' AND cn.nspname NOT IN ('pg_catalog', 'information_schema')")
    except RuntimeError as e:
        logger.warning(f"Could not read live foreign keys ({e}); using the extracted metadata only")
        return edges
    for r in rows:
        edges[key_of(r["child_schema"], r["child_table"])].add(key_of(r["parent_schema"], r["parent_table"]))
    return edges


def compute_levels(nodes, edges):
    """
    {node: level}. `edges` maps child -> parents; only edges between `nodes` count, self-references are ignored,
    strongly connected components (cycles) are collapsed into one level.
    """
    nodes = list(dict.fromkeys(nodes))
    node_set = set(nodes)
    parents = {n: sorted((p for p in edges.get(n, ()) if p in node_set and p != n)) for n in nodes}

    # Tarjan's strongly connected components (iterative)
    index, low, on_stack, stack, comp_of, comps = {}, {}, set(), [], {}, []
    counter = 0
    for root in nodes:
        if root in index:
            continue
        work = [(root, iter(parents[root]))]
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            v, it = work[-1]
            advanced = False
            for w in it:
                if w not in index:
                    index[w] = low[w] = counter
                    counter += 1
                    stack.append(w)
                    on_stack.add(w)
                    work.append((w, iter(parents[w])))
                    advanced = True
                    break
                if w in on_stack:
                    low[v] = min(low[v], index[w])
            if advanced:
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[v])
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp_of[w] = len(comps)
                    comp.append(w)
                    if w == v:
                        break
                comps.append(comp)

    comp_parents = [set() for _ in comps]
    for n in nodes:
        for p in parents[n]:
            if comp_of[p] != comp_of[n]:
                comp_parents[comp_of[n]].add(comp_of[p])

    level = {}

    def level_of(c):
        if c in level:
            return level[c]
        stack_c = [c]
        while stack_c:                    # iterative DFS (no recursion limit on deep chains)
            cur = stack_c[-1]
            pending = [p for p in comp_parents[cur] if p not in level]
            if pending:
                stack_c.extend(pending)
            else:
                level[cur] = 1 + max((level[p] for p in comp_parents[cur]), default=-1)
                stack_c.pop()
        return level[c]

    cycles = [c for c in comps if len(c) > 1]
    if cycles:
        logger.warning(f"{len(cycles)} group(s) of tables reference each other (cycle); each group is loaded in one level")
    return {n: level_of(comp_of[n]) for n in nodes}


def plan_levels(jobs):
    """
    Groups jobs into dependency levels. Returns [(level, [jobs...]), ...] ordered by level; inside a level the
    files are ordered small to large.
    """
    nodes = [key_of(j["target_schema"], j["target_table"]) for j in jobs]
    edges = defaultdict(set)
    for source in (extracted_edges(), live_edges()):
        for child, parents in source.items():
            edges[child] |= parents
    lv = compute_levels(nodes, edges)
    by_level = defaultdict(list)
    for j, n in zip(jobs, nodes):
        j["load_level"] = lv[n]
        by_level[lv[n]].append(j)
    out = []
    for level in sorted(by_level):
        files = sorted(by_level[level], key=lambda x: (int(x.get("size_bytes") or 0), x.get("filename", "")))
        out.append((level, files))
    return out
