#!/usr/bin/env python3
"""
oracle_08_audit.py - Comparison levels of the Oracle-dump vs PostgreSQL parity audit (Step 08).

Type normalisation helpers plus one function per audit level (schemas, tables, columns,
constraints, indexes, sequences, views/triggers, row counts). Each takes the parsed Oracle
metadata (`meta`) and the PostgreSQL catalogs (`pg_cat`) and returns its comparison rows;
`discrepancies` is an accumulator list shared across levels.
"""
import re
from collections import defaultdict

from sweep_common import get_logger, norm_col, oracle_to_pg

logger = get_logger("oracle_08_validate", "oracle_08_validate")

# Native FDICSweep application audit columns added by the framework
AUDIT_COLUMNS = {"createdby_m", "createddate_m", "modifiedby", "modifieddate"}


# -----------------------------------------------------------------------------
# Type Normalization & Compatibility Engine
# -----------------------------------------------------------------------------
def normalize_pg_type(t_str):
    """Normalizes PostgreSQL data type representations for exact parity checking."""
    if not t_str:
        return ""
    t = str(t_str).lower().strip()
    t = re.sub(r"\s+", " ", t)
    # Remove unnecessary modifier clauses
    t = re.sub(r"\bwithout time zone\b", "", t).strip()
    t = re.sub(r"\bcharacter varying\b", "varchar", t)
    t = re.sub(r"\bcharacter\b", "char", t)
    t = re.sub(r"\bbpchar\b", "char", t)
    if t in ("integer", "int4", "int"):
        return "integer"
    if t in ("bigint", "int8"):
        return "bigint"
    if t in ("smallint", "int2"):
        return "smallint"
    if t in ("double precision", "float8"):
        return "double precision"
    if t in ("real", "float4"):
        return "real"
    if t in ("boolean", "bool"):
        return "boolean"
    if t.startswith("timestamp") and "with time zone" in t:
        return "timestamptz"
    # numeric(p, 0) -> numeric(p,0)
    t = re.sub(r",\s+", ",", t)
    return t


def is_type_compatible(expected_type, actual_type):
    """
    Checks if target PostgreSQL type is compatible with expected mapped type.
    Handles standard SQL type widening and equivalent representations.
    """
    exp = normalize_pg_type(expected_type)
    act = normalize_pg_type(actual_type)

    if exp == act:
        return True, "EXACT_MATCH"

    # varchar(N) vs text
    if exp.startswith("varchar") and act == "text":
        return True, "COMPATIBLE_TEXT"
    if act.startswith("varchar") and exp == "text":
        return True, "COMPATIBLE_VARCHAR"

    # numeric(p,0) vs integer / bigint / smallint (symmetrically checked)
    m_exp_num = re.match(r"^numeric\((\d+),0\)$", exp)
    if m_exp_num:
        prec = int(m_exp_num.group(1))
        if prec <= 4 and act in ("smallint", "integer", "bigint", exp):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 10 and act in ("integer", "bigint", exp):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 18 and act in ("bigint", exp):
            return True, "COMPATIBLE_BIGINT"

    m_act_num = re.match(r"^numeric\((\d+),0\)$", act)
    if m_act_num:
        prec = int(m_act_num.group(1))
        if prec <= 4 and exp in ("smallint", "integer", "bigint", act):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 10 and exp in ("integer", "bigint", act):
            return True, "COMPATIBLE_INTEGER"
        if prec <= 18 and exp in ("bigint", act):
            return True, "COMPATIBLE_BIGINT"

    # numeric generic
    if exp == "numeric" and (act.startswith("numeric") or act in ("double precision", "real", "bigint", "integer")):
        return True, "COMPATIBLE_NUMERIC"
    if act == "numeric" and (exp.startswith("numeric") or exp in ("double precision", "real", "bigint", "integer")):
        return True, "COMPATIBLE_NUMERIC"

    # timestamp(0) vs timestamp
    if exp in ("timestamp(0)", "timestamp") and act in ("timestamp(0)", "timestamp"):
        return True, "COMPATIBLE_TIMESTAMP"

    # timestamptz vs timestamp with time zone
    if exp in ("timestamptz", "timestamp with time zone") and act in ("timestamptz", "timestamp with time zone"):
        return True, "COMPATIBLE_TIMESTAMPTZ"

    # char(1) vs varchar(1)
    if exp in ("char(1)", "varchar(1)") and act in ("char(1)", "varchar(1)"):
        return True, "COMPATIBLE_CHAR"

    return False, "TYPE_MISMATCH"


# -----------------------------------------------------------------------------
# Database Catalog Query Helpers (Pure psql CLI - Zero psycopg2 dependency)
# -----------------------------------------------------------------------------


def compare_schemas(db_connected, discrepancies, meta, pg_cat, schemas_filter):
    # -------------------------------------------------------------------------
    # LEVEL 1: Schema-Level Comparison
    # -------------------------------------------------------------------------
    logger.info("[4/7] Performing Schema-level, Table-level, and Column-level comparisons...")
    oracle_schemas = set()
    for col in meta["tables"]:
        o = (col.get("owner") or "").upper().strip()
        if o: oracle_schemas.add(o)
    for obj in meta["objects"]:
        o = (obj.get("owner") or "").upper().strip()
        if o: oracle_schemas.add(o)
    for s in meta["sequences"]:
        o = (s.get("owner") or "").upper().strip()
        if o: oracle_schemas.add(o)

    pg_schemas_set = {s["schema_name"].lower() for s in pg_cat["schemas"]}
    all_schemas = sorted(set(s.lower() for s in oracle_schemas) | pg_schemas_set)

    schema_comparison = []
    for s in all_schemas:
        if schemas_filter and s not in schemas_filter:
            continue
        in_oracle = s.upper() in oracle_schemas
        in_pg = s in pg_schemas_set

        # Count tables, views, sequences
        ora_tbl_cnt = len(set(col["table_name"].upper() for col in meta["tables"] if col.get("owner", "").upper() == s.upper()))
        pg_tbl_cnt = sum(1 for t in pg_cat["tables"] if t["table_schema"].lower() == s and not t["is_partition_child"])
        ora_view_cnt = sum(1 for o in meta["objects"] if o.get("owner", "").upper() == s.upper() and "VIEW" in o.get("object_type", "").upper())
        pg_view_cnt = sum(1 for v in pg_cat["views"] if v["view_schema"].lower() == s)
        ora_seq_cnt = sum(1 for sq in meta["sequences"] if sq.get("owner", "").upper() == s.upper())
        pg_seq_cnt = sum(1 for sq in pg_cat["sequences"] if sq["sequence_schema"].lower() == s)

        if not db_connected:
            status = "DB_UNREACHABLE"
            notes = "Cannot reach PostgreSQL to verify schema"
        elif in_oracle and in_pg:
            status = "MATCH"
            notes = "Schema exists in both Oracle dump and PostgreSQL"
        elif in_oracle and not in_pg:
            status = "MISSING_IN_PG"
            notes = "Schema defined in Oracle dump but missing in PostgreSQL"
            discrepancies.append({
                "Object_Category": "SCHEMA", "Target_Schema": s, "Target_Table": "",
                "Object_Name": s, "Discrepancy_Type": "MISSING_SCHEMA_IN_PG",
                "Oracle_Expected": "Present in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Execute 'CREATE SCHEMA IF NOT EXISTS \"{s}\";'"
            })
        else:
            status = "EXTRA_IN_PG"
            notes = "Schema exists in PostgreSQL but not in Oracle dump"

        schema_comparison.append({
            "target_schema": s,
            "in_oracle_dump": "YES" if in_oracle else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "tables_in_oracle": ora_tbl_cnt,
            "tables_in_postgres": pg_tbl_cnt,
            "views_in_oracle": ora_view_cnt,
            "views_in_postgres": pg_view_cnt,
            "sequences_in_oracle": ora_seq_cnt,
            "sequences_in_postgres": pg_seq_cnt,
            "status": status,
            "notes": notes
        })
    return oracle_schemas, schema_comparison


def compare_tables(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter):
    # -------------------------------------------------------------------------
    # LEVEL 2: Table-Level Comparison
    # -------------------------------------------------------------------------
    oracle_tables = {}
    for col in meta["tables"]:
        o = (col.get("owner") or "").lower().strip()
        t = norm_col(col.get("table_name") or "")
        orig_t = (col.get("table_name") or "").upper().strip()
        is_part = str(col.get("partitioned") or "").upper() == "YES"
        if o and t:
            if (o, t) not in oracle_tables:
                oracle_tables[(o, t)] = {
                    "owner": o, "table_name": t, "orig_table": orig_t,
                    "partitioned": is_part, "child_partitions": set()
                }

    # Populate child partition names from oracle_metadata_partitions.csv
    for p in meta["partitions"]:
        o = (p.get("owner") or "").lower().strip()
        t = norm_col(p.get("table_name") or "")
        part_name = norm_col(p.get("partition_name") or "")
        if (o, t) in oracle_tables:
            oracle_tables[(o, t)]["partitioned"] = True
            oracle_tables[(o, t)]["child_partitions"].add(part_name)

    # Index PostgreSQL tables
    pg_tables_map = {}
    pg_child_partitions = defaultdict(list)
    for t_row in pg_cat["tables"]:
        s = t_row["table_schema"].lower()
        t = t_row["table_name"].lower()
        pg_tables_map[(s, t)] = t_row
        if t_row["is_partition_child"]:
            ps = (t_row.get("parent_schema") or s).lower()
            pt = (t_row.get("parent_table") or "").lower()
            if pt:
                pg_child_partitions[(ps, pt)].append(t)

    all_table_keys = sorted(set(oracle_tables.keys()) | set(pg_tables_map.keys()))
    table_comparison = []

    for s, t in all_table_keys:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_t = oracle_tables.get((s, t))
        pg_t = pg_tables_map.get((s, t))
        in_oracle = ora_t is not None
        in_pg = pg_t is not None

        ora_part = ora_t["partitioned"] if ora_t else False
        ora_child_cnt = len(ora_t["child_partitions"]) if ora_t else 0
        pg_child_cnt = len(pg_child_partitions.get((s, t), []))
        pg_type = pg_t["table_type"] if pg_t else "MISSING"

        if not db_connected:
            tbl_status = "DB_UNREACHABLE"
            notes = "PostgreSQL catalog offline"
        elif in_oracle and in_pg:
            if ora_part and pg_t["relkind"] != "p":
                tbl_status = "PARTITION_MISMATCH"
                notes = f"Table partitioned in Oracle but created as {pg_type} in PostgreSQL"
                discrepancies.append({
                    "Object_Category": "TABLE", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": t, "Discrepancy_Type": "PARTITIONING_TYPE_MISMATCH",
                    "Oracle_Expected": "PARTITIONED TABLE", "Postgres_Actual": pg_type,
                    "Actionable_Recommendation": "Re-run Step 04 and Step 05 to create declarative partitioned table."
                })
            else:
                tbl_status = "MATCH"
                notes = f"Table exists with {pg_child_cnt} child partitions in PG"
        elif in_oracle and not in_pg:
            tbl_status = "MISSING_IN_PG"
            notes = "Table defined in Oracle dump but missing in PostgreSQL"
            discrepancies.append({
                "Object_Category": "TABLE", "Target_Schema": s, "Target_Table": t,
                "Object_Name": t, "Discrepancy_Type": "MISSING_TABLE_IN_PG",
                "Oracle_Expected": "Present in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": "Run Step 05: python sweep.py oracle apply-ddl"
            })
        else:
            tbl_status = "EXTRA_IN_PG"
            notes = "Table exists in PostgreSQL but not in Oracle dump"

        table_comparison.append({
            "target_schema": s,
            "target_table": t,
            "in_oracle_dump": "YES" if in_oracle else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "oracle_partitioned": "YES" if ora_part else "NO",
            "pg_table_type": pg_type,
            "oracle_child_partitions_count": ora_child_cnt,
            "pg_child_partitions_count": pg_child_cnt,
            "table_status": tbl_status,
            "notes": notes
        })
    return oracle_tables, pg_child_partitions, table_comparison


def compare_columns(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter):
    # -------------------------------------------------------------------------
    # LEVEL 3: Column & Datatype-Level Comparison
    # -------------------------------------------------------------------------
    oracle_cols_map = defaultdict(dict)
    for col in meta["tables"]:
        s = (col.get("owner") or "").lower().strip()
        t = norm_col(col.get("table_name") or "")
        c = norm_col(col.get("column_name") or "")
        if s and t and c:
            oracle_cols_map[(s, t)][c] = col

    pg_cols_map = defaultdict(dict)
    for col in pg_cat["columns"]:
        s = col["table_schema"].lower().strip()
        t = col["table_name"].lower().strip()
        c = col["column_name"].lower().strip()
        pg_cols_map[(s, t)][c] = col

    column_comparison = []
    evaluated_tables = set(oracle_cols_map.keys()) | set(pg_cols_map.keys())

    for s, t in sorted(evaluated_tables):
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_c_dict = oracle_cols_map.get((s, t), {})
        pg_c_dict = pg_cols_map.get((s, t), {})
        all_cols = sorted(set(ora_c_dict.keys()) | set(pg_c_dict.keys()))

        for c in all_cols:
            ora_c = ora_c_dict.get(c)
            pg_c = pg_c_dict.get(c)
            in_ora = ora_c is not None
            in_pg = pg_c is not None
            is_audit = c in AUDIT_COLUMNS

            ora_dt = ora_c.get("data_type", "") if ora_c else ""
            ora_len = ora_c.get("data_length", "") if ora_c else ""
            ora_prec = ora_c.get("data_precision", "") if ora_c else ""
            ora_scale = ora_c.get("data_scale", "") if ora_c else ""
            expected_pg = oracle_to_pg(ora_dt, ora_len, ora_prec, ora_scale) if ora_c else ""

            actual_pg = pg_c.get("formatted_type", "") if pg_c else ""
            cid = ora_c.get("column_id", "") if ora_c else pg_c.get("column_id", "")

            ora_null = ora_c.get("nullable", "Y") if ora_c else ""
            pg_null = "YES" if pg_c and pg_c.get("is_nullable") else ("NO" if pg_c else "")

            # Check Type Parity
            if not db_connected:
                overall_status = "DB_UNREACHABLE"
                type_match = "N/A"
                null_match = "N/A"
            elif is_audit and not in_ora and in_pg:
                overall_status = "AUDIT_COLUMN_OK"
                type_match = "MATCH (APPLICATION AUDIT)"
                null_match = "MATCH"
            elif in_ora and in_pg:
                compat, reason = is_type_compatible(expected_pg, actual_pg)
                type_match = "MATCH" if compat else "MISMATCH"

                # Nullability check
                exp_null_bool = (ora_null.upper() == "Y")
                act_null_bool = bool(pg_c.get("is_nullable"))
                null_match = "MATCH" if exp_null_bool == act_null_bool else "MISMATCH"

                if not compat:
                    overall_status = "TYPE_MISMATCH"
                    discrepancies.append({
                        "Object_Category": "COLUMN", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": c, "Discrepancy_Type": "DATA_TYPE_MISMATCH",
                        "Oracle_Expected": f"{ora_dt} -> {expected_pg}", "Postgres_Actual": actual_pg,
                        "Actionable_Recommendation": f"Alter column type: ALTER TABLE \"{s}\".\"{t}\" ALTER COLUMN \"{c}\" TYPE {expected_pg};"
                    })
                elif null_match == "MISMATCH":
                    overall_status = "NULLABILITY_MISMATCH"
                    discrepancies.append({
                        "Object_Category": "COLUMN", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": c, "Discrepancy_Type": "NULLABILITY_MISMATCH",
                        "Oracle_Expected": f"NULLABLE={ora_null}", "Postgres_Actual": f"NULLABLE={pg_null}",
                        "Actionable_Recommendation": f"Adjust nullability: ALTER TABLE \"{s}\".\"{t}\" ALTER COLUMN \"{c}\" {'DROP' if exp_null_bool else 'SET'} NOT NULL;"
                    })
                else:
                    overall_status = "MATCH"

            elif in_ora and not in_pg:
                overall_status = "MISSING_IN_PG"
                type_match = "MISSING"
                null_match = "MISSING"
                discrepancies.append({
                    "Object_Category": "COLUMN", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": c, "Discrepancy_Type": "MISSING_COLUMN_IN_PG",
                    "Oracle_Expected": f"{c} ({expected_pg})", "Postgres_Actual": "Missing in table",
                    "Actionable_Recommendation": f"Add missing column: ALTER TABLE \"{s}\".\"{t}\" ADD COLUMN \"{c}\" {expected_pg};"
                })
            else:
                overall_status = "EXTRA_IN_PG"
                type_match = "EXTRA"
                null_match = "EXTRA"

            column_comparison.append({
                "target_schema": s,
                "target_table": t,
                "column_name": c,
                "column_id": cid,
                "in_oracle_dump": "YES" if in_ora else "NO",
                "in_postgres": "YES" if in_pg else "NO",
                "oracle_data_type": ora_dt,
                "expected_pg_type": expected_pg,
                "actual_pg_type": actual_pg,
                "type_match": type_match,
                "oracle_nullable": ora_null,
                "pg_nullable": pg_null,
                "nullable_match": null_match,
                "is_audit_column": "YES" if is_audit else "NO",
                "overall_status": overall_status
            })
    return column_comparison, oracle_cols_map


def compare_constraints(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter):
    # -------------------------------------------------------------------------
    # LEVEL 4: Constraints Comparison
    # -------------------------------------------------------------------------
    logger.info("[5/7] Comparing Constraints, Indexes, Sequences, Views, and Triggers...")
    oracle_cons_map = defaultdict(dict)
    for c_row in meta["constraints"]:
        s = (c_row.get("owner") or "").lower().strip()
        t = norm_col(c_row.get("table_name") or "")
        cn = norm_col(c_row.get("constraint_name") or "")
        ctype = (c_row.get("ctype") or "").upper().strip()
        cols = ",".join(norm_col(x) for x in c_row.get("columns", "").split(",") if x.strip())
        if s and t:
            oracle_cons_map[(s, t)][cn] = {
                "name": cn, "ctype": ctype, "columns": cols,
                "ref_table": norm_col(c_row.get("ref_table") or ""),
                "ref_columns": ",".join(norm_col(x) for x in c_row.get("ref_columns", "").split(",") if x.strip())
            }

    pg_cons_map = defaultdict(dict)
    for c_row in pg_cat["constraints"]:
        s = c_row["table_schema"].lower().strip()
        t = c_row["table_name"].lower().strip()
        cn = c_row["constraint_name"].lower().strip()
        pg_cons_map[(s, t)][cn] = c_row

    constraints_comparison = []
    all_cons_tables = sorted(set(oracle_cons_map.keys()) | set(pg_cons_map.keys()))

    for s, t in all_cons_tables:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_c = oracle_cons_map.get((s, t), {})
        pg_c = pg_cons_map.get((s, t), {})
        all_cn = sorted(set(ora_c.keys()) | set(pg_c.keys()))

        for cn in all_cn:
            oc = ora_c.get(cn)
            pc = pg_c.get(cn)
            in_ora = oc is not None
            in_pg = pc is not None

            ctype_map = {"P": "PRIMARY KEY", "U": "UNIQUE", "R": "FOREIGN KEY", "C": "CHECK"}
            ctype_pg_map = {"p": "PRIMARY KEY", "u": "UNIQUE", "f": "FOREIGN KEY", "c": "CHECK"}

            exp_type = ctype_map.get(oc["ctype"], oc["ctype"]) if oc else ""
            act_type = ctype_pg_map.get(pc["constraint_type"], pc.get("constraint_type", "")) if pc else ""
            cols = oc["columns"] if oc else (pc.get("columns") or "")
            ref_tbl = oc["ref_table"] if oc else (pc.get("ref_table") or "")
            ref_cols = oc["ref_columns"] if oc else (pc.get("ref_columns") or "")

            is_val = pc.get("is_validated") if pc else None
            is_val_str = "YES" if is_val is True else ("NO" if is_val is False else "N/A")

            if not db_connected:
                status = "DB_UNREACHABLE"
            elif in_ora and in_pg:
                if is_val is False:
                    status = "UNVALIDATED"
                    discrepancies.append({
                        "Object_Category": "CONSTRAINT", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": cn, "Discrepancy_Type": "CONSTRAINT_NOT_VALIDATED",
                        "Oracle_Expected": f"{exp_type} ({cols}) VALIDATED", "Postgres_Actual": "NOT VALID",
                        "Actionable_Recommendation": f"Execute: ALTER TABLE \"{s}\".\"{t}\" VALIDATE CONSTRAINT \"{cn}\";"
                    })
                else:
                    status = "MATCH"
            elif in_ora and not in_pg:
                status = "MISSING_IN_PG"
                discrepancies.append({
                    "Object_Category": "CONSTRAINT", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": cn, "Discrepancy_Type": "MISSING_CONSTRAINT_IN_PG",
                    "Oracle_Expected": f"{exp_type} on ({cols})", "Postgres_Actual": "Missing in DB",
                    "Actionable_Recommendation": "Run Step 07: python sweep.py oracle load-obj"
                })
            else:
                status = "EXTRA_IN_PG"

            constraints_comparison.append({
                "target_schema": s,
                "target_table": t,
                "constraint_name": cn,
                "constraint_type": exp_type or act_type,
                "columns": cols,
                "ref_table": ref_tbl,
                "ref_columns": ref_cols,
                "in_oracle_dump": "YES" if in_ora else "NO",
                "in_postgres": "YES" if in_pg else "NO",
                "pg_is_validated": is_val_str,
                "status": status
            })
    return constraints_comparison, oracle_cons_map


def compare_indexes(db_connected, discrepancies, meta, pg_cat, schemas_filter, tables_filter):
    # -------------------------------------------------------------------------
    # LEVEL 5: Indexes Comparison
    # -------------------------------------------------------------------------
    oracle_idx_map = defaultdict(dict)
    for idx in meta["indexes"]:
        s = (idx.get("table_owner") or idx.get("owner") or "").lower().strip()
        t = norm_col(idx.get("table_name") or "")
        iname = norm_col(idx.get("index_name") or "")
        is_uniq = str(idx.get("uniqueness") or "").upper() == "UNIQUE"
        cols = ",".join(norm_col(x) for x in idx.get("columns", "").split(",") if x.strip())
        if s and t and iname:
            oracle_idx_map[(s, t)][iname] = {
                "name": iname, "unique": is_uniq, "columns": cols
            }

    pg_idx_map = defaultdict(dict)
    for idx in pg_cat["indexes"]:
        s = idx["table_schema"].lower().strip()
        t = idx["table_name"].lower().strip()
        iname = idx["index_name"].lower().strip()
        pg_idx_map[(s, t)][iname] = idx

    index_comparison = []
    all_idx_tables = sorted(set(oracle_idx_map.keys()) | set(pg_idx_map.keys()))

    for s, t in all_idx_tables:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        ora_i = oracle_idx_map.get((s, t), {})
        pg_i = pg_idx_map.get((s, t), {})
        all_inames = sorted(set(ora_i.keys()) | set(pg_i.keys()))

        for iname in all_inames:
            oi = ora_i.get(iname)
            pi = pg_i.get(iname)
            in_ora = oi is not None
            in_pg = pi is not None

            uniq_str = "UNIQUE" if (oi and oi["unique"]) or (pi and pi.get("is_unique")) else "NON-UNIQUE"
            cols = oi["columns"] if oi else (pi.get("columns") or "")
            is_valid = "YES" if (pi and pi.get("is_valid")) else ("NO" if pi else "N/A")

            if not db_connected:
                status = "DB_UNREACHABLE"
            elif in_ora and in_pg:
                status = "MATCH"
            elif in_ora and not in_pg:
                # Check if backing index for PK or Unique constraint satisfies it
                backing_found = any(pi_obj.get("columns") == cols for pi_obj in pg_i.values())
                if backing_found:
                    status = "MATCH (CONSTRAINT BACKING INDEX)"
                else:
                    status = "MISSING_IN_PG"
                    discrepancies.append({
                        "Object_Category": "INDEX", "Target_Schema": s, "Target_Table": t,
                        "Object_Name": iname, "Discrepancy_Type": "MISSING_INDEX_IN_PG",
                        "Oracle_Expected": f"{uniq_str} INDEX on ({cols})", "Postgres_Actual": "Missing in DB",
                        "Actionable_Recommendation": f"Execute: CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{iname}\" ON \"{s}\".\"{t}\" ({cols});"
                    })
            else:
                status = "EXTRA_IN_PG"

            index_comparison.append({
                "target_schema": s,
                "target_table": t,
                "index_name": iname,
                "uniqueness": uniq_str,
                "columns": cols,
                "in_oracle_dump": "YES" if in_ora else "NO",
                "in_postgres": "YES" if in_pg else "NO",
                "pg_is_valid": is_valid,
                "status": status
            })
    return index_comparison, oracle_idx_map


def compare_sequences(db_connected, discrepancies, meta, pg_cat, schemas_filter):
    # -------------------------------------------------------------------------
    # LEVEL 6: Sequences Comparison
    # -------------------------------------------------------------------------
    oracle_seqs = {}
    for sq in meta["sequences"]:
        s = (sq.get("owner") or "").lower().strip()
        sq_name = norm_col(sq.get("sequence_name") or "")
        if s and sq_name:
            oracle_seqs[(s, sq_name)] = sq

    pg_seqs = {(sq["sequence_schema"].lower(), sq["sequence_name"].lower()) for sq in pg_cat["sequences"]}
    all_seq_keys = sorted(set(oracle_seqs.keys()) | pg_seqs)

    sequence_comparison = []
    for s, sq_name in all_seq_keys:
        if schemas_filter and s not in schemas_filter:
            continue
        in_ora = (s, sq_name) in oracle_seqs
        in_pg = (s, sq_name) in pg_seqs
        ora_info = oracle_seqs.get((s, sq_name), {})

        if not db_connected:
            status = "DB_UNREACHABLE"
        elif in_ora and in_pg:
            status = "MATCH"
        elif in_ora and not in_pg:
            status = "MISSING_IN_PG"
            discrepancies.append({
                "Object_Category": "SEQUENCE", "Target_Schema": s, "Target_Table": "",
                "Object_Name": sq_name, "Discrepancy_Type": "MISSING_SEQUENCE_IN_PG",
                "Oracle_Expected": "Present in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Execute: CREATE SEQUENCE IF NOT EXISTS \"{s}\".\"{sq_name}\";"
            })
        else:
            status = "EXTRA_IN_PG"

        sequence_comparison.append({
            "target_schema": s,
            "sequence_name": sq_name,
            "in_oracle_dump": "YES" if in_ora else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "oracle_min_value": ora_info.get("min_value", ""),
            "oracle_max_value": ora_info.get("max_value", ""),
            "oracle_increment_by": ora_info.get("increment_by", ""),
            "status": status
        })
    return oracle_seqs, sequence_comparison


def compare_views_and_triggers(db_connected, discrepancies, meta, pg_cat, schemas_filter):
    # -------------------------------------------------------------------------
    # LEVEL 7: Views and Triggers Comparison
    # -------------------------------------------------------------------------
    oracle_views = {}
    oracle_triggers = {}
    for obj in meta["objects"]:
        s = (obj.get("owner") or "").lower().strip()
        oname = norm_col(obj.get("object_name") or "")
        otype = (obj.get("object_type") or "").upper().strip()
        if "VIEW" in otype:
            oracle_views[(s, oname)] = otype
        elif "TRIGGER" in otype:
            oracle_triggers[(s, oname)] = otype

    pg_views = {(v["view_schema"].lower(), v["view_name"].lower()): v["view_type"] for v in pg_cat["views"]}
    pg_triggers = {(t["trigger_schema"].lower(), t["trigger_name"].lower()): t for t in pg_cat["triggers"]}

    views_triggers_comparison = []

    # Check Views
    all_view_keys = sorted(set(oracle_views.keys()) | set(pg_views.keys()))
    for s, vname in all_view_keys:
        if schemas_filter and s not in schemas_filter:
            continue
        in_ora = (s, vname) in oracle_views
        in_pg = (s, vname) in pg_views
        vtype = oracle_views.get((s, vname), pg_views.get((s, vname), "VIEW"))

        if not db_connected:
            status = "DB_UNREACHABLE"
        elif in_ora and in_pg:
            status = "MATCH"
        elif in_ora and not in_pg:
            status = "MISSING_IN_PG"
            discrepancies.append({
                "Object_Category": "VIEW", "Target_Schema": s, "Target_Table": "",
                "Object_Name": vname, "Discrepancy_Type": "MISSING_VIEW_IN_PG",
                "Oracle_Expected": f"{vtype} in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Deploy view from outputs/post_load/{s}_views.sql"
            })
        else:
            status = "EXTRA_IN_PG"

        views_triggers_comparison.append({
            "target_schema": s,
            "object_type": vtype,
            "object_name": vname,
            "target_table": "N/A",
            "in_oracle_dump": "YES" if in_ora else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "status": status
        })

    # Check Triggers
    all_trg_keys = sorted(set(oracle_triggers.keys()) | set(pg_triggers.keys()))
    for s, trgname in all_trg_keys:
        if schemas_filter and s not in schemas_filter:
            continue
        in_ora = (s, trgname) in oracle_triggers
        in_pg = (s, trgname) in pg_triggers
        pg_trg_info = pg_triggers.get((s, trgname), {})
        tbl_name = pg_trg_info.get("table_name", "N/A")

        if not db_connected:
            status = "DB_UNREACHABLE"
        elif in_ora and in_pg:
            status = "MATCH"
        elif in_ora and not in_pg:
            status = "MISSING_IN_PG"
            discrepancies.append({
                "Object_Category": "TRIGGER", "Target_Schema": s, "Target_Table": tbl_name,
                "Object_Name": trgname, "Discrepancy_Type": "MISSING_TRIGGER_IN_PG",
                "Oracle_Expected": "Trigger in Dump", "Postgres_Actual": "Missing in DB",
                "Actionable_Recommendation": f"Deploy trigger from outputs/post_load/{s}_triggers.sql"
            })
        else:
            status = "EXTRA_IN_PG"

        views_triggers_comparison.append({
            "target_schema": s,
            "object_type": "TRIGGER",
            "object_name": trgname,
            "target_table": tbl_name,
            "in_oracle_dump": "YES" if in_ora else "NO",
            "in_postgres": "YES" if in_pg else "NO",
            "status": status
        })
    return oracle_triggers, oracle_views, views_triggers_comparison


def reconcile_row_counts(db_connected, discrepancies, exact_counts, meta, oracle_tables, pg_cat, pg_child_partitions, s3_loaded_map, s3_planned_map, schemas_filter, tables_filter):
    # -------------------------------------------------------------------------
    # LEVEL 8: Approximate Row Counts & S3 Parity Reconciliation
    # -------------------------------------------------------------------------
    logger.info("[6/7] Reconciling table row counts: PostgreSQL approximate live rows vs S3 mapped files...")
    # Oracle Source Stats Mapping
    oracle_stats_rows = {}
    for st in meta["table_stats"]:
        o = (st.get("owner") or "").lower().strip()
        t = norm_col(st.get("table_name") or "")
        nr = st.get("num_rows")
        if nr not in ("", None, "None"):
            oracle_stats_rows[(o, t)] = int(float(nr))

    # Aggregate approximate rows from pg_class (and sum child partitions for partitioned tables)
    pg_table_approx = {}
    for t_row in pg_cat["tables"]:
        s = t_row["table_schema"].lower()
        t = t_row["table_name"].lower()
        # Prefer n_live_tup if available; otherwise reltuples
        raw_cnt = t_row.get("stat_live_rows")
        if raw_cnt in (None, "", "None"):
            raw_cnt = t_row.get("approx_row_count", 0)
        try:
            cnt = int(float(raw_cnt or 0))
        except (ValueError, TypeError):
            cnt = 0
        pg_table_approx[(s, t)] = max(cnt, 0)

    # For parent partitioned tables, calculate total rows as the sum of child partitions
    for (ps, pt), children in pg_child_partitions.items():
        child_sum = sum(pg_table_approx.get((ps, ct), 0) for ct in children)
        pg_table_approx[(ps, pt)] = child_sum

    row_count_audit = []
    all_audit_tables = sorted(set(oracle_tables.keys()) | set(s3_planned_map.keys()) | set(s3_loaded_map.keys()))

    for s, t in all_audit_tables:
        if tables_filter and f"{s}.{t}" not in tables_filter and t not in tables_filter:
            continue
        if schemas_filter and s not in schemas_filter:
            continue

        planned_info = s3_planned_map.get((s, t), {"files": 0, "size_mb": 0.0})
        loaded_info = s3_loaded_map.get((s, t), {"files": 0, "rows": 0, "bytes": 0, "seconds": 0.0})
        s3_loaded_rows = loaded_info["rows"]
        ora_source_rows = oracle_stats_rows.get((s, t))

        # Determine PG Row Count
        if exact_counts and (s, t) in pg_cat["exact_row_counts"]:
            pg_rows = pg_cat["exact_row_counts"][(s, t)]
            count_method = "EXACT (COUNT(*))"
        else:
            pg_rows = pg_table_approx.get((s, t), 0)
            count_method = "APPROXIMATE (pg_catalog)"

        # Parity comparison: PG row count vs S3 loaded rows
        if not db_connected:
            parity_status = "DB_UNREACHABLE"
            diff = 0
            var_pct = "N/A"
        elif s3_loaded_rows > 0:
            diff = pg_rows - s3_loaded_rows
            diff_pct = round((diff / s3_loaded_rows) * 100, 2)
            var_pct = f"{diff_pct}%"

            if diff == 0:
                parity_status = "EXACT_MATCH"
            elif not exact_counts and abs(diff) / max(s3_loaded_rows, 1) <= 0.05:
                # Within 5% tolerance for PostgreSQL pg_class statistical approximation
                parity_status = "APPROX_MATCH"
            else:
                parity_status = "MISMATCH"
                discrepancies.append({
                    "Object_Category": "ROW_COUNT", "Target_Schema": s, "Target_Table": t,
                    "Object_Name": t, "Discrepancy_Type": "ROW_COUNT_DISCREPANCY",
                    "Oracle_Expected": f"S3 Streamed: {s3_loaded_rows:,} rows",
                    "Postgres_Actual": f"PG Count: {pg_rows:,} rows (Diff: {diff:+,})",
                    "Actionable_Recommendation": f"Investigate streaming load for {s}.{t} in sweep_ctl.load_log."
                })
        elif s3_loaded_rows == 0 and planned_info["files"] > 0:
            diff = 0
            var_pct = "0%"
            parity_status = "PENDING_INGESTION"
        elif s3_loaded_rows == 0 and pg_rows == 0:
            diff = 0
            var_pct = "0%"
            parity_status = "EMPTY_TABLE"
        else:
            diff = pg_rows
            var_pct = "100%"
            parity_status = "LOADED_WITHOUT_LOG"

        row_count_audit.append({
            "target_schema": s,
            "target_table": t,
            "pg_table_rows": pg_rows if db_connected else "N/A",
            "count_method": count_method if db_connected else "N/A",
            "s3_mapped_files_planned": planned_info["files"],
            "s3_files_loaded": loaded_info["files"],
            "s3_loaded_rows": s3_loaded_rows,
            "oracle_source_rows": ora_source_rows if ora_source_rows is not None else "N/A",
            "row_difference": diff if db_connected else "N/A",
            "variance_pct": var_pct,
            "parity_status": parity_status
        })
    return row_count_audit

