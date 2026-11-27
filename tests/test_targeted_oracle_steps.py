"""Oracle Steps 01, 02, 03, 04, 05: targeted runs change only their own entries; selectors are validated up front."""
import csv
import gzip
from pathlib import Path

import pytest

from oracle import oracle_01_extract_mtd as o1
from oracle import oracle_02_inventory as o2
from oracle import oracle_03_type_gate as o3
from oracle import oracle_04_generate_ddl as o4
from sweep_ddlfrag import FragmentStore
from sweep_targeting import Selector

DUMP = '''-- CONNECT BSS01001
CREATE TABLE "BSS01001"."CUSTOMER" 
   (	"ID" NUMBER(10,0) NOT NULL ENABLE, 
	"NAME" VARCHAR2(40 BYTE), 
	 CONSTRAINT "PK_CUSTOMER" PRIMARY KEY ("ID")
   ) ;
CREATE TABLE "BSS01001"."ORDERS" 
   (	"ORDER_ID" NUMBER(10,0) NOT NULL ENABLE, 
	"CUSTOMER_ID" NUMBER(10,0), 
	 CONSTRAINT "PK_ORDERS" PRIMARY KEY ("ORDER_ID")
   ) ;
ALTER TABLE "BSS01001"."ORDERS" ADD CONSTRAINT "FK_ORD_CUST" FOREIGN KEY ("CUSTOMER_ID") REFERENCES "BSS01001"."CUSTOMER" ("ID") ENABLE;
CREATE SEQUENCE "BSS01001"."SEQ1" MINVALUE 1 MAXVALUE 999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE ;
-- CONNECT BSS01104
CREATE TABLE "BSS01104"."CUSTOMER" 
   (	"ID" NUMBER(10,0) NOT NULL ENABLE
   ) ;
'''


@pytest.fixture()
def env(tmp_path, monkeypatch):
    meta, out, pre, post, rep = (tmp_path / n for n in ("meta", "out", "pre", "post", "rep"))
    for d in (meta, out, pre, post, rep):
        d.mkdir()
    names = {"TABLES": "tables", "CONSTRAINTS": "constraints", "INDEXES": "indexes", "PARTITIONS": "partitions",
             "TABLE_STATS": "stats", "SEQUENCES": "sequences", "OBJECTS": "objects"}
    for key, fname in names.items():
        for mod in (o1, o2, o3, o4):
            if hasattr(mod, f"META_{key}_CSV"):
                monkeypatch.setattr(mod, f"META_{key}_CSV", meta / f"{fname}.csv")
    for mod in (o1, o2, o3, o4):
        for attr, path in (("ORACLE_REPORTS_DIR", rep), ("ORACLE_OUTPUTS_DIR", out)):
            if hasattr(mod, attr):
                monkeypatch.setattr(mod, attr, path)
    monkeypatch.setattr(o4, "ORACLE_PRE_LOAD_DIR", pre)
    monkeypatch.setattr(o4, "ORACLE_POST_LOAD_DIR", post)
    monkeypatch.setattr(o4, "DDL_FRAGMENT_ROOT", out / "ddl_fragments")
    dump = tmp_path / "dump.sql"
    dump.write_text(DUMP)
    monkeypatch.setattr(o4, "extract_views_and_triggers_from_dump", lambda: ({}, {}))
    return type("Env", (), {"meta": meta, "out": out, "pre": pre, "post": post, "dump": dump, "tmp": tmp_path})


def _rows(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def _lines_of(path, owner, table):
    return [l for l in Path(path).read_text().splitlines()[1:] if l.startswith(f"{owner},{table},")]


def test_step01_targeted_run_merges_one_table_and_keeps_the_rest(env):
    assert o1.extract_metadata_from_master(str(env.dump)) is True                  # full run
    before = (env.meta / "tables.csv").read_text().splitlines()
    sel = Selector(tables=[("bss01001", "customer")])
    assert o1.extract_metadata_from_master(str(env.dump), selector=sel) is True    # targeted run
    after = (env.meta / "tables.csv").read_text().splitlines()
    assert {r["table_name"] for r in _rows(env.meta / "tables.csv")} == {"CUSTOMER", "ORDERS"}
    for owner, table in (("BSS01001", "ORDERS"), ("BSS01104", "CUSTOMER")):        # untouched entries are byte-identical
        assert [l for l in before if l.startswith(f"{owner},{table},")] == [l for l in after if l.startswith(f"{owner},{table},")]
    assert (env.meta / "sequences.csv").exists() and len(_rows(env.meta / "sequences.csv")) == 1   # --tables leaves owner-level files alone
    assert len(_rows(env.meta / "constraints.csv")) == 4      # PK_CUSTOMER, PK_ORDERS, FK_ORD_CUST + the NOT_NULL row of BSS01104.CUSTOMER.ID (no primary key there)


def test_step01_schema_selector_and_dry_run_and_unknown_table(env, capsys):
    o1.extract_metadata_from_master(str(env.dump))
    stamp = (env.meta / "tables.csv").read_bytes()
    assert o1.extract_metadata_from_master(str(env.dump), selector=Selector(schemas={"bss01104"}, dry_run=True)) is True
    assert (env.meta / "tables.csv").read_bytes() == stamp                         # dry run writes nothing
    with pytest.raises(SystemExit) as e:
        o1.extract_metadata_from_master(str(env.dump), selector=Selector(tables=[(None, "nosuchtable")]))
    assert e.value.code == 2 and (env.meta / "tables.csv").read_bytes() == stamp   # rejected before anything was written


def test_step04_targeted_run_rewrites_only_its_fragments(env):
    o1.extract_metadata_from_master(str(env.dump))
    assert o4.generate_ddl() is True
    store = FragmentStore(env.out / "ddl_fragments")
    assert store.tables("bss01001") == ["customer", "orders"] and store.tables("bss01104") == ["customer"]
    orders_before = (env.out / "ddl_fragments/fragments/bss01001/orders.tables.sql").read_bytes()
    schema_script = (env.pre / "bss01001_tables.sql").read_text()
    assert "CUSTOMER" in schema_script.upper() and '"orders"' in schema_script

    frag_customer = env.out / "ddl_fragments/fragments/bss01001/customer.tables.sql"
    frag_customer.write_text("-- stale\n")                                          # a targeted run must regenerate this one only
    assert o4.generate_ddl(selector=Selector(tables=[("bss01001", "customer")])) is True
    assert "stale" not in frag_customer.read_text()
    assert (env.out / "ddl_fragments/fragments/bss01001/orders.tables.sql").read_bytes() == orders_before
    assert '"orders"' in (env.pre / "bss01001_tables.sql").read_text()              # assembled script still holds every table
    assert '"customer"' in (env.pre / "bss01104_tables.sql").read_text()


def test_step04_dry_run_writes_nothing_and_full_run_prunes_removed_tables(env):
    o1.extract_metadata_from_master(str(env.dump))
    assert o4.generate_ddl(selector=Selector(dry_run=True)) is True
    assert not (env.out / "ddl_fragments").exists()
    o4.generate_ddl()
    gone = env.out / "ddl_fragments/fragments/bss01001/ghost.tables.sql"
    gone.write_text("-- table that left the metadata\n")
    o4.generate_ddl()
    assert not gone.exists()


def test_step02_targeted_inventory_merges_file_map_and_reranks(env):
    o1.extract_metadata_from_master(str(env.dump))
    files = [
        {"uri": "s3://b/BSS01001.CUSTOMER.csv.gz", "key": "BSS01001.CUSTOMER.csv.gz", "filename": "BSS01001.CUSTOMER.csv.gz", "size_bytes": 500,
         "is_gz": True, "is_csv": False, "s3_last_modified": "t1", "s3_etag": "e1"},
        {"uri": "s3://b/BSS01001.ORDERS.csv.gz", "key": "BSS01001.ORDERS.csv.gz", "filename": "BSS01001.ORDERS.csv.gz", "size_bytes": 900,
         "is_gz": True, "is_csv": False, "s3_last_modified": "t1", "s3_etag": "e2"},
    ]
    assert o2.build_inventory(mock_files=files) is True
    full = _rows(env.out / "oracle_02_s3_file_map.csv")
    assert [r["table_name"] for r in full] == ["CUSTOMER", "ORDERS"]
    orders_before = next(r for r in full if r["table_name"] == "ORDERS")

    files[0] = {**files[0], "size_bytes": 5000, "s3_etag": "e1b"}                     # CUSTOMER was re-delivered, bigger now
    assert o2.build_inventory(mock_files=files, selector=Selector(tables=[("bss01001", "customer")])) is True
    rows = _rows(env.out / "oracle_02_s3_file_map.csv")
    cust = next(r for r in rows if r["table_name"] == "CUSTOMER")
    assert cust["s3_etag"] == "e1b" and cust["_run_id"]
    assert [r["table_name"] for r in rows] == ["ORDERS", "CUSTOMER"]                 # ranks recomputed small -> large across the merge
    assert [r["load_order_rank"] for r in rows] == ["1", "2"]
    assert next(r for r in rows if r["table_name"] == "ORDERS")["s3_etag"] == "e2"    # untouched row kept
    assert _rows(env.out / "s3_file_map.csv") == rows                                  # the legacy copy is merged the same way
    orders_after = next(r for r in rows if r["table_name"] == "ORDERS")
    assert {k: v for k, v in orders_after.items() if k != "load_order_rank"} == {k: v for k, v in orders_before.items() if k != "load_order_rank"}

    with pytest.raises(SystemExit) as e:                                              # unknown file: clear message, exit 2
        o2.build_inventory(mock_files=files, selector=Selector(files=["nope.csv.gz"]))
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        o2.build_inventory(mock_files=files, selector=Selector(tables=[(None, "ghost")]))
    assert e.value.code == 2


def test_step03_targeted_gate_merges_results_and_pass_list(env, tmp_path):
    o1.extract_metadata_from_master(str(env.dump))
    paths = {}
    for t in ("CUSTOMER", "ORDERS"):
        p = tmp_path / f"BSS01001.{t}.csv.gz"
        header = "ID,NAME\n1,a\n" if t == "CUSTOMER" else "ORDER_ID,CUSTOMER_ID\n1,1\n"
        p.write_bytes(gzip.compress(header.encode()))
        paths[t] = p
    with open(env.out / "oracle_02_s3_file_map.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["load_order_rank", "owner", "table_name", "target_schema", "target_table", "file_format",
                                           "size_bytes", "size_mb", "uri", "filename"])
        w.writeheader()
        for i, (t, p) in enumerate(paths.items(), 1):
            w.writerow({"load_order_rank": i, "owner": "BSS01001", "table_name": t, "target_schema": "bss01001", "target_table": t.lower(),
                        "file_format": "gzip", "size_bytes": p.stat().st_size, "size_mb": 0, "uri": str(p), "filename": p.name})
    assert o3.run_type_gate() is True
    assert {r["table_name"] for r in _rows(env.out / "oracle_03_gate_results.csv")} == {"CUSTOMER", "ORDERS"}
    orders = _lines_of(env.out / "oracle_03_gate_results.csv", "BSS01001", "ORDERS")
    assert o3.run_type_gate(selector=Selector(tables=[(None, "customer")])) is True
    assert _lines_of(env.out / "oracle_03_gate_results.csv", "BSS01001", "ORDERS") == orders      # untouched
    assert sorted((env.out / "oracle_03_gate_pass_tables.txt").read_text().split()) == ["BSS01001.CUSTOMER", "BSS01001.ORDERS"]
    with pytest.raises(SystemExit) as e:
        o3.run_type_gate(selector=Selector(tables=[(None, "ghost")]))
    assert e.value.code == 2
