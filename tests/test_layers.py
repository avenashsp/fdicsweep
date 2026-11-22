"""Layer profiles, the database guard, manifests and the model scan (F1, F2, F4 refusals)."""
import argparse
import csv
import json

import pytest

import sweep_common as c
import sweep_layer_cmds as lc
import sweep_layers as sl
from conftest import write_layers_file


@pytest.fixture()
def layers_file(tmp_path, monkeypatch):
    f = write_layers_file(tmp_path / "sweep_layers.json", {"bronze": "SWEEPB", "silver": "SWEEPS", "gold_backup": "SWEEPGB", "gold": "SWEEPG"})
    monkeypatch.setattr(sl, "LAYERS_FILE", f)
    return f


def test_shipped_profile_lists_the_four_layers_in_chain_order():
    layers = sl.load_layers(sl.BASE_DIR / "sweep_layers.json")
    assert [l.name for l in layers] == ["bronze", "silver", "gold_backup", "gold"]
    assert [l.database for l in layers] == ["SWEEPB", "SWEEPS", "SWEEPGB", "SWEEPG"]
    assert [l.upstream for l in layers] == [None, "bronze", "silver", "gold_backup"]
    assert [l.downstream for l in layers] == ["silver", "gold_backup", "gold", None]
    assert [l.allow_ddl for l in layers] == [True, True, False, False]
    assert layers[3].lock_timeout == "5s" and layers[0].env_file == "sweep.bronze.env"
    raw = json.loads((sl.BASE_DIR / "sweep_layers.json").read_text())
    assert not any(k.lower() in ("password", "pgpassword", "secret", "user") for l in raw["layers"] for k in l)      # no secrets in the profile


def test_bad_profiles_are_rejected(tmp_path):
    f = tmp_path / "l.json"
    f.write_text(json.dumps({"layers": [{"name": "a", "database": "A", "env_file": "a.env", "upstream": None},
                                        {"name": "b", "database": "B", "env_file": "b.env", "upstream": "zzz"}]}))
    with pytest.raises(sl.LayerError, match="upstream must be 'a'"):
        sl.load_layers(f)
    f.write_text("{not json")
    with pytest.raises(sl.LayerError, match="cannot read"):
        sl.load_layers(f)
    with pytest.raises(sl.LayerError, match="unknown layer 'platinum'"):
        sl.get_layer("platinum")


def test_layer_is_required_and_unknown_layers_are_refused(layers_file, grab):
    msgs = grab(sl.logger)
    with pytest.raises(SystemExit) as e:
        sl.ensure_layer(argparse.Namespace(layer=None), required=True, db=False)
    assert e.value.code == sl.EXIT_NO_LAYER and "--layer is required" in msgs[-1]
    with pytest.raises(SystemExit) as e:
        sl.ensure_layer(argparse.Namespace(layer="platinum"), db=False)
    assert e.value.code == sl.EXIT_NO_LAYER
    assert sl.ensure_layer(argparse.Namespace(layer=None), required=False, db=False) is None
    assert sl.ensure_layer(argparse.Namespace(layer="GOLD"), db=False).name == "gold"


@pytest.mark.integration
def test_wrong_database_is_refused_and_the_right_one_accepted(pg_db, tmp_path, monkeypatch, grab):
    msgs = grab(sl.logger)
    f = write_layers_file(tmp_path / "l.json", {"bronze": "SOME_OTHER_DB", "silver": pg_db})
    monkeypatch.setattr(sl, "LAYERS_FILE", f)
    with pytest.raises(SystemExit) as e:
        sl.ensure_layer(argparse.Namespace(layer="bronze"))
    assert e.value.code == sl.EXIT_GUARD
    out = msgs[-1]
    assert "layer guard" in out and "SOME_OTHER_DB" in out and pg_db in out
    assert sl.ensure_layer(argparse.Namespace(layer="silver")).database == pg_db       # case-insensitive match of the right one


def test_ddl_is_refused_on_gold_unless_allowed_and_s3_loading_only_on_bronze(layers_file, grab):
    msgs = grab(sl.logger)
    gold, silver, bronze = sl.get_layer("gold"), sl.get_layer("silver"), sl.get_layer("bronze")
    with pytest.raises(SystemExit) as e:
        sl.require_ddl(gold, argparse.Namespace(allow_ddl=False), what="Step 05")
    assert e.value.code == sl.EXIT_NO_LAYER and "allow_ddl=false" in msgs[-1]
    sl.require_ddl(gold, argparse.Namespace(allow_ddl=True))
    sl.require_ddl(silver, argparse.Namespace(allow_ddl=False))
    sl.require_ddl(None, argparse.Namespace(allow_ddl=False))
    sl.require_bronze(bronze)
    with pytest.raises(SystemExit):
        sl.require_bronze(silver)
    assert "promote --from bronze --to silver" in msgs[-1]


def test_manifest_roundtrip_validation_and_no_silent_overwrite(layers_file, tmp_path):
    rows = [{"track": "oracle", "schema": "S1", "table": "A", "promote_mode": "upsert", "conflict_policy": "source_wins", "enabled": True},
            {"track": "model_output", "schema": "m", "table": "out1", "promote_mode": "insert_only", "conflict_policy": "keep_target", "enabled": False}]
    path = sl.write_manifest("silver", rows)
    assert [r["table"] for r in sl.read_manifest("silver")] == ["a"]                       # disabled rows are not active
    assert len(sl.read_manifest("silver", include_disabled=True)) == 2
    assert sl.manifest_tables("silver") == {("s1", "a"): sl.read_manifest("silver")[0]}
    with pytest.raises(sl.LayerError, match="exists"):
        sl.write_manifest("silver", rows)
    sl.write_manifest("silver", rows[:1], force=True)
    assert path.with_name(path.name + ".bak").exists() and len(sl.read_manifest("silver", include_disabled=True)) == 1
    with pytest.raises(sl.LayerError, match="unknown promote_mode"):
        sl.validate_manifest_rows([{**rows[0], "promote_mode": "delete"}])
    assert sl.read_manifest("gold") == []                                                  # no manifest file -> nothing is owned


def test_restrict_to_manifest_keeps_only_listed_tables_and_gold_needs_a_manifest(layers_file):
    sl.write_manifest("gold", [{"track": "oracle", "schema": "s1", "table": "a", "promote_mode": "upsert", "conflict_policy": "source_wins", "enabled": True}])
    kept = sl.restrict_to_manifest(sl.get_layer("gold"), [("s1", "a"), ("s1", "fe_created_table")])
    assert kept == [("s1", "a")]
    with pytest.raises(SystemExit):
        sl.restrict_to_manifest(sl.get_layer("gold_backup"), [("s1", "a")])                # allow_ddl=false and no manifest
    assert sl.restrict_to_manifest(sl.get_layer("silver"), [("s1", "a")]) == [("s1", "a")]  # no manifest, DDL allowed: warn only


def test_use_layer_switches_the_psql_helpers_and_restores_them(layers_file):
    before = (c.PGDATABASE, c.PGHOST)
    with sl.use_layer("gold") as conn:
        assert c.PGDATABASE == "SWEEPG" == conn.dbname
    assert (c.PGDATABASE, c.PGHOST) == before


def test_init_manifests_drafts_the_layer_layout(layers_file, tmp_path, monkeypatch):
    meta = tmp_path / "meta"
    meta.mkdir()
    with open(meta / "tables.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["owner", "table_name", "column_name"])
        w.writeheader()
        for o, t in (("BSS01001", "CUSTOMER"), ("BSS01001", "LOG")):
            w.writerow({"owner": o, "table_name": t, "column_name": "ID"})
    with open(meta / "cons.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["owner", "table_name", "ctype"])
        w.writeheader()
        w.writerow({"owner": "BSS01001", "table_name": "CUSTOMER", "ctype": "P"})
    out = tmp_path / "vsam_out"
    out.mkdir()
    with open(out / "vsam_02_load_matrix.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["target_schema", "target_table", "key_columns"])
        w.writeheader()
        w.writerow({"target_schema": "bss01104", "target_table": "stg_ach_v", "key_columns": "acct"})
        w.writerow({"target_schema": "bcs", "target_table": "stg_xref_v", "key_columns": ""})
    monkeypatch.setattr(lc, "META_TABLES_CSV", meta / "tables.csv")
    monkeypatch.setattr(lc, "META_CONSTRAINTS_CSV", meta / "cons.csv")
    monkeypatch.setattr(lc, "VSAM_OUTPUTS_DIR", out)
    drafts = lc.init_manifests()
    assert set(drafts) == {"bronze", "silver", "gold_backup", "gold"}
    bronze = {(r["track"], r["schema"], r["table"]): r["promote_mode"] for r in sl.read_manifest("bronze")}
    assert bronze == {("oracle", "bss01001", "customer"): "upsert", ("oracle", "bss01001", "log"): "insert_only",
                      ("vsam", "bss01104", "stg_ach_v"): "upsert", ("vsam", "bcs", "stg_xref_v"): "replace"}
    for layer in ("silver", "gold_backup", "gold"):                                        # VSAM only in bronze until reviewed
        assert {r["track"] for r in sl.read_manifest(layer)} == {"oracle"}
    again = lc.init_manifests()                                                            # reviewed manifests are never overwritten
    assert all(path is None for path, _ in again.values())


MODEL = '''
import pandas as pd
SQL = """
    WITH recent AS (SELECT * FROM bss01104.stg_ach_v WHERE b_korg = 104)
    INSERT INTO models.account_summary (acct, total)
    SELECT a.acct, sum(x.amount) FROM recent a JOIN bss01001.customer x ON x.id = a.acct GROUP BY a.acct
"""
def run(con, schema):
    con.execute(SQL)
    df = pd.read_sql("select * from bcs.stg_xref_v", con)
    df.to_sql("xref_copy", con, schema="models")
    con.execute(f"UPDATE models.account_summary SET total = 0 WHERE acct IN (SELECT acct FROM {schema}.skip)")
'''


def test_scan_models_drafts_vsam_reads_and_outputs_for_review(layers_file, tmp_path):
    sl.write_manifest("bronze", [
        {"track": "oracle", "schema": "bss01001", "table": "customer", "promote_mode": "upsert", "conflict_policy": "source_wins", "enabled": True},
        {"track": "vsam", "schema": "bss01104", "table": "stg_ach_v", "promote_mode": "upsert", "conflict_policy": "source_wins", "enabled": True},
        {"track": "vsam", "schema": "bcs", "table": "stg_xref_v", "promote_mode": "replace", "conflict_policy": "source_wins", "enabled": True}])
    models = tmp_path / "models"
    models.mkdir()
    (models / "m1.py").write_text(MODEL)
    (models / "broken.py").write_text("def x(:\n")
    report, vsam_rows, out_rows = lc.scan_models(models)
    assert [(r["schema"], r["table"], r["enabled"]) for r in vsam_rows] == [("bcs", "stg_xref_v", "false"), ("bss01104", "stg_ach_v", "false")]
    assert {(r["schema"], r["table"]) for r in out_rows} == {("models", "account_summary"), ("models", "xref_copy")}
    assert all(r["track"] == "model_output" and r["enabled"] == "false" for r in out_rows)       # drafts are never active
    assert "recent" not in {r["table"] for r in out_rows}                                         # CTE names are not tables
    assert report["unparsed"][0]["script"] == "broken.py"
    assert report["scripts"]["m1.py"]["reads"] == ["bcs.stg_xref_v", "bss01001.customer", "bss01104.stg_ach_v", "x.skip"]
    layers_dir = tmp_path / "layers"
    assert (layers_dir / "silver_vsam_tables.draft.csv").exists() and (layers_dir / "model_output_tables.draft.csv").exists()
    assert not (layers_dir / "silver_tables.csv").exists()                                         # nothing was applied to a manifest


def test_gold_must_be_selected_explicitly_never_by_default(layers_file, monkeypatch, grab):
    msgs = grab(sl.logger)
    monkeypatch.setenv("SWEEP_LAYER", "gold")
    with pytest.raises(SystemExit) as e:
        sl.ensure_layer(argparse.Namespace(layer=None), db=False)             # SWEEP_LAYER alone does not select gold
    assert e.value.code == sl.EXIT_NO_LAYER and "must be selected explicitly with --layer gold" in msgs[-1]
    assert sl.ensure_layer(argparse.Namespace(layer="gold"), db=False).name == "gold"
    monkeypatch.setenv("SWEEP_LAYER", "silver")
    assert sl.ensure_layer(argparse.Namespace(layer=None), db=False).name == "silver"   # the other layers may come from the environment
