# Layer manifests

`layers/<layer>_tables.csv` lists the tables each layer (bronze, silver, gold_backup, gold) owns:
`track` (oracle | vsam | model_output), `schema`, `table`, `promote_mode` (upsert | replace | insert_only),
`conflict_policy` (source_wins | keep_target), `enabled`.

A layer only ever touches the tables of its own manifest. Create drafts with `python3 sweep.py layers init-manifests`
and `python3 sweep.py layers scan-models` (reads the `data_models/` headers), **review them**, and commit the reviewed manifests here
(they contain no secrets). See [docs/layers_and_promote.md](../docs/layers_and_promote.md).
