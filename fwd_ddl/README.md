# FE SQL files

Put the forward-engineering SQL here as `V<number>__<description>.sql` (structure only). They are applied in numeric order, once per layer database,
by `python3 sweep.py fwd-ddl apply --layer <layer>` and `fwd-ddl promote --from <layer> --to <layer>`. **An applied file is never edited** (its SHA-256 is
recorded): a correction is a new file. See [docs/fwd_ddl.md](../docs/fwd_ddl.md).
