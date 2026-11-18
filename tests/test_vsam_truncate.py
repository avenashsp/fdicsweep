import pytest

import vsam.vsam_06_load_data as v6


@pytest.fixture(autouse=True)
def fresh_state(monkeypatch):
    monkeypatch.setattr(v6, "_CLAIMED_TABLES", set())
    monkeypatch.setattr(v6, "FORCE_TRUNCATE", False)
    calls = []
    monkeypatch.setattr(v6, "run_psql", lambda sql, *a, **k: calls.append(sql))
    return calls


TRUNC = {"action": "TRUNCATE_AND_LOAD", "has_pk": False, "pk_cols": []}
FRESH = {"action": "INSERT_FRESH", "has_pk": True, "pk_cols": ["id"]}


def test_refuses_to_truncate_without_flag(fresh_state):
    with pytest.raises(RuntimeError, match="--force-truncate"):
        v6.claim_table("bcs", "t", TRUNC)
    assert fresh_state == []                      # nothing was truncated


def test_truncates_exactly_once_per_run_with_flag(fresh_state, monkeypatch):
    monkeypatch.setattr(v6, "FORCE_TRUNCATE", True)
    assert v6.claim_table("bcs", "t", TRUNC) is True
    assert len(fresh_state) == 1 and fresh_state[0].startswith("TRUNCATE TABLE")
    # a second FILE mapping to the same table must append, not wipe the first file's rows
    assert v6.claim_table("bcs", "t", TRUNC) is False
    assert len(fresh_state) == 1


def test_first_file_into_empty_table_claims_it_so_second_file_cannot_truncate(fresh_state, monkeypatch):
    monkeypatch.setattr(v6, "FORCE_TRUNCATE", True)
    assert v6.claim_table("bcs", "t", FRESH) is False      # fresh table: nothing to truncate
    assert v6.claim_table("bcs", "t", TRUNC) is False      # now has rows from file 1 -> append
    assert fresh_state == []


def test_schemas_are_claimed_independently(fresh_state, monkeypatch):
    monkeypatch.setattr(v6, "FORCE_TRUNCATE", True)
    assert v6.claim_table("bss01104", "t", TRUNC) is True
    assert v6.claim_table("bss01112", "t", TRUNC) is True
    assert len(fresh_state) == 2


def test_failed_truncate_is_not_swallowed(monkeypatch):
    monkeypatch.setattr(v6, "FORCE_TRUNCATE", True)

    def boom(sql, *a, **k):
        raise RuntimeError("lock timeout")
    monkeypatch.setattr(v6, "run_psql", boom)
    with pytest.raises(RuntimeError, match="lock timeout"):
        v6.claim_table("bcs", "t", TRUNC)
    assert ("bcs", "t") not in v6._CLAIMED_TABLES          # not marked claimed -> a retry still sees it
