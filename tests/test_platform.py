import json
import multiprocessing
import os
import subprocess
import sys
import time

import pytest

import sweep_platform as sp


def test_atomic_write_json_roundtrip_and_no_temp_left(tmp_path):
    p = tmp_path / "x.json"
    sp.atomic_write_json(p, {"a": 1})
    assert json.loads(p.read_text()) == {"a": 1}
    sp.atomic_write_json(p, {"a": 2})
    assert sp.read_json_or(p, None) == {"a": 2}
    assert [f.name for f in tmp_path.iterdir() if not f.name.endswith(".lock")] == ["x.json"]


def test_read_json_or_handles_missing_and_corrupt(tmp_path):
    assert sp.read_json_or(tmp_path / "nope.json", {"d": 1}) == {"d": 1}
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    assert sp.read_json_or(bad, []) == []


def _bump(path, n):
    for _ in range(n):
        with sp.file_lock(path):
            data = sp.read_json_or(path, {"n": 0})
            data["n"] += 1
            sp.atomic_write_json(path, data)


@pytest.mark.skipif(sp.IS_WINDOWS, reason="fork-based multiprocessing check")
def test_file_lock_serialises_read_modify_write(tmp_path):
    p = tmp_path / "counter.json"
    procs = [multiprocessing.Process(target=_bump, args=(p, 25)) for _ in range(4)]
    [x.start() for x in procs]
    [x.join(60) for x in procs]
    assert sp.read_json_or(p, {})["n"] == 100       # no lost updates


def test_file_lock_times_out_when_held(tmp_path):
    p = tmp_path / "held"
    with sp.file_lock(p):
        with pytest.raises(TimeoutError):
            with sp.file_lock(p, timeout=0.3):
                pass


def test_pid_helpers():
    assert sp.pid_alive(os.getpid())
    assert not sp.pid_alive(0) and not sp.pid_alive(-5) and not sp.pid_alive("x")
    assert not sp.pid_matches(os.getpid(), "definitely-not-in-this-cmdline-xyz")


@pytest.mark.skipif(sp.IS_WINDOWS, reason="POSIX process groups")
def test_terminate_tree_kills_children_too(tmp_path):
    marker = tmp_path / "child.pid"
    code = (
        "import subprocess,sys,time;"
        "c=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);"
        f"open(r'{marker}','w').write(str(c.pid));time.sleep(60)"
    )
    leader = subprocess.Popen([sys.executable, "-c", code], **sp.detached_popen_kwargs())
    for _ in range(100):
        if marker.exists() and marker.read_text():
            break
        time.sleep(0.05)
    child_pid = int(marker.read_text())
    assert sp.pid_alive(child_pid)
    assert sp.terminate_tree(leader.pid)
    leader.wait(5)
    time.sleep(0.3)
    assert not sp.pid_alive(child_pid)
