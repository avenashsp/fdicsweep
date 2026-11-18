import json
import multiprocessing
import subprocess
import sys

import pytest

import sweep_jobs as jobs
import sweep_platform as sp


@pytest.fixture()
def registry(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "JOBS_METADATA_FILE", tmp_path / "jobs.json")
    return tmp_path / "jobs.json"


def _add(path_str, name):
    import sweep_jobs as j
    from pathlib import Path
    j.JOBS_METADATA_FILE = Path(path_str)
    for i in range(10):
        with j.registry_transaction() as reg:
            reg[f"{name}-{i}"] = {"pid": 0}


@pytest.mark.skipif(sp.IS_WINDOWS, reason="fork-based multiprocessing check")
def test_registry_transactions_do_not_lose_updates(registry):
    procs = [multiprocessing.Process(target=_add, args=(str(registry), f"w{n}")) for n in range(4)]
    [p.start() for p in procs]
    [p.join(60) for p in procs]
    assert len(json.loads(registry.read_text())) == 40


def test_corrupt_registry_reads_as_empty(registry):
    registry.write_text("{broken")
    assert jobs._load_jobs_registry() == {}


def test_stop_never_signals_unrelated_process_with_reused_pid(registry):
    bystander = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        registry.write_text(json.dumps({
            "stale": {"pid": bystander.pid, "status": "RUNNING", "started_at": "2026-01-01T00:00:00"},
        }))
        stopped = jobs.stop_jobs("stale")
        assert stopped == 0
        assert bystander.poll() is None                      # still running: it was not our supervisor
        assert json.loads(registry.read_text())["stale"]["status"] == "STOPPED"
    finally:
        bystander.kill()
        bystander.wait()


def test_stop_terminates_a_genuine_supervisor_and_its_children(registry, tmp_path):
    runner = tmp_path / "sweep_bg.py"          # command line contains the supervisor marker
    runner.write_text(
        "import subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        "time.sleep(60)\n")
    leader = subprocess.Popen([sys.executable, str(runner)], **sp.detached_popen_kwargs())
    try:
        registry.write_text(json.dumps({
            "real": {"pid": leader.pid, "status": "RUNNING", "started_at": "2026-01-01T00:00:00"},
        }))
        assert jobs.stop_jobs("real") == 1
        leader.wait(10)
        assert json.loads(registry.read_text())["real"]["status"] == "STOPPED"
    finally:
        if leader.poll() is None:
            leader.kill()
            leader.wait()


def test_finished_jobs_keep_their_status_when_stop_is_run(registry):
    registry.write_text(json.dumps({"done": {"pid": 0, "status": "FINISHED", "started_at": "2026-01-01T00:00:00"}}))
    jobs.stop_jobs()
    assert json.loads(registry.read_text())["done"]["status"] == "FINISHED"
