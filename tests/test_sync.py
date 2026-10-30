"""scripts/sync.sh against a fake VPS: a local dir reached through an ssh shim."""

import os
import subprocess
import time
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts" / "sync.sh"
DAY = 86_400


def make(path: Path, age_days: float) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(path.name.encode())
    t = time.time() - age_days * DAY
    os.utime(path, (t, t))
    return path


def test_sync_copies_finished_days_and_cleans_only_old_verified_files(tmp_path):
    today = time.strftime("%Y%m%d", time.gmtime())
    vps, laptop = tmp_path / "vps", tmp_path / "laptop"
    old = make(vps / "raw/bybit/btcusdt_20261001.gz", 10)
    recent = make(vps / "raw/bybit/btcusdt_20261029.gz", 1)
    live = make(vps / f"raw/bybit/btcusdt_{today}.gz", 0)
    old_snap = make(vps / "snapshots/exchange=bybit/date=2026-10-01/1.parquet", 10)
    tmp = make(vps / "snapshots/exchange=bybit/date=2026-10-30/2.tmp", 0)

    # "ssh host cmd..." -> run cmd locally; rsync uses the same shim as its remote shell.
    shim = tmp_path / "fake-ssh"
    shim.write_text('#!/bin/sh\nshift\nexec sh -c "$*"\n')
    shim.chmod(0o755)

    env = os.environ | {"DST": str(laptop), "SSH": str(shim), "KEEP_DAYS": "3"}
    subprocess.run(["bash", SCRIPT, f"fakevps:{vps}"], env=env, check=True)

    rel = lambda p: p.relative_to(vps)  # noqa: E731
    # Laptop got every finished file, but not the live day or temp files.
    assert (laptop / rel(old)).exists() and (laptop / rel(recent)).exists()
    assert (laptop / rel(old_snap)).exists()
    assert not (laptop / rel(live)).exists() and not (laptop / rel(tmp)).exists()
    # VPS kept the live day, recent days and temp files; dropped only the old ones.
    assert live.exists() and recent.exists() and tmp.exists()
    assert not old.exists() and not old_snap.exists()
    assert not old_snap.parent.exists()  # empty snapshot dirs are pruned
