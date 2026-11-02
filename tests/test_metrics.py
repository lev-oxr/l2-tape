import json
import zlib
from collections import defaultdict
from pathlib import Path

from l2tape.check import read_raw
from l2tape.metrics import GzTail, Stats, account, exposition

FIXTURES = Path(__file__).parent / "fixtures"


def append(path: Path, z, text: bytes, finish: bool = False) -> None:
    """Write like the collector: deflate output as it comes, one gzip member per run."""
    with open(path, "ab") as f:
        f.write(z.compress(text) + (z.flush() if finish else z.flush(zlib.Z_SYNC_FLUSH)))


def test_tail_reads_a_growing_multi_member_gzip(tmp_path):
    f = tmp_path / "btcusdt_20261102.gz"
    run1 = zlib.compressobj(wbits=31)
    append(f, run1, b"1 old\n2 ol")
    tail = GzTail(f, from_start=False)  # health starts now: what is there is not counted
    assert tail.lines() == []

    append(f, run1, b"d\n3 new\n4 half")
    assert tail.lines() == [b"2 old", b"3 new"]  # a line that straddles the start counts
    append(f, run1, b" line\n", finish=True)  # collector stops: member ends

    run2 = zlib.compressobj(wbits=31)  # collector restarts: a new member is appended
    append(f, run2, b"5 next run\n")
    assert tail.lines() == [b"4 half line", b"5 next run"]
    assert tail.lines() == []


def test_tail_of_a_new_file_reads_from_the_start(tmp_path):
    f = tmp_path / "btcusdt_20261103.gz"
    append(f, zlib.compressobj(wbits=31), b"1 a\n2 b\n", finish=True)
    assert GzTail(f, from_start=True).lines() == [b"1 a", b"2 b"]


def lines_of(name: str) -> list[bytes]:
    return [f"{ts} {json.dumps(m)}".encode() for ts, m in read_raw(FIXTURES / name)]


def test_account_counts_messages_resyncs_and_latency():
    for exchange in ("binancefutures", "bybit"):
        s = Stats()
        lines = lines_of(f"check_{exchange}_btcusdt.gz")
        for line in lines:
            account(s, exchange, line)
        assert s.messages == len(lines) and s.resyncs == 1, exchange  # the collector's start
        assert 10 < sorted(s.latency_ms)[len(s.latency_ms) // 2] < 1000, exchange  # ms, not ns


def test_exposition_is_prometheus_text_and_resets_latency():
    stats = defaultdict(Stats)
    stats["bybit BTCUSDT"] = Stats(messages=7, resyncs=1, latency_ms=[90.0, 100.0, 300.0])
    text = exposition(stats, {"bybit BTCUSDT": 1.5, "bybit ETHUSDT": 400.0}, 6e10)
    assert 'l2tape_messages_total{exchange="bybit",symbol="BTCUSDT"} 7' in text
    assert 'l2tape_last_message_age_seconds{exchange="bybit",symbol="ETHUSDT"} 400.0' in text
    assert 'l2tape_latency_ms{exchange="bybit",symbol="BTCUSDT",quantile="0.5"} 100.0' in text
    assert "l2tape_disk_free_bytes 60000000000" in text
    assert stats["bybit BTCUSDT"].latency_ms == []  # the next scrape covers new messages only
