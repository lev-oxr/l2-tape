import json
from collections import defaultdict
from pathlib import Path

from l2tape.metrics import Stats, account, exposition
from l2tape.raw import read_raw

FIXTURES = Path(__file__).parent / "fixtures"


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
