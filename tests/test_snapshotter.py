import json
from itertools import pairwise
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from l2tape import snapshotter

FIXTURES = Path(__file__).parent / "fixtures"
LOCAL_TS = 1_793_390_650_500_000_000


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.mark.parametrize(
    ("exchange", "fixture"),
    [("binancefutures", "rest_binancefutures_btcusdt.json"), ("bybit", "rest_bybit_btcusdt.json")],
)
def test_parse_keeps_every_level_in_book_order(exchange, fixture):
    t = snapshotter.parse(exchange, "BTCUSDT", LOCAL_TS, load(fixture)).to_pylist()
    bids = [r for r in t if r["side"] == "bid"]
    asks = [r for r in t if r["side"] == "ask"]

    assert len(bids) == len(asks) == 1000
    assert [r["level"] for r in bids] == list(range(1000))
    assert all(a["price"] > b["price"] for a, b in pairwise(bids))  # bids: best = highest
    assert all(a["price"] < b["price"] for a, b in pairwise(asks))  # asks: best = lowest
    assert bids[0]["price"] < asks[0]["price"]  # book is not crossed
    assert all(r["qty"] > 0 for r in t)


def test_binance_ids_and_time_units():
    body = load("rest_binancefutures_btcusdt.json")
    row = snapshotter.parse("binancefutures", "BTCUSDT", LOCAL_TS, body).to_pylist()[0]
    assert row["last_update_id"] == body["lastUpdateId"]
    assert row["seq"] is None
    assert row["exch_ts"] == body["E"] * 1_000_000  # exchange ms -> ns
    assert 0 < row["local_ts"] - row["exch_ts"] < 10**9  # same units: under a second apart


def test_bybit_ids_and_error():
    body = load("rest_bybit_btcusdt.json")
    row = snapshotter.parse("bybit", "BTCUSDT", LOCAL_TS, body).to_pylist()[0]
    assert row["last_update_id"] == body["result"]["u"]
    assert row["seq"] == body["result"]["seq"]
    assert row["exch_ts"] == body["result"]["ts"] * 1_000_000

    with pytest.raises(ValueError):
        snapshotter.parse("bybit", "BTCUSDT", LOCAL_TS, {"retCode": 10001, "retMsg": "bad"})


def test_write_atomic_roundtrip(tmp_path):
    table = snapshotter.parse("bybit", "BTCUSDT", LOCAL_TS, load("rest_bybit_btcusdt.json"))
    path = tmp_path / "a" / "b.parquet"
    snapshotter.write_atomic(table, path)
    assert pq.read_table(path).equals(table)
    assert list(path.parent.iterdir()) == [path]


def test_write_atomic_leaves_nothing_on_crash(tmp_path, monkeypatch):
    def crash_midway(table, f):
        f.write(b"PAR1 half a file")
        raise OSError("disk full")

    monkeypatch.setattr(snapshotter.pq, "write_table", crash_midway)
    table = snapshotter.parse("bybit", "BTCUSDT", LOCAL_TS, load("rest_bybit_btcusdt.json"))
    path = tmp_path / "b.parquet"
    with pytest.raises(OSError):
        snapshotter.write_atomic(table, path)
    assert list(tmp_path.iterdir()) == []  # neither the final file nor the temp one
