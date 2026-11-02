from pathlib import Path

import numpy as np

from l2tape.check import load_ref
from l2tape.convert import bybit_rows, write_events
from l2tape.heatmap import window
from l2tape.raw import read_raw

FIXTURES = Path(__file__).parent / "fixtures"
S = 10**9


def row(t, kind, side, price, qty, conn):
    return t * S, t * S, kind, side, price, qty, None, None, None, None, conn


def test_book_is_sampled_with_trades_delta_and_reconnect(tmp_path):
    rows = [
        row(0, "snapshot", "bid", 99.0, 1.0, 1),
        row(0, "snapshot", "ask", 101.0, 2.0, 1),
        row(1, "delta", "bid", 100.0, 3.0, 1),  # before t0: part of the starting book
        row(3, "trade", "buy", 101.0, 0.5, 1),
        row(4, "delta", "bid", 99.0, 0.0, 1),  # level removed
        row(6, "snapshot", "bid", 98.0, 4.0, 2),  # reconnect: 100.0 must not linger
        row(6, "snapshot", "ask", 99.0, 1.0, 2),
        row(7, "trade", "sell", 98.0, 2.0, 2),
    ]
    write_events(iter(rows), "x", "Y", tmp_path / "e.parquet")
    w = window(str(tmp_path / "*.parquet"), 2 * S, 8 * S, 2 * S, tick=1.0, pad=0.05, rows=100)

    assert list(w.times) == [4 * S, 6 * S, 8 * S]
    assert list(w.best_bid) == [100.0, 98.0, 98.0] and list(w.best_ask) == [101.0, 99.0, 99.0]
    assert list(w.cum_delta) == [0.5, 0.5, -1.5]
    at = lambda price, k: w.depth[np.searchsorted(w.prices, price), k]  # noqa: E731
    assert at(99.0, 0) == 0.0  # removed at 4 s, the sample at 4 s includes it
    assert at(100.0, 0) == 3.0 and at(100.0, 2) == 0.0  # gone after the reconnect
    assert at(99.0, 2) == 1.0 and at(98.0, 2) == 4.0
    assert list(w.trades["side"]) == [1, -1]


def test_last_sample_matches_the_rest_reference(tmp_path):
    rows = list(bybit_rows(read_raw(FIXTURES / "check_bybit_btcusdt.gz")))
    write_events(iter(rows), "bybit", "BTCUSDT", tmp_path / "e.parquet")
    t0, t1 = rows[0][0], rows[-1][0]
    w = window(str(tmp_path / "*.parquet"), t0, t1, t1 - t0, tick=0.1)
    _, ref = load_ref(FIXTURES / "check_bybit_btcusdt_ref.parquet")
    assert (w.best_bid[-1], w.best_ask[-1]) == (max(ref.bids), min(ref.asks))
    near = [q for p, q in ref.bids.items() if p >= w.prices[0]]
    near += [q for p, q in ref.asks.items() if p < w.prices[-1] + (w.prices[1] - w.prices[0])]
    assert np.isclose(w.depth[:, -1].sum(), sum(near))  # every resting lot in range is binned
