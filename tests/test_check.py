"""Replay on real recordings (collector format) ending at a REST reference snapshot.

check_<exchange>_btcusdt.gz: raw depth messages from start until the reference's update id;
check_<exchange>_btcusdt_ref.parquet: the snapshotter's REST snapshot at that id.
"""

from pathlib import Path

from l2tape.check import check_binance, check_bybit, day_holes, load_ref, read_raw

FIXTURES = Path(__file__).parent / "fixtures"
S = 10**9  # ns per second


def load(exchange: str):
    lines = list(read_raw(FIXTURES / f"check_{exchange}_btcusdt.gz"))
    rid, ref = load_ref(FIXTURES / f"check_{exchange}_btcusdt_ref.parquet")
    return lines, {rid: ref}


def depth_events(lines):
    return [m["data"] for _, m in lines if m.get("data", {}).get("e") == "depthUpdate"]


def test_binance_snapshot_plus_deltas_equals_next_snapshot():
    lines, refs = load("binancefutures")
    r = check_binance(lines, refs)
    assert len(r.syncs) == 1 and r.gaps == [] and r.unchecked == 0
    [(rid, compared, differ)] = r.checks
    assert compared > 1000 and differ == 0


def test_bybit_snapshot_plus_deltas_equals_next_snapshot():
    lines, refs = load("bybit")
    r = check_bybit(lines, refs)
    assert len(r.syncs) == 1 and r.gaps == [] and r.unchecked == 0
    assert r.checks == [(next(iter(refs)), 2000, 0)]  # full 1000 levels per side, exact


def test_binance_lost_event_is_a_gap():
    lines, refs = load("binancefutures")
    lost = depth_events(lines)[400]
    lines = [(ts, m) for ts, m in lines if m.get("data") is not lost]

    r = check_binance(lines, refs)
    assert len(r.gaps) == 1
    assert r.checks == [] and len(r.syncs) == 2  # the reference after the gap becomes the sync


def test_day_file_starting_mid_stream_syncs_from_the_reference():
    # Day rotation: the new file has no snapshot, just deltas from wherever the stream is.
    for exchange, check in (("binancefutures", check_binance), ("bybit", check_bybit)):
        lines, refs = load(exchange)
        r = check(lines[len(lines) // 2 :], refs)
        assert r.gaps == [] and len(r.syncs) == 1 and r.unchecked == 0, exchange


def test_silently_corrupted_delta_is_caught_by_the_reference():
    # Update ids intact, content wrong: only the comparison with REST can see this.
    lines, refs = load("bybit")
    deltas = [m["data"] for _, m in lines if m.get("type") == "delta" and m["data"]["a"]]
    deltas[len(deltas) // 2]["a"] = []

    r = check_bybit(lines, refs)
    assert r.gaps == []
    assert r.checks[0][2] > 0


def msg(kind: str, u: int, bids=(), t=0):
    """Bybit message received at t seconds; exchange time = receive time."""
    data = {"u": u, "b": list(bids), "a": []}
    return t * S, {"topic": "orderbook.50.X", "type": kind, "ts": t * 1000, "data": data}


def test_bybit_snapshot_mid_stream_and_u1_reset_are_not_gaps():
    lines = [
        msg("snapshot", 100, [("1", "1")]),
        msg("delta", 101),
        msg("snapshot", 500, [("2", "1")]),  # exchange re-sent a snapshot: reset, not a gap
        msg("delta", 501),
        msg("snapshot", 1, [("3", "1")]),  # service restart: counter starts over
        msg("delta", 2),
    ]
    r = check_bybit(lines, {}, depth=50)
    assert r.gaps == [] and len(r.syncs) == 3


def test_bybit_jump_in_u_is_a_gap():
    lines = [msg("snapshot", 100), msg("delta", 101), msg("delta", 103)]
    r = check_bybit(lines, {}, depth=50)
    assert len(r.gaps) == 1


def test_references_are_not_modified():
    lines, refs = load("binancefutures")
    before = {rid: (dict(b.bids), dict(b.asks)) for rid, b in refs.items()}
    check_binance(lines[len(lines) // 2 :], refs)  # syncs from the reference, then applies deltas
    assert {rid: (b.bids, b.asks) for rid, b in refs.items()} == before


def test_reconnect_hole_is_measured_in_time_and_counts_against_completeness():
    lines = [
        msg("snapshot", 100, t=0),
        msg("delta", 101, t=1),
        msg("delta", 102, t=2),
        msg("snapshot", 90, t=10),  # reconnect: u may step back, the book is valid again
        msg("delta", 91, t=11),
    ]
    r = check_bybit(lines, {}, depth=50)
    assert r.gaps == [] and r.holes == [(2 * S, 10 * S)]
    # day = [0, 20 s]: 8 s hole + 9 s after the last message
    assert day_holes(r, 0, 20 * S) == [(0, 0), (2 * S, 10 * S), (11 * S, 20 * S)]


def test_binance_lost_trades_are_counted_from_trade_ids():
    trades = [(0, {"data": {"e": "trade", "t": t}}) for t in (5, 6, 8, 11, 12)]
    assert check_binance(trades, {}).lost_trades == 3  # 7, 9 and 10 never arrived


def test_latency_is_in_ns_and_plausible():
    # exchange ms vs local ns: a unit slip makes this 10^6 times off
    for exchange, check in (("binancefutures", check_binance), ("bybit", check_bybit)):
        lat = sorted(check(*load(exchange)).latency)
        assert 1e6 < lat[len(lat) // 2] < 1e9, exchange  # median between 1 ms and 1 s
