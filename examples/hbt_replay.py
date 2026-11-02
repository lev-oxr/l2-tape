"""hftbacktest: replay one converted day and compare its book with our REST snapshots.

    uv run python examples/hbt_replay.py bybit BTCUSDT 2026-11-01

At each REST snapshot's receive time the backtester's best bid/ask and top 10 levels per
side are compared with the snapshot. Not an exact check: the snapshot reflects an update id,
the replay a receive time, and the top of a busy book changes within milliseconds (the exact
check, by update id, is `l2tape check`). Shifting the comparison time by ±25 ms or more
only makes it worse (2026-11-01): the data is aligned in time.
"""

import sys
from pathlib import Path

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest

from l2tape.check import load_ref
from l2tape.convert import SPECS

exchange, symbol, day = sys.argv[1:4]
tick, lot = SPECS[(exchange, symbol)]
data = f"data/hbt/{exchange}_{symbol}_{day}.npz"
asset = (
    BacktestAsset()
    .data([data])
    .initial_snapshot(data.replace(".npz", "_snapshot.npz"))
    .linear_asset(1.0)
    .constant_order_latency(0, 0)
    .risk_adverse_queue_model()
    .no_partial_fill_exchange()
    .trading_value_fee_model(0.0, 0.0)
    .tick_size(tick)
    .lot_size(lot)
)
hbt = HashMapMarketDepthBacktest([asset])
hbt.wait_next_feed(False, 60 * 10**9)  # before the first event the clock reads i64::MAX
refs = sorted(
    Path(f"data/snapshots/exchange={exchange}/symbol={symbol}/date={day}").glob("*.parquet")
)

print("time (UTC)  REST bid / ask  hftbacktest bid / ask  top-10 levels differ")
total = same_bbo = 0
for f in refs[1:]:  # the first one is the initial snapshot
    if hbt.elapse(int(f.stem) - hbt.current_timestamp) != 0:
        break
    d, (_, ref) = hbt.depth(0), load_ref(f)
    top = [(p, q, d.bid_qty_at_tick(round(p / tick))) for p, q in sorted(ref.bids.items())[-10:]]
    top += [(p, q, d.ask_qty_at_tick(round(p / tick))) for p, q in sorted(ref.asks.items())[:10]]
    differ = sum(abs(q - mine) > lot / 2 for _, q, mine in top)
    rest, mine = (max(ref.bids), min(ref.asks)), (d.best_bid, d.best_ask)
    total += differ
    same_bbo += [round(x / tick) for x in rest] == [round(x / tick) for x in mine]
    t = f"{int(f.stem) // 10**9 % 86400 // 3600:02}:{int(f.stem) // 10**9 % 3600 // 60:02}"
    rest_s, mine_s = (f"{b:g} / {a:g}" for b, a in (rest, mine))
    print(f"{t}  {rest_s:>22}  {mine_s:>22}  {differ:3}")
hbt.close()
n = len(refs) - 1
print(f"{n} snapshots: best bid/ask equal in {same_bbo}, {total} of {20 * n} top-10 levels differ")
