# l2-tape

A 24/7 recorder of the L2 order book and trades for BTCUSDT and ETHUSDT perpetuals on
Binance USDⓈ-M and Bybit linear — and, mainly, the proof that the recording is complete
and correct.

Public market data only: no API keys, no orders.

## How it works

| Part | What it does |
|---|---|
| collector | [hftbacktest](https://github.com/nkaz001/hftbacktest) collector, pinned and patched ([docker/](docker/)): raw WebSocket messages → `data/raw/<exchange>/<symbol>_<YYYYMMDD>.gz` |
| `l2tape snapshot` | REST order book snapshot (1000 levels) every 15 min → `data/snapshots/` (Parquet) |
| `l2tape health` | Telegram alert when a stream stops growing for 5 min, or the disk runs low; `/metrics` for Prometheus |
| Prometheus + Grafana | trends of the recorder for 30 days: messages/s, latency p50/p99, resyncs, free disk |
| `scripts/sync.sh` | copies finished days to the laptop, verifies checksums, only then prunes the VPS |
| `l2tape check` | replays a day, rebuilds the book, compares it with every REST snapshot → quality report |
| `l2tape convert` | a day → normalized Parquet (`data/events/`) and hftbacktest `.npz` (`data/hbt/`) |

Raw format: [docs/data-format.md](docs/data-format.md).

## Run

On the VPS (Docker Compose; `.env` from `.env.example` holds the Telegram token):

```bash
docker compose build && docker compose up -d
```

On the laptop:

```bash
scripts/sync.sh root@<vps>:/root/l2-tape/data
uv run l2tape check --date 2026-11-01     # → data/reports/2026-11-01.md
scripts/install-sync.sh root@<vps>:/root/l2-tape/data   # sync daily at 09:00 (launchd)
```

## Use the data

```bash
uv run l2tape convert --date 2026-11-01
# DuckDB: spread and top-10 imbalance at every minute end
uv run python examples/book_by_minute.py data/events/exchange=bybit/symbol=BTCUSDT/date=2026-11-01
# hftbacktest: replay the day, compare its book with the REST snapshots
uv run python examples/hbt_replay.py bybit BTCUSDT 2026-11-01
# book heatmap with trades, spread, imbalance and delta (opens in the browser)
uv run marimo run notebooks/book_heatmap.py
```

`data/events` has one row per book level or trade, the same columns for both exchanges:
`local_ts`, `exch_ts` (ns), `kind` (snapshot / delta / trade), `side`, `price`, `qty`,
update ids, `seq`, and `conn_id`, which grows with every snapshot: levels of an older
connection are stale. The day files carry no snapshot at 00:00, so the hftbacktest data
comes with `_snapshot.npz` built from the day's first REST snapshot.

`health` pings a dead man's switch (`HEALTHCHECK_URL` in `.env`, e.g. healthchecks.io)
every 5 min, so a dead VPS or a broken Telegram setup raises an alert from outside.

Grafana listens on the VPS's localhost only. Open it through an SSH tunnel:

```bash
ssh -N -L 3000:localhost:3000 root@<vps>   # then http://localhost:3000, dashboard "l2-tape recorder"
```

## Measured quality, first 3 days (VPS in Lauterbourg, FR)

30.10 is a partial day (from 20:53 UTC). Latency is local receive time minus exchange time
(Binance `E`, Bybit `ts`), ± 8.2 ms clock uncertainty (chrony: root delay / 2 + root dispersion).

| Stream | Day | Complete | Lost, s | Lost trades | Latency p50 / p99, ms | REST checks: ok / differ | Raw, MB |
|---|---|---|---|---|---|---|---|
| Binance BTCUSDT | 30.10 | 99.9968 % | 0.4 | 0 | 132 / 332 | 12 / 0 | 66 |
| | 31.10 | 99.9995 % | 0.4 | 0 | 132 / 315 | 95 / 0 | 414 |
| | 01.11 | 99.9967 % | 2.8 | 2 308 | 132 / 330 | 94 / 0 ¹ | 400 |
| Binance ETHUSDT | 30.10 | 99.9951 % | 0.5 | 0 | 132 / 333 | 12 / 0 | 64 |
| | 31.10 | 99.9990 % | 0.9 | 0 | 132 / 315 | 95 / 0 | 413 |
| | 01.11 | 99.9963 % | 3.2 | 4 608 | 132 / 330 | 95 / 0 | 462 |
| Bybit BTCUSDT | 30.10 | 96.9483 % ² | 341.1 | — | 83 / 255 | 11 / 0 | 46 |
| | 31.10 | 99.5596 % ³ | 380.5 | — | 93 / 256 | 95 / 0 | 267 |
| | 01.11 | 99.9844 % | 13.5 | — | 93 / 258 | 95 / 0 | 251 |
| Bybit ETHUSDT | 30.10 | 96.9484 % ² | 341.1 | — | 82 / 247 | 11 / 0 | 40 |
| | 31.10 | 99.5582 % ³ | 381.7 | — | 92 / 256 | 95 / 0 | 243 |
| | 01.11 | 99.9835 % | 14.3 | — | 92 / 256 | 95 / 0 | 242 |

**805 comparisons of the rebuilt book with the exchange's own REST snapshot, 0 differing
levels** (Bybit: all 2000 levels; Binance: 241–1999 levels, the part both books cover).

¹ One more snapshot fell out of depth: price moved > $250 in 15 min, beyond the 1000 levels
  (≈ $130 per side for BTC) of both snapshots, so there was nothing to compare.
² Planned test stop of the Bybit collector (5.7 min) to check the Telegram alert.
³ 6 min 19 s of a dead connection: Bybit stopped sending at 03:38:24, the collector noticed
  only at the TCP reset at 03:44:43. Fixed since 2026-11-02: a connection silent for 5 s
  is dropped and reconnected (a healthy one is never silent for more than 1.4 s).

Notes:
- Binance had one break in 51 h (01.11 22:21, both symbols on one connection, 2.5 s).
  Trade ids are consecutive, so lost trades are an exact count.
- Bybit reconnects 1–5 times a day, 1–8 s each. Its update id can step back after a
  reconnect, so lost messages are measured in time only; trade ids are UUIDs.
- Each stretch is counted from the last message before a break to the next sync, so a hole
  is overstated by up to one message interval (Binance ~30 ms, Bybit ~200 ms). The 1–3 s at
  00:00 on Bybit is the checker's own warm-up, not lost data: the day file starts without a
  snapshot, and the four REST snapshots are taken one after another (Bybit at 00:00:02–03).
- Volume: ≈ 1.3 GB/day for all 4 streams on a weekend, ≈ 2 GB on a weekday.
