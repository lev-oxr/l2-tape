"""Record a public WebSocket stream for N seconds into the hftbacktest collector format.

Each line is "<local_ts_ns> <raw message>", gzip-compressed — same as the collector's
daily files, so fixtures and real data share one reader.

    uv run scripts/record_ws.py URL OUT.gz SECONDS [SUBSCRIBE_JSON]

Fixtures in tests/fixtures/ were recorded with (2026-10-30, 120 s each). Streams match the
collector's, minus bookTicker (~1100 msg/s, useless for book tests) and minus Bybit
orderbook.500 (no longer exists, rejects the whole subscription):

    uv run scripts/record_ws.py \
      "wss://fstream.binance.com/stream?streams=btcusdt@depth@0ms/btcusdt@trade" \
      tests/fixtures/binancefutures_btcusdt.gz 120
    uv run scripts/record_ws.py "wss://stream.bybit.com/v5/public/linear" \
      tests/fixtures/bybit_btcusdt.gz 120 \
      '{"op":"subscribe","args":["orderbook.50.BTCUSDT","publicTrade.BTCUSDT"]}'
    uv run scripts/record_ws.py \
      "wss://fstream.binance.com/stream?streams=ethusdt@depth@0ms/ethusdt@trade" \
      tests/fixtures/binancefutures_ethusdt.gz 120
    uv run scripts/record_ws.py "wss://stream.bybit.com/v5/public/linear" \
      tests/fixtures/bybit_ethusdt.gz 120 \
      '{"op":"subscribe","args":["orderbook.50.ETHUSDT","publicTrade.ETHUSDT"]}'

"""

import asyncio
import gzip
import json
import sys
import time

import websockets


async def record(url: str, out: str, seconds: float, subscribe: str | None) -> None:
    deadline = time.monotonic() + seconds
    count = 0
    with gzip.open(out, "wt") as f:
        async with websockets.connect(url, max_size=None) as ws:
            if subscribe:
                await ws.send(subscribe)
            last_ping = time.monotonic()
            while (left := deadline - time.monotonic()) > 0:
                # Bybit drops idle connections without an app-level ping; 20 s per its docs.
                if subscribe and time.monotonic() - last_ping > 20:
                    await ws.send(json.dumps({"op": "ping"}))
                    last_ping = time.monotonic()
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=min(left, 5))
                except TimeoutError:
                    continue
                f.write(f"{time.time_ns()} {msg}\n")
                count += 1
    print(f"{out}: {count} messages in {seconds:.0f} s")


if __name__ == "__main__":
    url, out, seconds = sys.argv[1], sys.argv[2], float(sys.argv[3])
    asyncio.run(record(url, out, seconds, sys.argv[4] if len(sys.argv) > 4 else None))
