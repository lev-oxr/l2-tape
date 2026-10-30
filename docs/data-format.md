# Raw data format (hftbacktest collector)

Source of truth: collector code at `nkaz001/hftbacktest@5f3ec40` (`collector/src/`) plus
our patch `docker/collector.patch`. Everything below was checked against that code and a
10-minute recording on 2026-10-30, not taken from documentation.

## Files

```text
data/raw/<exchange>/<symbol>_<YYYYMMDD>.gz
```

- One process per exchange; the first CLI argument is the output directory, so each
  exchange gets its own directory (symbol file names would otherwise collide).
- `<symbol>` is lower-case, `<YYYYMMDD>` is the **UTC date of the local receive time**.
  The file rotates on the first message after UTC midnight.
- The collector does not create the directory: if it is missing, it logs `write error` and
  exits with code 0. The Docker entrypoint runs `mkdir -p` first.
- gzip. A file can contain **several gzip members** — one per collector run that day
  (our `append` patch). `gzcat` and Python `gzip.open` read all members; Rust
  `flate2::GzDecoder` reads only the first (use `MultiGzDecoder`).
- The current day's file is incomplete gzip until the collector stops or the day rotates.
  Only SIGINT finishes it cleanly; SIGTERM/SIGKILL lose the encoder buffer
  (measured: ~6 s of Bybit BTCUSDT) and leave a truncated member.

## Lines

```text
<local_ts_ns> <raw message>\n
```

- `local_ts_ns`: `Utc::now()` in nanoseconds, taken when the WebSocket text frame was
  received (after TLS and framing). Wall clock, not monotonic: a clock step would show up
  here. For REST snapshots it is taken after the HTTP response arrives.
- One space, then the message exactly as received. Messages contain no newlines.
- Lines are in receive order (0 decreasing `local_ts` in 319 399 lines, 2026-10-30).

## Binance USDⓈ-M (`binancefutures`)

Connection: `wss://fstream.binance.com/stream?streams=` (legacy combined-stream URL) with
`<s>@trade`, `<s>@depth@0ms` for every symbol on one connection. Upstream also subscribes
`<s>@bookTicker`; our patch removes it (see Known behaviour).

| Message | Shape | Approx. rate, BTCUSDT |
|---|---|---|
| depth | `{"stream":"btcusdt@depth@0ms","data":{"e":"depthUpdate","E","T","s","U","u","pu","b","a"}}` | 30 /s |
| trade | `{"stream":"btcusdt@trade","data":{"e":"trade",...}}` | 36 /s |
| REST snapshot | `{"lastUpdateId","E","T","bids","asks"}` — **no wrapper** | on each gap |

The collector checks `pu == previous u` per symbol. On the first depth event and on every
mismatch it logs `missing depth feed has been detected`, fetches
`GET /fapi/v1/depth?symbol=<S>&limit=1000` and writes the response as its own line.
The map of previous `u` survives reconnects, so **every reconnect = one REST snapshot line**.
There is no explicit reconnect marker in the file.

## Bybit linear (`bybit`)

Connection: `wss://stream.bybit.com/v5/public/linear`, topics `orderbook.1`, `orderbook.50`,
`orderbook.1000` (patched from the non-existent `orderbook.500`), `publicTrade`.

| Message | Notes | Approx. rate, BTCUSDT |
|---|---|---|
| `orderbook.1` | `type: snapshot` only (by Bybit design) | 29 /s |
| `orderbook.50` | 1 snapshot after subscribe, then `delta` | 44 /s |
| `orderbook.1000` | 1 snapshot after subscribe, then `delta` | 5 /s |
| `publicTrade` | array of trades in `data` | 10 /s |

- Only messages with a `topic` are written; subscribe/pong responses are dropped.
- No sequence check in the collector. After a reconnect, Bybit sends a fresh `snapshot`
  for each topic — that snapshot is the only trace of a reconnect in the file.
- A failed subscription (`success:false`) is only logged; the collector then records
  nothing and does not exit.

## Known behaviour (measured 2026-10-30, laptop in Ukraine)

- With `bookTicker` in the Binance subscription the connection was reset every 4–106 s
  (15 resets in 10 min); without it, no resets in 3 min. Each reset is visible in the file
  only as a `pu` break followed by a REST snapshot line. Hence our patch drops `bookTicker`;
  best bid/ask is reconstructed from `depth@0ms`.
