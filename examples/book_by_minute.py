"""DuckDB: spread and top-10 imbalance at the end of every minute, from data/events.

    uv run python examples/book_by_minute.py \
        data/events/exchange=bybit/symbol=BTCUSDT/date=2026-11-01

The book at time t is, for every price, the last update at or before t (ASOF JOIN) within
the current connection: after a reconnect the new snapshot replaces the old book, so levels
of an older conn_id are dropped instead of lingering as phantoms.
"""

import sys

import duckdb

SQL = """
WITH book AS (
    SELECT local_ts, conn_id, side, price, qty FROM read_parquet($events)
    WHERE kind IN ('snapshot', 'delta')
),
grid AS (  -- minute ends that the recording covers
    SELECT unnest(range(
        (min(local_ts) // $step + 1) * $step, max(local_ts) + 1, $step
    )) AS t FROM book
),
conn_at AS (
    SELECT g.t, c.conn_id FROM grid g
    ASOF JOIN (SELECT DISTINCT local_ts, conn_id FROM book) c ON c.local_ts <= g.t
),
levels AS (SELECT DISTINCT conn_id, side, price FROM book),
state AS (
    SELECT g.t, l.side, l.price, e.qty
    FROM conn_at g JOIN levels l USING (conn_id)
    ASOF JOIN book e
        ON e.conn_id = l.conn_id AND e.side = l.side AND e.price = l.price AND e.local_ts <= g.t
),
top AS (
    SELECT *, row_number() OVER (
        PARTITION BY t, side ORDER BY CASE side WHEN 'bid' THEN -price ELSE price END
    ) AS lvl
    FROM state WHERE qty > 0
)
SELECT
    make_timestamp(t // 1000) AS minute_end_utc,
    round(min(price) FILTER (side = 'ask' AND lvl = 1)
        - max(price) FILTER (side = 'bid' AND lvl = 1), 8) AS spread,
    (sum(qty) FILTER (side = 'bid') - sum(qty) FILTER (side = 'ask')) / sum(qty) AS imbalance10
FROM top WHERE lvl <= 10
GROUP BY t ORDER BY t
"""


def book_by_minute(events: str, step_s: int = 60) -> duckdb.DuckDBPyRelation:
    return duckdb.sql(SQL, params={"events": f"{events}/*.parquet", "step": step_s * 10**9})


if __name__ == "__main__":
    rel = book_by_minute(sys.argv[1])
    rel.show()
    print(
        duckdb.sql(
            "SELECT count(*) AS minutes, round(avg(spread), 4) AS avg_spread, "
            "count(*) FILTER (spread <= 0) AS crossed, round(avg(imbalance10), 3) AS avg_imbalance "
            "FROM rel"
        )
    )
