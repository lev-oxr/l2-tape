import marimo

__generated_with = "0.25.2"
app = marimo.App(width="full", app_title="l2-tape: book heatmap")


@app.cell
def _():
    from datetime import UTC, datetime
    from pathlib import Path

    import marimo as mo
    import numpy as np
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    from l2tape.convert import SPECS
    from l2tape.heatmap import window

    DATA = Path(mo.notebook_dir()).parent / "data" / "events"
    return DATA, SPECS, UTC, datetime, go, make_subplots, mo, np, window


@app.cell
def _(mo):
    mo.md("""
    # Order book heatmap
    Price × time, colour = resting quantity (log scale). Lines: best bid / ask.
    Bubbles: trades, size ∝ √volume, green = aggressive buy, red = aggressive sell.
    Needs `l2tape convert --date <day>` first.
    """)
    return


@app.cell
def _(mo):
    exchange = mo.ui.dropdown(["binancefutures", "bybit"], value="binancefutures", label="Exchange")
    symbol = mo.ui.dropdown(["BTCUSDT", "ETHUSDT"], value="BTCUSDT", label="Symbol")
    day = mo.ui.text("2026-11-01", label="Day (UTC)")
    start = mo.ui.text("23:00", label="Start, HH:MM UTC")
    minutes = mo.ui.slider(1, 60, value=15, label="Minutes", show_value=True)
    mo.hstack([exchange, symbol, day, start, minutes], justify="start", gap=2)
    return day, exchange, minutes, start, symbol


@app.cell
def _(DATA, SPECS, UTC, datetime, day, exchange, minutes, mo, start, symbol, window):
    t0 = datetime.fromisoformat(f"{day.value}T{start.value}").replace(tzinfo=UTC)
    t0 = int(t0.timestamp()) * 10**9
    t1 = t0 + minutes.value * 60 * 10**9
    step = max(10**8, (t1 - t0) // 1500)  # ~1500 samples: about the plot's width in pixels
    events = DATA / f"exchange={exchange.value}/symbol={symbol.value}/date={day.value}"
    try:
        w = window(f"{events}/*.parquet", t0, t1, step, SPECS[(exchange.value, symbol.value)][0])
    except Exception as e:
        mo.stop(True, mo.callout(f"{type(e).__name__}: {e}", kind="danger"))
    return step, w


@app.cell
def _(go, make_subplots, np, step, w):
    x = w.times.astype("datetime64[ns]")
    # float32 halves what goes to the browser (marimo caps a cell's output size)
    z, qty = np.log1p(w.depth).astype(np.float32), w.depth.astype(np.float32)
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.64, 0.12, 0.12, 0.12],
        subplot_titles=(
            f"book, step {step / 1e6:g} ms, bin {w.prices[1] - w.prices[0]:g}",
            "spread",
            "imbalance, top 10 levels",
            "cumulative delta",
        ),
    )
    fig.add_trace(
        go.Heatmap(
            x=x,
            y=w.prices,
            z=z,
            customdata=qty,
            zmin=0,
            zmax=np.percentile(z[z > 0], 99.5),  # one wall must not wash out the rest
            colorscale="Inferno",
            showscale=False,
            hovertemplate="%{x}<br>%{y}<br>qty %{customdata:.3f}<extra></extra>",
        ),
        1,
        1,
    )
    for y, color in ((w.best_bid, "#2ecc71"), (w.best_ask, "#e74c3c")):
        fig.add_trace(go.Scatter(x=x, y=y, line={"color": color, "width": 1}), 1, 1)
    t = w.trades
    if len(t["qty"]):
        size = 2 + 12 * np.sqrt(np.minimum(t["qty"] / np.percentile(t["qty"], 99.9), 1))
        fig.add_trace(
            go.Scattergl(
                x=t["local_ts"].astype("datetime64[ns]"),
                y=t["price"],
                mode="markers",
                marker={
                    "size": size,
                    "color": t["side"].astype(np.int8),
                    "colorscale": [[0, "#e74c3c"], [1, "#2ecc71"]],
                    "opacity": 0.6,
                    "line": {"width": 0},
                },
                customdata=t["qty"],
                hovertemplate="%{x}<br>%{y}<br>trade %{customdata}<extra></extra>",
            ),
            1,
            1,
        )
    fig.add_trace(go.Scatter(x=x, y=w.best_ask - w.best_bid, line={"width": 1}), 2, 1)
    fig.add_trace(go.Scatter(x=x, y=w.imbalance10, line={"width": 1}), 3, 1)
    fig.add_trace(go.Scatter(x=x, y=w.cum_delta, line={"width": 1}), 4, 1)
    fig.update_layout(height=950, template="plotly_dark", showlegend=False, margin={"t": 40})
    fig
    return


if __name__ == "__main__":
    app.run()
