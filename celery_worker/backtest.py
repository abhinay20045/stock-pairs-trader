# celery_worker/backtest.py
import uuid
import math
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pymongo import MongoClient
import yfinance as yf

MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "trading_db"   # keep same db
COL_RUNS = "bt_runs"
COL_TRADES = "bt_trades"
COL_PNL = "bt_pnl"
COL_SERIES = "bt_series"  # NEW: per-bar series for prices/z/spread/beta

def _ts(dt):  # ISO8601 UTC
    if isinstance(dt, (int, float, np.int64, np.float64)):
        return datetime.utcfromtimestamp(dt).replace(tzinfo=timezone.utc).isoformat()
    if isinstance(dt, pd.Timestamp):
        return dt.tz_convert("UTC").isoformat() if dt.tzinfo else dt.tz_localize("UTC").isoformat()
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).isoformat()
    return pd.to_datetime(dt, utc=True).isoformat()

def _rolling_beta(y, x, lookback):
    """OLS slope on last `lookback` samples (no intercept)."""
    yv = y[-lookback:].values
    xv = x[-lookback:].values
    if len(xv) < lookback:
        return None
    vx = xv.std()
    if vx == 0 or np.isnan(vx):
        return None
    # slope = Cov(y,x)/Var(x)
    return float(np.cov(yv, xv, ddof=1)[0, 1] / np.var(xv, ddof=1))

def run_pairs_backtest(
    symbols=("AAPL", "MSFT"),
    start="2023-01-01",
    end=None,
    interval="1d",              # "1d","1h","5m","1m" (note: 1m is ~30 days max on Yahoo)
    lookback=60,                # bars used for beta + z-score stats
    entry_z=2.0,
    exit_z=0.5,
    notional_per_leg=1000.0,    # dollars per leg
    fee_bps=1.0,                # round-trip fee/slippage in basis points per leg
    run_id=None
):
    """
    Backtests a simple pairs (z-score) strategy and writes:
      - bt_runs: run metadata + summary
      - bt_trades: entry/exit fills with PnL
      - bt_pnl: cumulative pnl per bar
      - bt_series: per-bar series used by the model (price1, price2, spread, z, beta)  [NEW]
    """
    if end is None:
        end = datetime.utcnow().date().isoformat()
    if run_id is None:
        run_id = str(uuid.uuid4())

    cli = MongoClient(MONGO_URI)
    db = cli[DB_NAME]
    runs = db[COL_RUNS]
    trades = db[COL_TRADES]
    pnlc = db[COL_PNL]
    seriesc = db[COL_SERIES]

    # Idempotency: if re-running with same run_id, clear old rows
    trades.delete_many({"run_id": run_id})
    pnlc.delete_many({"run_id": run_id})
    seriesc.delete_many({"run_id": run_id})

    # Upsert run header
    runs.update_one(
        {"run_id": run_id},
        {"$set": {
            "run_id": run_id,
            "status": "RUNNING",
            "params": {
                "symbols": list(symbols),
                "start": start,
                "end": end,
                "interval": interval,
                "lookback": lookback,
                "entry_z": entry_z,
                "exit_z": exit_z,
                "notional_per_leg": notional_per_leg,
                "fee_bps": fee_bps
            },
            "started_at": _ts(datetime.utcnow())
        }},
        upsert=True
    )

    s1, s2 = symbols
    df = yf.download(list(symbols), start=start, end=end, interval=interval,
                     auto_adjust=True, progress=False)

    # yfinance returns multi-index columns for multiple tickers
    if isinstance(df.columns, pd.MultiIndex):
        px1 = df["Close"][s1].dropna().rename(s1)
        px2 = df["Close"][s2].dropna().rename(s2)
    else:
        raise ValueError("Expected multi-ticker data; got single.")

    # Align and drop missing
    prices = pd.concat([px1, px2], axis=1).dropna()
    if len(prices) < lookback + 10:
        raise ValueError("Not enough bars for the chosen lookback.")

    # strategy state
    position = None    # None | "LONG_SPREAD" | "SHORT_SPREAD"
    entry_idx = None
    entry_p1 = entry_p2 = 0.0
    qty1 = qty2 = 0.0
    cumulative_pnl = 0.0

    # fee model (very simple): applied on entry and exit, both legs
    fee_mult = 1.0 - (fee_bps / 10000.0)

    pnl_rows = []
    trade_rows = []
    series_rows = []  # NEW

    for i in range(len(prices)):
        # current bar
        p1 = float(prices.iloc[i, 0])
        p2 = float(prices.iloc[i, 1])
        ts = prices.index[i]

        # defaults for series row
        beta = None
        z = None
        spread_now = None

        if i >= lookback:
            window = prices.iloc[:i]  # exclude current bar to avoid look-ahead
            beta = _rolling_beta(window[s1], window[s2], lookback)

            if beta is not None and not math.isnan(beta):
                spread_series = window[s1] - beta * window[s2]
                mu = float(spread_series[-lookback:].mean())
                sd = float(spread_series[-lookback:].std(ddof=1)) or 1e-9
                spread_now = p1 - beta * p2
                z = (spread_now - mu) / sd

                # === Trading logic only when z is defined ===
                if position is None:
                    if z >= entry_z:
                        # SHORT_SPREAD: short s1, long s2*beta
                        qty1 = -(notional_per_leg / p1)              # short shares of s1
                        qty2 = +(notional_per_leg * beta / p2)       # long shares of s2
                        entry_p1, entry_p2 = p1 * fee_mult, p2 / fee_mult
                        position = "SHORT_SPREAD"; entry_idx = i
                        trade_rows.append({
                            "run_id": run_id, "timestamp": _ts(ts), "side": "ENTER_SHORT_SPREAD",
                            "s1": s1, "s2": s2, "beta": float(beta), "qty1": float(qty1), "qty2": float(qty2),
                            "price1": p1, "price2": p2
                        })
                    elif z <= -entry_z:
                        # LONG_SPREAD: long s1, short s2*beta
                        qty1 = +(notional_per_leg / p1)
                        qty2 = -(notional_per_leg * beta / p2)
                        entry_p1, entry_p2 = p1 / fee_mult, p2 * fee_mult
                        position = "LONG_SPREAD"; entry_idx = i
                        trade_rows.append({
                            "run_id": run_id, "timestamp": _ts(ts), "side": "ENTER_LONG_SPREAD",
                            "s1": s1, "s2": s2, "beta": float(beta), "qty1": float(qty1), "qty2": float(qty2),
                            "price1": p1, "price2": p2
                        })
                else:
                    # exit rules: mean reversion towards 0 (or tighter band)
                    should_exit = abs(z) <= exit_z
                    if should_exit:
                        # exit at current simulated bar price
                        if position == "SHORT_SPREAD":
                            # short s1 covered at p1 / fee_mult, long s2 sold at p2 * fee_mult
                            pnl = qty1 * (p1 / fee_mult - entry_p1) + qty2 * (p2 * fee_mult - entry_p2)
                            side = "EXIT_SHORT_SPREAD"
                        else:
                            pnl = qty1 * (p1 * fee_mult - entry_p1) + qty2 * (p2 / fee_mult - entry_p2)
                            side = "EXIT_LONG_SPREAD"

                        cumulative_pnl += float(pnl)
                        trade_rows.append({
                            "run_id": run_id, "timestamp": _ts(ts), "side": side,
                            "s1": s1, "s2": s2, "qty1": float(qty1), "qty2": float(qty2),
                            "price1": p1, "price2": p2, "pnl": float(pnl),
                            "cum_pnl": float(cumulative_pnl), "bars_held": int(i - entry_idx)
                        })
                        position = None
                        qty1 = qty2 = 0.0

        # Mark-to-market curve (keeps zeros during warmup)
        pnl_rows.append({
            "run_id": run_id,
            "timestamp": _ts(ts),
            "cumulative_pnl": float(cumulative_pnl)
        })

        # NEW: persist the series bar (z/spread/beta may be None during warmup)
        series_rows.append({
            "run_id": run_id,
            "timestamp": _ts(ts),
            "s1": s1,
            "s2": s2,
            "price1": p1,
            "price2": p2,
            "spread": None if spread_now is None else float(spread_now),
            "z": None if z is None else float(z),
            "beta": None if beta is None else float(beta),
        })

    # persist
    if trade_rows:
        trades.insert_many(trade_rows, ordered=False)
    if pnl_rows:
        pnlc.insert_many(pnl_rows, ordered=False)
    if series_rows:
        seriesc.insert_many(series_rows, ordered=False)

    # summary metrics
    if pnl_rows:
        idx = [pd.Timestamp(r["timestamp"]) for r in pnl_rows]
        pnl_series = pd.Series([r["cumulative_pnl"] for r in pnl_rows], index=idx)
        dd = (pnl_series.cummax() - pnl_series).max()
        ret = pnl_series.iloc[-1]
    else:
        dd = 0.0
        ret = 0.0

    num_trades = sum(1 for r in trade_rows if r["side"].startswith("EXIT"))
    avg_hold = float(np.mean([r["bars_held"] for r in trade_rows if "bars_held" in r] or [0]))

    runs.update_one(
        {"run_id": run_id},
        {"$set": {
            "status": "DONE",
            "finished_at": _ts(datetime.utcnow()),
            "summary": {
                "cum_pnl": float(ret),
                "max_drawdown": float(dd) if not pd.isna(dd) else 0.0,
                "trades": int(num_trades),
                "avg_bars_held": float(avg_hold)
            }
        }},
        upsert=True
    )
    return {"run_id": run_id}
