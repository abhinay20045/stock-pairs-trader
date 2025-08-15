# celery_worker/tasks.py
from __future__ import annotations

import os, math, logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import yfinance as yf
from celery import Celery, chain
from pymongo import MongoClient, UpdateOne, ASCENDING

# For proper Engle–Granger cointegration:
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

# =========================
# CONFIG (env-overridable)
# =========================
MODEL_NAME    = os.getenv("MODEL_NAME", "zscore")           # "zscore" | "eg_ci"
STRATEGY_NAME = os.getenv("STRATEGY_NAME", "zscore")        # "zscore" | "cointegration"
MONGO_URL     = os.getenv("MONGO_URL", "mongodb://mongo:27017")
REDIS_URL     = os.getenv("REDIS_URL", "redis://redis:6379/0")
DB_NAME       = os.getenv("DB_NAME", "trading_db")

# Symbols
SYMBOL_A = os.getenv("SYMBOL_A", "AAPL")
SYMBOL_B = os.getenv("SYMBOL_B", "MSFT")
SYMBOLS  = [SYMBOL_A, SYMBOL_B]

# Data policy
INTERVAL = os.getenv("INTERVAL", "1m")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
SKIP_TODAY_CANDLES = os.getenv("SKIP_TODAY_CANDLES", "false").lower() == "true"

# Z-score defaults
ROLLING_WINDOW_MIN = int(os.getenv("ROLLING_WINDOW_MIN", "1440"))  # ~1 trading day of minutes
ENTRY_Z = float(os.getenv("ENTRY_Z", "1.0"))
EXIT_Z  = float(os.getenv("EXIT_Z",  "0.0"))

# Cointegration (EG) defaults
EG_WINDOW_MIN = int(os.getenv("EG_WINDOW_MIN", "390"))
ADF_PVAL_MAX  = float(os.getenv("ADF_PVAL_MAX", "0.20"))
RESID_ENTRY_Z = float(os.getenv("RESID_ENTRY_Z", ".6"))
RESID_EXIT_Z  = float(os.getenv("RESID_EXIT_Z",  "0.0"))

SCALE_IN = os.getenv("SCALE_IN", "true").lower() == "true"
SHARES_PER_TRADE = int(os.getenv("SHARES_PER_TRADE", "1"))
CLEANUP_ON_CYCLE = os.getenv("CLEANUP_ON_CYCLE", "true").lower() == "true"

# Scheduling (only ONE scheduled pipeline at a time)
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "60"))  # set 0 to disable beat scheduling

# =========================
# Boilerplate
# =========================
app = Celery("celery_worker", broker=REDIS_URL)
app.conf.timezone = "UTC"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

_mongo = MongoClient(MONGO_URL, tz_aware=True, tzinfo=timezone.utc)
_db = _mongo[DB_NAME]
def db(): return _db

# Indexes
_db.symbol_price_data.create_index([("symbol", ASCENDING), ("Date", ASCENDING)], unique=True)
_db.spread_data.create_index([("timestamp", ASCENDING)], unique=True)
_db.trades.create_index([("status", ASCENDING), ("timestamp", ASCENDING)])

# =========================
# Helpers
# =========================
def _now() -> datetime:
    return datetime.now(timezone.utc)

def _max_lookback() -> datetime:
    return _now() - timedelta(days=LOOKBACK_DAYS)

def _tz(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def _normalize_close(df: pd.DataFrame, sym: str) -> pd.DataFrame:
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    # Prefer sym-specific column (e.g., AAPL_Close); fallback to Close
    col = f"{sym}_Close" if f"{sym}_Close" in df.columns else ("Close" if "Close" in df.columns else None)
    if not col:
        guess = df.filter(like="Close").columns
        if not len(guess):
            raise ValueError(f"no close for {sym}")
        col = guess[0]
    return df[["Date", col]].rename(columns={col: f"{sym}_Close"}).sort_values("Date").reset_index(drop=True)

def _merge_prices(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(a, b, on="Date", how="inner").sort_values("Date").reset_index(drop=True)

# =========================
# DATA FETCH
# =========================
def fetch_new_prices(symbols: List[str]) -> int:
    """
    Pull recent candles via yfinance and upsert into symbol_price_data:
      { symbol, Date (UTC datetime), <SYMBOL>_Close, Close, ... }
    """
    database = db()
    today = _now()
    max_lb = _max_lookback()
    total = 0

    for symbol in symbols:
        latest = database.symbol_price_data.find_one({"symbol": symbol}, sort=[("Date", -1)])
        start_dt = max_lb
        if latest and isinstance(latest.get("Date"), datetime):
            start_dt = max(latest["Date"] + timedelta(minutes=1), max_lb)
        if start_dt >= today:
            continue

        df = yf.download(symbol, start=start_dt, end=today, interval=INTERVAL, auto_adjust=False, progress=False)
        if df.empty:
            continue

        df = df.reset_index()
        df.columns = ["Date", f"{symbol}_Open", f"{symbol}_High", f"{symbol}_Low",
                      f"{symbol}_Close", f"{symbol}_Adj Close", f"{symbol}_Volume"]
        df["symbol"] = symbol

        ops = []
        for _, r in df.iterrows():
            dt = r["Date"]
            if not isinstance(dt, datetime):
                continue
            dt = _tz(dt)
            if dt < max_lb:
                continue
            if SKIP_TODAY_CANDLES and dt.date() == today.date():
                continue
            d = r.to_dict()
            d["Date"] = dt
            # also store generic "Close" for convenience
            d["Close"] = float(d.get(f"{symbol}_Close"))
            ops.append(UpdateOne({"symbol": symbol, "Date": dt}, {"$set": d}, upsert=True))

        if ops:
            res = database.symbol_price_data.bulk_write(ops, ordered=False)
            up = (res.upserted_count or 0) + (res.modified_count or 0)
            total += up
            log.info("[ingest] %s upserted=%d", symbol, up)

    return total

# =========================
# MODEL INTERFACE
# =========================
class BaseModel:
    """price history -> per-timestamp metric doc(s) in spread_data"""
    metric_name: str = "z_score"
    def backfill(self) -> int: raise NotImplementedError
    def latest(self) -> Optional[Dict[str, Any]]: raise NotImplementedError

# ---- Model #1: Z-score of spread (classic pairs) -----------------------------
class ZScoreSpreadModel(BaseModel):
    metric_name = "z_score"

    def __init__(self, window: int = ROLLING_WINDOW_MIN):
        self.window = window

    def backfill(self) -> int:
        database = db()
        a = list(database.symbol_price_data.find({"symbol": SYMBOL_A}))
        b = list(database.symbol_price_data.find({"symbol": SYMBOL_B}))
        if not a or not b: return 0

        adf = _normalize_close(pd.DataFrame(a), SYMBOL_A)
        bdf = _normalize_close(pd.DataFrame(b), SYMBOL_B)
        merged = _merge_prices(adf, bdf)
        if len(merged) < self.window: return 0

        z = merged.copy()
        z["spread"] = z[f"{SYMBOL_A}_Close"] - z[f"{SYMBOL_B}_Close"]
        z["mean"]   = z["spread"].rolling(self.window).mean()
        z["std"]    = z["spread"].rolling(self.window).std()
        z["z_score"]= (z["spread"] - z["mean"]) / z["std"]
        z = z.dropna(subset=["z_score"])

        ops = []
        for _, r in z.iterrows():
            ts = _tz(pd.Timestamp(r["Date"]).to_pydatetime())
            doc = {
                "timestamp": ts,
                "aapl_price": float(r[f"{SYMBOL_A}_Close"]),
                "msft_price": float(r[f"{SYMBOL_B}_Close"]),
                "spread": float(r["spread"]),
                "z_score": float(r["z_score"]),
                "model": "zscore",
            }
            ops.append(UpdateOne({"timestamp": ts}, {"$set": doc}, upsert=True))
        if ops:
            res = database.spread_data.bulk_write(ops, ordered=False)
            return (res.upserted_count or 0) + (res.modified_count or 0)
        return 0

    def latest(self) -> Optional[Dict[str, Any]]:
        return db().spread_data.find_one(sort=[("timestamp", -1)])

# ---- Model #2: Engle–Granger cointegration (rolling OLS + ADF) --------------
class EngleGrangerModel(BaseModel):
    metric_name = "resid_z"

    def __init__(self, window: int = EG_WINDOW_MIN, adf_pval_max: float = ADF_PVAL_MAX):
        self.window = window
        self.adf_pval_max = adf_pval_max

    @staticmethod
    def _ols_alpha_beta(a: np.ndarray, b: np.ndarray) -> Tuple[float, float]:
        # OLS with intercept: A = alpha + beta*B + eps
        X = sm.add_constant(b)  # [1, B]
        model = sm.OLS(a, X, missing="drop")
        res = model.fit()
        alpha, beta = float(res.params[0]), float(res.params[1])
        return alpha, beta

    def backfill(self) -> int:
        database = db()
        a_docs = list(database.symbol_price_data.find({"symbol": SYMBOL_A}))
        b_docs = list(database.symbol_price_data.find({"symbol": SYMBOL_B}))
        if not a_docs or not b_docs: return 0

        adf_df = _normalize_close(pd.DataFrame(a_docs), SYMBOL_A)
        bdf_df = _normalize_close(pd.DataFrame(b_docs), SYMBOL_B)
        merged = _merge_prices(adf_df, bdf_df)
        if len(merged) < self.window: return 0

        A = merged[f"{SYMBOL_A}_Close"].to_numpy(float)
        B = merged[f"{SYMBOL_B}_Close"].to_numpy(float)

        # Rolling OLS hedge ratio (alpha/beta) over the window
        alpha_arr = np.full(len(A), np.nan)
        beta_arr  = np.full(len(A), np.nan)
        for i in range(self.window - 1, len(A)):
            a_seg = A[i - self.window + 1 : i + 1]
            b_seg = B[i - self.window + 1 : i + 1]
            try:
                alpha, beta = self._ols_alpha_beta(a_seg, b_seg)
            except Exception:
                alpha, beta = np.nan, np.nan
            alpha_arr[i] = alpha
            beta_arr[i]  = beta

        resid = A - (alpha_arr + beta_arr * B)

        # Rolling standardization -> resid_z
        resid_s = pd.Series(resid)
        mu = resid_s.rolling(self.window).mean()
        sd = resid_s.rolling(self.window).std()
        resid_z = (resid_s - mu) / sd

        # ADF on the latest window of residuals (cheap)
        last_win = resid_s.iloc[-self.window:].dropna().to_numpy()
        try:
            _, adf_pval_now, *_ = adfuller(last_win, maxlag=1, autolag="AIC")
            adf_pval_now = float(adf_pval_now)
        except Exception:
            adf_pval_now = 1.0

        ops = []
        for i in range(len(merged)):
            rz = float(resid_z.iloc[i]) if np.isfinite(resid_z.iloc[i]) else None
            if rz is None:
                continue
            ts = _tz(pd.Timestamp(merged["Date"].iloc[i]).to_pydatetime())
            doc = {
                "timestamp": ts,
                "aapl_price": float(A[i]),
                "msft_price": float(B[i]),
                "hedge_alpha": float(alpha_arr[i]) if np.isfinite(alpha_arr[i]) else None,
                "hedge_beta":  float(beta_arr[i])  if np.isfinite(beta_arr[i])  else None,
                "resid": float(resid[i]) if np.isfinite(resid[i]) else None,
                "resid_z": rz,
                "adf_pval": adf_pval_now if i == len(merged) - 1 else None,
                "model": "eg_ci",
            }
            ops.append(UpdateOne({"timestamp": ts}, {"$set": doc}, upsert=True))

        if ops:
            res = database.spread_data.bulk_write(ops, ordered=False)
            return (res.upserted_count or 0) + (res.modified_count or 0)
        return 0

    def latest(self) -> Optional[Dict[str, Any]]:
        return db().spread_data.find_one(sort=[("timestamp", -1)])

# Model registry
MODEL_REGISTRY: Dict[str, BaseModel] = {
    "zscore": ZScoreSpreadModel(ROLLING_WINDOW_MIN),
    "eg_ci":  EngleGrangerModel(EG_WINDOW_MIN, ADF_PVAL_MAX),
}
def get_model(name: str) -> BaseModel:
    m = MODEL_REGISTRY.get(name)
    if m is None:
        raise ValueError(f"unknown model: {name}")
    return m

# =========================
# STRATEGIES
# =========================
@dataclass
class BaseStrategy:
    def action(self, metric_doc: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        raise NotImplementedError
    def allow_scale_in(self) -> bool: return SCALE_IN
    def shares(self) -> int: return SHARES_PER_TRADE

# Z-score strategy
@dataclass
class PairsZScoreStrategy(BaseStrategy):
    entry_z: float = ENTRY_Z
    exit_z:  float = EXIT_Z
    def action(self, d: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        z = d.get("z_score")
        if z is None: return (None, True)
        if z > self.entry_z:  return ("short_aapl_long_msft", True)
        if z < -self.entry_z: return ("long_aapl_short_msft", True)
        return (None, True)

# Cointegration strategy
@dataclass
class PairsCointegrationStrategy(BaseStrategy):
    adf_pval_max: float = ADF_PVAL_MAX
    entry_z: float = RESID_ENTRY_Z
    exit_z:  float = RESID_EXIT_Z
    def action(self, d: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        p = d.get("adf_pval", 1.0)
        rz = d.get("resid_z")
        if rz is None: return (None, True)
        # Only trade when cointegration holds (p <= threshold)
        if p <= self.adf_pval_max:
            if rz > self.entry_z:  return ("short_aapl_long_msft", True)
            if rz < -self.entry_z: return ("long_aapl_short_msft", True)
        return (None, True)

STRATEGY_REGISTRY: Dict[str, BaseStrategy] = {
    "zscore":        PairsZScoreStrategy(),
    "cointegration": PairsCointegrationStrategy(),
}
def get_strategy(name: str) -> BaseStrategy:
    s = STRATEGY_REGISTRY.get(name)
    if s is None:
        raise ValueError(f"unknown strategy: {name}")
    return s

# =========================
# TRADING PIPE
# =========================
def _close_trades(database, opens: List[Dict[str, Any]], a: float, m: float, z_like: float, t: datetime):
    total = 0.0
    for tr in opens:
        sh = tr.get("shares", 1)
        if tr["action"] == "long_aapl_short_msft":
            pnl = (a - tr["aapl_price"]) - (m - tr["msft_price"])
        else:
            pnl = (m - tr["msft_price"]) - (a - tr["aapl_price"])
        pnl *= sh; total += pnl
        database.trades.update_one({"_id": tr["_id"]}, {"$set": {
            "status":"closed","pnl":pnl,"close_timestamp":t,
            "close_aapl_price":a,"close_msft_price":m,"close_z_like":z_like
        }})
    log.info("[close] count=%d pnl=%.2f", len(opens), total)

def evaluate_and_trade(strategy: BaseStrategy, model: BaseModel) -> str:
    database = db()
    d = model.latest()
    if not d: return "no metric"

    a = d.get("aapl_price"); m = d.get("msft_price")
    z_like = d.get("z_score", d.get("resid_z"))
    t = d.get("timestamp");  spread = d.get("spread", d.get("resid"))
    entry_side, should_try_close = strategy.action(d)

    # Close logic (on reversion)
    if should_try_close:
        for side in ("long_aapl_short_msft", "short_aapl_long_msft"):
            opens = list(database.trades.find({"status":"open","action":side}))
            if not opens: continue
            if isinstance(strategy, PairsZScoreStrategy):
                z = d.get("z_score", 0.0)
                do_close = (side=="long_aapl_short_msft" and z>=strategy.exit_z) or (side=="short_aapl_long_msft" and z<=-strategy.exit_z)
            else:
                rz = d.get("resid_z", 0.0)
                do_close = (side=="long_aapl_short_msft" and rz>=strategy.exit_z) or (side=="short_aapl_long_msft" and rz<=-strategy.exit_z)
            if do_close and a is not None and m is not None:
                _close_trades(database, opens, a, m, z_like, t)

    if entry_side is None:
        return "no entry"

    # Scale-in guard
    same_side = database.trades.find_one({"status":"open","action":entry_side})
    if same_side and not strategy.allow_scale_in():
        return "open exists (no scale)"

    database.trades.insert_one({
        "timestamp": t,
        "action": entry_side,
        "aapl_price": a,
        "msft_price": m,
        "spread": spread,
        "z_like": z_like,
        "model": d.get("model"),
        "status": "open",
        "shares": strategy.shares()
    })
    return f"entered {entry_side}"

# =========================
# CLEANUP
# =========================
def cleanup(days: int = LOOKBACK_DAYS) -> Dict[str,int]:
    database = db()
    cutoff = _now() - timedelta(days=days)
    a = database.symbol_price_data.delete_many({"symbol":SYMBOL_A, "Date":{"$lt":cutoff}}).deleted_count
    b = database.symbol_price_data.delete_many({"symbol":SYMBOL_B, "Date":{"$lt":cutoff}}).deleted_count
    s = database.spread_data.delete_many({"timestamp":{"$lt":cutoff}}).deleted_count
    return {"aapl":a,"msft":b,"spread":s}

# =========================
# CELERY TASKS
# =========================
@app.task(name="celery_worker.tasks.fetch_and_store_prices_task")
def fetch_and_store_prices_task(symbols: Optional[List[str]] = None):
    n = fetch_new_prices(symbols or SYMBOLS)
    return {"upserted": n}

@app.task(name="celery_worker.tasks.model_backfill_task")
def model_backfill_task(model_name: str = MODEL_NAME):
    model = get_model(model_name)
    n = model.backfill()
    return {"model": model_name, "upserted": n}

@app.task(name="celery_worker.tasks.evaluate_and_place_trade_task")
def evaluate_and_place_trade_task(model_name: str = MODEL_NAME, strategy_name: str = STRATEGY_NAME):
    model = get_model(model_name)
    strat = get_strategy(strategy_name)
    res = evaluate_and_trade(strat, model)
    return {"model": model_name, "strategy": strategy_name, "result": res}

@app.task(name="celery_worker.tasks.cleanup_old_data_task")
def cleanup_old_data_task(days: int = LOOKBACK_DAYS):
    return cleanup(days)

@app.task(name="celery_worker.tasks.trigger_chain")
def trigger_chain(model_name: str = MODEL_NAME, strategy_name: str = STRATEGY_NAME):
    """
    Full pipeline: fetch -> backfill(model) -> evaluate(strategy) -> (optional cleanup)
    """
    sig = chain(
        fetch_and_store_prices_task.si(SYMBOLS),
        model_backfill_task.si(model_name),
        evaluate_and_place_trade_task.si(model_name, strategy_name),
    )
    if CLEANUP_ON_CYCLE:
        sig = sig | cleanup_old_data_task.si(LOOKBACK_DAYS)
    r = sig.apply_async()
    return {"task_id": r.id, "model": model_name, "strategy": strategy_name}

# =========================
# BEAT SCHEDULE (ONLY ONE ACTIVE)
# =========================
# If CYCLE_SECONDS == 0, nothing is scheduled (manual / API-triggered only).
if CYCLE_SECONDS > 0:
    app.conf.beat_schedule = {
        "single-active-cycle": {
            "task": "celery_worker.tasks.trigger_chain",
            "schedule": float(CYCLE_SECONDS),
            "kwargs": {"model_name": MODEL_NAME, "strategy_name": STRATEGY_NAME},
        }
    }
    log.info("[beat] scheduling '%s' every %ss with model=%s strategy=%s",
             "single-active-cycle", CYCLE_SECONDS, MODEL_NAME, STRATEGY_NAME)
else:
    app.conf.beat_schedule = {}
    log.info("[beat] disabled (CYCLE_SECONDS=0) — use manual triggers")
