# celery_worker/tasks.py
from __future__ import annotations
import logging, math
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import yfinance as yf
from celery import Celery, chain
from pymongo import MongoClient, UpdateOne, ASCENDING

# =========================
# CONFIG (edit these 2 to switch behavior)
# =========================
MODEL_NAME    = "zscore"        # "zscore" or "eg_ci"
STRATEGY_NAME = "zscore"        # "zscore" or "cointegration"

# Symbols / infra
SYMBOL_A = "AAPL"; SYMBOL_B = "MSFT"
SYMBOLS  = [SYMBOL_A, SYMBOL_B]
MONGO_URL = "mongodb://mongo:27017"
REDIS_URL = "redis://redis:6379/0"
DB_NAME   = "trading_db"

# Data policy
INTERVAL = "1m"
LOOKBACK_DAYS = 7
SKIP_TODAY_CANDLES = False

# Z-score defaults
ROLLING_WINDOW_MIN = 1440    # ~1 day of minutes
ENTRY_Z = 1.0
EXIT_Z  = 0.0

# Cointegration defaults
EG_WINDOW_MIN = 1440         # rolling window for OLS hedge ratio & residuals
ADF_PVAL_MAX  = 0.10         # consider cointegrated if p<=0.10
RESID_ENTRY_Z = 1.0
RESID_EXIT_Z  = 0.0

SCALE_IN = True
SHARES_PER_TRADE = 1
CLEANUP_ON_CYCLE = True

# =========================
# Boilerplate
# =========================
app = Celery("tasks", broker=REDIS_URL)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

_mongo = MongoClient(MONGO_URL)
_db = _mongo[DB_NAME]
def db(): return _db
_db.symbol_price_data.create_index([("symbol", ASCENDING), ("Date", ASCENDING)], unique=True)
_db.spread_data.create_index([("timestamp", ASCENDING)], unique=True)
_db.trades.create_index([("status", ASCENDING), ("timestamp", ASCENDING)])

def _now() -> datetime: return datetime.now(timezone.utc)
def _max_lookback() -> datetime: return _now() - timedelta(days=LOOKBACK_DAYS)
def _tz(dt: datetime) -> datetime: return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def _normalize_close(df: pd.DataFrame, sym: str) -> pd.DataFrame:
    df = df.copy(); df["Date"] = pd.to_datetime(df["Date"])
    col = f"{sym}_Close" if f"{sym}_Close" in df.columns else ("Close" if "Close" in df.columns else None)
    if not col:
        guess = df.filter(like="Close").columns
        if not len(guess): raise ValueError(f"no close for {sym}")
        col = guess[0]
    return df[["Date", col]].rename(columns={col: f"{sym}_Close"}).sort_values("Date").reset_index(drop=True)

def _merge_prices(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(a, b, on="Date", how="inner").sort_values("Date").reset_index(drop=True)

# =========================
# DATA FETCH
# =========================
def fetch_new_prices(symbols: List[str]) -> int:
    database = db()
    today = _now(); max_lb = _max_lookback()
    total = 0
    for symbol in symbols:
        latest = database.symbol_price_data.find_one({"symbol": symbol}, sort=[("Date", -1)])
        start_dt = max_lb
        if latest and isinstance(latest.get("Date"), datetime):
            start_dt = max(latest["Date"] + timedelta(minutes=1), max_lb)
        if start_dt >= today:
            continue
        df = yf.download(symbol, start=start_dt, end=today, interval=INTERVAL, auto_adjust=False)
        if df.empty: continue
        df = df.reset_index()
        df.columns = ["Date", f"{symbol}_Open", f"{symbol}_High", f"{symbol}_Low",
                      f"{symbol}_Close", f"{symbol}_Adj Close", f"{symbol}_Volume"]
        df["symbol"] = symbol
        ops = []
        for _, r in df.iterrows():
            dt = r["Date"]; 
            if not isinstance(dt, datetime): continue
            dt = _tz(dt)
            if dt < max_lb: continue
            if SKIP_TODAY_CANDLES and dt.date()==today.date(): continue
            d = r.to_dict(); d["Date"]=dt
            ops.append(UpdateOne({"symbol": symbol, "Date": dt}, {"$set": d}, upsert=True))
        if ops:
            res = database.symbol_price_data.bulk_write(ops, ordered=False)
            total += res.upserted_count + res.modified_count
    return total

# =========================
# MODEL INTERFACE
# =========================
class BaseModel:
    """
    Converts price history -> per-timestamp metric document(s) stored in spread_data.
    Must implement:
      - backfill() : recompute whole history (windowed) and upsert docs
      - latest()   : return last metric doc (dict) for strategy consumption
    """
    metric_name: str = "z_score"  # primary metric key produced
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
            return res.upserted_count + res.modified_count
        return 0

    def latest(self) -> Optional[Dict[str, Any]]:
        return db().spread_data.find_one(sort=[("timestamp", -1)])

# ---- Model #2: Engle–Granger cointegration (rolling) ------------------------
# - rolling OLS: A_t ~ beta * B_t (+ intercept off for “hedge ratio” style)
# - residuals r_t = A_t - beta*B_t
# - standardize residuals over the same window -> resid_z
# - (Optional) run a simple ADF on residuals; here we compute a quick p-value proxy
#   to avoid heavy deps; if you already have statsmodels, you can plug in ADF directly.
class EngleGrangerModel(BaseModel):
    metric_name = "resid_z"

    def __init__(self, window: int = EG_WINDOW_MIN, adf_pval_max: float = ADF_PVAL_MAX):
        self.window = window
        self.adf_pval_max = adf_pval_max

    @staticmethod
    def _rolling_beta(a: np.ndarray, b: np.ndarray, w: int) -> np.ndarray:
        # beta_t = cov(a,b)/var(b) over rolling window
        beta = np.full_like(a, np.nan, dtype=float)
        for i in range(w-1, len(a)):
            bseg = b[i-w+1:i+1]; aseg = a[i-w+1:i+1]
            vb = np.var(bseg, ddof=1)
            if vb == 0: 
                beta[i] = np.nan
            else:
                beta[i] = np.cov(aseg, bseg, ddof=1)[0,1] / vb
        return beta

    @staticmethod
    def _adf_proxy_pvalue(series: np.ndarray) -> float:
        # A tiny heuristic proxy (NOT a real ADF): larger |rho-1| => smaller p
        # You can drop-in statsmodels.tsa.stattools.adfuller for real ADF if available.
        if len(series) < 5: return 1.0
        x = np.asarray(series, float)
        x = x - np.nanmean(x)
        x1, x2 = x[:-1], x[1:]
        denom = np.sum(x1*x1)
        if denom == 0: return 1.0
        rho = np.sum(x1*x2) / denom
        # map rho in (0.8..1.05) -> p in (0.2..1.0) roughly
        p = min(1.0, max(0.0, (rho - 0.8) / (1.05 - 0.8)))
        return p

    def backfill(self) -> int:
        database = db()
        a = list(database.symbol_price_data.find({"symbol": SYMBOL_A}))
        b = list(database.symbol_price_data.find({"symbol": SYMBOL_B}))
        if not a or not b: return 0
        adf = _normalize_close(pd.DataFrame(a), SYMBOL_A)
        bdf = _normalize_close(pd.DataFrame(b), SYMBOL_B)
        merged = _merge_prices(adf, bdf)
        if len(merged) < self.window: return 0

        A = merged[f"{SYMBOL_A}_Close"].to_numpy(float)
        B = merged[f"{SYMBOL_B}_Close"].to_numpy(float)

        beta = self._rolling_beta(A, B, self.window)
        resid = A - beta * B
        # standardize residuals (rolling)
        resid_s = pd.Series(resid).rolling(self.window)
        resid_mu = resid_s.mean().to_numpy()
        resid_sd = resid_s.std().to_numpy()
        resid_z = (resid - resid_mu) / resid_sd

        ops = []
        for i in range(len(merged)):
            if not math.isfinite(resid_z[i]): 
                continue
            ts = _tz(pd.Timestamp(merged["Date"].iloc[i]).to_pydatetime())
            pval = self._adf_proxy_pvalue(resid[max(0, i-self.window+1):i+1])
            doc = {
                "timestamp": ts,
                "aapl_price": float(A[i]),
                "msft_price": float(B[i]),
                "hedge_beta": float(beta[i]) if math.isfinite(beta[i]) else None,
                "resid": float(resid[i]),
                "resid_z": float(resid_z[i]),
                "adf_pval": float(pval),
                "model": "eg_ci",
            }
            ops.append(UpdateOne({"timestamp": ts}, {"$set": doc}, upsert=True))
        if ops:
            res = database.spread_data.bulk_write(ops, ordered=False)
            return res.upserted_count + res.modified_count
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
    if m is None: raise ValueError(f"unknown model: {name}")
    return m

# =========================
# STRATEGY INTERFACE
# =========================
@dataclass
class BaseStrategy:
    def action(self, metric_doc: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        """
        Return (entry_side, should_try_close_existing) where entry_side is:
          'short_aapl_long_msft' | 'long_aapl_short_msft' | None
        """
        raise NotImplementedError

    def allow_scale_in(self) -> bool: return True
    def shares(self) -> int: return SHARES_PER_TRADE

# Z-score strategy (original)
@dataclass
class PairsZScoreStrategy(BaseStrategy):
    entry_z: float = ENTRY_Z
    exit_z: float  = EXIT_Z
    scale_in_flag: bool = SCALE_IN

    def action(self, d: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        z = d.get("z_score")
        if z is None: return (None, True)
        if z > self.entry_z:  return ("short_aapl_long_msft", True)
        if z < -self.entry_z: return ("long_aapl_short_msft", True)
        # no new entry – but consider closing if reverted
        return (None, True)

    def allow_scale_in(self) -> bool: return self.scale_in_flag

# Cointegration strategy
@dataclass
class PairsCointegrationStrategy(BaseStrategy):
    adf_pval_max: float = ADF_PVAL_MAX
    entry_z: float = RESID_ENTRY_Z
    exit_z:  float = RESID_EXIT_Z
    scale_in_flag: bool = SCALE_IN

    def action(self, d: Dict[str, Any]) -> Tuple[Optional[str], bool]:
        p = d.get("adf_pval", 1.0)
        rz = d.get("resid_z")
        if rz is None: return (None, True)
        # only trade when cointegration is “on”
        if p <= self.adf_pval_max:
            if rz > self.entry_z:  return ("short_aapl_long_msft", True)
            if rz < -self.entry_z: return ("long_aapl_short_msft", True)
        return (None, True)

    def allow_scale_in(self) -> bool: return self.scale_in_flag

STRATEGY_REGISTRY: Dict[str, BaseStrategy] = {
    "zscore":       PairsZScoreStrategy(),
    "cointegration":PairsCointegrationStrategy(),
}

def get_strategy(name: str) -> BaseStrategy:
    s = STRATEGY_REGISTRY.get(name)
    if s is None: raise ValueError(f"unknown strategy: {name}")
    return s

# =========================
# TRADING PIPE
# =========================
def evaluate_and_trade(strategy: BaseStrategy, model: BaseModel) -> str:
    database = db()
    d = model.latest()
    if not d: return "no metric"

    a = d.get("aapl_price"); m = d.get("msft_price"); z_like = d.get("z_score", d.get("resid_z"))
    t = d.get("timestamp");  spread = d.get("spread", d.get("resid"))
    entry_side, should_try_close = strategy.action(d)

    # Close logic (reversion thresholds)
    if should_try_close:
        for side in ("long_aapl_short_msft", "short_aapl_long_msft"):
            opens = list(database.trades.find({"status":"open","action":side}))
            if not opens: continue
            if isinstance(strategy, PairsZScoreStrategy):
                z = d.get("z_score", 0.0)
                close = (side=="long_aapl_short_msft" and z>=strategy.exit_z) or (side=="short_aapl_long_msft" and z<=-strategy.exit_z)
            else:
                rz = d.get("resid_z", 0.0)
                close = (side=="long_aapl_short_msft" and rz>=strategy.exit_z) or (side=="short_aapl_long_msft" and rz<=-strategy.exit_z)
            if close and a is not None and m is not None:
                _close_trades(database, opens, a, m, z_like, t)

    if entry_side is None:
        return "no entry"

    # Scale-in control
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

# =========================
# CLEANUP
# =========================
def cleanup(days: int = LOOKBACK_DAYS) -> Dict[str,int]:
    database = db()
    cutoff = _now() - timedelta(days=days)
    a = database.symbol_price_data.delete_many({"symbol":SYMBOL_A,"Date":{"$lt":cutoff}}).deleted_count
    b = database.symbol_price_data.delete_many({"symbol":SYMBOL_B,"Date":{"$lt":cutoff}}).deleted_count
    s = database.spread_data.delete_many({"timestamp":{"$lt":cutoff}}).deleted_count
    return {"aapl":a,"msft":b,"spread":s}

# =========================
# CELERY TASKS
# =========================
@app.task
def fetch_and_store_prices_task(symbols: Optional[List[str]] = None):
    n = fetch_new_prices(symbols or SYMBOLS)
    return {"upserted": n}

@app.task
def model_backfill_task(model_name: str = MODEL_NAME):
    model = get_model(model_name)
    n = model.backfill()
    return {"model": model_name, "upserted": n}

@app.task
def evaluate_and_place_trade_task(model_name: str = MODEL_NAME, strategy_name: str = STRATEGY_NAME):
    model = get_model(model_name); strat = get_strategy(strategy_name)
    res = evaluate_and_trade(strat, model)
    return {"model": model_name, "strategy": strategy_name, "result": res}

@app.task
def cleanup_old_data_task(days: int = LOOKBACK_DAYS):
    return cleanup(days)

@app.task
def trigger_chain(model_name: str = MODEL_NAME, strategy_name: str = STRATEGY_NAME):
    sig = chain(
        fetch_and_store_prices_task.si(SYMBOLS),
        model_backfill_task.si(model_name),
        evaluate_and_place_trade_task.si(model_name, strategy_name),
    )
    if CLEANUP_ON_CYCLE:
        sig = sig | cleanup_old_data_task.si(LOOKBACK_DAYS)
    r = sig.apply_async()
    return {"task_id": r.id, "model": model_name, "strategy": strategy_name}
