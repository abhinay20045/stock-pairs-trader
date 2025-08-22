import { useEffect, useMemo, useState, useCallback, useRef } from "react";
import { Card, CardContent } from "./components/Card";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  LineElement,
  CategoryScale,
  LinearScale,
  PointElement,
  Tooltip,
  Legend
} from "chart.js";

ChartJS.register(LineElement, CategoryScale, LinearScale, PointElement, Tooltip, Legend);

// ---------- helpers ----------
const parseTS = (t) => new Date(t);
const avg = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null);

const intervalToMs = (interval) => {
  switch (interval) {
    case "1m": return 60 * 1000;
    case "5m": return 5 * 60 * 1000;
    case "1h": return 60 * 60 * 1000;
    case "1d":
    default: return 24 * 60 * 60 * 1000;
  }
};

// Aggregate [{timestamp,value}] into fixed bins (ms).
// mode: "avg" (default) or "last" (for cumulative series).
// startAtMs aligns bins to a fixed start so charts don't jitter.
function aggregateSeries(points, binMs, mode = "avg", startAtMs) {
  if (!points || points.length === 0) return [];
  const out = [];
  const sorted = [...points].sort((a, b) => parseTS(a.timestamp) - parseTS(b.timestamp));

  let binStart = startAtMs ?? Math.floor(parseTS(sorted[0].timestamp).getTime() / binMs) * binMs;
  let binEnd = binStart + binMs;
  let bucket = [];

  const flush = () => {
    if (!bucket.length) return;
    const ts = new Date(binEnd - 1).toISOString();
    const v = mode === "last" ? bucket[bucket.length - 1] : avg(bucket);
    out.push({ timestamp: ts, value: v });
    bucket = [];
  };

  for (const p of sorted) {
    const t = parseTS(p.timestamp).getTime();
    if (t < binStart) continue;
    while (t >= binEnd) {
      flush();
      binStart = binEnd;
      binEnd += binMs;
    }
    bucket.push(p.value);
  }
  flush();
  return out;
}

// ---- Timezone helpers (US/Eastern) ----
function getETParts(ms) {
  const d = new Date(ms);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  }).formatToParts(d);
  const pick = (t) => parseInt(parts.find((p) => p.type === t).value, 10);
  return { y: pick("year"), m: pick("month"), d: pick("day"), h: pick("hour"), min: pick("minute") };
}

function getETOffsetMinutesForYMD(y, m, d) {
  const probe = new Date(Date.UTC(y, m - 1, d, 12, 0, 0));
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      timeZoneName: "shortOffset"
    }).formatToParts(probe);
    const tzn = parts.find((p) => p.type === "timeZoneName")?.value || "GMT-5";
    const mMatch = tzn.match(/GMT([+-]\d{1,2})(?::(\d{2}))?/);
    if (mMatch) {
      const sign = mMatch[1].startsWith("-") ? -1 : 1;
      const hours = Math.abs(parseInt(mMatch[1], 10));
      const minutes = mMatch[2] ? parseInt(mMatch[2], 10) : 0;
      return sign * (hours * 60 + minutes); // e.g. -240 for EDT
    }
  } catch {}
  return -300; // fallback to EST
}

function etWallTimeToUtcMs(y, m, d, h = 0, min = 0) {
  const offsetMin = getETOffsetMinutesForYMD(y, m, d); // ET = UTC + offset
  return Date.UTC(y, m - 1, d, h, min, 0) - offsetMin * 60_000;
}

function getEtDaySessionRangeUtc(anchorMs) {
  const { y, m, d } = getETParts(anchorMs);
  const openUtc = etWallTimeToUtcMs(y, m, d, 9, 30);
  const closeUtc = etWallTimeToUtcMs(y, m, d, 16, 0);
  return { start: openUtc, end: closeUtc };
}

function getEtHourRangeUtc(anchorMs) {
  const { y, m, d, h } = getETParts(anchorMs);
  const start = etWallTimeToUtcMs(y, m, d, h, 0);
  return { start, end: start + 60 * 60 * 1000 };
}

const fmtLabel = (ts, timeframe) => {
  const d = new Date(ts);
  if (timeframe === "hour") return d.toLocaleTimeString();
  if (timeframe === "day")
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return (
    d.toLocaleDateString([], { month: "2-digit", day: "2-digit" }) +
    " " +
    d.toLocaleTimeString([], { hour: "2-digit" })
  );
};

// ---------- defaults used when launching a backtest ----------
const DEFAULT_BT_PARAMS = {
  symbols: ["AAPL", "MSFT"],
  start: "2023-01-01",
  interval: "1d",
  lookback: 60,
  entry_z: 2.0,
  exit_z: 0.5,
  notional_per_leg: 1000,
  fee_bps: 1.0
};

// Utility: nearest index in aggregated points by timestamp (ms)
function nearestIndexByTime(aggPoints, targetMs) {
  if (!aggPoints || aggPoints.length === 0) return -1;
  let best = -1, bestDiff = Infinity;
  for (let i = 0; i < aggPoints.length; i++) {
    const ms = new Date(aggPoints[i].timestamp).getTime();
    const d = Math.abs(ms - targetMs);
    if (d < bestDiff) { best = i; bestDiff = d; }
  }
  return best;
}

export default function Dashboard() {
  // Mode
  const [mode, setMode] = useState("live"); // 'live' | 'backtest'

  // ----- Strategy switching (frontend-only) -----
  const STRATEGIES = useMemo(
    () => [
      { id: "pairs_basic", label: "Pairs: Basic" },
      { id: "pairs_kalman", label: "Pairs: Kalman" },
      { id: "pairs_ml", label: "Pairs: ML" }
    ],
    []
  );
  const [strategy, setStrategy] = useState(STRATEGIES[0].id);

  // Live data
  const [liveTrades, setLiveTrades] = useState(null);
  const [livePrices, setLivePrices] = useState(null);   // {AAPL:[{timestamp,close}], MSFT:[...]}
  const [liveZ, setLiveZ] = useState(null);             // [{timestamp,z_score}]
  const [livePnl, setLivePnl] = useState(null);         // [{timestamp,cumulative_pnl}]

  // Backtest data + state
  const [run, setRun] = useState(null);                 // latest backtest run meta
  const [btTrades, setBtTrades] = useState(null);
  const [btPnl, setBtPnl] = useState(null);             // [{timestamp,cumulative_pnl}]
  const [btSeries, setBtSeries] = useState(null);       // [{timestamp,z,p1,p2,beta}]

  // Backtest lifecycle state
  const [btPhase, setBtPhase] = useState("idle");       // 'idle'|'starting'|'waiting'|'streaming'|'error'
  const [btMsg, setBtMsg] = useState("");
  const launchEpochRef = useRef(null);                  // when the user clicked Backtest
  const pollRunTimerRef = useRef(null);
  const pollDataTimerRef = useRef(null);

  // Live timeframe selector
  const [timeframe, setTimeframe] = useState("week"); // 'week' | 'day' | 'hour'

  // ------- fetchers -------
  // LIVE (poll every 5s)
  useEffect(() => {
    if (mode !== "live") return;
    let stop = false;
    async function fetchLive() {
      try {
        // include strategy param (non-breaking if backend ignores it)
        const qs = `strategy=${encodeURIComponent(strategy)}`;
        const [tradeRes, priceRes, zscoreRes, pnlRes] = await Promise.all([
          fetch(`http://localhost:5050/trade-history?${qs}`),
          fetch(`http://localhost:5050/prices?symbols=AAPL,MSFT&limit=50000&${qs}`),
          fetch(`http://localhost:5050/stock-zscores?${qs}`),
          fetch(`http://localhost:5050/pnl-history?${qs}`)
        ]);
        if (stop) return;
        const [tradeJson, priceJson, zscoreJson, pnlJson] = await Promise.all([
          tradeRes.json(),
          priceRes.json(),
          zscoreRes.json(),
          pnlRes.json()
        ]);
        setLiveTrades(tradeJson);
        setLivePrices(priceJson);
        setLiveZ(zscoreJson);
        setLivePnl(pnlJson);
      } catch (e) {
        console.error("Live fetch error:", e);
      }
    }
    fetchLive();
    const id = setInterval(fetchLive, 5000);
    return () => { stop = true; clearInterval(id); };
  }, [mode, strategy]); // <- re-fetch when strategy changes

  // ------------------------ BACKTEST: launch + polling ------------------------

  // Helper: fetch JSON (throws if !ok)
  const getJSON = useCallback(async (url, init) => {
    const res = await fetch(url, init);
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText} at ${url}${body ? ` — ${body}` : ""}`);
    }
    return res.json();
  }, []);

  // Start a new backtest then begin waiting for the new run to appear
  const handleBacktestClick = useCallback(async () => {
    if (mode === "backtest" && (btPhase === "starting" || btPhase === "waiting")) return;
    // switch mode first so UI shows the backtest area
    setMode("backtest");
    setBtPhase("starting");
    setBtMsg("Starting backtest…");

    // mark the time of launch so we can identify the new run
    launchEpochRef.current = Date.now();

    try {
      await fetch("http://localhost:5050/backtest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...DEFAULT_BT_PARAMS,
          strategy // <-- include chosen strategy
        })
      });
      setBtPhase("waiting");
      setBtMsg("Backtest started. Preparing data…");
    } catch (e) {
      console.error(e);
      setBtPhase("error");
      setBtMsg("Failed to start backtest.");
    }
  }, [mode, btPhase, strategy]);

  // Poll for the latest run that started after we clicked Backtest.
  useEffect(() => {
    if (mode !== "backtest") return;
    if (btPhase !== "waiting") return;

    // Clear any stale timer
    if (pollRunTimerRef.current) {
      clearInterval(pollRunTimerRef.current);
      pollRunTimerRef.current = null;
    }

    const launchedAfter = launchEpochRef.current ?? 0;
    const fudge = 2000; // ms allow small clock skew in started_at comparison

    async function checkLatestRun() {
      try {
        const res = await fetch("http://localhost:5050/backtest/latest-run");
        if (!res.ok) return; // still 404; keep waiting
        const runJson = await res.json();

        // If this run is new enough, adopt it and move to streaming
        const startedAt = runJson?.started_at ? new Date(runJson.started_at).getTime() : 0;
        if (startedAt >= launchedAfter - fudge || !startedAt) {
          setRun(runJson);
          setBtPhase("streaming");
          setBtMsg("Streaming backtest results…");
        }
      } catch {
        // ignore & keep polling
      }
    }

    // first check immediately, then every 2s
    checkLatestRun();
    pollRunTimerRef.current = setInterval(checkLatestRun, 2000);

    return () => {
      if (pollRunTimerRef.current) {
        clearInterval(pollRunTimerRef.current);
        pollRunTimerRef.current = null;
      }
    };
  }, [mode, btPhase]);

  // With a chosen run, poll its data frequently until mode changes
  useEffect(() => {
    if (mode !== "backtest") return;
    if (btPhase !== "streaming") return;
    if (!run?.run_id) return;

    // Clear any prior data poller
    if (pollDataTimerRef.current) {
      clearInterval(pollDataTimerRef.current);
      pollDataTimerRef.current = null;
    }

    let stopped = false;

    async function pullAll() {
      try {
        const q = new URLSearchParams({ run_id: run.run_id });

        const [pnlRes, tradesRes, seriesRes, runRes] = await Promise.all([
          fetch(`http://localhost:5050/backtest/pnl?${q}`),
          fetch(`http://localhost:5050/backtest/trades?${q}`),
          fetch(`http://localhost:5050/backtest/series?${q}`),
          fetch(`http://localhost:5050/backtest/latest-run`)
        ]);

        const [pnlJson, tradesJson, seriesJson, latestRun] = await Promise.all([
          pnlRes.json(),
          tradesRes.json(),
          seriesRes.json(),
          runRes.ok ? runRes.json() : Promise.resolve(run)
        ]);

        if (stopped) return;

        // normalize
        const pnlPoints = (pnlJson.points || []).map((p) => ({
          timestamp: p.t,
          cumulative_pnl: p.pnl
        }));
        const seriesPoints = (seriesJson.points || []).map((p) => ({
          timestamp: p.t,
          z: p.z,
          p1: p.p1 ?? p.price1,
          p2: p.p2 ?? p.price2,
          beta: p.beta
        }));

        setRun(latestRun || run);
        setBtPnl(pnlPoints);
        setBtTrades(tradesJson.trades || []);
        setBtSeries(seriesPoints);
      } catch (e) {
        console.error("Backtest polling error:", e);
      }
    }

    // initial pull + poll every 3s
    pullAll();
    pollDataTimerRef.current = setInterval(pullAll, 3000);

    return () => {
      stopped = true;
      if (pollDataTimerRef.current) {
        clearInterval(pollDataTimerRef.current);
        pollDataTimerRef.current = null;
      }
    };
  }, [mode, btPhase, run?.run_id]);

  // Reset backtest state when leaving backtest mode
  useEffect(() => {
    if (mode !== "live") return;
    setBtPhase("idle");
    setBtMsg("");
    setRun(null);
    setBtTrades(null);
    setBtPnl(null);
    setBtSeries(null);
    if (pollRunTimerRef.current) clearInterval(pollRunTimerRef.current);
    if (pollDataTimerRef.current) clearInterval(pollDataTimerRef.current);
    pollRunTimerRef.current = null;
    pollDataTimerRef.current = null;
  }, [mode]);

  // ---------- pick the active datasets ----------
  const activePrices = useMemo(() => {
    if (mode === "live") return livePrices || {};
    if (btSeries) {
      return {
        AAPL: btSeries.map((r) => ({ timestamp: r.timestamp, close: r.p1 })),
        MSFT: btSeries.map((r) => ({ timestamp: r.timestamp, close: r.p2 }))
      };
    }
    return null;
  }, [mode, livePrices, btSeries]);

  const activeZ = useMemo(() => {
    if (mode === "live") return liveZ || [];
    if (btSeries) return btSeries.map((r) => ({ timestamp: r.timestamp, z_score: r.z }));
    return [];
  }, [mode, liveZ, btSeries]);

  const activePnl = useMemo(() => {
    if (mode === "live") return livePnl || [];
    return btPnl || [];
  }, [mode, livePnl, btPnl]);

  // ---------- timestamps present in active data ----------
  const allTimestamps = useMemo(() => {
    const ts = [];
    (activeZ ?? []).forEach((d) => ts.push(parseTS(d.timestamp).getTime()));
    (activePnl ?? []).forEach((d) => ts.push(parseTS(d.timestamp).getTime()));
    if (activePrices) {
      (activePrices.AAPL ?? []).forEach((p) => ts.push(parseTS(p.timestamp).getTime()));
      (activePrices.MSFT ?? []).forEach((p) => ts.push(parseTS(p.timestamp).getTime()));
    }
    return ts.sort((a, b) => a - b);
  }, [activeZ, activePnl, activePrices]);

  const lastDataMs = useMemo(
    () => (allTimestamps.length ? allTimestamps[allTimestamps.length - 1] : Date.now()),
    [allTimestamps]
  );

  const hasDataBetween = useCallback(
    (start, end) => allTimestamps.some((t) => t >= start && t <= end),
    [allTimestamps]
  );

  // ---------- compute visible window + binning ----------
  const { windowStart, windowEnd, binMs, maxTicks } = useMemo(() => {
    // Backtest: lock to tested range and use the run's interval
    if (mode === "backtest" && run?.params) {
      const start = parseTS(run.params.start).getTime();
      const end = parseTS(run.params.end || run.finished_at || run.started_at || Date.now()).getTime();
      const bins = intervalToMs(run.params.interval || "1d");
      const spanDays = Math.max(1, Math.round((end - start) / (24 * 60 * 60 * 1000)));
      const ticks = Math.min(20, Math.max(8, Math.round(spanDays / 15)));
      return { windowStart: start, windowEnd: end, binMs: bins, maxTicks: ticks };
    }

    // Live (existing behavior)
    let start, end, bins, ticks;
    const nowMs = Date.now();

    if (timeframe === "day") {
      const today = getEtDaySessionRangeUtc(nowMs);
      const liveHas = hasDataBetween(today.start, today.end);
      if (liveHas) {
        start = today.start;
        end = Math.min(today.end, nowMs);
      } else {
        const last = getEtDaySessionRangeUtc(lastDataMs);
        start = last.start;
        end = Math.min(last.end, lastDataMs);
      }
      bins = 5 * 60 * 1000;
      ticks = 24;
    } else if (timeframe === "hour") {
      const currHr = getEtHourRangeUtc(nowMs);
      const liveHas = hasDataBetween(currHr.start, currHr.end);
      if (liveHas) {
        start = currHr.start;
        end = Math.min(currHr.end, nowMs);
      } else {
        const lastHr = getEtHourRangeUtc(lastDataMs);
        start = lastHr.start;
        end = Math.min(lastHr.end, lastDataMs);
      }
      bins = 60 * 1000;
      ticks = 12;
    } else {
      end = lastDataMs;
      start = end - 7 * 24 * 60 * 60 * 1000;
      bins = 30 * 60 * 1000;
      ticks = 14;
    }

    return { windowStart: start, windowEnd: end, binMs: bins, maxTicks: ticks };
  }, [mode, run, timeframe, lastDataMs, hasDataBetween]);

  const alignStart = useMemo(
    () => Math.floor(windowStart / binMs) * binMs,
    [windowStart, binMs]
  );

  // ---------- build series bounded by [windowStart, windowEnd] ----------
  const zRaw = useMemo(() => {
  // accept either an array or an object like { points: [...] }
  const src = Array.isArray(activeZ) ? activeZ : (activeZ?.points ?? []);
  return src
    .map((d) => {
      const t = new Date(d.timestamp).getTime();
      const v = Number(
        d.z_score ??
        d.resid_z ??
        d.z_like ??
        d.z ??
        (typeof d.value === "number" ? d.value : undefined)
      );
      return { t, v, ts: d.timestamp };
    })
    .filter((d) => Number.isFinite(d.t) && Number.isFinite(d.v))
    .filter((d) => d.t >= windowStart && d.t <= windowEnd)
    .map((d) => ({ timestamp: d.ts, value: d.v }));
}, [activeZ, windowStart, windowEnd]);

  const pnlRaw = useMemo(() => {
    const src = activePnl || [];
    return src
      .map((d) => ({
        t: parseTS(d.timestamp).getTime(),
        v: Number(d.cumulative_pnl ?? d.value ?? 0),
        ts: d.timestamp
      }))
      .filter((d) => d.t >= windowStart && d.t <= windowEnd)
      .map((d) => ({ timestamp: d.ts, value: d.v }));
  }, [activePnl, windowStart, windowEnd]);

  const aaplRaw = useMemo(() => {
    const src = (activePrices && activePrices.AAPL) || [];
    return src
      .map((p) => ({ t: parseTS(p.timestamp).getTime(), v: Number(p.close), ts: p.timestamp }))
      .filter((p) => p.t >= windowStart && p.t <= windowEnd)
      .map((p) => ({ timestamp: p.ts, value: p.v }));
  }, [activePrices, windowStart, windowEnd]);

  const msftRaw = useMemo(() => {
    const src = (activePrices && activePrices.MSFT) || [];
    return src
      .map((p) => ({ t: parseTS(p.timestamp).getTime(), v: Number(p.close), ts: p.timestamp }))
      .filter((p) => p.t >= windowStart && p.t <= windowEnd)
      .map((p) => ({ timestamp: p.ts, value: p.v }));
  }, [activePrices, windowStart, windowEnd]);

  // Aggregate (aligned to the visible window)
  const zAgg = useMemo(
    () => aggregateSeries(zRaw, binMs, "avg", alignStart),
    [zRaw, binMs, alignStart]
  );
  const pnlAgg = useMemo(
    () => aggregateSeries(pnlRaw, binMs, "last", alignStart),
    [pnlRaw, binMs, alignStart]
  );
  const pnlDisplayAgg = useMemo(() => {
    if (!pnlAgg || pnlAgg.length !== 1) return pnlAgg;

    const v = pnlAgg[0].value;
    // avoid duplicate timestamps; nudge by ±1ms if needed
    const startIso = new Date(windowStart + 1).toISOString();
    const endIso   = new Date(windowEnd - 1).toISOString();

    return [
      { timestamp: startIso, value: v },
      pnlAgg[0],
      { timestamp: endIso, value: v }
    ];
  }, [pnlAgg, windowStart, windowEnd]);
  const aaplAgg = useMemo(
    () => aggregateSeries(aaplRaw, binMs, "avg", alignStart),
    [aaplRaw, binMs, alignStart]
  );
  const msftAgg = useMemo(
    () => aggregateSeries(msftRaw, binMs, "avg", alignStart),
    [msftRaw, binMs, alignStart]
  );

  // ---------- trade markers (entries/exits) on Z-score chart ----------
  // Normalize trades from live/backtest into {tsMs, kind:'entry'|'exit', dir:'long'|'short'|null, pnl?:number}
  const normalizedTrades = useMemo(() => {
    const out = [];
    if (mode === "live" && Array.isArray(liveTrades)) {
      for (const t of liveTrades) {
        const dir = (t.action || "").includes("long_aapl") ? "long" :
                    (t.action || "").includes("short_aapl") ? "short" : null;
        // entry
        if (t.timestamp) {
          out.push({ tsMs: parseTS(t.timestamp).getTime(), kind: "entry", dir, pnl: null });
        }
        // exit (only if closed)
        if (t.status === "closed" && t.close_timestamp) {
          out.push({
            tsMs: parseTS(t.close_timestamp).getTime(),
            kind: "exit",
            dir,
            pnl: typeof t.pnl === "number" ? t.pnl : (t.pnl ? Number(t.pnl) : null)
          });
        }
      }
    } else if (mode === "backtest" && Array.isArray(btTrades)) {
      for (const t of btTrades) {
        const tsStr = t.timestamp || t.t;
        if (!tsStr) continue;
        const side = (t.side || "").toUpperCase();
        const kind = side.includes("ENTRY") ? "entry" : (side.includes("EXIT") ? "exit" : "entry");
        const dir = side.includes("SHORT") ? "short" : (side.includes("LONG") ? "long" : null);
        out.push({
          tsMs: parseTS(tsStr).getTime(),
          kind,
          dir,
          pnl: typeof t.pnl === "number" ? t.pnl : (t.pnl ? Number(t.pnl) : null)
        });
      }
    }
    return out.sort((a, b) => a.tsMs - b.tsMs);
  }, [mode, liveTrades, btTrades]);

  // Build per-point arrays aligned to zAgg indices for Chart.js
  const tradeMarkerSeries = useMemo(() => {
    const len = zAgg.length;
    const mkEntry = Array(len).fill(null);
    const mkExit  = Array(len).fill(null);
    const entryColor = Array(len).fill(null);
    const exitColor  = Array(len).fill(null);
    const entryStyle = Array(len).fill("triangle");
    const exitStyle  = Array(len).fill("circle");
    const entryRadius= Array(len).fill(5);
    const exitRadius = Array(len).fill(5);
    const entryTip   = Array(len).fill(null);
    const exitTip    = Array(len).fill(null);

    for (const tr of normalizedTrades) {
      const idx = nearestIndexByTime(zAgg, tr.tsMs);
      if (idx < 0) continue;
      const y = zAgg[idx]?.value ?? null;
      if (y == null) continue;

      if (tr.kind === "entry") {
        mkEntry[idx] = y;
        entryColor[idx] = tr.dir === "short" ? "#ef4444" : "#10b981"; // red short / green long
        entryRadius[idx] = 6;
        const when = new Date(tr.tsMs).toLocaleString();
        entryTip[idx] = `Entry${tr.dir ? ` (${tr.dir})` : ""} — ${when}`;
      } else {
        mkExit[idx] = y;
        // P/L -> color
        let c = "#9ca3af";
        if (typeof tr.pnl === "number") c = tr.pnl > 0 ? "#10b981" : (tr.pnl < 0 ? "#ef4444" : "#9ca3af");
        exitColor[idx] = c;
        // radius scaled by |pnl|
        const r = Math.max(4, Math.min(9, 4 + Math.log10(Math.abs(tr.pnl || 0) + 1) * 3));
        exitRadius[idx] = r;
        const when = new Date(tr.tsMs).toLocaleString();
        const pnlTxt = (typeof tr.pnl === "number") ? ` — PnL: ${tr.pnl.toFixed(2)}` : "";
        exitTip[idx] = `Exit${pnlTxt} — ${when}`;
      }
    }

    return {
      mkEntry, mkExit,
      entryColor, exitColor,
      entryStyle, exitStyle,
      entryRadius, exitRadius,
      entryTip, exitTip
    };
  }, [zAgg, normalizedTrades]);

  // ---------- shared chart options ----------
  const baseOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "#ffffff" } },
        tooltip: { mode: "index", intersect: false }
      },
      scales: {
        x: {
          ticks: { color: "#9ca3af", maxTicksLimit: maxTicks },
          grid: { color: "rgba(156,163,175,0.2)" }
        },
        y: {
          ticks: { color: "#9ca3af" },
          grid: { color: "rgba(156,163,175,0.2)" }
        }
      },
      elements: { point: { radius: 0, hoverRadius: 3 } }
    }),
    [maxTicks]
  );

  // Custom tooltip for Z chart to show trade info
  const zOptions = useMemo(() => {
    return {
      ...baseOptions,
      plugins: {
        ...baseOptions.plugins,
        tooltip: {
          ...baseOptions.plugins.tooltip,
          callbacks: {
            label: (ctx) => {
              const ds = ctx.dataset;
              const idx = ctx.dataIndex;
              const tipArr = ds.tooltipInfo || [];
              const tip = tipArr[idx];
              if (tip) return tip;
              return `${ds.label}: ${ctx.formattedValue}`;
            }
          }
        }
      }
    };
  }, [baseOptions, tradeMarkerSeries]);

  // ---------- datasets ----------
  const labelTimeframe = mode === "backtest" ? "week" : timeframe;

  const zScoreChartData = useMemo(
    () => ({
      labels: zAgg.map((p) => fmtLabel(p.timestamp, labelTimeframe)),
      datasets: [
        {
          label: "Z-Score",
          data: zAgg.map((p) => p.value),
          borderColor: "#3b82f6",
          fill: false,
          tension: 0.35
        },
        // Entries overlay
        {
          label: "Entry",
          data: tradeMarkerSeries.mkEntry,
          borderColor: "rgba(0,0,0,0)",
          pointBackgroundColor: tradeMarkerSeries.entryColor,
          pointBorderColor: tradeMarkerSeries.entryColor,
          pointRadius: tradeMarkerSeries.entryRadius,
          pointStyle: tradeMarkerSeries.entryStyle,
          showLine: false,
          tooltipInfo: tradeMarkerSeries.entryTip
        },
        // Exits overlay
        {
          label: "Exit",
          data: tradeMarkerSeries.mkExit,
          borderColor: "rgba(0,0,0,0)",
          pointBackgroundColor: tradeMarkerSeries.exitColor,
          pointBorderColor: tradeMarkerSeries.exitColor,
          pointRadius: tradeMarkerSeries.exitRadius,
          pointStyle: tradeMarkerSeries.exitStyle,
          showLine: false,
          tooltipInfo: tradeMarkerSeries.exitTip
        }
      ]
    }),
    [zAgg, labelTimeframe, tradeMarkerSeries]
  );

  const pnlChartData = useMemo(
  () => ({
    labels: (pnlDisplayAgg ?? []).map((p) => fmtLabel(p.timestamp, labelTimeframe)),
    datasets: [
      {
        label: "Cumulative PnL",
        data: (pnlDisplayAgg ?? []).map((p) => p.value),
        borderColor: "#10b981",
        backgroundColor: "rgba(16,185,129,0.12)",
        fill: true,
        tension: 0.35
      }
    ]
  }),
  [pnlDisplayAgg, labelTimeframe]
);

  const aaplChartData = useMemo(
    () => ({
      labels: aaplAgg.map((p) => fmtLabel(p.timestamp, labelTimeframe)),
      datasets: [
        {
          label: "AAPL",
          data: aaplAgg.map((p) => p.value),
          borderColor: "#22c55e",
          fill: false,
          tension: 0.35
        }
      ]
    }),
    [aaplAgg, labelTimeframe]
  );

  const msftChartData = useMemo(
    () => ({
      labels: msftAgg.map((p) => fmtLabel(p.timestamp, labelTimeframe)),
      datasets: [
        {
          label: "MSFT",
          data: msftAgg.map((p) => p.value),
          borderColor: "#a78bfa",
          fill: false,
          tension: 0.35
        }
      ]
    }),
    [msftAgg, labelTimeframe]
  );

  // readiness flags
  const isLiveReady = liveTrades && livePrices && liveZ && livePnl;
  const isBacktestReady =
    (btPhase === "streaming") &&
    run && btTrades && btPnl && btSeries;

  const isLoading =
    (mode === "live" && !isLiveReady) ||
    (mode === "backtest" && !isBacktestReady);

  // --------- trade tables (open + closed) ----------
  const openTrades = useMemo(() => {
    if (mode === "live" && Array.isArray(liveTrades)) {
      return liveTrades.filter(t => t.status === "open");
    } else if (mode === "backtest" && Array.isArray(btTrades)) {
      // backtest doesn't keep "open" state easily; approximate most recent ENTRYs not followed by EXIT yet
      const stack = [];
      for (const t of btTrades) {
        const side = (t.side || "").toUpperCase();
        if (side.includes("ENTRY")) stack.push(t);
        else if (side.includes("EXIT")) stack.pop();
      }
      return stack;
    }
    return [];
  }, [mode, liveTrades, btTrades]);

  const closedTrades = useMemo(() => {
    if (mode === "live" && Array.isArray(liveTrades)) {
      return liveTrades.filter(t => t.status === "closed").slice(-10);
    } else if (mode === "backtest" && Array.isArray(btTrades)) {
      return btTrades.filter(t => (t.side || "").toUpperCase().includes("EXIT")).slice(-10);
    }
    return [];
  }, [mode, liveTrades, btTrades]);

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-white">
      {/* Mode Toggle */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex space-x-3">
          <button
            onClick={() => setMode("live")}
            className={`px-6 py-3 rounded-lg text-lg font-semibold transition ${
              mode === "live" ? "bg-green-600" : "bg-gray-700 hover:bg-green-700"
            }`}
          >
            Live
          </button>
          <button
            onClick={handleBacktestClick}
            className={`px-6 py-3 rounded-lg text-lg font-semibold transition ${
              mode === "backtest" ? "bg-blue-600" : "bg-gray-700 hover:bg-blue-700"
            }`}
          >
            Backtest
          </button>
        </div>

        {/* Right-side controls: Strategy selector + Live timeframe */}
        <div className="flex items-center gap-4">
          {/* Strategy selector (shown in both modes) */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-400">Strategy</span>
            <select
              value={strategy}
              onChange={(e) => setStrategy(e.target.value)}
              className="bg-gray-800 text-white text-sm rounded px-3 py-2"
            >
              {STRATEGIES.map((s) => (
                <option key={s.id} value={s.id}>{s.label}</option>
              ))}
            </select>
          </div>

          {/* Live timeframe buttons (hidden in Backtest) */}
          {mode === "live" && (
            <div className="flex space-x-3">
              {["week", "day", "hour"].map((tf) => (
                <button
                  key={tf}
                  onClick={() => setTimeframe(tf)}
                  className={`px-4 py-2 rounded transition ${
                    timeframe === tf ? "bg-blue-500" : "bg-gray-700 hover:bg-blue-600"
                  }`}
                >
                  {tf.charAt(0).toUpperCase() + tf.slice(1)}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Backtest status + meta */}
      {mode === "backtest" && (
        <div className="mb-6">
          {btPhase !== "idle" && (
            <div className="text-sm text-gray-300 mb-2">
              {btMsg || "Preparing…"}
            </div>
          )}

          {run && run.params && (
            <div className="text-sm text-gray-300">
              <div className="flex flex-wrap items-center gap-x-6 gap-y-2">
                <span>
                  <span className="text-gray-400">Run:</span>{" "}
                  <span className="font-mono">{run.run_id}</span>
                </span>
                <span>
                  <span className="text-gray-400">Symbols:</span>{" "}
                  {Array.isArray(run.params.symbols) ? run.params.symbols.join(" / ") : String(run.params.symbols)}
                </span>
                <span>
                  <span className="text-gray-400">Interval:</span> {run.params.interval}
                </span>
                <span>
                  <span className="text-gray-400">Lookback:</span> {run.params.lookback}
                </span>
                <span>
                  <span className="text-gray-400">Entry Z:</span> {run.params.entry_z}
                </span>
                <span>
                  <span className="text-gray-400">Exit Z:</span> {run.params.exit_z}
                </span>
              </div>
              <div className="mt-1">
                <span className="text-gray-400">Tested Range:</span>{" "}
                {run.params.start ? new Date(run.params.start).toLocaleDateString() : "—"} →{" "}
                {run.params.end ? new Date(run.params.end).toLocaleDateString() : "—"}
              </div>
            </div>
          )}
        </div>
      )}

      {isLoading ? (
        <div className="text-center p-6">Loading…</div>
      ) : (
        <>
          {/* TOP ROW: Z-Score & PnL */}
          <div className="grid grid-cols-2 gap-6 mb-6">
            <Card className="bg-gray-800">
              <CardContent>
                <h2 className="text-lg font-semibold mb-3">Z-Score (with Trade Markers)</h2>
                <div className="h-80">
                  <Line data={zScoreChartData} options={zOptions} />
                </div>
                <div className="text-xs text-gray-400 mt-2 space-x-4">
                  <span>▲ Entry (green = long AAPL / red = short AAPL)</span>
                  <span>● Exit (green = profit / red = loss / gray = flat)</span>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gray-800">
              <CardContent>
                <h2 className="text-lg font-semibold mb-3">PnL</h2>
                <div className="h-80">
                  <Line data={pnlChartData} options={baseOptions} />
                </div>
              </CardContent>
            </Card>
          </div>

          {/* BOTTOM ROW: AAPL & MSFT side-by-side */}
          <div className="grid grid-cols-2 gap-6 mb-6">
            <Card className="bg-gray-800">
              <CardContent>
                <h2 className="text-lg font-semibold mb-3">AAPL</h2>
                <div className="h-80">
                  <Line data={aaplChartData} options={baseOptions} />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gray-800">
              <CardContent>
                <h2 className="text-lg font-semibold mb-3">MSFT</h2>
                <div className="h-80">
                  <Line data={msftChartData} options={baseOptions} />
                </div>
              </CardContent>
            </Card>
          </div>

          {/* TRADE TABLES */}
          <div className="grid grid-cols-2 gap-6">
            <Card className="bg-gray-800">
              <CardContent>
                <h2 className="text-lg font-semibold mb-3">Open Trades</h2>
                {openTrades.length === 0 ? (
                  <div className="text-sm text-gray-400">None</div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="text-gray-400">
                        <tr>
                          <th className="text-left p-2">Time</th>
                          <th className="text-left p-2">Side</th>
                          <th className="text-right p-2">AAPL</th>
                          <th className="text-right p-2">MSFT</th>
                          <th className="text-right p-2">z-like</th>
                          <th className="text-right p-2">Qty</th>
                        </tr>
                      </thead>
                      <tbody>
                        {mode === "live" && openTrades.map((t, i) => (
                          <tr key={i} className="border-t border-gray-700">
                            <td className="p-2">{new Date(t.timestamp).toLocaleString()}</td>
                            <td className="p-2">{t.action}</td>
                            <td className="p-2 text-right">{t.aapl_price?.toFixed?.(2) ?? "—"}</td>
                            <td className="p-2 text-right">{t.msft_price?.toFixed?.(2) ?? "—"}</td>
                            <td className="p-2 text-right">{t.z_like?.toFixed?.(2) ?? "—"}</td>
                            <td className="p-2 text-right">{t.shares ?? 1}</td>
                          </tr>
                        ))}
                        {mode === "backtest" && openTrades.map((t, i) => (
                          <tr key={i} className="border-t border-gray-700">
                            <td className="p-2">{new Date(t.timestamp || t.t).toLocaleString()}</td>
                            <td className="p-2">{t.side}</td>
                            <td className="p-2 text-right">{t.price1?.toFixed?.(2) ?? "—"}</td>
                            <td className="p-2 text-right">{t.price2?.toFixed?.(2) ?? "—"}</td>
                            <td className="p-2 text-right">—</td>
                            <td className="p-2 text-right">{t.qty1 ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>

            <Card className="bg-gray-800">
              <CardContent>
                <h2 className="text-lg font-semibold mb-3">Recent Closed Trades</h2>
                {closedTrades.length === 0 ? (
                  <div className="text-sm text-gray-400">None</div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="text-gray-400">
                        <tr>
                          <th className="text-left p-2">Closed</th>
                          <th className="text-left p-2">Side</th>
                          <th className="text-right p-2">PnL</th>
                          <th className="text-right p-2">Held</th>
                        </tr>
                      </thead>
                      <tbody>
                        {mode === "live" && closedTrades.map((t, i) => (
                          <tr key={i} className="border-t border-gray-700">
                            <td className="p-2">{t.close_timestamp ? new Date(t.close_timestamp).toLocaleString() : (t.timestamp ? new Date(t.timestamp).toLocaleString() : "—")}</td>
                            <td className="p-2">{t.action}</td>
                            <td className={`p-2 text-right ${t.pnl > 0 ? "text-green-400" : (t.pnl < 0 ? "text-red-400" : "text-gray-300")}`}>
                              {typeof t.pnl === "number" ? t.pnl.toFixed(2) : "—"}
                            </td>
                            <td className="p-2 text-right">—</td>
                          </tr>
                        ))}
                        {mode === "backtest" && closedTrades.map((t, i) => (
                          <tr key={i} className="border-t border-gray-700">
                            <td className="p-2">{new Date(t.timestamp || t.t).toLocaleString()}</td>
                            <td className="p-2">{t.side}</td>
                            <td className={`p-2 text-right ${t.pnl > 0 ? "text-green-400" : (t.pnl < 0 ? "text-red-400" : "text-gray-300")}`}>
                              {typeof t.pnl === "number" ? t.pnl.toFixed(2) : "—"}
                            </td>
                            <td className="p-2 text-right">{t.bars_held ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </div>
  );
}
