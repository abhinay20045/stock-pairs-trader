import { useEffect, useMemo, useState, useCallback } from "react";
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

export default function Dashboard() {
  // Mode
  const [mode, setMode] = useState("live"); // 'live' | 'backtest'

  // Live data
  const [liveTrades, setLiveTrades] = useState(null);
  const [livePrices, setLivePrices] = useState(null);   // {AAPL:[{timestamp,close}], MSFT:[...]}
  const [liveZ, setLiveZ] = useState(null);             // [{timestamp,z_score}]
  const [livePnl, setLivePnl] = useState(null);         // [{timestamp,cumulative_pnl}]

  // Backtest data
  const [run, setRun] = useState(null);                 // latest backtest run meta
  const [btTrades, setBtTrades] = useState(null);
  const [btPnl, setBtPnl] = useState(null);             // [{timestamp,cumulative_pnl}] (mapped)
  const [btSeries, setBtSeries] = useState(null);       // [{timestamp,z,p1,p2,beta}] (mapped)

  // Live timeframe selector
  const [timeframe, setTimeframe] = useState("week"); // 'week' | 'day' | 'hour'

  // ------- fetchers -------
  // LIVE
  useEffect(() => {
    if (mode !== "live") return;
    let stop = false;
    async function fetchLive() {
      try {
        const [tradeRes, priceRes, zscoreRes, pnlRes] = await Promise.all([
          fetch("http://localhost:5050/trade-history"),
          fetch("http://localhost:5050/prices?symbols=AAPL,MSFT&limit=50000"),
          fetch("http://localhost:5050/stock-zscores"),
          fetch("http://localhost:5050/pnl-history")
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
  }, [mode]);

  // BACKTEST
  useEffect(() => {
    if (mode !== "backtest") return;
    let stop = false;
    async function fetchBacktest() {
      try {
        // 1) latest run
        const runRes = await fetch("http://localhost:5050/backtest/latest-run");
        const runJson = await runRes.json();
        if (stop) return;
        setRun(runJson);

        // 2) data for that run
        const q = new URLSearchParams({ run_id: runJson.run_id });
        const [pnlRes, tradesRes, seriesRes] = await Promise.all([
          fetch(`http://localhost:5050/backtest/pnl?${q}`),
          fetch(`http://localhost:5050/backtest/trades?${q}`),
          fetch(`http://localhost:5050/backtest/series?${q}`)
        ]);
        const [pnlJson, tradesJson, seriesJson] = await Promise.all([
          pnlRes.json(),
          tradesRes.json(),
          seriesRes.json()
        ]);

        // Normalize shapes for charts
        const pnlPoints = (pnlJson.points || []).map((p) => ({
          timestamp: p.t,
          cumulative_pnl: p.pnl
        }));
        const seriesPoints = (seriesJson.points || []).map((p) => ({
          timestamp: p.t,
          z: p.z,
          p1: p.p1,
          p2: p.p2,
          beta: p.beta
        }));

        if (stop) return;
        setBtPnl(pnlPoints);
        setBtTrades(tradesJson.trades || []);
        setBtSeries(seriesPoints);
      } catch (e) {
        console.error("Backtest fetch error:", e);
      }
    }
    fetchBacktest();
    return () => { stop = true; };
  }, [mode]);

  // ---------- normalize once to stable references (fixes eslint deps warnings) ----------
  const prices = useMemo(() => {
    if (mode === "live") {
      return {
        AAPL: livePrices?.AAPL ?? [],
        MSFT: livePrices?.MSFT ?? [],
      };
    }
    if (btSeries) {
      return {
        AAPL: btSeries.map((r) => ({ timestamp: r.timestamp, close: r.p1 })),
        MSFT: btSeries.map((r) => ({ timestamp: r.timestamp, close: r.p2 })),
      };
    }
    return { AAPL: [], MSFT: [] };
  }, [mode, livePrices, btSeries]);

  const zscores = useMemo(() => {
    if (mode === "live") return liveZ ?? [];
    if (btSeries) return btSeries.map((r) => ({ timestamp: r.timestamp, z_score: r.z }));
    return [];
  }, [mode, liveZ, btSeries]);

  const pnl = useMemo(() => {
    return mode === "live" ? (livePnl ?? []) : (btPnl ?? []);
  }, [mode, livePnl, btPnl]);

  // ---------- timestamps present in active data ----------
  const allTimestamps = useMemo(() => {
    const ts = [];
    zscores.forEach((d) => ts.push(parseTS(d.timestamp).getTime()));
    pnl.forEach((d) => ts.push(parseTS(d.timestamp).getTime()));
    prices.AAPL.forEach((p) => ts.push(parseTS(p.timestamp).getTime()));
    prices.MSFT.forEach((p) => ts.push(parseTS(p.timestamp).getTime()));
    return ts.sort((a, b) => a - b);
  }, [zscores, pnl, prices]);

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
    if (mode === "backtest" && run) {
      const start = parseTS(run.params.start).getTime();
      const end = parseTS(run.params.end || run.finished_at || run.started_at).getTime();
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
    return zscores
      .map((d) => ({ t: parseTS(d.timestamp).getTime(), v: Number(d.z_score ?? d.z ?? 0), ts: d.timestamp }))
      .filter((d) => d.t >= windowStart && d.t <= windowEnd)
      .map((d) => ({ timestamp: d.ts, value: d.v }));
  }, [zscores, windowStart, windowEnd]);

  const pnlRaw = useMemo(() => {
    return pnl
      .map((d) => ({
        t: parseTS(d.timestamp).getTime(),
        v: Number(d.cumulative_pnl ?? d.value ?? 0),
        ts: d.timestamp
      }))
      .filter((d) => d.t >= windowStart && d.t <= windowEnd)
      .map((d) => ({ timestamp: d.ts, value: d.v }));
  }, [pnl, windowStart, windowEnd]);

  const aaplRaw = useMemo(() => {
    return prices.AAPL
      .map((p) => ({ t: parseTS(p.timestamp).getTime(), v: Number(p.close ?? p.p1 ?? p.price1), ts: p.timestamp }))
      .filter((p) => p.t >= windowStart && p.t <= windowEnd)
      .map((p) => ({ timestamp: p.ts, value: p.v }));
  }, [prices, windowStart, windowEnd]);

  const msftRaw = useMemo(() => {
    return prices.MSFT
      .map((p) => ({ t: parseTS(p.timestamp).getTime(), v: Number(p.close ?? p.p2 ?? p.price2), ts: p.timestamp }))
      .filter((p) => p.t >= windowStart && p.t <= windowEnd)
      .map((p) => ({ timestamp: p.ts, value: p.v }));
  }, [prices, windowStart, windowEnd]);

  // Aggregate (aligned to the visible window)
  const zAgg = useMemo(
    () => aggregateSeries(zRaw, binMs, "avg", alignStart),
    [zRaw, binMs, alignStart]
  );
  const pnlAgg = useMemo(
    () => aggregateSeries(pnlRaw, binMs, "last", alignStart),
    [pnlRaw, binMs, alignStart]
  );
  const aaplAgg = useMemo(
    () => aggregateSeries(aaplRaw, binMs, "avg", alignStart),
    [aaplRaw, binMs, alignStart]
  );
  const msftAgg = useMemo(
    () => aggregateSeries(msftRaw, binMs, "avg", alignStart),
    [msftRaw, binMs, alignStart]
  );

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

  // ---------- datasets ----------
  const labelTf = mode === "backtest" ? "week" : timeframe;

  const zScoreChartData = useMemo(
    () => ({
      labels: zAgg.map((p) => fmtLabel(p.timestamp, labelTf)),
      datasets: [
        {
          label: "Z-Score",
          data: zAgg.map((p) => p.value),
          borderColor: "#3b82f6",
          fill: false,
          tension: 0.35
        }
      ]
    }),
    [zAgg, labelTf]
  );

  const pnlChartData = useMemo(
    () => ({
      labels: pnlAgg.map((p) => fmtLabel(p.timestamp, labelTf)),
      datasets: [
        {
          label: "Cumulative PnL",
          data: pnlAgg.map((p) => p.value),
          borderColor: "#10b981",
          backgroundColor: "rgba(16,185,129,0.12)",
          fill: true,
          tension: 0.35
        }
      ]
    }),
    [pnlAgg, labelTf]
  );

  const aaplChartData = useMemo(
    () => ({
      labels: aaplAgg.map((p) => fmtLabel(p.timestamp, labelTf)),
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
    [aaplAgg, labelTf]
  );

  const msftChartData = useMemo(
    () => ({
      labels: msftAgg.map((p) => fmtLabel(p.timestamp, labelTf)),
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
    [msftAgg, labelTf]
  );

  const isLiveReady = liveTrades && livePrices && liveZ && livePnl;
  const isBacktestReady = run && btTrades && btPnl && btSeries;
  const isLoading = (mode === "live" && !isLiveReady) || (mode === "backtest" && !isBacktestReady);

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
            onClick={() => setMode("backtest")}
            className={`px-6 py-3 rounded-lg text-lg font-semibold transition ${
              mode === "backtest" ? "bg-blue-600" : "bg-gray-700 hover:bg-blue-700"
            }`}
          >
            Backtest
          </button>
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

      {/* Backtest meta */}
      {mode === "backtest" && run && (
        <div className="mb-6 text-sm text-gray-300">
          <div className="flex flex-wrap items-center gap-x-6 gap-y-2">
            <span>
              <span className="text-gray-400">Run:</span> <span className="font-mono">{run.run_id}</span>
            </span>
            <span>
              <span className="text-gray-400">Symbols:</span> {run.params.symbols.join(" / ")}
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
            {new Date(run.params.start).toLocaleDateString()} →{" "}
            {new Date(run.params.end).toLocaleDateString()}
          </div>
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
                <h2 className="text-lg font-semibold mb-3">Z-Score</h2>
                <div className="h-80">
                  <Line data={zScoreChartData} options={baseOptions} />
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
          <div className="grid grid-cols-2 gap-6">
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
        </>
      )}
    </div>
  );
}
