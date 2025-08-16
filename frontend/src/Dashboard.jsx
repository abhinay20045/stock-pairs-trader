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

// Aggregate [{timestamp,value}] into fixed bins (ms).
// mode: "avg" (default) or "last" (for cumulative series).
// startAtMs aligns bins to a fixed start so charts don't jitter.
function aggregateSeries(points, binMs, mode = "avg", startAtMs) {
  if (!points || points.length === 0) return [];
  const out = [];
  const sorted = [...points].sort((a, b) => parseTS(a.timestamp) - parseTS(b.timestamp));

  let binStart =
    startAtMs ?? Math.floor(parseTS(sorted[0].timestamp).getTime() / binMs) * binMs;
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
  // Probe noon ET that day to get the correct DST offset.
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
  // week
  return (
    d.toLocaleDateString([], { month: "2-digit", day: "2-digit" }) +
    " " +
    d.toLocaleTimeString([], { hour: "2-digit" })
  );
};

export default function Dashboard() {
  const [data, setData] = useState(null);             // trades
  const [priceData, setPriceData] = useState(null);   // {AAPL:[{timestamp,close}], MSFT:[...]}
  const [zscoreData, setZscoreData] = useState(null); // [{timestamp,z_score,spread,...}]
  const [pnlData, setPnlData] = useState(null);       // [{timestamp,cumulative_pnl,...}]
  const [timeframe, setTimeframe] = useState("week"); // 'week' | 'day' | 'hour'

  useEffect(() => {
    async function fetchData() {
      try {
        const [tradeRes, priceRes, zscoreRes, pnlRes] = await Promise.all([
          fetch("http://localhost:5050/trade-history"),
          fetch("http://localhost:5050/prices?symbols=AAPL,MSFT&limit=50000"),
          fetch("http://localhost:5050/stock-zscores"),
          fetch("http://localhost:5050/pnl-history")
        ]);

        const [tradeJson, priceJson, zscoreJson, pnlJson] = await Promise.all([
          tradeRes.json(),
          priceRes.json(),
          zscoreRes.json(),
          pnlRes.json()
        ]);

        setData(tradeJson);
        setPriceData(priceJson);
        setZscoreData(zscoreJson);
        setPnlData(pnlJson);
      } catch (e) {
        console.error("Fetch error:", e);
      }
    }

    fetchData();
    const id = setInterval(fetchData, 5000);
    return () => clearInterval(id);
  }, []);

  // ---------- timestamps present in data ----------
  const allTimestamps = useMemo(() => {
    const ts = [];
    (zscoreData ?? []).forEach((d) => ts.push(parseTS(d.timestamp).getTime()));
    (pnlData ?? []).forEach((d) => ts.push(parseTS(d.timestamp).getTime()));
    if (priceData) {
      (priceData.AAPL ?? []).forEach((p) => ts.push(parseTS(p.timestamp).getTime()));
      (priceData.MSFT ?? []).forEach((p) => ts.push(parseTS(p.timestamp).getTime()));
    }
    return ts.sort((a, b) => a - b);
  }, [zscoreData, pnlData, priceData]);

  const lastDataMs = useMemo(
    () => (allTimestamps.length ? allTimestamps[allTimestamps.length - 1] : Date.now()),
    [allTimestamps]
  );

  const hasDataBetween = useCallback(
    (start, end) => allTimestamps.some((t) => t >= start && t <= end),
    [allTimestamps]
  );

  // ---------- compute the visible window (start/end) + binning ----------
  const { windowStart, windowEnd, binMs, maxTicks } = useMemo(() => {
    let start, end, bins, ticks;
    const nowMs = Date.now();

    if (timeframe === "day") {
      const today = getEtDaySessionRangeUtc(nowMs);
      const liveHas = hasDataBetween(today.start, today.end);
      if (liveHas) {
        start = today.start;
        end = Math.min(today.end, nowMs); // fills as the day goes on
      } else {
        const last = getEtDaySessionRangeUtc(lastDataMs);
        start = last.start;
        end = Math.min(last.end, lastDataMs);
      }
      bins = 5 * 60 * 1000; // 5-minute bars
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
      bins = 60 * 1000; // 1-minute bars
      ticks = 12;
    } else {
      // week: anchor to last datapoint
      end = lastDataMs;
      start = end - 7 * 24 * 60 * 60 * 1000;
      bins = 30 * 60 * 1000; // 30-minute bars
      ticks = 14;
    }

    return { windowStart: start, windowEnd: end, binMs: bins, maxTicks: ticks };
  }, [timeframe, lastDataMs, hasDataBetween]);

  const alignStart = useMemo(
    () => Math.floor(windowStart / binMs) * binMs,
    [windowStart, binMs]
  );

  // ---------- build series bounded by [windowStart, windowEnd] ----------
  const zRaw = useMemo(() => {
    const src = zscoreData || [];
    return src
      .map((d) => ({ t: parseTS(d.timestamp).getTime(), v: Number(d.z_score), ts: d.timestamp }))
      .filter((d) => d.t >= windowStart && d.t <= windowEnd)
      .map((d) => ({ timestamp: d.ts, value: d.v }));
  }, [zscoreData, windowStart, windowEnd]);

  const pnlRaw = useMemo(() => {
    const src = pnlData || [];
    return src
      .map((d) => ({
        t: parseTS(d.timestamp).getTime(),
        v: Number(d.cumulative_pnl),
        ts: d.timestamp
      }))
      .filter((d) => d.t >= windowStart && d.t <= windowEnd)
      .map((d) => ({ timestamp: d.ts, value: d.v }));
  }, [pnlData, windowStart, windowEnd]);

  const aaplRaw = useMemo(() => {
    const src = (priceData && priceData.AAPL) || [];
    return src
      .map((p) => ({ t: parseTS(p.timestamp).getTime(), v: Number(p.close), ts: p.timestamp }))
      .filter((p) => p.t >= windowStart && p.t <= windowEnd)
      .map((p) => ({ timestamp: p.ts, value: p.v }));
  }, [priceData, windowStart, windowEnd]);

  const msftRaw = useMemo(() => {
    const src = (priceData && priceData.MSFT) || [];
    return src
      .map((p) => ({ t: parseTS(p.timestamp).getTime(), v: Number(p.close), ts: p.timestamp }))
      .filter((p) => p.t >= windowStart && p.t <= windowEnd)
      .map((p) => ({ timestamp: p.ts, value: p.v }));
  }, [priceData, windowStart, windowEnd]);

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
  const zScoreChartData = useMemo(
    () => ({
      labels: zAgg.map((p) => fmtLabel(p.timestamp, timeframe)),
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
    [zAgg, timeframe]
  );

  const pnlChartData = useMemo(
    () => ({
      labels: pnlAgg.map((p) => fmtLabel(p.timestamp, timeframe)),
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
    [pnlAgg, timeframe]
  );

  const aaplChartData = useMemo(
    () => ({
      labels: aaplAgg.map((p) => fmtLabel(p.timestamp, timeframe)),
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
    [aaplAgg, timeframe]
  );

  const msftChartData = useMemo(
    () => ({
      labels: msftAgg.map((p) => fmtLabel(p.timestamp, timeframe)),
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
    [msftAgg, timeframe]
  );

  const isLoading = !data || !priceData || !zscoreData || !pnlData;

  if (isLoading) {
    return <div className="text-center p-6 text-white bg-gray-900 min-h-screen">Loading...</div>;
  }

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-white">
      {/* Timeframe Buttons */}
      <div className="flex space-x-3 mb-6">
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
    </div>
  );
}
