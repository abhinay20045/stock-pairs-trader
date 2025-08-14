import { useEffect, useMemo, useState } from "react";
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

// Aggregate an array of points [{timestamp, value}] into fixed bins (ms).
// mode: "avg" (default) or "last" (step-like; good for cumulative PnL)
function aggregateSeries(points, binMs, mode = "avg") {
  if (!points || points.length === 0) return [];
  const out = [];
  const sorted = [...points].sort((a, b) => parseTS(a.timestamp) - parseTS(b.timestamp));
  let binStart = Math.floor(parseTS(sorted[0].timestamp).getTime() / binMs) * binMs;
  let binEnd = binStart + binMs;
  let bucket = [];

  const flush = () => {
    if (!bucket.length) return;
    const ts = new Date(binEnd - 1);
    const v = mode === "last" ? bucket[bucket.length - 1] : avg(bucket);
    out.push({ timestamp: ts.toISOString(), value: v });
    bucket = [];
  };

  for (const p of sorted) {
    const t = parseTS(p.timestamp).getTime();
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
  const [data, setData] = useState(null);           // trades
  const [priceData, setPriceData] = useState(null); // {AAPL:[{timestamp,close}], MSFT:[...]}
  const [zscoreData, setZscoreData] = useState(null); // [{timestamp,z_score,spread,...}]
  const [pnlData, setPnlData] = useState(null);       // [{timestamp,cumulative_pnl,...}]
  const [timeframe, setTimeframe] = useState("week"); // 'week' | 'day' | 'hour'

  useEffect(() => {
    async function fetchData() {
      try {
        const [tradeRes, priceRes, zscoreRes, pnlRes] = await Promise.all([
          fetch("http://localhost:5050/trade-history"),
          // large limit so “week” has enough history; aggregation keeps it readable
          fetch("http://localhost:5050/prices?symbols=AAPL,MSFT&limit=50000"),
          fetch("http://localhost:5050/stock-zscores"),
          fetch("http://localhost:5050/pnl-history"),
        ]);

        const [tradeJson, priceJson, zscoreJson, pnlJson] = await Promise.all([
          tradeRes.json(),
          priceRes.json(),
          zscoreRes.json(),
          pnlRes.json(),
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

  // ---------- compute timeframe parameters (no conditional hooks) ----------
  const now = Date.now();
  const { cutoff, binMs, maxTicks } = useMemo(() => {
    const c =
      timeframe === "hour"
        ? now - 60 * 60 * 1000
        : timeframe === "day"
        ? now - 24 * 60 * 60 * 1000
        : now - 7 * 24 * 60 * 60 * 1000;

    const b =
      timeframe === "hour" ? 60 * 1000 : timeframe === "day" ? 5 * 60 * 1000 : 30 * 60 * 1000;

    const ticks = timeframe === "hour" ? 12 : timeframe === "day" ? 24 : 14;

    return { cutoff: c, binMs: b, maxTicks: ticks };
  }, [timeframe, now]);

  // ---------- build series with filtering + aggregation (hooks always run) ----------
  // z-score
  const zRaw = useMemo(() => {
    const src = zscoreData || [];
    return src
      .filter((d) => parseTS(d.timestamp).getTime() >= cutoff)
      .map((d) => ({ timestamp: d.timestamp, value: Number(d.z_score) }));
  }, [zscoreData, cutoff]);

  const zAgg = useMemo(() => aggregateSeries(zRaw, binMs, "avg"), [zRaw, binMs]);

  // PnL (cumulative -> last per bin)
  const pnlRaw = useMemo(() => {
    const src = pnlData || [];
    return src
      .filter((d) => parseTS(d.timestamp).getTime() >= cutoff)
      .map((d) => ({ timestamp: d.timestamp, value: Number(d.cumulative_pnl) }));
  }, [pnlData, cutoff]);

  const pnlAgg = useMemo(() => aggregateSeries(pnlRaw, binMs, "last"), [pnlRaw, binMs]);

  // prices
  const aaplRaw = useMemo(() => {
    const src = (priceData && priceData.AAPL) || [];
    return src
      .filter((p) => parseTS(p.timestamp).getTime() >= cutoff)
      .map((p) => ({ timestamp: p.timestamp, value: Number(p.close) }));
  }, [priceData, cutoff]);

  const msftRaw = useMemo(() => {
    const src = (priceData && priceData.MSFT) || [];
    return src
      .filter((p) => parseTS(p.timestamp).getTime() >= cutoff)
      .map((p) => ({ timestamp: p.timestamp, value: Number(p.close) }));
  }, [priceData, cutoff]);

  const aaplAgg = useMemo(() => aggregateSeries(aaplRaw, binMs, "avg"), [aaplRaw, binMs]);
  const msftAgg = useMemo(() => aggregateSeries(msftRaw, binMs, "avg"), [msftRaw, binMs]);

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

  const priceChartData = useMemo(() => {
    const labels = aaplAgg.map((p) => fmtLabel(p.timestamp, timeframe));
    return {
      labels,
      datasets: [
        {
          label: "AAPL",
          data: aaplAgg.map((p) => p.value),
          borderColor: "#22c55e",
          fill: false,
          tension: 0.35
        },
        {
          label: "MSFT",
          data: msftAgg.map((p) => p.value),
          borderColor: "#a78bfa",
          fill: false,
          tension: 0.35
        }
      ]
    };
  }, [aaplAgg, msftAgg, timeframe]);

  // After all hooks are declared, it’s safe to do a loading guard for the UI:
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

      {/* TOP ROW: Z-Score & PnL side-by-side */}
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

      {/* BOTTOM ROW: Stock Prices full-width */}
      <Card className="bg-gray-800">
        <CardContent>
          <h2 className="text-lg font-semibold mb-3">Stock Prices</h2>
          <div className="h-80">
            <Line data={priceChartData} options={baseOptions} />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
