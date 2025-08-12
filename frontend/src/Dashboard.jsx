import { useEffect, useState } from "react";
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

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [priceData, setPriceData] = useState(null);
  const [zscoreData, setZscoreData] = useState(null);
  const [pnlData, setPnlData] = useState(null);
  const [timeframe, setTimeframe] = useState("week"); // 'week', 'day', 'hour'

  useEffect(() => {
    async function fetchData() {
      try {
        const [tradeRes, priceRes, zscoreRes, pnlRes] = await Promise.all([
          fetch("http://localhost:5050/trade-history"),
          fetch("http://localhost:5050/prices?symbols=AAPL,MSFT&limit=5000"),
          fetch("http://localhost:5050/stock-zscores"),
          fetch("http://localhost:5050/pnl-history")
        ]);
        
        const tradeJson = await tradeRes.json();
        const priceJson = await priceRes.json();
        const zscoreJson = await zscoreRes.json();
        const pnlJson = await pnlRes.json();
        
        setData(tradeJson);
        setPriceData(priceJson);
        setZscoreData(zscoreJson);
        setPnlData(pnlJson);
      } catch (error) {
        console.error("Error fetching data:", error);
      }
    }
    
    fetchData();
    const interval = setInterval(fetchData, 5000); 
    return () => clearInterval(interval);
  }, []);

  const filterByTimeframe = (arr) => {
    if (!arr || arr.length === 0) return [];
    const now = new Date();
    let cutoff;
    if (timeframe === "hour") cutoff = new Date(now.getTime() - 60 * 60 * 1000);
    if (timeframe === "day") cutoff = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    if (timeframe === "week") cutoff = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    return arr.filter(d => new Date(d.timestamp) >= cutoff);
  };

  if (!data || !priceData || !zscoreData || !pnlData) 
    return <div className="text-center p-6">Loading...</div>;

  const filteredZScores = filterByTimeframe(zscoreData);
  const filteredPricesAAPL = filterByTimeframe(priceData.AAPL || []);
  const filteredPricesMSFT = filterByTimeframe(priceData.MSFT || []);
  const filteredPnL = filterByTimeframe(pnlData);

  const zScoreChartData = {
    labels: filteredZScores.map(d => new Date(d.timestamp).toLocaleTimeString()),
    datasets: [
      {
        label: "Z-Score",
        data: filteredZScores.map(d => d.z_score),
        borderColor: "#3b82f6",
        fill: false,
        tension: 0.4
      }
    ]
  };

  const priceChartData = {
    labels: filteredPricesAAPL.map(p => new Date(p.timestamp).toLocaleTimeString()),
    datasets: [
      {
        label: "AAPL",
        data: filteredPricesAAPL.map(p => p.close),
        borderColor: "#22c55e",
        fill: false,
        tension: 0.4
      },
      {
        label: "MSFT",
        data: filteredPricesMSFT.map(p => p.close),
        borderColor: "#a78bfa",
        fill: false,
        tension: 0.4
      }
    ]
  };

  const pnlChartData = {
    labels: filteredPnL.map(d => new Date(d.timestamp).toLocaleTimeString()),
    datasets: [
      {
        label: "Cumulative PnL",
        data: filteredPnL.map(d => d.cumulative_pnl),
        borderColor: "#10b981",
        fill: true,
        tension: 0.4
      }
    ]
  };

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-white">
      {/* Timeframe Buttons */}
      <div className="flex space-x-4 mb-6">
        {["week", "day", "hour"].map(tf => (
          <button
            key={tf}
            onClick={() => setTimeframe(tf)}
            className={`px-4 py-2 rounded ${timeframe === tf ? "bg-blue-500" : "bg-gray-700"} hover:bg-blue-600`}
          >
            {tf.charAt(0).toUpperCase() + tf.slice(1)}
          </button>
        ))}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card className="bg-gray-800">
          <CardContent>
            <h2 className="text-xl font-bold mb-4">Z-Score</h2>
            <Line data={zScoreChartData} />
          </CardContent>
        </Card>

        <Card className="bg-gray-800">
          <CardContent>
            <h2 className="text-xl font-bold mb-4">Stock Prices</h2>
            <Line data={priceChartData} />
          </CardContent>
        </Card>

        <Card className="bg-gray-800 md:col-span-2">
          <CardContent>
            <h2 className="text-xl font-bold mb-4">PnL</h2>
            <Line data={pnlChartData} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}