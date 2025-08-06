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

  useEffect(() => {
    async function fetchData() {
      try {
        const [tradeRes, priceRes, zscoreRes] = await Promise.all([
          fetch("http://localhost:5050/trade-history"),
          fetch("http://localhost:5050/prices?symbols=AAPL,MSFT&limit=5000"),
          fetch("http://localhost:5050/stock-zscores")
        ]);
        
        const tradeJson = await tradeRes.json();
        const priceJson = await priceRes.json();
        const zscoreJson = await zscoreRes.json();
        
        setData(tradeJson);
        setPriceData(priceJson);
        setZscoreData(zscoreJson);
      } catch (error) {
        console.error("Error fetching data:", error);
      }
    }
    
    fetchData();
    const interval = setInterval(fetchData, 5000); // Update every 5 seconds
    return () => clearInterval(interval);
  }, []);

  if (!data || !priceData || !zscoreData) return <div className="text-center p-6">Loading...</div>;

  // Dedicated Z-Score chart data using entire database
  const zScoreData = {
    labels: zscoreData.map(d => new Date(d.timestamp).toLocaleDateString()),
    datasets: [
      {
        label: "Z-Score",
        data: zscoreData.map(d => d.z_score),
        borderColor: "#3b82f6",
        backgroundColor: "rgba(59, 130, 246, 0.1)",
        borderWidth: 2,
        tension: 0.4,
        fill: true,
        pointRadius: 2,
        pointHoverRadius: 6
      }
    ]
  };

  // Z-Score chart options with threshold lines
  const zScoreOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          color: "#ffffff"
        }
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        titleColor: '#ffffff',
        bodyColor: '#ffffff'
      }
    },
    scales: {
      x: {
        ticks: {
          color: "#9ca3af"
        },
        grid: {
          color: "rgba(156, 163, 175, 0.2)"
        }
      },
      y: {
        ticks: {
          color: "#9ca3af"
        },
        grid: {
          color: "rgba(156, 163, 175, 0.2)"
        },
        suggestedMin: -3,
        suggestedMax: 3
      }
    },
    elements: {
      point: {
        hoverBackgroundColor: "#3b82f6"
      }
    }
  };

  const chartData = {
    labels: data.map(d => new Date(d.timestamp).toLocaleDateString()),
    datasets: [
      {
        label: "Z-Score",
        data: data.map(d => d.z_score),
        borderColor: "#38bdf8",
        tension: 0.4,
        fill: false
      },
      {
        label: "Spread",
        data: data.map(d => d.spread),
        borderColor: "#f87171",
        tension: 0.4,
        fill: false
      }
    ]
  };

  // Prepare price chart data
  const aapl = priceData.AAPL || [];
  const msft = priceData.MSFT || [];
  
  const priceChartData = {
    labels: aapl.map(p => new Date(p.timestamp).toLocaleDateString()),
    datasets: [
      {
        label: "AAPL",
        data: aapl.map(p => p.close),
        borderColor: "#22c55e",
        tension: 0.4,
        fill: false
      },
      {
        label: "MSFT",
        data: msft.map(p => p.close),
        borderColor: "#a78bfa",
        tension: 0.4,
        fill: false
      }
    ]
  };

  const latestTrade = data[data.length - 1];
  const latestZScore = zscoreData.length > 0 ? zscoreData[zscoreData.length - 1].z_score : 0;
  
  // Determine z-score status and color
  const getZScoreStatus = (zScore) => {
    if (zScore > 2) return { status: "Overbought", color: "#ef4444", bgColor: "bg-red-500" };
    if (zScore < -2) return { status: "Oversold", color: "#22c55e", bgColor: "bg-green-500" };
    if (zScore > 1) return { status: "Bullish", color: "#f59e0b", bgColor: "bg-yellow-500" };
    if (zScore < -1) return { status: "Bearish", color: "#8b5cf6", bgColor: "bg-purple-500" };
    return { status: "Neutral", color: "#6b7280", bgColor: "bg-gray-500" };
  };

  const zScoreStatus = getZScoreStatus(latestZScore);

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 p-6 bg-gray-900 min-h-screen text-white">
      {/* Dedicated Z-Score Chart */}
      <Card className="bg-gray-800 md:col-span-2">
        <CardContent>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-bold">Z-Score Analysis</h2>
            <div className={`px-3 py-1 rounded-full text-sm font-semibold ${zScoreStatus.bgColor}`}>
              {zScoreStatus.status}
            </div>
          </div>
          <div className="h-[400px]" style={{ minHeight: '400px', height: '400px' }}>
            <Line data={zScoreData} options={zScoreOptions} />
          </div>
          <div className="mt-4 grid grid-cols-3 gap-4 text-sm">
            <div className="bg-gray-700 p-3 rounded">
              <div className="text-gray-400">Current Z-Score</div>
              <div className="text-xl font-bold" style={{ color: zScoreStatus.color }}>
                {latestZScore.toFixed(2)}
              </div>
            </div>
            <div className="bg-gray-700 p-3 rounded">
              <div className="text-gray-400">Total Data Points</div>
              <div className="text-xl font-bold">{zscoreData.length}</div>
            </div>
            <div className="bg-gray-700 p-3 rounded">
              <div className="text-gray-400">Last Updated</div>
              <div className="text-sm font-semibold">
                {zscoreData.length > 0 ? new Date(zscoreData[zscoreData.length - 1].timestamp).toLocaleTimeString() : 'N/A'}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-gray-800">
        <CardContent>
          <h2 className="text-xl font-bold mb-4">Stock Prices</h2>
          <Line data={priceChartData} />
        </CardContent>
      </Card>

      <Card className="bg-gray-800">
        <CardContent>
          <h2 className="text-xl font-bold mb-4">Latest Trade</h2>
          <p><strong>Action:</strong> {latestTrade.action}</p>
          <p><strong>Z-Score:</strong> {latestTrade.z_score.toFixed(2)}</p>
          <p><strong>Spread:</strong> {latestTrade.spread.toFixed(2)}</p>
          <p><strong>AAPL Price:</strong> ${latestTrade.aapl_price.toFixed(2)}</p>
          <p><strong>MSFT Price:</strong> ${latestTrade.msft_price.toFixed(2)}</p>
          <p><strong>Timestamp:</strong> {new Date(latestTrade.timestamp).toLocaleString()}</p>
        </CardContent>
      </Card>
    </div>
  );
}

