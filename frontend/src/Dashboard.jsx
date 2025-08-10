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
    const interval = setInterval(fetchData, 5000); // Update every 5 seconds
    return () => clearInterval(interval);
  }, []);

  if (!data || !priceData || !zscoreData || !pnlData) return <div className="text-center p-6">Loading...</div>;
  
  // Additional check to ensure we have data to display
  if (data.length === 0 && zscoreData.length === 0) {
    return (
      <div className="text-center p-6 bg-gray-900 min-h-screen text-white">
        <div className="bg-gray-800 p-8 rounded-lg max-w-md mx-auto">
          <h2 className="text-xl font-bold mb-4">No Data Available</h2>
          <p className="text-gray-400">The system is still collecting data. Please check back later.</p>
        </div>
      </div>
    );
  }

  // Dedicated Z-Score chart data using entire database
  const zScoreData = {
    labels: zscoreData.length > 0 ? zscoreData.map(d => new Date(d.timestamp).toLocaleDateString()) : [],
    datasets: [
      {
        label: "Z-Score",
        data: zscoreData.length > 0 ? zscoreData.map(d => d.z_score) : [],
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
    labels: data.length > 0 ? data.map(d => new Date(d.timestamp).toLocaleDateString()) : [],
    datasets: [
      {
        label: "Z-Score",
        data: data.length > 0 ? data.map(d => d.z_score) : [],
        borderColor: "#38bdf8",
        tension: 0.4,
        fill: false
      },
      {
        label: "Spread",
        data: data.length > 0 ? data.map(d => d.spread) : [],
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
    labels: aapl.length > 0 ? aapl.map(p => new Date(p.timestamp).toLocaleDateString()) : [],
    datasets: [
      {
        label: "AAPL",
        data: aapl.length > 0 ? aapl.map(p => p.close) : [],
        borderColor: "#22c55e",
        tension: 0.4,
        fill: false
      },
      {
        label: "MSFT",
        data: msft.length > 0 ? msft.map(p => p.close) : [],
        borderColor: "#a78bfa",
        tension: 0.4,
        fill: false
      }
    ]
  };

  const latestTrade = data.length > 0 ? data[data.length - 1] : null;
  const latestZScore = zscoreData.length > 0 ? zscoreData[zscoreData.length - 1].z_score : 0;
  
  // Prepare PnL chart data
  const pnlChartData = {
    labels: pnlData.length > 0 ? pnlData.map(d => new Date(d.timestamp).toLocaleDateString()) : [],
    datasets: [
      {
        label: "Cumulative PnL",
        data: pnlData.length > 0 ? pnlData.map(d => d.cumulative_pnl) : [],
        borderColor: "#10b981",
        backgroundColor: "rgba(16, 185, 129, 0.1)",
        borderWidth: 3,
        tension: 0.4,
        fill: true,
        pointRadius: 4,
        pointHoverRadius: 8,
        pointBackgroundColor: "#10b981",
        pointBorderColor: "#ffffff"
      }
    ]
  };

  // PnL chart options
  const pnlChartOptions = {
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
        bodyColor: '#ffffff',
        callbacks: {
          label: function(context) {
            return `PnL: $${context.parsed.y.toFixed(2)}`;
          }
        }
      },
      annotation: {
        annotations: {
          zeroLine: {
            type: 'line',
            yMin: 0,
            yMax: 0,
            borderColor: 'rgba(156, 163, 175, 0.5)',
            borderWidth: 2,
            borderDash: [5, 5]
          }
        }
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
          color: "#9ca3af",
          callback: function(value) {
            return `$${value.toFixed(2)}`;
          }
        },
        grid: {
          color: "rgba(156, 163, 175, 0.2)"
        },
        beginAtZero: false
      }
    },
    elements: {
      point: {
        hoverBackgroundColor: "#10b981"
      }
    }
  };
  
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
      {/* PnL Chart */}
      <Card className="bg-gray-800 md:col-span-2">
        <CardContent>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-bold">Profit & Loss History</h2>
            <div className="flex items-center space-x-4">
              <div className="text-sm text-gray-400">
                Total Trades: {data.filter(t => t.status === 'closed').length}
              </div>
              <div className="text-sm text-gray-400">
                Open Position: {data.filter(t => t.status === 'open').length > 0 ? 'Yes' : 'No'}
              </div>
              <div className={`text-sm font-semibold ${pnlData.length > 0 && pnlData[pnlData.length - 1].cumulative_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                ${pnlData.length > 0 ? pnlData[pnlData.length - 1].cumulative_pnl.toFixed(2) : '0.00'}
              </div>
            </div>
          </div>
          <div className="h-[400px]" style={{ minHeight: '400px', height: '400px' }}>
            <Line data={pnlChartData} options={pnlChartOptions} />
          </div>
          <div className="mt-4 grid grid-cols-3 gap-4 text-sm">
            <div className="bg-gray-700 p-3 rounded">
              <div className="text-gray-400">Current PnL</div>
              <div className={`text-xl font-bold ${pnlData.length > 0 && pnlData[pnlData.length - 1].cumulative_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                ${pnlData.length > 0 ? pnlData[pnlData.length - 1].cumulative_pnl.toFixed(2) : '0.00'}
              </div>
            </div>
            <div className="bg-gray-700 p-3 rounded">
              <div className="text-gray-400">Total Trades</div>
              <div className="text-xl font-bold">{data.filter(t => t.status === 'closed').length}</div>
            </div>
            <div className="bg-gray-700 p-3 rounded">
              <div className="text-gray-400">Last Updated</div>
              <div className="text-sm font-semibold">
                {pnlData.length > 0 ? new Date(pnlData[pnlData.length - 1].timestamp).toLocaleTimeString() : 'N/A'}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

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
          {latestTrade ? (
            <>
              <p><strong>Action:</strong> {latestTrade.action}</p>
              <p><strong>Z-Score:</strong> {latestTrade.z_score.toFixed(2)}</p>
              <p><strong>Spread:</strong> {latestTrade.spread.toFixed(2)}</p>
              <p><strong>AAPL Price:</strong> ${latestTrade.aapl_price.toFixed(2)}</p>
              <p><strong>MSFT Price:</strong> ${latestTrade.msft_price.toFixed(2)}</p>
              <p><strong>Timestamp:</strong> {new Date(latestTrade.timestamp).toLocaleString()}</p>
            </>
          ) : (
            <p className="text-gray-400">No trades available yet</p>
          )}
        </CardContent>
      </Card>

      <Card className="bg-gray-800">
        <CardContent>
          <h2 className="text-xl font-bold mb-4">PnL Summary</h2>
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-gray-400">Total Realized PnL:</span>
              <span className={`font-semibold ${pnlData.filter(p => p.type === 'realized').length > 0 ? 
                (pnlData.filter(p => p.type === 'realized').slice(-1)[0].cumulative_pnl >= 0 ? 'text-green-400' : 'text-red-400') : 'text-gray-400'}`}>
                ${pnlData.filter(p => p.type === 'realized').length > 0 ? 
                  pnlData.filter(p => p.type === 'realized').slice(-1)[0].cumulative_pnl.toFixed(2) : '0.00'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Current Unrealized PnL:</span>
              <span className={`font-semibold ${pnlData.filter(p => p.type === 'unrealized').length > 0 ? 
                (pnlData.filter(p => p.type === 'unrealized').slice(-1)[0].cumulative_pnl >= 0 ? 'text-green-400' : 'text-red-400') : 'text-gray-400'}`}>
                ${pnlData.filter(p => p.type === 'unrealized').length > 0 ? 
                  (pnlData.filter(p => p.type === 'unrealized').slice(-1)[0].cumulative_pnl - 
                   (pnlData.filter(p => p.type === 'realized').length > 0 ? 
                     pnlData.filter(p => p.type === 'realized').slice(-1)[0].cumulative_pnl : 0)).toFixed(2) : '0.00'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Win Rate:</span>
              <span className="font-semibold">
                {data.filter(t => t.status === 'closed' && t.pnl > 0).length > 0 ? 
                  ((data.filter(t => t.status === 'closed' && t.pnl > 0).length / 
                    data.filter(t => t.status === 'closed').length) * 100).toFixed(1) : '0'}%
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Average Trade PnL:</span>
              <span className={`font-semibold ${data.filter(t => t.status === 'closed' && t.pnl).length > 0 ? 
                (data.filter(t => t.status === 'closed').reduce((sum, t) => sum + (t.pnl || 0), 0) / 
                 data.filter(t => t.status === 'closed').length >= 0 ? 'text-green-400' : 'text-red-400') : 'text-gray-400'}`}>
                ${data.filter(t => t.status === 'closed' && t.pnl).length > 0 ? 
                  (data.filter(t => t.status === 'closed').reduce((sum, t) => sum + (t.pnl || 0), 0) / 
                   data.filter(t => t.status === 'closed').length).toFixed(2) : '0.00'}
              </span>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

