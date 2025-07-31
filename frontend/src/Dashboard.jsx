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

  useEffect(() => {
    async function fetchData() {
      const res = await fetch("http://localhost:5050/trade-history");
      const json = await res.json();
      setData(json);
    }
    fetchData();
  }, []);

  if (!data) return <div className="text-center p-6">Loading...</div>;

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

  const latestTrade = data[data.length - 1];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 p-6 bg-gray-900 min-h-screen text-white">
      <Card className="bg-gray-800">
        <CardContent>
          <h2 className="text-xl font-bold mb-4">Z-Score and Spread Over Time</h2>
          <Line data={chartData} />
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

