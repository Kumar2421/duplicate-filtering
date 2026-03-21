import React from 'react';
import { useSystemMetrics } from '../hooks/useMetrics';
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'; // Later

const Dashboard: React.FC = () => {
  const { data: metrics, isLoading } = useSystemMetrics();

  // Mock stats if not available
  const stats = metrics?.stats || {
    totalVisits: 2542,
    totalImages: 8642,
    uniqueCustomers: 341,
    duplicateCases: 12,
  };

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-3xl font-bold">System Overview</h1>
      
      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          { label: 'Total Visits', value: stats.totalVisits },
          { label: 'Total Images', value: stats.totalImages },
          { label: 'Unique Customers', value: stats.uniqueCustomers },
          { label: 'Duplicate Cases', value: stats.duplicateCases },
        ].map((s) => (
          <Card key={s.label}>
            <CardHeader className="py-4">
              <CardTitle className="text-sm font-medium text-gray-500 uppercase tracking-wider">{s.label}</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{s.value.toLocaleString()}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* System Metrics */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {[
          { label: 'GPU usage', usage: metrics?.gpuUsage || 45 },
          { label: 'CPU usage', usage: metrics?.cpuUsage || 32 },
          { label: 'Memory usage', usage: metrics?.memoryUsage || 60 },
        ].map((m) => (
          <Card key={m.label}>
            <CardHeader>
              <CardTitle className="text-sm font-medium">{m.label}</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                <div className="w-full bg-gray-200 rounded-full h-2.5">
                  <div 
                    className="bg-blue-600 h-2.5 rounded-full transition-all duration-500" 
                    style={{ width: `${m.usage}%` }}
                  ></div>
                </div>
                <p className="text-right text-sm font-semibold">{m.usage}%</p>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Chart Placeholder */}
      <Card className="p-4 bg-gray-50 h-[300px] flex items-center justify-center text-gray-400 border-dashed">
        <div className="text-center">
          <p className="font-medium">System Processing History</p>
          <p className="text-sm">(Chart placeholder - Use static SVG/Canvas in real setup)</p>
          <div className="mt-4 w-full h-24 flex items-end gap-1 justify-center">
            {Array.from({length: 20}).map((_, i) => (
              <div 
                key={i} 
                className="w-2 bg-blue-300" 
                style={{ height: `${Math.random() * 100}%` }}
              ></div>
            ))}
          </div>
        </div>
      </Card>
    </div>
  );
};

export default Dashboard;
