import React, { useState, useEffect } from 'react';
import { useSystemMetrics } from '../hooks/useMetrics';
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card';
import { useAppStore } from '../store/useStore';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { triggerIngest, checkIngestStatus } from '../services/api';
import { Database, Calendar, Key, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';

const Dashboard: React.FC = () => {
  const { currentBranch, setCurrentBranch } = useAppStore();
  const [apiKey, setApiKey] = useState('');
  const [manualBranch, setManualBranch] = useState(currentBranch);
  const [targetDate, setTargetDate] = useState(new Date().toISOString().split('T')[0]);
  const [isProcessing, setIsProcessing] = useState(false);
  const { data: metrics, refetch: refetchMetrics } = useSystemMetrics(currentBranch, targetDate);

  const [ingestMessage, setIngestMessage] = useState('');

  useEffect(() => {
    setManualBranch(currentBranch);
  }, [currentBranch]);

  useEffect(() => {
    let pollInterval: NodeJS.Timeout;
    if (isProcessing) {
      pollInterval = setInterval(async () => {
        try {
          const res = await checkIngestStatus(manualBranch, targetDate);
          if (res.status === 'completed') {
            setIsProcessing(false);
            setIngestMessage('');
            toast.success(`Data Ingestion for ${manualBranch} Completed!`);

            if (manualBranch === currentBranch) {
              refetchMetrics();
            } else {
              toast.info(`Switching to branch ${manualBranch} to view new data`);
              setCurrentBranch(manualBranch);
            }
            clearInterval(pollInterval);
          } else if (res.message) {
            setIngestMessage(res.message);
          }
        } catch (e) {
          console.error("Polling error", e);
        }
      }, 5000);
    }
    return () => clearInterval(pollInterval);
  }, [isProcessing, manualBranch, targetDate, refetchMetrics, currentBranch, setCurrentBranch]);

  const handleIngest = async () => {
    if (!apiKey) {
      toast.error("Please enter an API Key");
      return;
    }
    if (!manualBranch) {
      toast.error("Please enter a Branch ID");
      return;
    }
    setIsProcessing(true);
    try {
      await triggerIngest(manualBranch, targetDate, apiKey);
      toast.info(`Ingestion started for ${manualBranch} in background...`);
    } catch (err) {
      setIsProcessing(false);
      toast.error("Failed to start ingestion");
    }
  };

  // Mock stats if not available
  const stats = metrics?.stats || {
    totalVisits: 0,
    totalImages: 0,
    uniqueCustomers: 0,
    duplicateCases: 0,
    conflictCases: 0,
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-bold">System Overview</h1>
      </div>

      {/* Control Panel: API Fetching */}
      <Card className="border-blue-100 bg-blue-50/30">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Database className="w-5 h-5 text-blue-600" />
            Manual Data Ingestion
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="space-y-2">
              <label className="text-xs font-bold uppercase text-slate-500">Branch ID</label>
              <div className="relative">
                <Database className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <Input
                  placeholder="e.g. EA-NATURALS"
                  value={manualBranch}
                  onChange={(e) => setManualBranch(e.target.value)}
                  className="pl-10 h-11 bg-white border-slate-200"
                />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-xs font-bold uppercase text-slate-500">Target Date</label>
              <div className="relative">
                <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <Input
                  type="date"
                  value={targetDate}
                  onChange={(e) => setTargetDate(e.target.value)}
                  className="pl-10 h-11 bg-white border-slate-200"
                />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-xs font-bold uppercase text-slate-500">API Key</label>
              <div className="relative">
                <Key className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <Input
                  placeholder="Enter token..."
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  className="pl-10 h-11 bg-white border-slate-200"
                />
              </div>
            </div>
            <div className="flex items-end">
              <Button
                onClick={handleIngest}
                disabled={isProcessing}
                className={`w-full h-11 font-bold transition-all ${isProcessing ? 'bg-slate-200' : 'bg-blue-600 hover:bg-blue-700 shadow-lg text-white'}`}
              >
                {isProcessing ? (
                  <>
                    <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                    Processing...
                  </>
                ) : (
                  "Start Ingestion"
                )}
              </Button>
            </div>
          </div>

          {isProcessing && (
            <div className="mt-4 p-3 bg-white rounded-lg border border-blue-100 flex items-center gap-3 animate-pulse">
              <div className="w-2 h-2 bg-blue-600 rounded-full animate-ping" />
              <p className="text-xs font-semibold text-blue-800">
                {ingestMessage || `Backend is currently fetching visits and generating embeddings for ${manualBranch} on ${targetDate}.`}
              </p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        {[
          { label: 'Total Visits', value: stats.totalVisits },
          { label: 'Total Images', value: stats.totalImages },
          { label: 'Unique Customers', value: stats.uniqueCustomers },
          { label: 'Duplicate Groups', value: stats.duplicateCases, color: 'text-amber-600' },
          { label: 'Conflict Groups', value: stats.conflictCases, color: 'text-red-600' },
        ].map((s) => (
          <Card key={s.label}>
            <CardHeader className="py-4">
              <CardTitle className="text-sm font-medium text-gray-500 uppercase tracking-wider">{s.label}</CardTitle>
            </CardHeader>
            <CardContent>
              <p className={`text-2xl font-bold ${s.color || ''}`}>{s.value.toLocaleString()}</p>
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
            {Array.from({ length: 20 }).map((_, i) => (
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
