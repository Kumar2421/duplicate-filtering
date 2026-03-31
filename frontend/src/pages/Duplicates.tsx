import React, { useMemo, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchDuplicateClusters, fetchAvailableDates, sendConvertAction, deleteEvent, BASE_URL } from '../services/api';
import { Button } from '../components/ui/button';
import { Search, MapPin, Check, X, RefreshCw, AlertCircle, Layers, Code, Copy, AlertTriangle } from 'lucide-react';
import { toast } from 'sonner';
import { Input } from '../components/ui/input';
import { DateSelector } from '../components/DateSelector';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogFooter,
} from "../components/ui/dialog";
import { Checkbox } from "../components/ui/checkbox";
import { Label } from "../components/ui/label";

const Duplicates: React.FC = () => {
  const { currentBranch, dateRange, setDateRange } = useAppStore();
  const [searchQuery, setSearchQuery] = React.useState('');
  const [showSidebar, setShowSidebar] = React.useState(true);
  const [selectedCluster, setSelectedCluster] = React.useState<any>(null);
  const [selectedIds, setSelectedIds] = React.useState<string[]>([]);
  const [toEmployee, setToEmployee] = React.useState(false);
  const [isSubmitting, setIsSubmitting] = React.useState(false);
  const [apiKey, setApiKey] = React.useState('');

  const { data: availableDatesData } = useQuery({
    queryKey: ['available-dates', currentBranch],
    queryFn: () => fetchAvailableDates(currentBranch),
  });

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['duplicate-clusters', currentBranch, dateRange.startDate],
    queryFn: () => fetchDuplicateClusters(currentBranch, dateRange.startDate),
  });

  useEffect(() => {
    if (availableDatesData?.dates?.length > 0) {
      const latestDate = availableDatesData.dates[0];
      if (dateRange.startDate !== latestDate && !availableDatesData.dates.includes(dateRange.startDate)) {
        setDateRange({ startDate: latestDate, endDate: latestDate });
      }
    }
  }, [availableDatesData, currentBranch, setDateRange]);

  const handleIdToggle = (id: string) => {
    setSelectedIds(prev => {
      if (prev.includes(id)) {
        return prev.filter(i => i !== id);
      }
      if (prev.length >= 2) {
        toast.error("You can only select up to 2 IDs");
        return prev;
      }
      return [...prev, id];
    });
  };

  const handleConvertSubmit = async () => {
    if (selectedIds.length === 0) {
      toast.error("Select at least one ID");
      return;
    }

    const selectedVisits = selectedCluster?.visits?.filter((v: any) =>
      selectedIds.includes(v.customerId)
    ) || [];

    if (selectedVisits.length === 0) {
      toast.error("Could not find visit data for selected IDs");
      return;
    }

    setIsSubmitting(true);
    try {
      const result = await sendConvertAction({
        customerId1: selectedVisits[0].customerId,
        customerId2: (selectedVisits[1] || selectedVisits[0]).customerId,
        toEmployee: toEmployee,
        branchId: currentBranch,
        api_key: apiKey
      });

      if (result.success) {
        toast.success("Conversion request sent");
        setSelectedCluster(null);
        setSelectedIds([]);
        refetch();
      }
    } catch (err) {
      // toast.error handled in service
    } finally {
      setIsSubmitting(true); // Keep submitting true until page refreshes or dialog closes
      setTimeout(() => setIsSubmitting(false), 2000);
    }
  };

  const handleDeleteImage = async (visitId: string, eventId: string | null) => {
    if (!eventId) {
      toast.error("Cannot delete image: Missing Event ID");
      return;
    }

    if (!window.confirm("Are you sure you want to delete this image? This action cannot be undone.")) {
      return;
    }

    try {
      const result = await deleteEvent({
        branchId: currentBranch,
        visitId: visitId,
        eventId: eventId,
        api_key: apiKey
      });

      if (result.success) {
        toast.success("Image deleted successfully");
        refetch();
      }
    } catch (err) {
      // toast.error handled in service
    }
  };

  const filteredClusters = data?.clusters?.filter((c: any) => {
    const hasConflictIds = c.visits?.some((v: any) => v.conflictIds && v.conflictIds.length > 0);
    const isConflictType = c.type === 'conflict' || hasConflictIds;
    const hasMultipleVisits = (c.visits?.length || 0) >= 2;
    const isDuplicateType = c.type === 'duplicate' || hasMultipleVisits;

    if (!isConflictType && !isDuplicateType) return false;

    if (!searchQuery.trim()) return true;

    const query = searchQuery.toLowerCase().trim();
    const hasMatchingCustomerId = c.customerIds?.some((id: string) =>
      id.toLowerCase().includes(query)
    );
    const hasMatchingClusterId = c.clusterId?.toLowerCase().includes(query);
    const hasMatchingVisitId = c.visits?.some((v: any) =>
      v.visitId?.toLowerCase().includes(query)
    );

    return hasMatchingCustomerId || hasMatchingClusterId || hasMatchingVisitId;
  }) || [];

  const duplicateGroupsJson = useMemo(() => {
    if (!filteredClusters.length) return null;
    return filteredClusters.map((c: any) => ({
      clusterId: c.clusterId,
      type: c.type,
      customerIds: c.customerIds,
      visitIds: c.visits.map((v: any) => v.visitId)
    }));
  }, [filteredClusters]);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success("Copied to clipboard");
  };

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
        <AlertCircle className="w-12 h-12 text-red-500" />
        <h2 className="text-xl font-bold text-slate-800">Connection Failed</h2>
        <Button onClick={() => refetch()} variant="outline">Retry Sync</Button>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-slate-50 overflow-hidden">
      <div className="flex-1 overflow-y-auto">
        <div className="p-6 space-y-6">
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
            <div className="space-y-1">
              <h1 className="text-3xl font-black text-slate-900 tracking-tight flex items-center gap-3">
                <Layers className="text-blue-600" />
                Duplicate Detection
              </h1>
              <p className="text-slate-500 font-bold text-xs uppercase tracking-widest">
                Showing {filteredClusters.length} clusters for {dateRange.startDate}
              </p>
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                onClick={() => setShowSidebar(!showSidebar)}
                className="bg-white border shadow-sm font-black text-xs uppercase tracking-widest px-4 h-11 rounded-xl"
              >
                <Code className="w-4 h-4 mr-2" />
                {showSidebar ? 'Hide JSON' : 'Show JSON'}
              </Button>
              <Button
                variant="secondary"
                onClick={() => refetch()}
                disabled={isLoading}
                className="bg-white border shadow-sm font-black text-xs uppercase tracking-widest px-6 h-11 rounded-xl"
              >
                <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
                Refresh Registry
              </Button>
            </div>
          </div>

          <div className="flex flex-col md:flex-row gap-3 bg-white p-3 rounded-2xl border shadow-sm items-center">
            <div className="flex-1 relative w-full">
              <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 w-4 h-4" />
              <Input
                placeholder="Filter by Cluster ID or Customer Profile..."
                className="pl-12 h-11 bg-slate-50 border-none rounded-xl text-sm font-medium"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
            <div className="flex gap-2 w-full md:w-auto">
              <div className="flex-1 md:w-64 relative">
                <Input
                  type="password"
                  placeholder="Enter API Key..."
                  className="h-11 bg-white border-slate-200 rounded-xl text-sm font-medium pr-10"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                />
                <div className="absolute right-3 top-1/2 -translate-y-1/2">
                  <div className={`w-2 h-2 rounded-full ${apiKey ? 'bg-emerald-500' : 'bg-slate-300'}`} />
                </div>
              </div>
              <div className="hidden lg:flex items-center gap-2 px-4 bg-slate-100 rounded-xl text-[10px] font-black uppercase text-slate-600">
                <MapPin className="w-3 h-3 text-blue-500" /> {currentBranch}
              </div>
              <DateSelector />
              <Button className="flex-1 md:flex-initial h-11 bg-blue-600 hover:bg-blue-700 text-white font-black uppercase text-xs tracking-widest px-8 rounded-xl shadow-lg shadow-blue-100">
                Apply Filters
              </Button>
            </div>
          </div>

          {isLoading ? (
            <div className="space-y-8">
              {[1, 2, 3].map(i => (
                <div key={i} className="space-y-4">
                  <div className="h-6 w-48 bg-slate-200 animate-pulse rounded" />
                  <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                    {Array.from({ length: 8 }).map((_, j) => (
                      <div key={j} className="aspect-[3/4] bg-slate-200 animate-pulse rounded-xl" />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="space-y-10">
              {filteredClusters.map((cluster: any, idx: number) => (
                <div key={cluster.clusterId || idx} className="space-y-4">
                  <div className="flex items-center justify-between border-b border-slate-200 pb-3">
                    <div className="flex items-center gap-4">
                      <div className="flex flex-wrap gap-2">
                        {cluster.customerIds?.map((cid: string) => (
                          <span key={cid} className="text-[10px] font-black bg-slate-900 text-white px-2 py-0.5 rounded-lg uppercase tracking-tighter">
                            {cid}
                          </span>
                        ))}
                      </div>
                      <span className={`text-[9px] font-black uppercase tracking-widest px-2 py-0.5 rounded-full border ${(cluster.type === 'conflict' || cluster.visits?.some((v: any) => v.conflictIds?.length > 0))
                        ? 'bg-red-50 text-red-600 border-red-100'
                        : 'bg-amber-50 text-amber-600 border-amber-100'
                        }`}>
                        {(cluster.type === 'conflict' || cluster.visits?.some((v: any) => v.conflictIds?.length > 0)) ? 'Conflict' : 'Duplicate'}
                      </span>
                      <span className="text-xs font-bold text-slate-400">
                        {cluster.visits?.length || 0} Visits
                      </span>
                    </div>

                    <div className="flex gap-2">
                      <Dialog open={selectedCluster?.clusterId === cluster.clusterId} onOpenChange={(open: boolean) => {
                        if (open) {
                          setSelectedCluster(cluster);
                          setSelectedIds([]);
                        } else {
                          setSelectedCluster(null);
                        }
                      }}>
                        <DialogTrigger asChild>
                          <Button
                            size="sm"
                            className="h-8 bg-emerald-600 hover:bg-emerald-700 text-white font-black text-[10px] uppercase px-4 rounded-lg shadow-lg shadow-emerald-100/50"
                          >
                            <Check size={12} className="mr-1" />
                            Approve & Convert
                          </Button>
                        </DialogTrigger>
                        <DialogContent className="sm:max-w-[425px] bg-white rounded-2xl border-none shadow-2xl">
                          <DialogHeader>
                            <DialogTitle className="text-xl font-black uppercase tracking-tight text-slate-900">
                              Convert Customer IDs
                            </DialogTitle>
                            <p className="text-xs font-bold text-slate-500 uppercase tracking-widest mt-1">
                              Select up to 2 IDs to merge/convert
                            </p>
                          </DialogHeader>

                          <div className="py-6 space-y-6">
                            <div className="grid grid-cols-2 gap-3">
                              {Array.from(new Set(cluster.customerIds || [])).map((id: any) => (
                                <button
                                  key={id}
                                  onClick={() => handleIdToggle(id)}
                                  className={`p-4 rounded-xl border-2 transition-all flex flex-col items-center gap-2 group ${selectedIds.includes(id)
                                    ? 'border-blue-500 bg-blue-50/50 shadow-inner'
                                    : 'border-slate-100 bg-slate-50 hover:border-slate-200'
                                    }`}
                                >
                                  <div className={`w-6 h-6 rounded-full border-2 flex items-center justify-center transition-colors ${selectedIds.includes(id) ? 'bg-blue-500 border-blue-500 text-white' : 'border-slate-300 bg-white'
                                    }`}>
                                    {selectedIds.includes(id) && <Check size={12} strokeWidth={4} />}
                                  </div>
                                  <span className={`text-xs font-black uppercase tracking-tighter ${selectedIds.includes(id) ? 'text-blue-700' : 'text-slate-600'
                                    }`}>
                                    {id}
                                  </span>
                                </button>
                              ))}
                            </div>

                            <div className="flex items-center space-x-3 bg-slate-50 p-4 rounded-xl border border-slate-100">
                              <Checkbox
                                id="toEmployee"
                                checked={toEmployee}
                                onCheckedChange={(checked: boolean) => setToEmployee(checked)}
                                className="w-5 h-5 rounded-md border-slate-300 data-[state=checked]:bg-blue-600 data-[state=checked]:border-blue-600"
                              />
                              <div className="grid gap-1.5 leading-none">
                                <Label
                                  htmlFor="toEmployee"
                                  className="text-sm font-black uppercase tracking-widest text-slate-700 cursor-pointer"
                                >
                                  Convert to Employee
                                </Label>
                                <p className="text-[10px] font-bold text-slate-400 uppercase tracking-tighter">
                                  Mark these profiles as internal staff
                                </p>
                              </div>
                            </div>
                          </div>

                          <DialogFooter className="sm:justify-start gap-2 pt-2 border-t border-slate-100">
                            <Button
                              type="button"
                              disabled={selectedIds.length === 0 || isSubmitting}
                              onClick={handleConvertSubmit}
                              className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-black uppercase text-xs tracking-widest h-12 rounded-xl shadow-lg shadow-blue-200"
                            >
                              {isSubmitting ? (
                                <RefreshCw className="w-4 h-4 animate-spin" />
                              ) : (
                                `Convert ${selectedIds.length} ${selectedIds.length === 1 ? 'ID' : 'IDs'}`
                              )}
                            </Button>
                            <Button
                              type="button"
                              variant="ghost"
                              onClick={() => setSelectedCluster(null)}
                              className="px-6 text-slate-400 font-black uppercase text-[10px] tracking-widest hover:text-slate-600 hover:bg-slate-100 rounded-xl h-12"
                            >
                              Cancel
                            </Button>
                          </DialogFooter>
                        </DialogContent>
                      </Dialog>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                    {cluster.visits.flatMap((visit: any) => {
                      const images = visit.allImages && visit.allImages.length > 0
                        ? visit.allImages
                        : [{ url: visit.image || visit.imageUrl, name: 'primary.jpg', isPrimary: true }];

                      return images.map((img: any, iIdx: number) => (
                        <div key={`${visit.visitId}-${img.name}-${iIdx}`} className="group relative aspect-[3/4] rounded-xl overflow-hidden bg-white border border-slate-100 shadow-sm transition-all cursor-pointer">
                          <img
                            src={img.url.startsWith('/') ? `${BASE_URL}${img.url}` : img.url}
                            className="w-full h-full object-cover"
                            onError={(e: any) => e.target.src = 'https://placehold.co/300x400?text=No+Photo'}
                            loading="lazy"
                          />

                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              handleDeleteImage(visit.visitId, img.eventId);
                            }}
                            className="absolute top-2 right-2 z-20 p-1.5 bg-red-500/80 hover:bg-red-600 text-white rounded-lg opacity-0 group-hover:opacity-100 transition-all backdrop-blur-sm shadow-sm"
                            title="Reject/Delete Event"
                          >
                            <X size={12} />
                          </button>

                          {(visit.conflict || (visit.conflictIds && visit.conflictIds.length > 0)) && (
                            <div className="absolute inset-0 bg-slate-900/40 opacity-0 group-hover:opacity-100 transition-opacity flex flex-col justify-end p-2 pb-3 backdrop-blur-[1px]">
                              <div className="flex flex-wrap gap-1">
                                <span className="bg-red-500 text-white text-[7px] font-black px-1.5 py-0.5 rounded shadow-sm flex items-center gap-1 uppercase tracking-tighter">
                                  <AlertTriangle size={8} />
                                  Conflict
                                </span>
                              </div>
                              {visit.conflictIds && visit.conflictIds.length > 0 && (
                                <div className="mt-2 space-y-1">
                                  <p className="text-[7px] font-black text-red-400 uppercase">Conflicts Found:</p>
                                  <div className="flex flex-wrap gap-1">
                                    {visit.conflictIds.map((cid: string) => (
                                      <span key={cid} className="text-[7px] bg-red-500/80 text-white px-1 rounded font-bold">
                                        {cid.slice(-6)}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      ));
                    })}
                  </div>
                </div>
              ))}
            </div>
          )}

          {filteredClusters.length === 0 && !isLoading && (
            <div className="flex flex-col items-center justify-center py-32 bg-white rounded-3xl border border-dashed border-slate-200 shadow-inner">
              <AlertCircle className="w-16 h-16 text-slate-200 mb-4" />
              <h3 className="text-xl font-black text-slate-900 uppercase tracking-widest">Inbox Zero</h3>
              <p className="text-slate-400 font-bold mt-2">No pending duplicates detected for this period.</p>
            </div>
          )}
        </div>
      </div>

      {showSidebar && (
        <div className="w-80 bg-white border-l border-slate-200 flex flex-col h-full shadow-2xl relative z-10 animate-in slide-in-from-right duration-300">
          <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
            <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-500 flex items-center gap-2">
              <Code className="w-3 h-3 text-blue-500" />
              Duplicate Registry (JSON)
            </h2>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => copyToClipboard(JSON.stringify(duplicateGroupsJson, null, 2))}
              className="h-7 px-2 text-[9px] font-black uppercase tracking-tighter hover:bg-blue-50 hover:text-blue-600 transition-colors"
            >
              <Copy className="w-3 h-3 mr-1" />
              Copy All
            </Button>
          </div>

          <div className="flex-1 overflow-y-auto p-4 space-y-4 custom-scrollbar">
            {duplicateGroupsJson ? (
              duplicateGroupsJson.map((group: any, i: number) => (
                <div key={group.clusterId || i} className="group relative">
                  <div className="bg-slate-900 rounded-xl p-3 shadow-sm border border-slate-800 hover:border-blue-500/50 transition-colors">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-[8px] font-black text-blue-400 uppercase tracking-widest">
                        Group #{i + 1}
                      </span>
                      <span className={`text-[7px] font-black uppercase px-1.5 py-0.5 rounded border ${group.type === 'duplicate' ? 'bg-red-500/10 text-red-400 border-red-500/20' : 'bg-amber-500/10 text-amber-400 border-amber-500/20'}`}>
                        {group.type}
                      </span>
                    </div>
                    <pre className="text-[10px] text-slate-300 font-mono leading-relaxed overflow-x-auto">
                      {JSON.stringify({
                        customerIds: group.customerIds,
                        visitIds: group.visitIds
                      }, null, 2)}
                    </pre>
                    <button
                      onClick={() => copyToClipboard(JSON.stringify(group, null, 2))}
                      className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 p-1.5 bg-slate-800 rounded-lg text-slate-400 hover:text-white transition-all shadow-lg"
                    >
                      <Copy className="w-3 h-3" />
                    </button>
                  </div>
                </div>
              ))
            ) : (
              <div className="flex flex-col items-center justify-center h-full text-slate-400 space-y-2">
                <AlertCircle className="w-8 h-8 opacity-20" />
                <p className="text-[10px] font-black uppercase tracking-widest opacity-40">No Data to Export</p>
              </div>
            )}
          </div>

          <div className="p-4 bg-slate-50 border-t border-slate-100">
            <div className="flex items-center justify-between text-[8px] font-black text-slate-400 uppercase tracking-widest">
              <span>Total Groups: {filteredClusters.length}</span>
              <span>v1.0.4</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Duplicates;
