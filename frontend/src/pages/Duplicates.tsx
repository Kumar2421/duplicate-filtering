import React, { useMemo, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchDuplicateClusters, fetchAvailableDates, sendConvertAction, fetchConvertJobStatus, deleteEvent, fetchDeleteStats, BASE_URL } from '../services/api';
import { Button } from '../components/ui/button';
import { Search, MapPin, Check, X, RefreshCw, AlertCircle, Layers, Code, Copy, UserCircle, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Input } from '../components/ui/input';
import { DateSelector } from '../components/DateSelector';
import { TimePicker } from '../components/TimePicker';
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
  const [showSidebar, setShowSidebar] = React.useState(false);
  const [selectedCluster, setSelectedCluster] = React.useState<any>(null);
  const [selectedIds, setSelectedIds] = React.useState<string[]>([]);
  const [toEmployee, setToEmployee] = React.useState(false);
  const [isSubmitting, setIsSubmitting] = React.useState(false);
  const [deletingEventKeys, setDeletingEventKeys] = React.useState<Set<string>>(new Set());
  const [deletedEventKeys, setDeletedEventKeys] = React.useState<Set<string>>(new Set());
  const [timeFromDraft, setTimeFromDraft] = React.useState<string>('');
  const [timeFrom, setTimeFrom] = React.useState<string>('');
  const [timeTo, setTimeTo] = React.useState<string>('');

  const { data: availableDatesData } = useQuery({
    queryKey: ['available-dates', currentBranch],
    queryFn: () => fetchAvailableDates(currentBranch),
  });

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['duplicate-clusters', currentBranch, dateRange.startDate],
    queryFn: () => fetchDuplicateClusters(currentBranch, dateRange.startDate),
  });

  const { data: deleteStats, refetch: refetchStats } = useQuery({
    queryKey: ['delete-stats', currentBranch, dateRange.startDate],
    queryFn: () => fetchDeleteStats(currentBranch, dateRange.startDate),
    refetchInterval: 5000,
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
      // Get branch token from localStorage for direct API calls
      const branchToken = localStorage.getItem(`branch_token_${currentBranch}`);

      const result = await sendConvertAction({
        customerId1: selectedVisits[0].customerId,
        customerId2: (selectedVisits[1] || selectedVisits[0]).customerId,
        toEmployee: toEmployee,
        branchId: currentBranch,
        api_key: branchToken || undefined
      });

      const jobId = result?.jobId;
      if (!jobId) {
        toast.error(result?.error || "Convert job could not be started");
        return;
      }

      // Poll job status
      const startedAt = Date.now();
      const timeoutMs = 90_000;
      while (true) {
        if (Date.now() - startedAt > timeoutMs) {
          toast.error("Convert is taking too long. Please refresh and check again.");
          break;
        }

        await new Promise((r) => setTimeout(r, 1500));
        const status = await fetchConvertJobStatus(jobId);

        if (status?.status === 'success') {
          toast.success('Conversion successful');
          setSelectedCluster(null);
          setSelectedIds([]);
          refetch();
          refetchStats();
          break;
        }

        if (status?.status === 'error') {
          toast.error(status?.error || 'Conversion failed');
          break;
        }
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

    const eventKey = `${visitId}:${eventId}`;
    setDeletingEventKeys(prev => {
      const next = new Set(prev);
      next.add(eventKey);
      return next;
    });

    try {
      const branchToken = localStorage.getItem(`branch_token_${currentBranch}`);
      const result = await deleteEvent({
        branchId: currentBranch,
        visitId: visitId,
        eventId: eventId,
        api_key: branchToken || undefined
      });

      if (result.success) {
        toast.success("Image deleted successfully");
        setDeletedEventKeys(prev => {
          const next = new Set(prev);
          next.add(eventKey);
          return next;
        });
        refetch();
        refetchStats();
      }
    } catch (err) {
      // toast.error handled in service
    } finally {
      setDeletingEventKeys(prev => {
        const next = new Set(prev);
        next.delete(eventKey);
        return next;
      });
    }
  };

  const filteredClusters = data?.clusters?.filter((c: any) => {
    const hasConflictIds = c.visits?.some((v: any) => v.conflictIds && v.conflictIds.length > 0);
    const isConflictType = c.type === 'conflict' || hasConflictIds;
    const hasMultipleVisits = (c.visits?.length || 0) >= 2;
    const hasMultipleCustomerIds = (c.customerIds?.length || 0) > 1;
    const isDuplicateType = c.type === 'duplicate' || hasMultipleVisits || hasMultipleCustomerIds;

    // Filter out ANY cluster that contains an employee, even if it's a duplicate/conflict
    const hasEmployee = c.visits?.some((v: any) => v.isEmployee === true);
    if (hasEmployee) return false;

    if (!isConflictType && !isDuplicateType) return false;

    if (timeFrom || timeTo) {
      const anyInRange = (c.visits || []).some((v: any) => {
        const et = v.entryTime;
        const xt = v.exitTime;
        if (!et || typeof et !== 'string') return false;

        const entryDate = new Date(et);
        const exitDate = xt ? new Date(xt) : entryDate;

        const [fromHour, fromMinute] = timeFrom ? timeFrom.split(':').map(Number) : [0, 0];
        const [toHour, toMinute] = timeTo ? timeTo.split(':').map(Number) : [23, 59];

        const filterFromMinutes = fromHour * 60 + fromMinute;
        const filterToMinutes = toHour * 60 + toMinute;

        // Check if visit overlaps with the filter time range (within the same day)
        const entryMinutes = entryDate.getUTCHours() * 60 + entryDate.getUTCMinutes();
        const exitMinutes = exitDate.getUTCHours() * 60 + exitDate.getUTCMinutes();

        return (entryMinutes >= filterFromMinutes && entryMinutes <= filterToMinutes) ||
          (exitMinutes >= filterFromMinutes && exitMinutes <= filterToMinutes) ||
          (entryMinutes <= filterFromMinutes && exitMinutes >= filterToMinutes);
      });
      if (!anyInRange) return false;
    }

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

  const totalVisitorIds = useMemo(() => {
    const ids = new Set<string>();
    filteredClusters.forEach((c: any) => {
      c.customerIds?.forEach((id: string) => ids.add(id));
    });
    return ids.size;
  }, [filteredClusters]);

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
              <div className="flex items-center gap-4">
                <p className="text-slate-500 font-bold text-xs uppercase tracking-widest">
                  Showing {filteredClusters.length} clusters for {dateRange.startDate}
                </p>
                <div className="flex items-center gap-2 px-3 py-1 bg-red-50 text-red-600 rounded-full border border-red-100">
                  <Trash2 size={12} />
                  <span className="text-[10px] font-black uppercase tracking-tighter">Date Deleted: {deleteStats?.date_deleted ?? 0}</span>
                </div>
                <div className="h-4 w-[1px] bg-slate-200" />
                <div className="flex items-center gap-2">
                  <span className="text-[10px] font-black text-blue-600 uppercase bg-blue-50 px-2 py-0.5 rounded-md border border-blue-100">
                    Total Visitors: {totalVisitorIds ?? 0}
                  </span>
                </div>
              </div>
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
            <div className="flex flex-wrap gap-2 w-full md:w-auto">
              <div className="flex items-center gap-2">
                <TimePicker
                  value={timeFromDraft}
                  onChange={setTimeFromDraft}
                  placeholder="From Entry"
                />
                <span className="text-xs text-slate-500 font-medium">From Entry</span>
              </div>
              <div className="hidden lg:flex items-center gap-2 px-4 bg-slate-100 rounded-xl text-[10px] font-black uppercase text-slate-600">
                <MapPin className="w-3 h-3 text-blue-500" /> {currentBranch}
              </div>
              <DateSelector />
              <Button
                onClick={() => {
                  setTimeFrom(timeFromDraft);
                  setTimeTo(''); // Clear timeTo to disable upper bound
                }}
                className="flex-1 md:flex-initial h-11 bg-blue-600 hover:bg-blue-700 text-white font-black uppercase text-xs tracking-widest px-8 rounded-xl shadow-lg shadow-blue-100"
              >
                Apply Filter
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
                        {cluster.customerIds?.map((cid: string) => {
                          const isDeleted = cluster.visits?.some((v: any) => v.customerId === cid && v.isDeleted);
                          return (
                            <div key={cid} className={`flex items-center gap-1 text-white px-2 py-0.5 rounded-lg group/cid ${isDeleted ? 'bg-slate-400 opacity-70' : 'bg-slate-900'}`}>
                              <span className="text-[10px] font-black uppercase tracking-tighter">
                                {cid}
                              </span>
                              {isDeleted && (
                                <span className="text-[8px] font-black uppercase tracking-tighter bg-red-600 text-white px-1.5 py-0.5 rounded">
                                  Deleted
                                </span>
                              )}
                            </div>
                          );
                        })}
                      </div>
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
                            disabled={cluster.visits?.some((v: any) => v.isEmployee === true)}
                            className="h-8 bg-emerald-600 hover:bg-emerald-700 text-white font-black text-[10px] uppercase px-4 rounded-lg shadow-lg shadow-emerald-100/50 disabled:opacity-50 disabled:cursor-not-allowed"
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

                      return images.map((img: any, iIdx: number) => {
                        const eventKey = `${visit.visitId}:${img.eventId || 'primary'}`;
                        const isDeleted = img.isDeleted || deletedEventKeys.has(eventKey);

                        return (
                          <div key={`${visit.visitId}-${img.name}-${iIdx}`} className={`group relative aspect-[3/4] rounded-xl overflow-hidden bg-white border border-slate-100 shadow-sm transition-all cursor-pointer ${isDeleted ? 'opacity-50 grayscale' : ''}`}>
                            <img
                              src={img.url.startsWith('/') ? `${BASE_URL}${img.url}` : img.url}
                              className="w-full h-full object-cover"
                              onError={(e: any) => e.target.src = 'https://placehold.co/300x400?text=No+Photo'}
                              loading="lazy"
                            />

                            {isDeleted && (
                              <div className="absolute inset-0 z-30 flex items-center justify-center bg-black/20 backdrop-blur-[2px]">
                                <span className="bg-red-600 text-white text-[10px] font-black px-3 py-1 rounded-full uppercase tracking-widest shadow-xl">
                                  Deleted
                                </span>
                              </div>
                            )}

                            {(img.eventId || img.isPrimary) && !isDeleted && (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleDeleteImage(visit.visitId, img.eventId || 'primary');
                                }}
                                disabled={deletingEventKeys.has(eventKey)}
                                className="absolute top-2 right-2 z-20 p-1.5 bg-red-500/80 hover:bg-red-600 text-white rounded-lg opacity-0 group-hover:opacity-100 transition-all backdrop-blur-sm shadow-sm"
                                title="Reject/Delete Event"
                              >
                                {deletingEventKeys.has(eventKey) ? (
                                  <RefreshCw size={12} className="animate-spin" />
                                ) : (
                                  <X size={12} />
                                )}
                              </button>
                            )}

                            <div className="absolute inset-0 bg-slate-900/40 opacity-0 group-hover:opacity-100 transition-opacity flex flex-col justify-end p-2 pb-3 backdrop-blur-[1px]">
                              <div className="flex flex-wrap gap-1 mb-1">
                                {visit.isEmployee && (
                                  <span className="bg-blue-500 text-white text-[7px] font-black px-1.5 py-0.5 rounded shadow-sm flex items-center gap-1 uppercase tracking-tighter">
                                    <UserCircle size={8} />
                                    Employee
                                  </span>
                                )}
                              </div>
                              {visit.conflictIds && visit.conflictIds.length > 0 && (
                                <div className="mt-1 space-y-1">
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
                          </div>
                        );
                      });
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
                  <div className="bg-white rounded-xl p-4 shadow-sm border border-slate-100 hover:border-blue-500 transition-all">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex flex-col">
                        <span className="text-[10px] font-black text-blue-600 uppercase tracking-widest">
                          Cluster ID
                        </span>
                        <span className="text-xs font-bold text-slate-900 truncate max-w-[120px]">
                          {group.clusterId}
                        </span>
                      </div>
                      <span className={`text-[8px] font-black uppercase px-2 py-1 rounded-full border ${group.type === 'duplicate' ? 'bg-amber-50 text-amber-600 border-amber-100' : 'bg-red-50 text-red-600 border-red-100'}`}>
                        {group.type}
                      </span>
                    </div>

                    <div className="space-y-3">
                      <div>
                        <span className="text-[9px] font-bold text-slate-400 uppercase tracking-tighter block mb-1">Customer Profiles</span>
                        <div className="flex flex-wrap gap-1">
                          {group.customerIds.map((id: string) => (
                            <span key={id} className="text-[9px] bg-slate-100 text-slate-700 px-1.5 py-0.5 rounded font-black">
                              {id}
                            </span>
                          ))}
                        </div>
                      </div>

                      <div>
                        <span className="text-[9px] font-bold text-slate-400 uppercase tracking-tighter block mb-1">Visit History ({group.visitIds.length})</span>
                        <div className="flex flex-wrap gap-1">
                          {group.visitIds.map((id: string) => (
                            <span key={id} className="text-[9px] bg-blue-50 text-blue-600 px-1.5 py-0.5 rounded font-bold border border-blue-100">
                              {id}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>

                    <button
                      onClick={() => copyToClipboard(JSON.stringify(group, null, 2))}
                      className="absolute top-4 right-4 opacity-0 group-hover:opacity-100 p-1.5 bg-slate-900 rounded-lg text-white transition-all shadow-lg"
                      title="Copy JSON"
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
