import React, { useMemo, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchDuplicateClusters, fetchAvailableDates, deepDelete, deleteEvent, fetchDeleteStats, BASE_URL } from '../services/api';
import { Button } from '../components/ui/button';
import { Search, MapPin, RefreshCw, AlertCircle, Code, Copy, UserCircle, Trash2, X } from 'lucide-react';
import { toast } from 'sonner';
import { Input } from '../components/ui/input';
import { DateSelector } from '../components/DateSelector';
import { TimePicker } from '../components/TimePicker';

const Employees: React.FC = () => {
  const { currentBranch, dateRange, setDateRange } = useAppStore();
  const queryClient = useQueryClient();
  const [searchQuery, setSearchQuery] = React.useState('');
  const [showSidebar, setShowSidebar] = React.useState(false);
  const [timeFromDraft, setTimeFromDraft] = React.useState<string>('');
  const [timeToDraft, setTimeToDraft] = React.useState<string>('');
  const [timeFrom, setTimeFrom] = React.useState<string>('');
  const [timeTo, setTimeTo] = React.useState<string>('');
  const [deepDeletingCustomerIds, setDeepDeletingCustomerIds] = React.useState<Set<string>>(new Set());
  const [deletedCustomerIds, setDeletedCustomerIds] = React.useState<Set<string>>(new Set());
  const [deletingEventKeys, setDeletingEventKeys] = React.useState<Set<string>>(new Set());
  const [deletedEventKeys, setDeletedEventKeys] = React.useState<Set<string>>(new Set());

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

  const handleDeepDelete = async (customerId: string) => {
    if (!window.confirm(`Are you sure you want to PERMANENTLY delete all data for employee ${customerId}? This action is irreversible.`)) {
      return;
    }

    setDeepDeletingCustomerIds(prev => {
      const next = new Set(prev);
      next.add(customerId);
      return next;
    });

    try {
      const branchToken = localStorage.getItem(`branch_token_${currentBranch}`);
      const result = await deepDelete({
        branchId: currentBranch,
        customerId: customerId,
        api_key: branchToken || undefined
      });

      if (result.success) {
        toast.success("Employee data deleted successfully");
        setDeletedCustomerIds(prev => {
          const next = new Set(prev);
          next.add(customerId);
          return next;
        });
        refetchStats();

        queryClient.setQueryData(
          ['duplicate-clusters', currentBranch, dateRange.startDate],
          (old: any) => {
            if (!old?.clusters) return old;
            return {
              ...old,
              clusters: old.clusters.map((c: any) => ({
                ...c,
                visits: (c.visits || []).map((v: any) =>
                  v.customerId === customerId ? { ...v, isDeleted: true } : v
                ),
              })),
            };
          }
        );
      }
    } catch (err) {
      // toast.error handled in service
    } finally {
      setDeepDeletingCustomerIds(prev => {
        const next = new Set(prev);
        next.delete(customerId);
        return next;
      });
    }
  };

  const handleDeleteImage = async (visitId: string, eventId: string | null) => {
    if (!eventId) {
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
        refetchStats();

        queryClient.setQueryData(
          ['duplicate-clusters', currentBranch, dateRange.startDate],
          (old: any) => {
            if (!old?.clusters) return old;
            return {
              ...old,
              clusters: old.clusters.map((c: any) => ({
                ...c,
                visits: (c.visits || []).map((v: any) => {
                  if (v.visitId !== visitId) return v;
                  const allImages = (v.allImages || []).map((img: any) =>
                    img.eventId === eventId ? { ...img, isDeleted: true } : img
                  );
                  return { ...v, allImages };
                }),
              })),
            };
          }
        );
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
    // Filter logic: only show clusters that have at least one employee
    const hasEmployee = c.visits?.some((v: any) => v.isEmployee === true);
    if (!hasEmployee) return false;

    if (timeFrom || timeTo) {
      const anyInRange = (c.visits || []).some((v: any) => {
        if (!v.isEmployee) return false;
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

  const employeeJson = useMemo(() => {
    if (!filteredClusters.length) return null;
    return filteredClusters.map((c: any) => ({
      clusterId: c.clusterId,
      customerIds: c.customerIds,
      visitIds: c.visits.map((v: any) => v.visitId),
      employeeInfo: c.visits.filter((v: any) => v.isEmployee).map((v: any) => ({
        visitId: v.visitId,
        customerId: v.customerId
      }))
    }));
  }, [filteredClusters]);

  const totalEmployeesCount = useMemo(() => {
    const ids = new Set<string>();
    filteredClusters.forEach((c: any) => {
      c.customerIds?.forEach((id: string) => ids.add(id));
    });
    return ids.size;
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
                <UserCircle className="text-indigo-600" />
                Employee Management
              </h1>
              <div className="flex items-center gap-4">
                <p className="text-slate-500 font-bold text-xs uppercase tracking-widest">
                  Showing {filteredClusters.length} staff profiles for {dateRange.startDate}
                </p>
                <div className="flex items-center gap-2 px-3 py-1 bg-red-50 text-red-600 rounded-full border border-red-100">
                  <Trash2 size={12} />
                  <span className="text-[10px] font-black uppercase tracking-tighter">Date Deleted: {deleteStats?.date_deleted ?? 0}</span>
                </div>
                <div className="h-4 w-[1px] bg-slate-200" />
                <div className="flex items-center gap-2">
                  <span className="text-[10px] font-black text-indigo-600 uppercase bg-indigo-50 px-2 py-0.5 rounded-md border border-indigo-100">
                    Total Staff: {totalEmployeesCount ?? 0}
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
                placeholder="Search staff by ID or Visit ID..."
                className="pl-12 h-11 bg-slate-50 border-none rounded-xl text-sm font-medium"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
            <div className="flex flex-wrap gap-2 w-full md:w-auto">
              <TimePicker
                value={timeFromDraft}
                onChange={setTimeFromDraft}
                placeholder="From Time"
              />
              <TimePicker
                value={timeToDraft}
                onChange={setTimeToDraft}
                placeholder="To Time"
              />
              <div className="hidden lg:flex items-center gap-2 px-4 bg-slate-100 rounded-xl text-[10px] font-black uppercase text-slate-600">
                <MapPin className="w-3 h-3 text-indigo-500" /> {currentBranch}
              </div>
              <DateSelector />
              <Button
                onClick={() => {
                  setTimeFrom(timeFromDraft);
                  setTimeTo(timeToDraft);
                }}
                className="flex-1 md:flex-initial h-11 bg-indigo-600 hover:bg-indigo-700 text-white font-black uppercase text-xs tracking-widest px-8 rounded-xl shadow-lg shadow-indigo-100"
              >
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
                          (() => {
                            const isDeleted =
                              deletedCustomerIds.has(cid) ||
                              cluster.visits?.some((v: any) => v.customerId === cid && v.isDeleted);

                            return (
                              <div key={cid} className="flex items-center gap-1 bg-slate-900 text-white px-2 py-0.5 rounded-lg group/cid">
                                <span className="text-[10px] font-black uppercase tracking-tighter">
                                  {cid}
                                </span>
                                {isDeleted && (
                                  <span className="text-[8px] font-black uppercase tracking-tighter bg-red-600 text-white px-1.5 py-0.5 rounded">
                                    Deleted
                                  </span>
                                )}
                                <button
                                  onClick={() => handleDeepDelete(cid)}
                                  disabled={deepDeletingCustomerIds.has(cid) || isDeleted}
                                  className="ml-1 p-0.5 bg-red-500 hover:bg-red-600 rounded text-white transition-colors"
                                  title="Deep Delete Employee Data"
                                >
                                  {deepDeletingCustomerIds.has(cid) ? (
                                    <RefreshCw size={8} className="animate-spin" />
                                  ) : (
                                    <Trash2 size={8} />
                                  )}
                                </button>
                              </div>
                            );
                          })()
                        ))}
                      </div>
                      <span className="text-xs font-bold text-slate-400">
                        {cluster.visits?.length || 0} Total Visits in Cluster
                      </span>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                    {cluster.visits.flatMap((visit: any) => {
                      // Only show images if the visit is an employee visit
                      if (!visit.isEmployee) return [];

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

                          {(() => {
                            const eventKey = `${visit.visitId}:${img.eventId}`;
                            const isDeleted = Boolean(img.isDeleted) || deletedEventKeys.has(eventKey);
                            const canDelete = Boolean(img.eventId) && !isDeleted;

                            return (
                              <>
                                {canDelete && (
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      handleDeleteImage(visit.visitId, img.eventId);
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

                                {isDeleted && (
                                  <div className="absolute top-2 left-2 z-20 bg-red-600 text-white text-[9px] font-black uppercase tracking-widest px-2 py-1 rounded-lg shadow">
                                    Deleted
                                  </div>
                                )}
                              </>
                            );
                          })()}

                          <div className="absolute inset-0 bg-slate-900/40 opacity-0 group-hover:opacity-100 transition-opacity flex flex-col justify-end p-2 pb-3 backdrop-blur-[1px]">
                            <div className="flex flex-wrap gap-1 mb-1">
                              <span className="bg-indigo-500 text-white text-[7px] font-black px-1.5 py-0.5 rounded shadow-sm flex items-center gap-1 uppercase tracking-tighter">
                                <UserCircle size={8} />
                                Staff Profile
                              </span>
                            </div>
                            <div className="mt-1">
                              <p className="text-[8px] font-black text-white uppercase truncate">Visit: {visit.visitId}</p>
                              <p className="text-[8px] font-medium text-slate-200 uppercase truncate">ID: {visit.customerId}</p>
                            </div>
                          </div>
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
              <UserCircle className="w-16 h-16 text-slate-200 mb-4" />
              <h3 className="text-xl font-black text-slate-900 uppercase tracking-widest">No Staff Found</h3>
              <p className="text-slate-400 font-bold mt-2">No employee visits were detected for this period.</p>
            </div>
          )}
        </div>
      </div>

      {showSidebar && (
        <div className="w-80 bg-white border-l border-slate-200 flex flex-col h-full shadow-2xl relative z-10 animate-in slide-in-from-right duration-300">
          <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
            <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-500 flex items-center gap-2">
              <Code className="w-3 h-3 text-indigo-500" />
              Employee Registry (JSON)
            </h2>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => copyToClipboard(JSON.stringify(employeeJson, null, 2))}
              className="h-7 px-2 text-[9px] font-black uppercase tracking-tighter hover:bg-indigo-50 hover:text-indigo-600 transition-colors"
            >
              <Copy className="w-3 h-3 mr-1" />
              Copy All
            </Button>
          </div>

          <div className="flex-1 overflow-y-auto p-4 space-y-4 custom-scrollbar">
            {employeeJson ? (
              employeeJson.map((group: any, i: number) => (
                <div key={group.clusterId || i} className="group relative">
                  <div className="bg-white rounded-xl p-4 shadow-sm border border-slate-100 hover:border-indigo-500 transition-all">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex flex-col">
                        <span className="text-[10px] font-black text-indigo-600 uppercase tracking-widest">
                          Staff Cluster
                        </span>
                        <span className="text-xs font-bold text-slate-900 truncate max-w-[120px]">
                          {group.clusterId}
                        </span>
                      </div>
                      <span className="text-[8px] font-black uppercase px-2 py-1 rounded-full border bg-indigo-50 text-indigo-600 border-indigo-100">
                        Employee
                      </span>
                    </div>

                    <div className="space-y-3">
                      <div>
                        <span className="text-[9px] font-bold text-slate-400 uppercase tracking-tighter block mb-1">Staff Profiles</span>
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
                            <span key={id} className="text-[9px] bg-indigo-50 text-indigo-600 px-1.5 py-0.5 rounded font-bold border border-indigo-100">
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
                <p className="text-[10px] font-black uppercase tracking-widest opacity-40">No Staff Data</p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default Employees;
