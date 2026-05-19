import React, { useMemo, useEffect, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchEmployees, fetchAvailableDates, deepDelete, deleteEvent, fetchDeleteStats, BASE_URL } from '../services/api';
import { Button } from '../components/ui/button';
import { Search, MapPin, RefreshCw, AlertCircle, Code, Copy, UserCircle, Trash2, X, CheckSquare, Square, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { Input } from '../components/ui/input';
import { DateSelector } from '../components/DateSelector';
import { TimePicker } from '../components/TimePicker';

const Employees: React.FC = () => {
  const { currentBranch, dateRange, setDateRange } = useAppStore();
  const queryClient = useQueryClient();
  const [searchQuery, setSearchQuery] = useState('');
  const [showSidebar, setShowSidebar] = useState(false);
  const [timeFromDraft, setTimeFromDraft] = useState<string>('');
  const [timeToDraft, setTimeToDraft] = useState<string>('');
  const [timeFrom, setTimeFrom] = useState<string>('');
  const [timeTo, setTimeTo] = useState<string>('');
  
  const [selectedCustomerIds, setSelectedCustomerIds] = useState<Set<string>>(new Set());
  const [isBulkDeleting, setIsBulkDeleting] = useState(false);

  const [deepDeletingCustomerIds, setDeepDeletingCustomerIds] = useState<Set<string>>(new Set());
  const [deletedCustomerIds, setDeletedCustomerIds] = useState<Set<string>>(new Set());
  const [deletingVisitIds, setDeletingVisitIds] = useState<Set<string>>(new Set());
  const [deletedVisitIds, setDeletedVisitIds] = useState<Set<string>>(new Set());

  const { data: availableDatesData } = useQuery({
    queryKey: ['available-dates', currentBranch],
    queryFn: () => fetchAvailableDates(currentBranch),
  });

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['employees-data', currentBranch, dateRange.startDate],
    queryFn: () => fetchEmployees(currentBranch, dateRange.startDate),
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
      const result = await deepDelete({
        branchId: currentBranch,
        customerId: customerId,
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
          ['employees-data', currentBranch, dateRange.startDate],
          (old: any) => {
            if (!old?.employees) return old;
            return {
              ...old,
              employees: old.employees.map((e: any) =>
                e.customerId === customerId ? { ...e, isDeleted: true } : e
              ),
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

  const handleDeleteVisit = async (visitId: string) => {
    if (!window.confirm("Are you sure you want to delete this visit image? This action cannot be undone.")) {
      return;
    }

    setDeletingVisitIds(prev => {
      const next = new Set(prev);
      next.add(visitId);
      return next;
    });

    try {
      const result = await deleteEvent({
        branchId: currentBranch,
        visitId: visitId,
        eventId: 'primary',
      });

      if (result.success) {
        toast.success("Visit image deleted successfully");

        setDeletedVisitIds(prev => {
          const next = new Set(prev);
          next.add(visitId);
          return next;
        });
        refetchStats();

        queryClient.setQueryData(
          ['employees-data', currentBranch, dateRange.startDate],
          (old: any) => {
            if (!old?.employees) return old;
            return {
              ...old,
              employees: old.employees.map((e: any) =>
                e.visitId === visitId ? { ...e, isDeleted: true } : e
              ),
            };
          }
        );
      }
    } catch (err) {
      // toast.error handled in service
    } finally {
      setDeletingVisitIds(prev => {
        const next = new Set(prev);
        next.delete(visitId);
        return next;
      });
    }
  };

  const filteredEmployees = useMemo(() => {
    let list = data?.employees || [];

    if (timeFrom || timeTo) {
      list = list.filter((e: any) => {
        const et = e.entryTime;
        if (!et || typeof et !== 'string') return false;

        const entryDate = new Date(et);
        const [fromHour, fromMinute] = timeFrom ? timeFrom.split(':').map(Number) : [0, 0];
        const [toHour, toMinute] = timeTo ? timeTo.split(':').map(Number) : [23, 59];

        const filterFromMinutes = fromHour * 60 + fromMinute;
        const filterToMinutes = toHour * 60 + toMinute;

        const entryMinutes = entryDate.getUTCHours() * 60 + entryDate.getUTCMinutes();
        return entryMinutes >= filterFromMinutes && entryMinutes <= filterToMinutes;
      });
    }

    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase().trim();
      list = list.filter((e: any) =>
        e.customerId?.toLowerCase().includes(query) ||
        e.visitId?.toLowerCase().includes(query)
      );
    }

    return list;
  }, [data, searchQuery, timeFrom, timeTo]);

  const employeesByCustomer = useMemo(() => {
    const groups: Record<string, any[]> = {};
    filteredEmployees.forEach((e: any) => {
      if (!groups[e.customerId]) groups[e.customerId] = [];
      groups[e.customerId].push(e);
    });
    return groups;
  }, [filteredEmployees]);

  const handleToggleSelect = (customerId: string) => {
    const next = new Set(selectedCustomerIds);
    if (next.has(customerId)) next.delete(customerId);
    else next.add(customerId);
    setSelectedCustomerIds(next);
  };

  const handleSelectAll = () => {
    const allCids = Object.keys(employeesByCustomer);
    if (selectedCustomerIds.size === allCids.length) {
      setSelectedCustomerIds(new Set());
    } else {
      setSelectedCustomerIds(new Set(allCids));
    }
  };

  const handleBulkDeepDelete = async () => {
    if (selectedCustomerIds.size === 0) return;
    
    if (!window.confirm(`Are you sure you want to deep delete ALL data for ${selectedCustomerIds.size} selected staff members? This cannot be undone.`)) {
      return;
    }

    setIsBulkDeleting(true);
    const branchToken = localStorage.getItem(`branch_token_${currentBranch}`);
    const idsToDelete = Array.from(selectedCustomerIds);
    
    let successCount = 0;
    let failCount = 0;

    for (const customerId of idsToDelete) {
      try {
        await deepDelete({
          branchId: currentBranch,
          customerId: customerId,
          api_key: branchToken || undefined
        });
        successCount++;
        setDeletedCustomerIds(prev => {
          const next = new Set(prev);
          next.add(customerId);
          return next;
        });
      } catch (err) {
        failCount++;
        console.error(`Failed to delete customer ${customerId}:`, err);
      }
    }

    toast.success(`Bulk delete finished. Success: ${successCount}, Failed: ${failCount}`);
    setSelectedCustomerIds(new Set());
    setIsBulkDeleting(false);
    refetch();
    refetchStats();
  };

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
                  Showing {filteredEmployees.length} staff visits for {dateRange.startDate}
                </p>
                <div className="flex items-center gap-2 px-3 py-1 bg-red-50 text-red-600 rounded-full border border-red-100">
                  <Trash2 size={12} />
                  <span className="text-[10px] font-black uppercase tracking-tighter">Date Deleted: {deleteStats?.date_deleted ?? 0}</span>
                </div>
                <div className="h-4 w-[1px] bg-slate-200" />
                <div className="flex items-center gap-2">
                  <span className="text-[10px] font-black text-indigo-600 uppercase bg-indigo-50 px-2 py-0.5 rounded-md border border-indigo-100">
                    Total Customers: {Object.keys(employeesByCustomer).length}
                  </span>
                </div>
              </div>
            </div>
            
            <div className="flex gap-2">
              {selectedCustomerIds.size > 0 && (
                <Button
                  variant="destructive"
                  onClick={handleBulkDeepDelete}
                  disabled={isBulkDeleting}
                  className="font-black text-xs uppercase tracking-widest px-4 h-11 rounded-xl shadow-lg shadow-red-100 animate-in fade-in zoom-in duration-200"
                >
                  {isBulkDeleting ? (
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  ) : (
                    <Trash2 className="w-4 h-4 mr-2" />
                  )}
                  Bulk Delete ({selectedCustomerIds.size})
                </Button>
              )}
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
                placeholder="Search staff by Customer ID or Visit ID..."
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
              <div className="flex items-center gap-2 px-2">
                <Button 
                    variant="ghost" 
                    size="sm" 
                    onClick={handleSelectAll}
                    className="h-8 text-[10px] font-black uppercase tracking-widest text-slate-500 hover:text-indigo-600 transition-colors"
                >
                    {selectedCustomerIds.size === Object.keys(employeesByCustomer).length && Object.keys(employeesByCustomer).length > 0 ? (
                        <CheckSquare className="w-4 h-4 mr-2" />
                    ) : (
                        <Square className="w-4 h-4 mr-2" />
                    )}
                    Select All Staff ({Object.keys(employeesByCustomer).length})
                </Button>
              </div>

              {Object.entries(employeesByCustomer).map(([customerId, visits]) => {
                const isSelected = selectedCustomerIds.has(customerId);
                const isDeleted = deletedCustomerIds.has(customerId) || visits.every(v => v.isDeleted);
                
                return (
                  <div key={customerId} className={`space-y-4 p-4 rounded-3xl transition-all border ${isSelected ? 'bg-indigo-50/30 border-indigo-200' : 'border-transparent hover:bg-white/50'}`}>
                    <div className="flex items-center justify-between border-b border-slate-200 pb-3">
                      <div className="flex items-center gap-4">
                        <button 
                            onClick={() => handleToggleSelect(customerId)}
                            className={`p-1.5 rounded-lg transition-colors ${isSelected ? 'text-indigo-600 bg-indigo-100' : 'text-slate-300 hover:text-slate-400'}`}
                        >
                            {isSelected ? <CheckSquare size={20} /> : <Square size={20} />}
                        </button>
                        <div className="flex items-center gap-2 bg-slate-900 text-white px-2 py-0.5 rounded-lg group/cid">
                          <span className="text-[10px] font-black uppercase tracking-tighter cursor-pointer hover:text-indigo-300" onClick={() => copyToClipboard(customerId)}>
                            {customerId}
                          </span>
                          {isDeleted && (
                            <span className="text-[8px] font-black uppercase tracking-tighter bg-red-600 text-white px-1.5 py-0.5 rounded">
                              Deleted
                            </span>
                          )}
                          {!isDeleted && (
                            <button
                              onClick={() => handleDeepDelete(customerId)}
                              disabled={deepDeletingCustomerIds.has(customerId)}
                              className="ml-1 p-0.5 bg-red-500 hover:bg-red-600 rounded text-white transition-colors"
                              title="Deep Delete Employee Data"
                            >
                              {deepDeletingCustomerIds.has(customerId) ? (
                                <RefreshCw size={8} className="animate-spin" />
                              ) : (
                                <Trash2 size={8} />
                              )}
                            </button>
                          )}
                        </div>
                        <span className="text-xs font-bold text-slate-400">
                          {visits.length} Visit(s)
                        </span>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                      {visits.map((visit: any) => (
                        <div key={visit.visitId} className="group relative aspect-[3/4] rounded-xl overflow-hidden bg-white border border-slate-100 shadow-sm transition-all cursor-pointer">
                          <img
                            src={visit.image.startsWith('/') ? `${BASE_URL}${visit.image}` : visit.image}
                            className={`w-full h-full object-cover ${visit.isDeleted || deletedVisitIds.has(visit.visitId) ? 'opacity-30 grayscale' : ''}`}
                            onError={(e: any) => e.target.src = 'https://placehold.co/300x400?text=No+Photo'}
                            loading="lazy"
                          />

                          {(() => {
                            const isVisitDeleted = visit.isDeleted || deletedVisitIds.has(visit.visitId);
                            const canDelete = !isVisitDeleted;

                            return (
                              <>
                                {canDelete && (
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      handleDeleteVisit(visit.visitId);
                                    }}
                                    disabled={deletingVisitIds.has(visit.visitId)}
                                    className="absolute top-2 right-2 z-20 p-1.5 bg-red-500/80 hover:bg-red-600 text-white rounded-lg opacity-0 group-hover:opacity-100 transition-all backdrop-blur-sm shadow-sm"
                                    title="Delete Visit"
                                  >
                                    {deletingVisitIds.has(visit.visitId) ? (
                                      <RefreshCw size={12} className="animate-spin" />
                                    ) : (
                                      <X size={12} />
                                    )}
                                  </button>
                                )}

                                {isVisitDeleted && (
                                  <div className="absolute top-2 left-2 z-20 bg-red-600 text-white text-[9px] font-black uppercase tracking-widest px-2 py-1 rounded-lg shadow">
                                    Deleted
                                  </div>
                                )}
                              </>
                            );
                          })()}

                          <div className="absolute inset-0 bg-slate-900/40 opacity-0 group-hover:opacity-100 transition-opacity flex flex-col justify-end p-2 pb-3 backdrop-blur-[1px]">
                            <div className="mt-1">
                              <p className="text-[8px] font-black text-white uppercase truncate">Visit: {visit.visitId}</p>
                              <p className="text-[8px] font-medium text-slate-200 uppercase truncate">
                                {new Date(visit.entryTime).toLocaleTimeString()}
                              </p>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {filteredEmployees.length === 0 && !isLoading && (
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
              Employee Data (JSON)
            </h2>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => copyToClipboard(JSON.stringify(data?.employees, null, 2))}
              className="h-7 px-2 text-[9px] font-black uppercase tracking-tighter hover:bg-indigo-50 hover:text-indigo-600 transition-colors"
            >
              <Copy className="w-3 h-3 mr-1" />
              Copy All
            </Button>
          </div>

          <div className="flex-1 overflow-y-auto p-4 space-y-4 custom-scrollbar">
            {data?.employees ? (
              data.employees.map((emp: any, i: number) => (
                <div key={emp.visitId || i} className="group relative">
                  <div className="bg-white rounded-xl p-4 shadow-sm border border-slate-100 hover:border-indigo-500 transition-all">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex flex-col">
                        <span className="text-[10px] font-black text-indigo-600 uppercase tracking-widest">
                          Staff Visit
                        </span>
                        <span className="text-xs font-bold text-slate-900 truncate max-w-[120px]">
                          {emp.visitId}
                        </span>
                      </div>
                    </div>

                    <div className="space-y-3">
                      <div>
                        <span className="text-[9px] font-bold text-slate-400 uppercase tracking-tighter block mb-1">Customer ID</span>
                        <span className="text-[9px] bg-slate-100 text-slate-700 px-1.5 py-0.5 rounded font-black">
                          {emp.customerId}
                        </span>
                      </div>
                    </div>

                    <button
                      onClick={() => copyToClipboard(JSON.stringify(emp, null, 2))}
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
