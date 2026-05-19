import React, { useState, useMemo, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchAllVisits, fetchAvailableDates, deepDelete, BASE_URL } from '../services/api';
import { Input } from '../components/ui/input';
import { Button } from '../components/ui/button';
import { Search, MapPin, RefreshCw, X, AlertCircle, Loader2, ShieldCheck, Trash2, CheckSquare, Square } from 'lucide-react';
import { DateSelector } from '../components/DateSelector';
import { toast } from 'sonner';

const Employees2: React.FC = () => {
    const { currentBranch, dateRange, setDateRange } = useAppStore();
    const [searchQuery, setSearchQuery] = useState('');
    const [selectedItem, setSelectedItem] = useState<any>(null);
    const [timeFromDraft, setTimeFromDraft] = useState<string>('');
    const [timeFrom, setTimeFrom] = useState<string>('');
    const [timeTo, setTimeTo] = useState<string>('');
    
    const [selectedCustomerIds, setSelectedCustomerIds] = useState<Set<string>>(new Set());
    const [isBulkDeleting, setIsBulkDeleting] = useState(false);
    
    const queryClient = useQueryClient();

    const { data: availableDatesData } = useQuery({
        queryKey: ['available-dates', currentBranch],
        queryFn: () => fetchAvailableDates(currentBranch),
    });

    const { data, isLoading, error, refetch } = useQuery({
        queryKey: ['visits', currentBranch, dateRange.startDate],
        queryFn: () => fetchAllVisits(currentBranch, dateRange.startDate),
    });

    const flattenedVisits = useMemo(() => {
        if (!data?.visits) return [];

        // Filter for ONLY verified staff (isEmployee: true)
        let visits = data.visits.filter((visit: any) => visit.isEmployee === true);

        if (timeFrom || timeTo) {
            visits = visits.filter((v: any) => {
                const et = v.entryTime;
                if (!et || typeof et !== 'string') return false;

                const entryTimeStr = et.includes('T') ? et.split('T')[1].replace('Z', '') : et;
                const [entryHour, entryMinute] = entryTimeStr.split(':').map(Number);

                const [fromHour, fromMinute] = timeFrom ? timeFrom.split(':').map(Number) : [0, 0];
                const [toHour, toMinute] = timeTo ? timeTo.split(':').map(Number) : [23, 59];

                const entryMinutes = entryHour * 60 + entryMinute;
                const fromMinutes = fromHour * 60 + fromMinute;
                const toMinutes = toHour * 60 + toMinute;

                return entryMinutes >= fromMinutes && entryMinutes <= toMinutes;
            });
        }

        if (searchQuery.trim()) {
            const query = searchQuery.toLowerCase().trim();
            visits = visits.filter((v: any) =>
                v.customerId?.toLowerCase().includes(query) ||
                v.visitId?.toLowerCase().includes(query) ||
                (v.employeeNameMatched && v.employeeNameMatched.toLowerCase().includes(query))
            );
        }

        return visits;
    }, [data, searchQuery, timeFrom, timeTo, dateRange.startDate]);

    const handleToggleSelect = (customerId: string) => {
        const next = new Set(selectedCustomerIds);
        if (next.has(customerId)) next.delete(customerId);
        else next.add(customerId);
        setSelectedCustomerIds(next);
    };

    const handleSelectAll = () => {
        if (selectedCustomerIds.size === flattenedVisits.length) {
            setSelectedCustomerIds(new Set());
        } else {
            setSelectedCustomerIds(new Set(flattenedVisits.map((v: any) => v.customerId)));
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
            } catch (err) {
                failCount++;
                console.error(`Failed to delete customer ${customerId}:`, err);
            }
        }

        toast.success(`Bulk delete finished. Success: ${successCount}, Failed: ${failCount}`);
        setSelectedCustomerIds(new Set());
        setIsBulkDeleting(false);
        refetch();
    };

    useEffect(() => {
        if (availableDatesData?.dates?.length > 0) {
            const latestDate = availableDatesData.dates[0];
            if (dateRange.startDate !== latestDate && !availableDatesData.dates.includes(dateRange.startDate)) {
                setDateRange({ startDate: latestDate, endDate: latestDate });
            }
        }
    }, [availableDatesData, currentBranch, setDateRange]);

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
        <div className="p-6 space-y-6 bg-slate-50 min-h-screen">
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div className="space-y-1">
                    <h1 className="text-3xl font-black text-slate-900 tracking-tight flex items-center gap-3">
                        <ShieldCheck className="text-emerald-600" />
                        Staff Verification
                    </h1>
                    <p className="text-slate-500 font-bold text-xs uppercase tracking-widest">
                        Showing {flattenedVisits.length} verified staff visits for {dateRange.startDate}
                    </p>
                </div>
                
                <div className="flex items-center gap-2">
                    {selectedCustomerIds.size > 0 && (
                        <Button
                            variant="destructive"
                            onClick={handleBulkDeepDelete}
                            disabled={isBulkDeleting}
                            className="font-black text-xs uppercase tracking-widest px-6 h-11 rounded-xl shadow-lg shadow-red-100 animate-in fade-in zoom-in duration-200"
                        >
                            {isBulkDeleting ? (
                                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                            ) : (
                                <Trash2 className="w-4 h-4 mr-2" />
                            )}
                            Deep Delete ({selectedCustomerIds.size})
                        </Button>
                    )}
                    <Button
                        variant="secondary"
                        onClick={() => refetch()}
                        disabled={isLoading}
                        className="bg-white border shadow-sm font-black text-xs uppercase tracking-widest px-6 h-11 rounded-xl"
                    >
                        <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
                        Refresh Logs
                    </Button>
                </div>
            </div>

            {/* Filter Bar */}
            <div className="flex flex-col md:flex-row gap-3 bg-white p-3 rounded-2xl border shadow-sm items-center">
                <div className="flex-1 relative w-full">
                    <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 w-4 h-4" />
                    <Input
                        placeholder="Search by Staff Name, ID..."
                        className="pl-12 h-11 bg-slate-50 border-none rounded-xl text-sm font-medium"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                    />
                </div>
                <div className="flex flex-wrap gap-2 w-full md:w-auto">
                    <div className="flex items-center gap-2">
                        <Input
                            type="time"
                            className="h-11 bg-white border-slate-200 rounded-xl text-sm font-medium w-[120px] flex-none"
                            value={timeFromDraft}
                            onChange={(e) => setTimeFromDraft(e.target.value)}
                        />
                        <span className="text-xs text-slate-500 font-medium">From Entry</span>
                    </div>
                    <div className="hidden lg:flex items-center gap-2 px-4 bg-slate-100 rounded-xl text-[10px] font-black uppercase text-slate-600">
                        <MapPin className="w-3 h-3 text-emerald-500" /> {currentBranch}
                    </div>
                    <DateSelector />
                    <Button
                        onClick={() => {
                            setTimeFrom(timeFromDraft);
                            setTimeTo(''); 
                        }}
                        className="flex-1 md:flex-initial h-11 bg-emerald-600 hover:bg-emerald-700 text-white font-black uppercase text-xs tracking-widest px-8 rounded-xl shadow-lg shadow-emerald-100"
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
                    <div className="flex items-center gap-2 px-2">
                        <Button 
                            variant="ghost" 
                            size="sm" 
                            onClick={handleSelectAll}
                            className="h-8 text-[10px] font-black uppercase tracking-widest text-slate-500 hover:text-emerald-600 transition-colors"
                        >
                            {selectedCustomerIds.size === flattenedVisits.length && flattenedVisits.length > 0 ? (
                                <CheckSquare className="w-4 h-4 mr-2" />
                            ) : (
                                <Square className="w-4 h-4 mr-2" />
                            )}
                            Select All Staff ({flattenedVisits.length})
                        </Button>
                    </div>

                    {flattenedVisits?.map((visit: any, idx: number) => {
                        const images = visit.allImages && visit.allImages.length > 0
                            ? visit.allImages
                            : [{ url: visit.image || visit.imageUrl, name: 'primary.jpg', isPrimary: true }];

                        const isSelected = selectedCustomerIds.has(visit.customerId);

                        return (
                            <div key={visit.visitId || idx} className={`space-y-4 p-4 rounded-2xl transition-all border ${isSelected ? 'bg-emerald-50/30 border-emerald-200' : 'border-transparent hover:bg-white/50'}`}>
                                {/* Visit Header */}
                                <div className="flex items-center justify-between border-b border-slate-200 pb-3">
                                    <div className="flex items-center gap-4">
                                        <button 
                                            onClick={() => handleToggleSelect(visit.customerId)}
                                            className={`p-1.5 rounded-lg transition-colors ${isSelected ? 'text-emerald-600 bg-emerald-100' : 'text-slate-300 hover:text-slate-400'}`}
                                        >
                                            {isSelected ? <CheckSquare size={18} /> : <Square size={18} />}
                                        </button>
                                        <div className="flex items-center gap-4">
                                            <div className="flex flex-wrap gap-2 items-center">
                                                <span className="text-[10px] font-black bg-slate-900 text-white px-2 py-0.5 rounded-lg uppercase tracking-tighter">
                                                    {visit.customerId}
                                                </span>

                                                {/* Staff Badge */}
                                                <div className="flex items-center gap-1.5 px-2 py-0.5 bg-emerald-100 text-emerald-700 rounded-lg border border-emerald-200">
                                                    <ShieldCheck size={12} className="fill-emerald-200" />
                                                    <span className="text-[9px] font-black uppercase tracking-tighter">
                                                        STAFF: {visit.employeeNameMatched || 'Verified'}
                                                    </span>
                                                    {visit.matchSimilarity && (
                                                        <span className="text-[8px] font-bold opacity-60">
                                                            ({(visit.matchSimilarity * 100).toFixed(0)}%)
                                                        </span>
                                                    )}
                                                </div>
                                            </div>
                                            <span className="text-xs font-bold text-slate-400">
                                                Visit #{visit.visitId}
                                            </span>
                                            <span className="text-[9px] font-black uppercase tracking-widest text-slate-500">
                                                {visit.time}
                                            </span>
                                        </div>
                                    </div>
                                </div>

                                {/* Grid of Images for this visit */}
                                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                                    {images.map((img: any, iIdx: number) => (
                                        <div
                                            key={`${visit.visitId}-${img.name}-${iIdx}`}
                                            onClick={() => setSelectedItem({
                                                ...visit,
                                                currentUrl: img.url,
                                                currentName: img.name,
                                                isPrimary: img.isPrimary
                                            })}
                                            className="group relative aspect-[3/4] rounded-xl overflow-hidden bg-white border border-slate-100 shadow-sm transition-all cursor-pointer"
                                        >
                                            <img
                                                src={img.url.startsWith('/') ? `${BASE_URL}${img.url}` : img.url}
                                                className="w-full h-full object-cover"
                                                onError={(e: any) => e.target.src = 'https://placehold.co/300x400?text=No+Photo'}
                                                loading="lazy"
                                            />

                                            {/* Primary Badge */}
                                            {img.isPrimary && (
                                                <div className="absolute top-2 right-2">
                                                    <div className="w-2 h-2 bg-emerald-500 rounded-full shadow-[0_0_8px_rgba(16,185,129,1)]" />
                                                </div>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        );
                    })}
                </div>
            )}

            {flattenedVisits?.length === 0 && !isLoading && (
                <div className="flex flex-col items-center justify-center py-32 bg-white rounded-3xl border border-dashed border-slate-200 shadow-inner">
                    <ShieldCheck className="w-16 h-16 text-slate-200 mb-4" />
                    <h3 className="text-xl font-black text-slate-900 uppercase tracking-widest">No Staff Records Found</h3>
                    <p className="text-slate-400 font-bold mt-2">No verified staff visits found for this date.</p>
                </div>
            )}

            {/* Simple Detail Modal */}
            {selectedItem && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 sm:p-6 bg-slate-900/40 backdrop-blur-xl animate-in fade-in duration-200">
                    <div className="relative bg-white rounded-3xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col shadow-2xl overflow-y-auto">
                        <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => setSelectedItem(null)}
                            className="absolute top-4 right-4 z-10 bg-slate-100/50 hover:bg-slate-100 rounded-full"
                        >
                            <X className="w-5 h-5" />
                        </Button>

                        <div className="flex flex-col md:flex-row h-full">
                            {/* Large Image Preview */}
                            <div className="flex-1 bg-slate-50 flex items-center justify-center p-4">
                                <img
                                    src={selectedItem.currentUrl.startsWith('/') ? `${BASE_URL}${selectedItem.currentUrl}` : selectedItem.currentUrl}
                                    className="max-w-full max-h-[70vh] object-contain rounded-2xl shadow-xl"
                                    alt={selectedItem.currentName}
                                />
                            </div>

                            {/* Meta Sidebar */}
                            <div className="w-full md:w-80 p-8 flex flex-col justify-between border-l bg-white">
                                <div className="space-y-8">
                                    <div>
                                        <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-[.2em] mb-2">Image Source</h3>
                                        <p className="text-xl font-black text-slate-900 break-all">{selectedItem.currentName}</p>
                                    </div>

                                    <div className="grid grid-cols-1 gap-6">
                                        <div className="p-4 bg-slate-50 rounded-2xl border">
                                            <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Staff Details</p>
                                            <p className="text-sm font-bold text-slate-900">Name: {selectedItem.employeeNameMatched || 'Verified'}</p>
                                            <p className="text-xs text-slate-500">ID: {selectedItem.customerId}</p>
                                        </div>
                                        <div className="p-4 bg-slate-50 rounded-2xl border text-mono">
                                            <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Visit Session</p>
                                            <p className="text-sm font-bold text-slate-900">{selectedItem.visitId}</p>
                                            <p className="text-xs text-slate-500 mt-1">{selectedItem.time} | {selectedItem.branchId}</p>
                                        </div>
                                    </div>

                                    <div className="space-y-2">
                                        <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Available Views</h3>
                                        <div className="flex flex-wrap gap-2">
                                            {selectedItem.allImages?.map((img: any) => (
                                                <button
                                                    key={img.name}
                                                    onClick={() => setSelectedItem({ ...selectedItem, currentUrl: img.url, currentName: img.name, isPrimary: img.isPrimary })}
                                                    className={`w-12 h-12 rounded-lg border-2 overflow-hidden transition-all ${selectedItem.currentName === img.name ? 'border-emerald-600 scale-105' : 'border-slate-100 opacity-60 hover:opacity-100'}`}
                                                >
                                                    <img src={img.url.startsWith('/') ? `${BASE_URL}${img.url}` : img.url} className="w-full h-full object-cover" />
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                </div>

                                <Button className="w-full h-12 bg-emerald-600 hover:bg-emerald-700 text-white font-black uppercase tracking-widest text-xs rounded-2xl shadow-lg shadow-emerald-200 mt-8">
                                    Deep Analysis View
                                </Button>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Employees2;
