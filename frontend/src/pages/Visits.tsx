import React, { useState, useMemo, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchAllVisits, fetchAvailableDates, BASE_URL } from '../services/api';
import { Input } from '../components/ui/input';
import { Button } from '../components/ui/button';
import { Search, MapPin, RefreshCw, ImageIcon, X, AlertCircle } from 'lucide-react';
import { DateSelector } from '../components/DateSelector';

const Visits: React.FC = () => {
    const { currentBranch, dateRange, setDateRange } = useAppStore();
    const [searchQuery, setSearchQuery] = useState('');
    const [selectedItem, setSelectedItem] = useState<any>(null);

    const { data: availableDatesData } = useQuery({
        queryKey: ['available-dates', currentBranch],
        queryFn: () => fetchAvailableDates(currentBranch),
    });

    const { data, isLoading, error, refetch } = useQuery({
        queryKey: ['visits', currentBranch, dateRange.startDate],
        queryFn: () => fetchAllVisits(currentBranch, dateRange.startDate),
    });

    const flattenedVisits = useMemo(() => {
        if (!data?.clusters) return [];

        let visits = data.clusters.flatMap((cluster: any) =>
            cluster.visits.map((visit: any) => ({
                ...visit,
                clusterType: cluster.type,
                clusterCustomerIds: cluster.customerIds
            }))
        );

        if (searchQuery.trim()) {
            const query = searchQuery.toLowerCase().trim();
            visits = visits.filter((v: any) =>
                v.customerId?.toLowerCase().includes(query) ||
                v.visitId?.toLowerCase().includes(query) ||
                v.clusterCustomerIds?.some((id: string) => id.toLowerCase().includes(query))
            );
        }

        return visits;
    }, [data, searchQuery]);

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

    const stats = {
        totalImages: flattenedVisits?.reduce((acc: number, v: any) => acc + (v.allImages?.length || 1), 0) || 0,
        uniqueVisits: flattenedVisits?.length || 0,
        uniqueCustomers: new Set(flattenedVisits?.map((v: any) => v.customerId)).size
    };

    return (
        <div className="p-6 space-y-6 bg-slate-50 min-h-screen">
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div className="space-y-1">
                    <h1 className="text-3xl font-black text-slate-900 tracking-tight flex items-center gap-3">
                        <ImageIcon className="text-blue-600" />
                        Visit Records
                    </h1>
                    <p className="text-slate-500 font-bold text-xs uppercase tracking-widest">
                        Showing {stats.uniqueVisits} visits for {dateRange.startDate}
                    </p>
                </div>
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

            {/* Filter Bar */}
            <div className="flex flex-col md:flex-row gap-3 bg-white p-3 rounded-2xl border shadow-sm items-center">
                <div className="flex-1 relative w-full">
                    <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 w-4 h-4" />
                    <Input
                        placeholder="Search by Visitor ID, Visit ID..."
                        className="pl-12 h-11 bg-slate-50 border-none rounded-xl text-sm font-medium"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                    />
                </div>
                <div className="flex gap-2 w-full md:w-auto">
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
                    {data?.visits?.map((visit: any, idx: number) => {
                        const images = visit.allImages && visit.allImages.length > 0
                            ? visit.allImages
                            : [{ url: visit.image || visit.imageUrl, name: 'primary.jpg', isPrimary: true }];

                        return (
                            <div key={visit.visitId || idx} className="space-y-4">
                                {/* Visit Header */}
                                <div className="flex items-center justify-between border-b border-slate-200 pb-3">
                                    <div className="flex items-center gap-4">
                                        <div className="flex flex-wrap gap-2">
                                            <span className="text-[10px] font-black bg-slate-900 text-white px-2 py-0.5 rounded-lg uppercase tracking-tighter">
                                                {visit.customerId}
                                            </span>
                                        </div>
                                        <span className="text-xs font-bold text-slate-400">
                                            Visit #{visit.visitId}
                                        </span>
                                        <span className="text-[9px] font-black uppercase tracking-widest text-slate-500">
                                            {visit.time}
                                        </span>
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
                                                    <div className="w-2 h-2 bg-blue-500 rounded-full shadow-[0_0_8px_rgba(59,130,246,1)]" />
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

            {data?.visits?.length === 0 && !isLoading && (
                <div className="flex flex-col items-center justify-center py-32 bg-white rounded-3xl border border-dashed border-slate-200 shadow-inner">
                    <AlertCircle className="w-16 h-16 text-slate-200 mb-4" />
                    <h3 className="text-xl font-black text-slate-900 uppercase tracking-widest">No Visits Found</h3>
                    <p className="text-slate-400 font-bold mt-2">No visits recorded for this period.</p>
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
                                            <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Customer Details</p>
                                            <p className="text-sm font-bold text-slate-900">ID: {selectedItem.customerId}</p>
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
                                                    className={`w-12 h-12 rounded-lg border-2 overflow-hidden transition-all ${selectedItem.currentName === img.name ? 'border-blue-600 scale-105' : 'border-slate-100 opacity-60 hover:opacity-100'}`}
                                                >
                                                    <img src={img.url.startsWith('/') ? `${BASE_URL}${img.url}` : img.url} className="w-full h-full object-cover" />
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                </div>

                                <Button className="w-full h-12 bg-blue-600 hover:bg-blue-700 text-white font-black uppercase tracking-widest text-xs rounded-2xl shadow-lg shadow-blue-200 mt-8">
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

export default Visits;
