import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchAllVisits, BASE_URL } from '../services/api';
import { Card } from '../components/ui/card';
import { Input } from '../components/ui/input';
import { Button } from '../components/ui/button';
import { Search, MapPin, RefreshCw, User, ImageIcon, X } from 'lucide-react';
import { DateSelector } from '../components/DateSelector';

const Visits: React.FC = () => {
    const { currentBranch, dateRange } = useAppStore();
    const [selectedItem, setSelectedItem] = useState<any>(null);

    const { data, isLoading, error, refetch } = useQuery({
        queryKey: ['visits', currentBranch, dateRange.startDate],
        queryFn: () => fetchAllVisits(currentBranch, dateRange.startDate),
    });

    // Flatten all images from all visits
    const allImageItems = data?.visits?.flatMap((visit: any, vIdx: number) => {
        if (visit.allImages && visit.allImages.length > 0) {
            return visit.allImages.map((img: any, iIdx: number) => ({
                ...visit,
                // Ensure unique key by combining visitId, image name, and indices
                uniqueKey: `${visit.visitId || vIdx}-${img.name}-${iIdx}`,
                currentUrl: img.url,
                currentName: img.name,
                isPrimary: img.isPrimary
            }));
        }

        // Fallback to primary image if allImages is missing or empty
        return [{
            ...visit,
            uniqueKey: `${visit.visitId || vIdx}-fallback`,
            currentUrl: visit.image || visit.imageUrl,
            currentName: 'primary.jpg',
            isPrimary: true
        }];
    }) || [];

    const stats = {
        totalImages: allImageItems.length,
        uniqueVisits: data?.visits?.length || 0,
        uniqueCustomers: new Set(data?.visits?.map((v: any) => v.customerId)).size
    };

    return (
        <div className="min-h-screen bg-slate-50 p-6 space-y-6">
            {/* Header / Stats */}
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 bg-white p-6 rounded-2xl border shadow-sm">
                <div className="space-y-1">
                    <h1 className="text-3xl font-black text-slate-900 tracking-tight">Visit Gallery</h1>
                    <div className="flex items-center gap-4 text-xs font-bold text-slate-500 uppercase tracking-widest">
                        <span className="flex items-center gap-1.5"><User className="w-3.5 h-3.5 text-blue-500" /> {stats.uniqueCustomers} Customers</span>
                        <span className="flex items-center gap-1.5"><ImageIcon className="w-3.5 h-3.5 text-blue-500" /> {stats.totalImages} Images</span>
                    </div>
                </div>

                <div className="flex items-center gap-3">
                    <div className="hidden lg:flex items-center gap-2 px-3 h-10 bg-slate-100 rounded-xl border text-[10px] font-black uppercase text-slate-600">
                        <MapPin className="w-3 h-3 text-blue-500" /> {currentBranch}
                    </div>
                    <DateSelector />
                    <Button onClick={() => refetch()} variant="secondary" className="h-10 px-6 font-black text-xs uppercase tracking-widest gap-2 bg-slate-900 text-white hover:bg-slate-800 rounded-xl">
                        <RefreshCw className={`w-3.5 h-3.5 ${isLoading ? 'animate-spin' : ''}`} />
                        Sync Data
                    </Button>
                </div>
            </div>

            {/* Filter Bar */}
            <div className="relative group">
                <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 w-4 h-4" />
                <Input
                    placeholder="Search images by Visit ID, Customer ID, or tag..."
                    className="h-12 pl-12 bg-white border-slate-200 rounded-xl shadow-sm focus:ring-blue-500 transition-all font-medium text-slate-600 text-sm"
                />
            </div>

            {/* Flattened Image Grid */}
            {isLoading ? (
                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                    {Array.from({ length: 24 }).map((_, i) => (
                        <div key={i} className="aspect-[3/4] bg-slate-200 animate-pulse rounded-xl" />
                    ))}
                </div>
            ) : (
                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                    {allImageItems.map((item: any) => (
                        <div
                            key={item.uniqueKey}
                            onClick={() => setSelectedItem(item)}
                            className="group relative aspect-[3/4] overflow-hidden rounded-xl bg-white border border-slate-200 shadow-sm cursor-pointer hover:shadow-2xl hover:-translate-y-1 transition-all duration-300 ring-0 hover:ring-2 hover:ring-blue-500"
                        >
                            <img
                                src={item.currentUrl.startsWith('/') ? `${BASE_URL}${item.currentUrl}` : item.currentUrl}
                                loading="lazy"
                                className="w-full h-full object-cover transition-all duration-500 group-hover:scale-110"
                                onError={(e: any) => e.target.src = 'https://placehold.co/300x400?text=No+Photo'}
                            />

                            {/* Hover Overlay */}
                            <div className="absolute inset-0 bg-slate-900/70 opacity-0 group-hover:opacity-100 transition-all duration-300 flex flex-col justify-end p-2 pb-3 backdrop-blur-[2px]">
                                <div className="space-y-0.5">
                                    <p className="text-[8px] font-black text-blue-400 uppercase tracking-tighter">CID: {item.customerId}</p>
                                    <p className="text-[7px] font-bold text-white/50 uppercase tracking-widest">{item.branchId || currentBranch}</p>
                                    <div className="h-px bg-white/10 my-1" />
                                    <p className="text-[9px] font-black text-white leading-tight">Visit #{item.visitId || 'N/A'}</p>
                                    <p className="text-[8px] font-bold text-slate-300">{item.time || 'Unknown'}</p>
                                </div>
                            </div>

                            {/* Badges */}
                            <div className="absolute top-2 right-2">
                                <span className={`text-[7px] font-black px-1.5 py-0.5 rounded shadow-lg backdrop-blur-md uppercase ${item.isPrimary ? 'bg-blue-600 text-white' : 'bg-slate-800/80 text-slate-200'}`}>
                                    {item.isPrimary ? 'Primary' : 'Event'}
                                </span>
                            </div>
                        </div>
                    ))}
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
