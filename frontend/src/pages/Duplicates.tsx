import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { fetchDuplicateClusters, BASE_URL } from '../services/api';
import { Button } from '../components/ui/button';
import { Search, MapPin, Check, X, RefreshCw, AlertCircle, Layers } from 'lucide-react';
import { Input } from '../components/ui/input';
import { DateSelector } from '../components/DateSelector';

const Duplicates: React.FC = () => {
  const { currentBranch, dateRange } = useAppStore();
  const [searchQuery, setSearchQuery] = React.useState('');

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['duplicate-clusters', currentBranch, dateRange.startDate],
    queryFn: () => fetchDuplicateClusters(currentBranch, dateRange.startDate),
  });

  const handleAction = async (cluster: any, approve: boolean) => {
    // Approve/Reject functionality is currently on hold as per user request.
    console.log("Action on hold:", { clusterId: cluster.clusterId, approve });
    return;
  };

  const filteredClusters = data?.clusters?.filter((c: any) => {
    const matchesVisits = (c.visits?.length || 0) >= 2;
    if (!matchesVisits) return false;
    
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
            <Layers className="text-blue-600" />
            Duplicate Detection
          </h1>
          <p className="text-slate-500 font-bold text-xs uppercase tracking-widest">
            Showing {filteredClusters.length} clusters for {dateRange.startDate}
          </p>
        </div>
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

      {/* Filter Bar */}
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
              {/* Cluster Header */}
              <div className="flex items-center justify-between border-b border-slate-200 pb-3">
                <div className="flex items-center gap-4">
                  <div className="flex flex-wrap gap-2">
                    {cluster.customerIds?.map((cid: string) => (
                      <span key={cid} className="text-[10px] font-black bg-slate-900 text-white px-2 py-0.5 rounded-lg uppercase tracking-tighter">
                        {cid}
                      </span>
                    ))}
                  </div>
                  <span className={`text-[9px] font-black uppercase tracking-widest px-2 py-0.5 rounded-full border ${cluster.type === 'duplicate' ? 'bg-red-50 text-red-600 border-red-100' : 'bg-amber-50 text-amber-600 border-amber-100'}`}>
                    {cluster.type}
                  </span>
                  <span className="text-xs font-bold text-slate-400">
                    {cluster.visits?.length || 0} Visits
                  </span>
                </div>

                <div className="flex gap-2">
                  <Button
                    size="sm"
                    onClick={() => handleAction(cluster, true)}
                    className="h-8 bg-emerald-600 hover:bg-emerald-700 text-white font-black text-[10px] uppercase px-4 rounded-lg"
                  >
                    <Check size={12} className="mr-1" />
                    Approve Cluster
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => handleAction(cluster, false)}
                    className="h-8 text-red-600 border-red-100 hover:bg-red-50 font-black text-[10px] uppercase px-4 rounded-lg"
                  >
                    <X size={12} className="mr-1" /> Remove
                  </Button>
                </div>
              </div>

              {/* Grid of ALL images in cluster */}
              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 gap-3">
                {cluster.visits.flatMap((visit: any, vIdx: number) => {
                  const images = visit.allImages && visit.allImages.length > 0
                    ? visit.allImages
                    : [{ url: visit.image || visit.imageUrl, name: 'primary.jpg', isPrimary: true }];

                  return images.map((img: any, iIdx: number) => (
                    <div key={`${visit.visitId}-${img.name}-${iIdx}`} className="group relative aspect-[3/4] rounded-xl overflow-hidden bg-white border border-slate-100 shadow-sm transition-all hover:ring-2 hover:ring-blue-500 hover:shadow-xl">
                      <img
                        src={img.url.startsWith('/') ? `${BASE_URL}${img.url}` : img.url}
                        className="w-full h-full object-cover transition-transform group-hover:scale-105"
                        onError={(e: any) => e.target.src = 'https://placehold.co/300x400?text=No+Photo'}
                        loading="lazy"
                      />

                      {/* Hover Overlay */}
                      <div className="absolute inset-0 bg-slate-900/60 opacity-0 group-hover:opacity-100 transition-opacity flex flex-col justify-end p-2 pb-3 backdrop-blur-[1px]">
                        <p className="text-[8px] font-bold text-blue-300 uppercase tracking-tighter truncate">{img.name}</p>
                        <p className="text-[9px] font-black text-white leading-tight">Visit #{vIdx + 1}</p>
                        <p className="text-[8px] font-bold text-slate-300 mt-1">{visit.time}</p>

                        {/* Conflict IDs Display */}
                        {visit.conflictIds?.length > 0 && (
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

                      {/* Visit Index Badge */}
                      <div className="absolute top-2 left-2">
                        <span className="bg-white/80 backdrop-blur-md text-slate-900 text-[8px] font-black px-1.5 py-0.5 rounded shadow-sm">
                          V{vIdx + 1}
                        </span>
                      </div>

                      {/* Primary Badge */}
                      {img.isPrimary && (
                        <div className="absolute top-2 right-2">
                          <div className="w-2 h-2 bg-blue-500 rounded-full shadow-[0_0_8px_rgba(59,130,246,1)]" />
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
  );
};

export default Duplicates;
