import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { fetchAvailableDates } from '../services/api';
import { useAppStore } from '../store/useStore';
import { Calendar, ChevronDown } from 'lucide-react';

export const DateSelector: React.FC = () => {
  const { currentBranch, dateRange, setDateRange } = useAppStore();

  const { data, isLoading } = useQuery({
    queryKey: ['available-dates', currentBranch],
    queryFn: () => fetchAvailableDates(currentBranch),
  });

  const dates = data?.dates || [];

  return (
    <div className="relative group">
      <div className="flex items-center gap-2 px-3 h-11 bg-white border border-slate-200 rounded-xl text-[10px] font-black text-slate-700 shadow-sm hover:border-blue-500 transition-all cursor-pointer overflow-hidden min-w-[150px]">
        <Calendar className="w-3.5 h-3.5 text-blue-500 shrink-0" />
        
        <select
          value={dateRange.startDate}
          onChange={(e) => setDateRange({ ...dateRange, startDate: e.target.value, endDate: e.target.value })}
          className="appearance-none bg-transparent border-none outline-none cursor-pointer pr-6 w-full font-black uppercase tracking-tight focus:ring-0"
        >
          {isLoading ? (
            <option>Loading...</option>
          ) : dates.length > 0 ? (
            dates.map((d: string) => (
              <option key={d} value={d} className="font-bold text-sm bg-white text-slate-900">
                {d}
              </option>
            ))
          ) : (
            <option value={dateRange.startDate}>{dateRange.startDate}</option>
          )}
        </select>
        
        <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400 group-hover:text-blue-500 pointer-events-none transition-colors" />
      </div>
    </div>
  );
};
