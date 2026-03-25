import { useQuery } from '@tanstack/react-query';
import { fetchSystemMetrics } from '../services/api';

export const useSystemMetrics = (branchId?: string, date?: string) => {
  return useQuery({
    queryKey: ['system-metrics', branchId, date],
    queryFn: () => fetchSystemMetrics(branchId, date),
    refetchInterval: 10000, // Sync every 10s
  });
};
