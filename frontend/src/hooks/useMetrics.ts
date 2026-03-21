import { useQuery } from '@tanstack/react-query';
import { fetchSystemMetrics } from '../services/api';

export const useSystemMetrics = () => {
  return useQuery({
    queryKey: ['system-metrics'],
    queryFn: fetchSystemMetrics,
    refetchInterval: 10000, // Sync every 10s
  });
};
