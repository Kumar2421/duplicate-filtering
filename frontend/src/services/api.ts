import axios from 'axios';

// In production/staging, hit the actual server's domain/IP instead of 'localhost'
// to avoid "loopback address space" CORS blocks from secure domains like https://.
export const BASE_URL = typeof window !== 'undefined' && window.location.hostname !== 'localhost'
  ? `${window.location.protocol}//${window.location.hostname}:8009`
  : 'http://localhost:8009';

const api = axios.create({
  baseURL: BASE_URL,
});

export const fetchVisits = async (branchId: string, startDate: string, endDate: string) => {
  const response = await api.get('/fetch-visits', {
    params: {
      branchId,
      startDate,
      endDate,
    },
  });
  return response.data;
};

export const fetchSystemMetrics = async () => {
  const response = await api.get('/system-metrics');
  return response.data;
};

export const fetchDuplicateClusters = async (branchId: string, date: string) => {
  const response = await api.get('/api/duplicates', {
    params: {
      branchId,
      date,
    },
  });
  return response.data;
};

export const fetchAllVisits = async (branchId: string, date: string) => {
  const response = await api.get('/api/visits', {
    params: {
      branchId,
      date,
    },
  });
  return response.data;
};
export const sendConformationAction = async (data: { 
  id: string; 
  eventId: string; 
  approve: boolean;
  branchId?: string;
  date?: string;
}) => {
  const response = await api.put('/api/conformation/action', data);
  return response.data;
};

export const fetchAvailableDates = async (branchId: string) => {
  const response = await api.get('/api/available-dates', {
    params: { branchId },
  });
  return response.data;
};

export const fetchFullClusters = async (branchId: string, date: string) => {
  const response = await api.get('/api/clusters', {
    params: {
      branchId,
      date,
    },
  });
  return response.data;
};

export default api;
