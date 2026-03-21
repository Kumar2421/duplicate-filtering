import axios from 'axios';

export const BASE_URL = 'http://localhost:8000';

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
