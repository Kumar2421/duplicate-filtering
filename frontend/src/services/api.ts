import axios from 'axios';
import { toast } from 'sonner';

// In production/staging, hit the actual server's domain/IP instead of 'localhost'
// to avoid "loopback address space" CORS blocks from secure domains like https://.
// Backend is now accessible via the dedicated api subdomain.
export const BASE_URL = typeof window !== 'undefined' && window.location.hostname !== 'localhost'
  ? `https://api.duplicate.tools.thefusionapps.com`
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

export const fetchSystemMetrics = async (branchId?: string, date?: string) => {
  const response = await api.get('/system-metrics', {
    params: {
      branchId,
      date,
    },
  });
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
  try {
    const response = await api.put('/api/conformation/action', data);
    if (response.data.success) {
      toast.success(`Action ${data.approve ? 'Approved' : 'Rejected'} successfully`);
    } else {
      toast.error(`Action failed: ${response.data.error || 'Unknown error'}`);
    }
    return response.data;
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || 'Network error';
    toast.error(`Error: ${errorMsg}`);
    throw error;
  }
};

export const sendConvertAction = async (data: {
  customerId1: string;
  customerId2: string;
  toEmployee: boolean;
  branchId?: string;
}) => {
  try {
    const response = await api.post('/convert', data);
    if (response.data.success) {
      toast.success('Conversion successful');
    } else {
      toast.error(`Conversion failed: ${response.data.error || 'Unknown error'}`);
    }
    return response.data;
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || 'Network error';
    toast.error(`Error: ${errorMsg}`);
    throw error;
  }
};

export const fetchAvailableDates = async (branchId: string) => {
  const response = await api.get('/api/available-dates', {
    params: { branchId },
  });
  return response.data;
};

export const fetchBranches = async () => {
  const response = await api.get('/api/branches');
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
