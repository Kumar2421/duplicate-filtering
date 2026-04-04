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
  api_key?: string;
}) => {
  try {
    const response = await api.put('/api/conformation/action', data, {
      headers: {
        'x-cache-bypass': 'true',
      },
    });
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
  api_key?: string;
}) => {
  try {
    const response = await api.post('/api/convert', data, {
      headers: {
        'x-cache-bypass': 'true',
      },
    });
    return response.data;
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || 'Network error';
    toast.error(`Error: ${errorMsg}`);
    throw error;
  }
};

export const fetchConvertJobStatus = async (jobId: string) => {
  const response = await api.get('/api/convert/status', {
    params: { jobId },
    headers: {
      'x-cache-bypass': 'true',
    },
  });
  return response.data;
};

export const deleteEvent = async (data: {
  branchId?: string;
  visitId: string;
  eventId: string;
  api_key?: string;
}) => {
  try {
    const response = await api.delete('/api/delete-event', {
      data,
      headers: {
        'x-cache-bypass': 'true',
      },
    });
    if (response.data.success) {
      toast.success('Image deleted successfully');
    } else {
      toast.error(`Delete failed: ${response.data.error || 'Unknown error'}`);
    }
    return response.data;
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || 'Network error';
    toast.error(`Error: ${errorMsg}`);
    throw error;
  }
};

export const deepDelete = async (data: {
  branchId?: string;
  customerId: string;
  api_key?: string;
}) => {
  try {
    const response = await api.delete('/api/deep-delete', {
      data,
      headers: {
        'x-cache-bypass': 'true',
      },
    });
    if (response.data.success) {
      toast.success('Customer data deep deleted successfully');
    } else {
      toast.error(`Deep Delete failed: ${response.data.error || 'Unknown error'}`);
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

export const triggerIngest = async (branchId: string, date: string, apiKey?: string) => {
  const response = await api.post(
    '/api/ingest',
    { branchId, date, api_key: apiKey },
    {
      headers: {
        'x-cache-bypass': 'true',
      },
    }
  );
  return response.data;
};

export const checkIngestStatus = async (branchId: string, date: string) => {
  const response = await api.get('/api/ingest/status', { params: { branchId, date } });
  return response.data;
};

export default api;
