import { create } from 'zustand';

interface AppState {
  currentBranch: string;
  setCurrentBranch: (branch: string) => void;
  dateRange: {
    startDate: string;
    endDate: string;
  };
  setDateRange: (range: { startDate: string; endDate: string }) => void;
  sidebarOpen: boolean;
  setSidebarOpen: (open: boolean) => void;
  token: string | null;
  setToken: (token: string | null) => void;
  logout: () => void;
}

export const useAppStore = create<AppState>((set) => ({
  currentBranch: 'TMJ-CBE', // Initial branch
  setCurrentBranch: (branch) => set({ currentBranch: branch }),
  dateRange: {
    startDate: new Date().toISOString().split('T')[0],
    endDate: new Date().toISOString().split('T')[0],
  },
  setDateRange: (range) => set({ dateRange: range }),
  sidebarOpen: true,
  setSidebarOpen: (open) => set({ sidebarOpen: open }),
  token: localStorage.getItem('auth_token'),
  setToken: (token) => {
    if (token) {
      localStorage.setItem('auth_token', token);
      localStorage.setItem('auth_token_timestamp', Date.now().toString());
    } else {
      localStorage.removeItem('auth_token');
      localStorage.removeItem('auth_token_timestamp');
    }
    set({ token });
  },
  logout: () => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_token_timestamp');
    set({ token: null });
  },
}));

// Session timeout check (24 hours)
if (typeof window !== 'undefined') {
  const checkSession = () => {
    const timestamp = localStorage.getItem('auth_token_timestamp');
    if (timestamp) {
      const hoursSinceLogin = (Date.now() - parseInt(timestamp)) / (1000 * 60 * 60);
      if (hoursSinceLogin >= 24) {
        localStorage.removeItem('auth_token');
        localStorage.removeItem('auth_token_timestamp');
        window.location.href = '/login';
      }
    }
  };

  // Check on load
  checkSession();
  // Check every hour
  setInterval(checkSession, 1000 * 60 * 60);
}
