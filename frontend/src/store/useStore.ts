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
}));
