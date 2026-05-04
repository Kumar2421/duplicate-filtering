import React, { useMemo } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation, Navigate } from 'react-router-dom';
import { LayoutDashboard, Users, UserPlus, Menu, X, ChevronDown, LogOut, Settings, Check, Building2, User } from 'lucide-react';
import Dashboard from './pages/Dashboard';
import Visits from './pages/Visits';
import Duplicates from './pages/Duplicates';
import Employees from './pages/Employees';
import Login from './pages/Login';
import { useAppStore } from './store/useStore';
import { Button } from './components/ui/button';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { Toaster } from 'sonner';
import { fetchBranches } from './services/api';
import logo from './assets/logo.png';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "./components/ui/dropdown-menu";

const queryClient = new QueryClient();

const SidebarItem = ({ to, icon: Icon, label, active }: { to: string, icon: any, label: string, active: boolean }) => (
  <Link
    to={to}
    className={`flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200 ${active ? 'bg-blue-600 text-white shadow-lg shadow-blue-200 scale-[1.02]' : 'text-slate-500 hover:bg-slate-50 hover:text-slate-900'
      }`}
  >
    <Icon size={18} />
    <span className="font-bold text-sm tracking-tight">{label}</span>
  </Link>
);

const ProtectedRoute = ({ children }: { children: React.ReactNode }) => {
  const token = useAppStore((state) => state.token);
  const location = useLocation();

  if (!token) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  return <>{children}</>;
};

const Layout = ({ children }: { children: React.ReactNode }) => {
  const { pathname } = useLocation();
  const { sidebarOpen, setSidebarOpen, currentBranch, setCurrentBranch, logout, token } = useAppStore();

  const username = useMemo(() => {
    if (!token) return 'Administrator';
    try {
      // Decode JWT payload to get username (sub field)
      const payloadB64 = token.split('.')[1];
      const payloadJson = atob(payloadB64);
      const payload = JSON.parse(payloadJson);
      return payload.sub || 'Administrator';
    } catch (e) {
      return 'Administrator';
    }
  }, [token]);

  const { data: branchesData } = useQuery({
    queryKey: ['branches'],
    queryFn: fetchBranches,
  });

  const branches = branchesData?.branches || ['TMJ-CBE'];

  return (
    <div className="flex h-screen bg-white overflow-hidden font-sans text-slate-900">

      {/* Sidebar */}
      <aside className={`bg-white border-r border-slate-100 h-full flex flex-col transition-all duration-500 ease-in-out z-20 ${sidebarOpen ? 'w-72' : 'w-24'}`}>
        <div className="p-8 flex items-center gap-4 h-24">
          <div className="w-10 h-10 rounded-2xl bg-blue-600 flex items-center justify-center overflow-hidden shadow-lg shadow-blue-200 flex-shrink-0">
            <img src={logo} alt="Logo" className="w-6 h-6 object-contain brightness-0 invert" />
          </div>
          {sidebarOpen && (
            <div className="flex flex-col animate-in fade-in slide-in-from-left-4 duration-500">
              <span className="font-black text-xl tracking-tighter leading-none">Fusion</span>
              <span className="text-[10px] font-black uppercase tracking-[0.2em] text-blue-600 mt-1">Filtering System</span>
            </div>
          )}
        </div>

        <nav className="flex-1 px-4 py-2 space-y-1.5 overflow-y-auto">
          <div className={`px-4 mb-4 ${sidebarOpen ? 'block' : 'hidden'}`}>
            <span className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">Main Menu</span>
          </div>
          <SidebarItem to="/" icon={LayoutDashboard} label="Dashboard" active={pathname === '/'} />
          <SidebarItem to="/visits" icon={Users} label="Visits" active={pathname === '/visits'} />
          <SidebarItem to="/duplicates" icon={UserPlus} label="Duplicates" active={pathname === '/duplicates'} />
          <SidebarItem to="/employees" icon={Users} label="Employees" active={pathname === '/employees'} />
        </nav>

        <div className="p-4 mt-auto">
          <div className={`px-4 mb-4 ${sidebarOpen ? 'block' : 'hidden'}`}>
            <span className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">System</span>
          </div>
          <SidebarItem to="/settings" icon={Settings} label="Settings" active={pathname === '/settings'} />
          <button
            onClick={logout}
            className="flex w-full items-center gap-3 px-4 py-3 text-red-500 hover:bg-red-50 rounded-xl transition-all duration-200 font-bold text-sm mt-1"
          >
            <LogOut size={18} />
            {sidebarOpen && <span className="tracking-tight">Sign Out</span>}
          </button>
        </div>
      </aside>

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col overflow-hidden bg-slate-50/50">

        {/* Navbar - Redesigned as "Plane" Top Bar */}
        <header className="h-24 flex items-center justify-between px-10 z-10 transition-all duration-300">

          <div className="flex items-center gap-8">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="w-12 h-12 rounded-2xl bg-white border border-slate-100 shadow-sm hover:shadow-md transition-all text-slate-600 hover:text-blue-600"
            >
              {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
            </Button>

            <div className="flex flex-col">
              <h2 className="text-xl font-black text-slate-900 tracking-tight capitalize">
                {pathname === '/' ? 'Overview' : pathname.substring(1)}
              </h2>
              <div className="flex items-center gap-2 mt-0.5">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
                <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">System Active</span>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-6">
            {/* Branch selector */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <div className="flex items-center gap-3 px-5 py-3 bg-white border border-slate-100 rounded-2xl cursor-pointer hover:shadow-md transition-all group active:scale-[0.98]">
                  <div className="w-6 h-6 rounded-lg bg-blue-50 flex items-center justify-center text-blue-600">
                    <Building2 size={14} />
                  </div>
                  <div className="flex flex-col">
                    <span className="text-[9px] font-black text-slate-400 uppercase tracking-widest leading-none">Branch</span>
                    <span className="font-black text-xs text-slate-900 mt-0.5">{currentBranch}</span>
                  </div>
                  <ChevronDown size={14} className="text-slate-400 group-hover:text-blue-600 transition-colors ml-1" />
                </div>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-64 p-3 bg-white/80 backdrop-blur-xl rounded-2xl shadow-2xl border border-white/20 z-[100] mt-2">
                <div className="px-3 py-2 mb-2">
                  <span className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">Available Branches</span>
                </div>
                <div className="space-y-1">
                  {branches.map((branch: string) => (
                    <DropdownMenuItem
                      key={branch}
                      onClick={() => setCurrentBranch(branch)}
                      className={`flex items-center justify-between px-4 py-3 rounded-xl cursor-pointer transition-all duration-200 ${currentBranch === branch ? 'bg-blue-600 text-white shadow-lg shadow-blue-100' : 'hover:bg-slate-50 text-slate-700'
                        }`}
                    >
                      <div className="flex items-center gap-3">
                        <Building2 size={16} className={currentBranch === branch ? 'text-white' : 'text-slate-400'} />
                        <span className="font-bold text-sm">{branch}</span>
                      </div>
                      {currentBranch === branch && <Check size={14} className="text-white" />}
                    </DropdownMenuItem>
                  ))}
                </div>
              </DropdownMenuContent>
            </DropdownMenu>

            <div className="w-px h-10 bg-slate-200 mx-2"></div>

            <div className="flex items-center gap-4">
              <div className="flex flex-col text-right">
                <span className="text-sm font-black text-slate-900 leading-none tracking-tight">{username}</span>
                <span className="text-[10px] font-bold text-blue-600 uppercase tracking-widest mt-1">Full Access</span>
              </div>
              <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-slate-800 to-slate-900 flex items-center justify-center text-white shadow-xl shadow-slate-200 border-2 border-white overflow-hidden active:scale-95 transition-transform cursor-pointer">
                <User size={20} />
              </div>
            </div>
          </div>
        </header>

        {/* Dynamic Page Router */}
        <main className="flex-1 overflow-y-auto px-10 pb-10">
          <div className="max-w-[1600px] mx-auto">
            {children}
          </div>
        </main>
      </div>
    </div>
  );
};

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Toaster position="top-right" richColors />
      <Router>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/*" element={
            <ProtectedRoute>
              <Layout>
                <Routes>
                  <Route path="/" element={<Dashboard />} />
                  <Route path="/visits" element={<Visits />} />
                  <Route path="/duplicates" element={<Duplicates />} />
                  <Route path="/employees" element={<Employees />} />
                  <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
              </Layout>
            </ProtectedRoute>
          } />
        </Routes>
      </Router>
    </QueryClientProvider>
  );
}

export default App;
