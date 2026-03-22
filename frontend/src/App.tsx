import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, Users, UserPlus, Menu, X, Bell, ChevronDown, LogOut, Settings } from 'lucide-react';
import Dashboard from './pages/Dashboard';
import Visits from './pages/Visits';
import Duplicates from './pages/Duplicates';
import { useAppStore } from './store/useStore';
import { Button } from './components/ui/button';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const queryClient = new QueryClient();

const SidebarItem = ({ to, icon: Icon, label, active }: { to: string, icon: any, label: string, active: boolean }) => (
  <Link 
    to={to} 
    className={`flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${
      active ? 'bg-blue-600 text-white shadow-lg' : 'text-gray-600 hover:bg-gray-100'
    }`}
  >
    <Icon size={20} />
    <span className="font-semibold text-sm">{label}</span>
  </Link>
);

const Layout = ({ children }: { children: React.ReactNode }) => {
  const { pathname } = useLocation();
  const { sidebarOpen, setSidebarOpen, currentBranch } = useAppStore();

  return (
    <div className="flex h-screen bg-gray-50 overflow-hidden font-sans text-slate-900">
      
      {/* Sidebar */}
      <aside className={`bg-white border-r h-full flex flex-col transition-all duration-300 ${sidebarOpen ? 'w-64' : 'w-20'}`}>
         <div className="p-6 flex items-center gap-3 border-b h-16">
            <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-white font-black text-xl">D</div>
            {sidebarOpen && <span className="font-black text-lg tracking-tighter">DUPLICATE.AI</span>}
         </div>

         <nav className="flex-1 p-4 space-y-2 overflow-y-auto">
            <SidebarItem to="/" icon={LayoutDashboard} label="Dashboard" active={pathname === '/'} />
            <SidebarItem to="/visits" icon={Users} label="Visits" active={pathname === '/visits'} />
            <SidebarItem to="/duplicates" icon={UserPlus} label="Duplicates" active={pathname === '/duplicates'} />
         </nav>

         <div className="p-4 border-t space-y-2">
            <SidebarItem to="/settings" icon={Settings} label="Settings" active={pathname === '/settings'} />
            <button className="flex w-full items-center gap-3 px-4 py-3 text-red-600 hover:bg-red-50 rounded-lg transition-colors font-semibold text-sm">
                <LogOut size={20} />
                {sidebarOpen && <span>Logout</span>}
            </button>
         </div>
      </aside>

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col overflow-hidden">
        
        {/* Navbar */}
        <header className="h-16 bg-white border-b flex items-center justify-between px-6 shadow-sm z-10">
          
          <div className="flex items-center gap-4">
             <Button variant="ghost" size="icon" onClick={() => setSidebarOpen(!sidebarOpen)}>
                {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
             </Button>
             
             {/* Branch selector Dropdown */}
             <div className="flex items-center gap-2 px-3 py-1.5 border rounded-md cursor-pointer hover:bg-gray-50 group">
                <span className="text-xs font-bold text-gray-500 uppercase tracking-widest">Branch:</span>
                <span className="font-bold text-sm text-blue-600">{currentBranch}</span>
                <ChevronDown size={14} className="text-gray-400 group-hover:text-blue-500 transition-colors" />
             </div>
          </div>

          <div className="flex items-center gap-4">
             <Button variant="ghost" size="icon" className="relative">
                <Bell size={20} className="text-gray-600" />
                <span className="absolute top-2 right-2 w-2.5 h-2.5 bg-red-500 border-2 border-white rounded-full"></span>
             </Button>
             
             <div className="h-8 w-px bg-gray-200"></div>

             <div className="flex items-center gap-3">
                <div className="text-right hidden sm:block">
                   <p className="text-sm font-bold leading-tight">Admin User</p>
                   <p className="text-xs text-gray-500 font-medium leading-tight">Super Control</p>
                </div>
                <div className="w-10 h-10 rounded-full bg-gradient-to-tr from-blue-500 to-indigo-600 flex items-center justify-center text-white font-bold border-2 border-white shadow-md">
                   AU
                </div>
             </div>
          </div>
        </header>

        {/* Dynamic Page Router */}
        <main className="flex-1 overflow-y-auto bg-[#F9FAFB]">
          {children}
        </main>
      </div>
    </div>
  );
};

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <Layout>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/visits" element={<Visits />} />
            <Route path="/duplicates" element={<Duplicates />} />
            <Route path="*" element={<Dashboard />} />
          </Routes>
        </Layout>
      </Router>
    </QueryClientProvider>
  );
}

export default App;
