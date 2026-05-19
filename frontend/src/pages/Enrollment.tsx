import React, { useState, useCallback } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { useAppStore } from '../store/useStore';
import { enrollEmployee, fetchEnrolledEmployees, deleteEnrolledEmployee, BASE_URL } from '../services/api';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { 
  Upload, UserPlus, UserCheck, ShieldCheck, 
  Loader2, RefreshCw, Trash2, X, UserCircle 
} from 'lucide-react';
import { toast } from 'sonner';

const Enrollment: React.FC = () => {
  const { currentBranch } = useAppStore();
  const [enrollmentName, setEnrollmentName] = useState('');
  const [enrollmentId, setEnrollmentId] = useState('');
  const [enrollmentImage, setEnrollmentImage] = useState<string | null>(null);

  const { data: enrolledEmployees, isLoading: loadingEnrolled, refetch: refetchEnrolled } = useQuery({
    queryKey: ['enrolled-employees', currentBranch],
    queryFn: () => fetchEnrolledEmployees(currentBranch),
    enabled: !!currentBranch,
  });

  const enrollmentMutation = useMutation({
    mutationFn: enrollEmployee,
    onSuccess: () => {
      toast.success('Employee enrolled successfully');
      setEnrollmentName('');
      setEnrollmentId('');
      setEnrollmentImage(null);
      refetchEnrolled();
    },
    onError: (error: any) => {
      toast.error(`Enrollment failed: ${error.message}`);
    }
  });

  const deleteEnrollmentMutation = useMutation({
    mutationFn: ({ branchId, employeeId }: { branchId: string, employeeId: string }) => 
      deleteEnrolledEmployee(branchId, employeeId),
    onSuccess: () => {
      toast.success('Employee enrollment removed');
      refetchEnrolled();
    }
  });

  const handleFile = (file: File) => {
    if (!file.type.startsWith('image/')) {
      toast.error('Please upload an image file');
      return;
    }
    const reader = new FileReader();
    reader.onload = (e) => {
      setEnrollmentImage(e.target?.result as string);
    };
    reader.readAsDataURL(file);
  };

  const handleEnroll = () => {
    if (!enrollmentName || !enrollmentId || !enrollmentImage) {
      toast.error('Please provide name, ID and an image');
      return;
    }
    enrollmentMutation.mutate({
      branchId: currentBranch,
      employeeId: enrollmentId,
      name: enrollmentName,
      image: enrollmentImage
    });
  };

  return (
    <div className="space-y-6 p-6">
      <div className="w-full">
        {/* Enrolled Staff Grid Section */}
        <Card className="shadow-sm border-slate-200 rounded-2xl overflow-hidden flex flex-col min-h-[600px]">
          <CardHeader className="bg-slate-50/50 border-b border-slate-100">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-black uppercase tracking-widest flex items-center gap-2">
                <ShieldCheck className="w-4 h-4 text-emerald-600" />
                Staff Registry ({currentBranch})
              </CardTitle>
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={() => refetchEnrolled()}
                className="h-8 text-[10px] font-black uppercase tracking-tighter"
              >
                <RefreshCw className={`w-3 h-3 mr-1 ${loadingEnrolled ? 'animate-spin' : ''}`} />
                Sync
              </Button>
            </div>
          </CardHeader>
          <CardContent className="p-6">
            {loadingEnrolled ? (
              <div className="py-24 flex flex-col items-center justify-center gap-3">
                <Loader2 className="w-10 h-10 text-indigo-500 animate-spin" />
                <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Loading Registry...</p>
              </div>
            ) : enrolledEmployees?.employees?.length > 0 ? (
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 2xl:grid-cols-6 gap-6">
                {enrolledEmployees.employees.map((emp: any) => (
                  <div key={emp.employeeId} className="group relative bg-white border border-slate-100 rounded-2xl overflow-hidden shadow-sm hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
                    <div className="aspect-[3/4] overflow-hidden bg-slate-50">
                      {emp.image ? (
                         <img 
                            src={emp.image.startsWith('/') ? `${BASE_URL}${emp.image}` : emp.image} 
                            className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-110" 
                            alt={emp.name} 
                            onError={(e: any) => e.target.src = 'https://placehold.co/300x400?text=No+Image'}
                         />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center text-indigo-200">
                          <UserCircle size={64} strokeWidth={1} />
                        </div>
                      )}
                    </div>
                    
                    <div className="p-4 space-y-2">
                      <div className="flex justify-between items-start">
                        <div className="space-y-0.5">
                          <h4 className="text-xs font-black text-slate-900 uppercase tracking-tight truncate">{emp.name}</h4>
                          <span className="text-[9px] font-bold text-indigo-600 bg-indigo-50 px-1.5 py-0.5 rounded border border-indigo-100 uppercase tracking-tighter">
                            {emp.employeeId}
                          </span>
                        </div>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => {
                            if (window.confirm(`Remove enrollment for ${emp.name}?`)) {
                              deleteEnrollmentMutation.mutate({ branchId: currentBranch, employeeId: emp.employeeId });
                            }
                          }}
                          className="h-7 w-7 text-slate-300 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                        >
                          <Trash2 size={14} />
                        </Button>
                      </div>
                      <p className="text-[8px] font-medium text-slate-400 uppercase tracking-widest pt-1 border-t border-slate-50">
                        Added: {new Date(emp.enrolledAt * 1000).toLocaleDateString()}
                      </p>
                    </div>

                    <div className="absolute top-2 left-2 px-2 py-0.5 bg-emerald-500/90 backdrop-blur-sm text-white text-[8px] font-black uppercase tracking-widest rounded-full opacity-0 group-hover:opacity-100 transition-opacity">
                      Active Staff
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="py-32 flex flex-col items-center justify-center gap-3 text-slate-300">
                <UserCircle size={64} className="opacity-20" />
                <div className="text-center">
                  <p className="text-xs font-black uppercase tracking-[.2em] opacity-40">Empty Registry</p>
                  <p className="text-[10px] font-bold mt-1 opacity-30">No staff members have been enrolled yet.</p>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Enrollment;
