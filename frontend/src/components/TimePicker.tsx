import * as React from "react";
import { Clock, X } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "./ui/popover";
import { ScrollArea } from "./ui/scroll-area";
import { Button } from "./ui/button";

interface TimePickerProps {
  value: string; // HH:mm format
  onChange: (value: string) => void;
  placeholder?: string;
}

export const TimePicker: React.FC<TimePickerProps> = ({ value, onChange, placeholder = "Select time" }) => {
  const [open, setOpen] = React.useState(false);

  const hours = Array.from({ length: 24 }, (_, i) => i.toString().padStart(2, '0'));
  const minutes = Array.from({ length: 60 }, (_, i) => i.toString().padStart(2, '0'));
  const seconds = Array.from({ length: 60 }, (_, i) => i.toString().padStart(2, '0'));

  const [selectedHour, setSelectedHour] = React.useState('00');
  const [selectedMinute, setSelectedMinute] = React.useState('00');
  const [selectedSecond, setSelectedSecond] = React.useState('00');

  React.useEffect(() => {
    if (value && value.includes(':')) {
      const parts = value.split(':');
      setSelectedHour(parts[0] || '00');
      setSelectedMinute(parts[1] || '00');
      setSelectedSecond(parts[2] || '00');
    }
  }, [value]);

  const handleTimeChange = (h: string, m: string, s: string) => {
    onChange(`${h}:${m}:${s}`);
  };

  const clearTime = (e: React.MouseEvent) => {
    e.stopPropagation();
    onChange('');
    setSelectedHour('00');
    setSelectedMinute('00');
    setSelectedSecond('00');
    setOpen(false);
  };

  const formatDisplayTime = (val: string) => {
    if (!val) return placeholder;
    try {
      const [h, m, s] = val.split(':');
      const hour = parseInt(h);
      const ampm = hour >= 12 ? 'pm' : 'am';
      const displayHour = hour % 12 || 12;
      return `${displayHour}:${m}:${s || '00'} ${ampm}`;
    } catch (e) {
      return val;
    }
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          className="h-11 bg-white border-slate-200 rounded-xl text-sm font-medium w-[180px] justify-between text-left px-3 hover:border-blue-500 transition-all group"
        >
          <div className="flex items-center">
            <Clock className="mr-2 h-4 w-4 text-slate-400 group-hover:text-blue-500" />
            <span className={value ? "text-slate-900 font-bold" : "text-slate-400"}>
              {formatDisplayTime(value)}
            </span>
          </div>
          {value && (
            <X
              className="h-3.5 w-3.5 text-slate-400 hover:text-red-500 transition-colors"
              onClick={clearTime}
            />
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0" align="start">
        <div className="flex h-64">
          <div className="flex flex-col border-r">
            <div className="px-3 py-2 text-xs font-black uppercase text-slate-400 border-b">Hour</div>
            <ScrollArea className="flex-1 w-16">
              <div className="p-2 space-y-1">
                {hours.map((h) => (
                  <button
                    key={h}
                    onClick={() => {
                      setSelectedHour(h);
                      handleTimeChange(h, selectedMinute, selectedSecond);
                    }}
                    className={`w-full text-left px-3 py-1.5 rounded-md text-sm font-bold transition-colors ${selectedHour === h ? 'bg-blue-600 text-white' : 'hover:bg-slate-100 text-slate-700'
                      }`}
                  >
                    {h}
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
          <div className="flex flex-col border-r">
            <div className="px-3 py-2 text-xs font-black uppercase text-slate-400 border-b">Min</div>
            <ScrollArea className="flex-1 w-16">
              <div className="p-2 space-y-1">
                {minutes.map((m) => (
                  <button
                    key={m}
                    onClick={() => {
                      setSelectedMinute(m);
                      handleTimeChange(selectedHour, m, selectedSecond);
                    }}
                    className={`w-full text-left px-3 py-1.5 rounded-md text-sm font-bold transition-colors ${selectedMinute === m ? 'bg-blue-600 text-white' : 'hover:bg-slate-100 text-slate-700'
                      }`}
                  >
                    {m}
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
          <div className="flex flex-col">
            <div className="px-3 py-2 text-xs font-black uppercase text-slate-400 border-b">Sec</div>
            <ScrollArea className="flex-1 w-16">
              <div className="p-2 space-y-1">
                {seconds.map((s) => (
                  <button
                    key={s}
                    onClick={() => {
                      setSelectedSecond(s);
                      handleTimeChange(selectedHour, selectedMinute, s);
                    }}
                    className={`w-full text-left px-3 py-1.5 rounded-md text-sm font-bold transition-colors ${selectedSecond === s ? 'bg-blue-600 text-white' : 'hover:bg-slate-100 text-slate-700'
                      }`}
                  >
                    {s}
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
};
