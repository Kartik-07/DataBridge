import { ScrollArea } from "@/components/ui/scroll-area";
import { Terminal } from "lucide-react";
import { motion } from "framer-motion";

interface LogEntry {
  time: string;
  message: string;
  type: "info" | "success" | "warning" | "error";
}

const dotColors: Record<string, string> = {
  info: "bg-info",
  success: "bg-success",
  warning: "bg-warning",
  error: "bg-destructive",
};

const textColors: Record<string, string> = {
  info: "text-foreground",
  success: "text-success",
  warning: "text-warning",
  error: "text-destructive",
};

export function MigrationLog({ logs }: { logs: LogEntry[] }) {
  if (logs.length === 0) return null;

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      className="rounded-2xl bg-card shadow-apple border border-border/60 overflow-hidden"
    >
      <div className="px-5 pt-5 pb-3">
        <div className="flex items-center gap-2">
          <div className="h-7 w-7 rounded-lg bg-secondary flex items-center justify-center">
            <Terminal className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
          <h3 className="text-[15px] font-semibold tracking-tight">Migration Log</h3>
          <span className="text-[11px] text-muted-foreground font-mono ml-auto">{logs.length} entries</span>
        </div>
      </div>
      <div className="px-5 pb-5">
        <ScrollArea className="h-52 rounded-xl bg-secondary/30 border border-border/30">
          <div className="p-3 space-y-0.5">
            {logs.map((log, i) => (
              <motion.div
                key={i}
                initial={{ opacity: 0, x: -4 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ duration: 0.2, delay: i * 0.03 }}
                className="flex items-start gap-2.5 py-1 px-2 rounded-lg hover:bg-secondary/50 transition-colors"
              >
                <div className={`h-1.5 w-1.5 rounded-full mt-[7px] shrink-0 ${dotColors[log.type]}`} />
                <span className="text-[11px] text-muted-foreground font-mono shrink-0 mt-[1px]">{log.time}</span>
                <span className={`text-[12px] font-mono ${textColors[log.type]}`}>{log.message}</span>
              </motion.div>
            ))}
          </div>
        </ScrollArea>
      </div>
    </motion.div>
  );
}

export type { LogEntry };
