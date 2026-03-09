import { ChevronRight } from "lucide-react";
import { DatabaseIcon, getDatabaseLabel, type DbType } from "@/components/DatabaseIcon";
import { motion } from "framer-motion";

interface MigrationFlowProps {
  sourceType: DbType;
  targetType: DbType;
  sourceConnected: boolean;
  targetConnected: boolean;
}

export function MigrationFlow({ sourceType, targetType, sourceConnected, targetConnected }: MigrationFlowProps) {
  const bothConnected = sourceConnected && targetConnected;

  return (
    <motion.div
      initial={{ opacity: 0, y: -8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, ease: [0.25, 0.46, 0.45, 0.94] }}
      className="flex items-center justify-center gap-6 py-8"
    >
      <FlowNode
        type={sourceType}
        label="Source"
        connected={sourceConnected}
      />

      <div className="flex items-center gap-0.5 px-2">
        {[0, 1, 2].map((i) => (
          <motion.div
            key={i}
            animate={bothConnected ? {
              opacity: [0.3, 1, 0.3],
              x: [0, 3, 0],
            } : {}}
            transition={{
              duration: 1.5,
              repeat: Infinity,
              delay: i * 0.2,
              ease: "easeInOut",
            }}
          >
            <ChevronRight
              className={`h-4 w-4 ${
                bothConnected ? "text-primary" : "text-border"
              }`}
            />
          </motion.div>
        ))}
      </div>

      <FlowNode
        type={targetType}
        label="Target"
        connected={targetConnected}
      />
    </motion.div>
  );
}

function FlowNode({ type, label, connected }: { type: DbType; label: string; connected: boolean }) {
  return (
    <div className={`flex items-center gap-3 rounded-2xl px-5 py-3.5 transition-all duration-300 ${
      connected
        ? "bg-card shadow-apple border border-primary/15"
        : "bg-secondary/60 border border-border/40"
    }`}>
      <DatabaseIcon type={type} size="md" />
      <div>
        <p className="text-[14px] font-semibold tracking-tight">{getDatabaseLabel(type)}</p>
        <p className="text-[11px] text-muted-foreground font-medium">{label}</p>
      </div>
    </div>
  );
}
