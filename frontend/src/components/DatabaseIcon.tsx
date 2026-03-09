import { Database, Snowflake } from "lucide-react";

type DbType = "postgresql" | "mysql" | "snowflake" | "sqlite" | "sqlserver";

const configs: Record<DbType, { label: string; gradient: string; iconBg: string }> = {
  postgresql: {
    label: "PostgreSQL",
    gradient: "from-blue-500 to-indigo-600",
    iconBg: "bg-gradient-to-br from-blue-500 to-indigo-600",
  },
  mysql: {
    label: "MySQL",
    gradient: "from-orange-400 to-amber-500",
    iconBg: "bg-gradient-to-br from-orange-400 to-amber-500",
  },
  snowflake: {
    label: "Snowflake",
    gradient: "from-cyan-400 to-blue-500",
    iconBg: "bg-gradient-to-br from-cyan-400 to-blue-500",
  },
  sqlite: {
    label: "SQLite",
    gradient: "from-slate-500 to-slate-700",
    iconBg: "bg-gradient-to-br from-slate-500 to-slate-700",
  },
  sqlserver: {
    label: "SQL Server",
    gradient: "from-red-600 to-red-800",
    iconBg: "bg-gradient-to-br from-red-600 to-red-800",
  },
};

export function DatabaseIcon({ type, size = "md" }: { type: DbType; size?: "sm" | "md" | "lg" }) {
  const config = configs[type];
  const sizeMap = {
    sm: { box: "h-7 w-7 rounded-lg", icon: "h-3.5 w-3.5" },
    md: { box: "h-10 w-10 rounded-xl", icon: "h-5 w-5" },
    lg: { box: "h-12 w-12 rounded-2xl", icon: "h-6 w-6" },
  };
  const s = sizeMap[size];
  const Icon = type === "snowflake" ? Snowflake : Database;

  return (
    <div className={`${s.box} ${config.iconBg} flex items-center justify-center shadow-apple-sm`}>
      <Icon className={`${s.icon} text-primary-foreground`} />
    </div>
  );
}

export function getDatabaseLabel(type: DbType) {
  return configs[type].label;
}

export type { DbType };
