import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { getDatabaseLabel, type DbType } from "@/components/DatabaseIcon";
import { Check, Loader2, Link2, Wifi, Globe } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { testConnection, type ConnectionConfig } from "@/services/api";
import { useToast } from "@/hooks/use-toast";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";

interface ConnectionFormProps {
  title: string;
  subtitle?: string;
  icon?: React.ReactNode;
  linkText?: string;
  linkHref?: string;
  testButtonText?: string;
  dbType: DbType;
  onDbTypeChange: (type: DbType) => void;
  connected: boolean;
  onConnect: (config: ConnectionConfig) => void;
}

const dbOptions: DbType[] = ["postgresql", "mysql", "snowflake", "sqlite", "sqlserver"];

export interface ParsedUriDetails {
  scheme: string;
  user: string;
  password: string;
  host: string;
  port: string;
  database: string;
  schema?: string;
  warehouse?: string;
  role?: string;
}

/**
 * Parse database URI. Handles passwords with special characters (e.g. / or @)
 * and Snowflake-style paths (database/schema) and query params (warehouse, role).
 */
function parseDatabaseUri(uri: string): ParsedUriDetails | null {
  const trimmed = uri.trim();
  if (!trimmed) return null;

  const schemeMatch = trimmed.match(/^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/(.*)$/);
  if (!schemeMatch) return null;
  const scheme = schemeMatch[1].toLowerCase();
  const rest = schemeMatch[2];

  // Find the @ that separates userinfo from host (host part has no / until path)
  const authorityMatch = rest.match(/^(.+)@([^/]+)\/(.*)$/);
  if (!authorityMatch) return null;
  const [, userinfo, hostPort, pathAndQuery] = authorityMatch;

  const colonIndex = userinfo.indexOf(":");
  const user = colonIndex >= 0 ? decodeURIComponent(userinfo.slice(0, colonIndex)) : decodeURIComponent(userinfo);
  const password = colonIndex >= 0 ? decodeURIComponent(userinfo.slice(colonIndex + 1)) : "";

  const portMatch = hostPort.match(/^([^:]+)(?::(\d+))?$/);
  const host = portMatch ? portMatch[1] : hostPort;
  const defaultPort = scheme === "postgresql" ? "5432" : scheme === "mysql" ? "3306" : "443";
  const port = portMatch?.[2] ?? defaultPort;

  const [path, queryString] = pathAndQuery.includes("?") ? pathAndQuery.split("?", 2) : [pathAndQuery, ""];
  const pathSegments = path.replace(/\/$/, "").split("/").filter(Boolean);
  const database = pathSegments[0] ?? "";
  const schema = pathSegments[1];

  let warehouse: string | undefined;
  let role: string | undefined;
  if (queryString) {
    const params = new URLSearchParams(queryString);
    warehouse = params.get("warehouse") ?? undefined;
    role = params.get("role") ?? undefined;
  }

  return {
    scheme,
    user,
    password,
    host,
    port,
    database,
    ...(schema && { schema }),
    ...(warehouse && { warehouse }),
    ...(role && { role }),
  };
}

function schemeToDbType(scheme: string): DbType {
  const s = scheme.toLowerCase();
  if (s === "postgres" || s === "postgresql") return "postgresql";
  if (s === "mysql" || s === "mysql2") return "mysql";
  if (s === "snowflake") return "snowflake";
  if (s === "sqlite") return "sqlite";
  if (s === "sqlserver" || s === "mssql") return "sqlserver";
  return "postgresql";
}

const getDbImage = (db: DbType) => {
  if (db === "postgresql") return "/postgres.png";
  if (db === "mysql") return "/mysql.png";
  if (db === "snowflake") return "/snowflake.png";
  if (db === "sqlite") return "/sqlite.png";
  if (db === "sqlserver") return "/sqlserver.png";
  return "";
};

export function ConnectionForm({
  title,
  subtitle,
  icon,
  linkText,
  linkHref,
  testButtonText,
  dbType,
  onDbTypeChange,
  connected,
  onConnect
}: ConnectionFormProps) {
  const [testing, setTesting] = useState(false);
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [database, setDatabase] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [warehouse, setWarehouse] = useState("");
  const [schemaName, setSchemaName] = useState("");
  const [serverInfo, setServerInfo] = useState("");
  const [uriInput, setUriInput] = useState("");
  const [showUriInput, setShowUriInput] = useState(false);
  const [useBrowserLogin, setUseBrowserLogin] = useState(false);
  const { toast } = useToast();

  const handleParseUri = () => {
    const parsed = parseDatabaseUri(uriInput);
    if (!parsed) {
      toast({
        title: "Invalid URI",
        description: "Could not parse the database URI. Use format: postgresql://user:pass@host:5432/db",
        variant: "destructive",
      });
      return;
    }
    onDbTypeChange(schemeToDbType(parsed.scheme));
    setHost(parsed.host);
    setPort(parsed.port);
    setDatabase(parsed.database);
    setUsername(parsed.user);
    setPassword(parsed.password);
    if (parsed.schema) setSchemaName(parsed.schema);
    if (parsed.warehouse) setWarehouse(parsed.warehouse);
    setShowUriInput(false);
    toast({
      title: "URI parsed",
      description: "Connection fields have been filled from the URI.",
    });
  };

  const defaultPort =
    dbType === "postgresql" ? "5432" :
    dbType === "mysql" ? "3306" :
    dbType === "sqlserver" ? "1433" : "443";
  const defaultTestButtonText = `Test ${title} Connection`;

  const handleConnect = async () => {
    setTesting(true);
    const config: ConnectionConfig = {
      db_type: dbType,
      host: dbType === "sqlite" ? "" : host.trim(),
      port: dbType === "sqlite" ? 0 : parseInt(port || defaultPort, 10),
      database: database.trim(),
      username: dbType === "sqlite" ? "" : username.trim(),
      password: dbType === "snowflake" && useBrowserLogin ? "" : password,
      ...(dbType === "snowflake" && {
        warehouse: warehouse.trim() || undefined,
        schema_name: schemaName.trim() || undefined,
        use_browser_login: useBrowserLogin || undefined,
      }),
    };

    try {
      const result = await testConnection(config);
      if (result.success) {
        setServerInfo(result.message);
        toast({
          title: "Connection successful",
          description: `${result.message} — ${result.tables_count ?? 0} tables found`,
        });
        onConnect({
          ...config,
          server_version: result.server_version,
          tables_count: result.tables_count,
        });
      } else {
        toast({
          title: "Connection failed",
          description: result.message,
          variant: "destructive",
        });
      }
    } catch (err: any) {
      toast({
        title: "Connection error",
        description: err.message || "An unexpected error occurred",
        variant: "destructive",
      });
    } finally {
      setTesting(false);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: [0.25, 0.46, 0.45, 0.94] }}
      className="rounded-xl bg-card shadow-apple border border-border/60 overflow-hidden"
    >
      {/* Header */}
      <div className="px-6 pt-6 pb-4">
        <div className="flex items-start justify-between mb-2">
          <div className="flex gap-3 items-center">
            {icon && (
              <div className="text-primary h-6 w-6 flex items-center justify-center">
                {icon}
              </div>
            )}
            <div>
              <h3 className="text-[16px] font-bold tracking-tight text-foreground">{title}</h3>
              {subtitle && <p className="text-[12px] text-muted-foreground mt-0.5">{subtitle}</p>}
            </div>
          </div>
          <div className="flex flex-col items-end gap-2">
            {linkText && (
              <button
                type="button"
                onClick={() => setShowUriInput((v) => !v)}
                className="flex items-center gap-1 text-[11px] font-medium text-muted-foreground hover:text-primary transition-colors"
              >
                <Link2 className="h-3 w-3" /> {linkText}
              </button>
            )}
            <AnimatePresence>
              {connected && (
                <motion.span
                  initial={{ opacity: 0, scale: 0.8 }}
                  animate={{ opacity: 1, scale: 1 }}
                  className="flex items-center gap-1.5 text-[11px] font-medium text-success bg-success/10 px-2.5 py-1 rounded-full"
                >
                  <Check className="h-3 w-3" /> Connected
                </motion.span>
              )}
            </AnimatePresence>
          </div>
        </div>

        {/* URI input + Parse (only when URI link clicked) — above Database Engine */}
        <AnimatePresence>
          {showUriInput && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              className="space-y-2 mt-4 overflow-hidden"
            >
              <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">URI</Label>
              <div className="flex gap-2">
                <Input
                  value={uriInput}
                  onChange={(e) => setUriInput(e.target.value)}
                  placeholder="postgresql://user:pass@host:5432/db"
                  className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm flex-1"
                />
                <Button
                  type="button"
                  variant="outline"
                  onClick={handleParseUri}
                  className="h-10 px-4 rounded-lg text-[13px] font-semibold shrink-0 border-border bg-muted/30 hover:bg-muted/50"
                >
                  Parse
                </Button>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Database Engine Selector */}
        <div className="space-y-2 mt-6">
          <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Database Engine</Label>
          <Select value={dbType} onValueChange={(val) => onDbTypeChange(val as DbType)}>
            <SelectTrigger className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-medium w-full shadow-sm hover:bg-secondary/20 transition-colors">
              <SelectValue placeholder="Select a database engine" />
            </SelectTrigger>
            <SelectContent className="rounded-xl border-border/80 shadow-apple-lg">
              {dbOptions.map((db) => (
                <SelectItem key={db} value={db} className="text-[13px] rounded-lg cursor-pointer">
                  <div className="flex items-center gap-2">
                    <img src={getDbImage(db)} alt={db} className="w-5 h-5 object-contain" />
                    {getDatabaseLabel(db)}
                  </div>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* Form */}
      <div className="px-6 pb-6 space-y-4">
        {dbType === "sqlite" ? (
          <div className="space-y-2">
            <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Database File Path</Label>
            <Input
              value={database}
              onChange={(e) => setDatabase(e.target.value)}
              placeholder="/path/to/database.sqlite3 or :memory:"
              className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
            />
          </div>
        ) : (
          <>
        <div className="grid grid-cols-3 gap-3">
          <div className="col-span-2 space-y-2">
            <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Host</Label>
            <Input
              value={host}
              onChange={(e) => setHost(e.target.value)}
              placeholder={dbType === "snowflake" ? "account.snowflake.com" : dbType === "sqlserver" ? "server.database.windows.net" : "db.example.com"}
              className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-sans placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
            />
          </div>
          <div className="space-y-2">
            <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Port</Label>
            <Input
              value={port}
              onChange={(e) => setPort(e.target.value)}
              placeholder={defaultPort}
              className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-sans placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
            />
          </div>
        </div>

        <div className="space-y-2">
          <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Database Name</Label>
          <Input
            value={database}
            onChange={(e) => setDatabase(e.target.value)}
            placeholder="production_db"
            className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-sans placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="space-y-2">
            <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Username</Label>
            <Input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="admin_user"
              className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-sans placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
            />
          </div>
          <div className="space-y-2">
            <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Password</Label>
            <Input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-sans placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
            />
          </div>
        </div>
          </>
        )}

        <AnimatePresence>
          {dbType === "snowflake" && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              className="space-y-4 overflow-hidden"
            >
              <div className="flex items-center justify-between rounded-lg border border-border/60 bg-muted/10 px-4 py-3">
                <div className="flex items-center gap-2">
                  <Globe className="h-4 w-4 text-muted-foreground" />
                  <div>
                    <p className="text-[13px] font-medium text-foreground">Sign in with browser</p>
                    <p className="text-[11px] text-muted-foreground">Opens a browser for MFA (e.g. Duo)</p>
                  </div>
                </div>
                <Switch
                  checked={useBrowserLogin}
                  onCheckedChange={setUseBrowserLogin}
                  className="data-[state=checked]:bg-[#E85C1C]"
                />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Warehouse</Label>
                  <Input
                    value={warehouse}
                    onChange={(e) => setWarehouse(e.target.value)}
                    placeholder="COMPUTE_WH"
                    className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-sans placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
                  />
                </div>
                <div className="space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Schema</Label>
                  <Input
                    value={schemaName}
                    onChange={(e) => setSchemaName(e.target.value)}
                    placeholder="PUBLIC"
                    className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-sans placeholder:text-muted-foreground/40 focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm"
                  />
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        <Button
          onClick={handleConnect}
          disabled={testing || connected}
          className={`w-full mt-2 rounded-lg h-10 text-[13px] font-semibold transition-all duration-300 shadow-sm ${connected
            ? "bg-success/10 text-success border border-success/20"
            : "bg-background text-foreground border border-border/80 hover:bg-secondary/40"
            }`}
          variant={connected ? "ghost" : "outline"}
        >
          {testing ? (
            <Loader2 className="h-4 w-4 animate-spin mr-2" />
          ) : connected ? (
            <Check className="h-4 w-4 mr-2" />
          ) : (
            <Wifi className="h-4 w-4 mr-2 text-muted-foreground" />
          )}
          {testing ? "Testing Connection…" : connected ? "Connected Successfully" : testButtonText || defaultTestButtonText}
        </Button>
        {connected && serverInfo && (
          <p className="text-[11px] text-muted-foreground text-center mt-1">{serverInfo}</p>
        )}
      </div>
    </motion.div>
  );
}
