import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Check,
  Loader2,
  Wifi,
  HardDrive,
  Server,
  Cloud,
  File,
  FolderOpen,
  RefreshCw,
} from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import {
  testFileSource,
  listFiles,
  type FileSourceConfig,
  type FileSourceType,
  type FileFormat,
  type FileInfo,
} from "@/services/api";
import { fileSourceConfigs } from "@/components/DatabaseIcon";

interface FileSourceFormProps {
  connected: boolean;
  onConnect: (config: FileSourceConfig, selectedFiles: FileInfo[]) => void;
}

const fileSourceTypes: FileSourceType[] = ["local", "sftp", "s3"];

const formatOptions: { value: FileFormat | "auto"; label: string }[] = [
  { value: "auto", label: "Auto-detect" },
  { value: "csv", label: "CSV" },
  { value: "json", label: "JSON" },
  { value: "jsonl", label: "JSONL / NDJSON" },
  { value: "xlsx", label: "Excel (XLSX)" },
  { value: "parquet", label: "Parquet" },
];

const formatIconMap: Record<string, string> = {
  csv: "CSV",
  json: "JSON",
  jsonl: "JSONL",
  xlsx: "XLSX",
  parquet: "PQ",
};

const SourceTypeIcon = ({ type, size = 4 }: { type: FileSourceType; size?: number }) => {
  const cls = `h-${size} w-${size}`;
  if (type === "local") return <HardDrive className={cls} />;
  if (type === "sftp") return <Server className={cls} />;
  return <Cloud className={cls} />;
};

function formatBytes(bytes?: number): string {
  if (bytes === undefined || bytes === null) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function FileSourceForm({ connected, onConnect }: FileSourceFormProps) {
  const [sourceType, setSourceType] = useState<FileSourceType>("local");
  const [testing, setTesting] = useState(false);
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [selectedPaths, setSelectedPaths] = useState<Set<string>>(new Set());
  const [formatOverride, setFormatOverride] = useState<string>("auto");

  // Local
  const [localPaths, setLocalPaths] = useState("");

  // SFTP
  const [sftpHost, setSftpHost] = useState("");
  const [sftpPort, setSftpPort] = useState("22");
  const [sftpUser, setSftpUser] = useState("");
  const [sftpPass, setSftpPass] = useState("");
  const [sftpKeyPath, setSftpKeyPath] = useState("");
  const [useKeyAuth, setUseKeyAuth] = useState(false);
  const [sftpRemotePaths, setSftpRemotePaths] = useState("");

  // S3
  const [s3Bucket, setS3Bucket] = useState("");
  const [s3Region, setS3Region] = useState("");
  const [s3AccessKey, setS3AccessKey] = useState("");
  const [s3SecretKey, setS3SecretKey] = useState("");
  const [s3Keys, setS3Keys] = useState("");
  const [s3EndpointUrl, setS3EndpointUrl] = useState("");

  const { toast } = useToast();

  const buildConfig = (): FileSourceConfig => {
    const fmt = formatOverride === "auto" ? null : (formatOverride as FileFormat);
    if (sourceType === "local") {
      return {
        source_type: "local",
        file_paths: localPaths.split("\n").map(s => s.trim()).filter(Boolean),
        file_format: fmt,
      };
    }
    if (sourceType === "sftp") {
      return {
        source_type: "sftp",
        host: sftpHost,
        port: parseInt(sftpPort || "22", 10),
        username: sftpUser,
        password: useKeyAuth ? undefined : sftpPass,
        key_path: useKeyAuth ? sftpKeyPath : undefined,
        remote_paths: sftpRemotePaths.split("\n").map(s => s.trim()).filter(Boolean),
        file_format: fmt,
      };
    }
    // s3
    return {
      source_type: "s3",
      bucket: s3Bucket,
      region: s3Region || undefined,
      access_key_id: s3AccessKey || undefined,
      secret_access_key: s3SecretKey || undefined,
      keys: s3Keys.split("\n").map(s => s.trim()).filter(Boolean),
      endpoint_url: s3EndpointUrl || undefined,
      file_format: fmt,
    };
  };

  const handleTestAndList = async () => {
    setTesting(true);
    setFiles([]);
    setSelectedPaths(new Set());
    const config = buildConfig();
    try {
      const result = await testFileSource(config);
      if (!result.success) {
        toast({ title: "Connection failed", description: result.message, variant: "destructive" });
        return;
      }
      toast({ title: "Connected", description: result.message });

      setLoadingFiles(true);
      const fileList = await listFiles(config);
      setFiles(fileList);
      // Auto-select all by default
      setSelectedPaths(new Set(fileList.map(f => f.path)));
    } catch (err: any) {
      toast({ title: "Error", description: err.message, variant: "destructive" });
    } finally {
      setTesting(false);
      setLoadingFiles(false);
    }
  };

  const handleRefreshFiles = async () => {
    setLoadingFiles(true);
    const config = buildConfig();
    try {
      const fileList = await listFiles(config);
      setFiles(fileList);
    } catch (err: any) {
      toast({ title: "Error refreshing files", description: err.message, variant: "destructive" });
    } finally {
      setLoadingFiles(false);
    }
  };

  const toggleFile = (path: string) => {
    setSelectedPaths(prev => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  const toggleAll = () => {
    if (selectedPaths.size === files.length) {
      setSelectedPaths(new Set());
    } else {
      setSelectedPaths(new Set(files.map(f => f.path)));
    }
  };

  const handleConnect = () => {
    const config = buildConfig();
    const selected = files.filter(f => selectedPaths.has(f.path));
    if (selected.length === 0) {
      toast({ title: "No files selected", description: "Select at least one file to import.", variant: "destructive" });
      return;
    }
    onConnect(config, selected);
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
        <div className="flex items-start justify-between mb-4">
          <div className="flex gap-3 items-center">
            <div className="text-primary h-6 w-6 flex items-center justify-center">
              <File className="h-5 w-5 text-[#E85C1C]" />
            </div>
            <div>
              <h3 className="text-[16px] font-bold tracking-tight text-foreground">File Source</h3>
              <p className="text-[12px] text-muted-foreground mt-0.5">Import from local disk, SSH/SFTP, or AWS S3</p>
            </div>
          </div>
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

        {/* Source type selector */}
        <div className="space-y-2">
          <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Source Type</Label>
          <div className="grid grid-cols-3 gap-2">
            {fileSourceTypes.map(type => {
              const cfg = fileSourceConfigs[type];
              const active = sourceType === type;
              return (
                <button
                  key={type}
                  type="button"
                  onClick={() => { setSourceType(type); setFiles([]); setSelectedPaths(new Set()); }}
                  className={`flex flex-col items-center gap-1.5 rounded-lg border px-3 py-2.5 transition-all text-center ${
                    active
                      ? "border-[#E85C1C] bg-[#FFF6F0] text-[#E85C1C]"
                      : "border-border bg-muted/10 text-muted-foreground hover:border-border/80 hover:bg-muted/20"
                  }`}
                >
                  <SourceTypeIcon type={type} size={4} />
                  <span className="text-[11px] font-semibold leading-tight">{cfg.label.replace("Local File System", "Local FS")}</span>
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* Dynamic form fields */}
      <div className="px-6 pb-4 space-y-4">
        <AnimatePresence mode="wait">
          {sourceType === "local" && (
            <motion.div key="local" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-2">
              <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">
                File / Directory Paths <span className="normal-case font-normal">(one per line)</span>
              </Label>
              <textarea
                value={localPaths}
                onChange={e => setLocalPaths(e.target.value)}
                rows={3}
                placeholder={"/data/users.csv\n/exports/orders.parquet\n/reports/"}
                className="w-full rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono placeholder:text-muted-foreground/40 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm px-3 py-2 resize-none"
              />
            </motion.div>
          )}

          {sourceType === "sftp" && (
            <motion.div key="sftp" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-4">
              <div className="grid grid-cols-3 gap-3">
                <div className="col-span-2 space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Host</Label>
                  <Input value={sftpHost} onChange={e => setSftpHost(e.target.value)} placeholder="files.example.com" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px]" />
                </div>
                <div className="space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Port</Label>
                  <Input value={sftpPort} onChange={e => setSftpPort(e.target.value)} placeholder="22" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px]" />
                </div>
              </div>
              <div className="space-y-2">
                <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Username</Label>
                <Input value={sftpUser} onChange={e => setSftpUser(e.target.value)} placeholder="ubuntu" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px]" />
              </div>

              <div className="flex items-center justify-between rounded-lg border border-border/60 bg-muted/10 px-4 py-3">
                <span className="text-[13px] font-medium text-foreground">Use private key</span>
                <Switch checked={useKeyAuth} onCheckedChange={setUseKeyAuth} className="data-[state=checked]:bg-[#E85C1C]" />
              </div>

              {useKeyAuth ? (
                <div className="space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Private Key Path</Label>
                  <Input value={sftpKeyPath} onChange={e => setSftpKeyPath(e.target.value)} placeholder="~/.ssh/id_rsa" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono" />
                </div>
              ) : (
                <div className="space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Password</Label>
                  <Input type="password" value={sftpPass} onChange={e => setSftpPass(e.target.value)} placeholder="••••••••" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px]" />
                </div>
              )}

              <div className="space-y-2">
                <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">
                  Remote Paths <span className="normal-case font-normal">(one per line)</span>
                </Label>
                <textarea
                  value={sftpRemotePaths}
                  onChange={e => setSftpRemotePaths(e.target.value)}
                  rows={2}
                  placeholder={"/home/ubuntu/exports/\n/data/reports/sales.csv"}
                  className="w-full rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono placeholder:text-muted-foreground/40 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm px-3 py-2 resize-none"
                />
              </div>
            </motion.div>
          )}

          {sourceType === "s3" && (
            <motion.div key="s3" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-4">
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Bucket</Label>
                  <Input value={s3Bucket} onChange={e => setS3Bucket(e.target.value)} placeholder="my-data-bucket" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px]" />
                </div>
                <div className="space-y-2">
                  <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Region</Label>
                  <Input value={s3Region} onChange={e => setS3Region(e.target.value)} placeholder="us-east-1" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px]" />
                </div>
              </div>
              <div className="space-y-2">
                <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Access Key ID</Label>
                <Input value={s3AccessKey} onChange={e => setS3AccessKey(e.target.value)} placeholder="AKIA..." className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono" />
              </div>
              <div className="space-y-2">
                <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Secret Access Key</Label>
                <Input type="password" value={s3SecretKey} onChange={e => setS3SecretKey(e.target.value)} placeholder="••••••••" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px]" />
              </div>
              <div className="space-y-2">
                <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">
                  Object Keys / Prefixes <span className="normal-case font-normal">(one per line)</span>
                </Label>
                <textarea
                  value={s3Keys}
                  onChange={e => setS3Keys(e.target.value)}
                  rows={2}
                  placeholder={"exports/2024/\nreports/monthly.parquet"}
                  className="w-full rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono placeholder:text-muted-foreground/40 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring/30 shadow-sm px-3 py-2 resize-none"
                />
              </div>
              <div className="space-y-2">
                <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">
                  Endpoint URL <span className="normal-case font-normal">(optional, for MinIO etc.)</span>
                </Label>
                <Input value={s3EndpointUrl} onChange={e => setS3EndpointUrl(e.target.value)} placeholder="http://localhost:9000" className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-mono" />
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Format override */}
        <div className="space-y-2">
          <Label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">File Format</Label>
          <Select value={formatOverride} onValueChange={setFormatOverride}>
            <SelectTrigger className="h-10 rounded-lg bg-[#FCFDFE] border border-border text-[13px] font-medium w-full shadow-sm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent className="rounded-xl border-border/80 shadow-apple-lg">
              {formatOptions.map(opt => (
                <SelectItem key={opt.value} value={opt.value} className="text-[13px] rounded-lg cursor-pointer">
                  {opt.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Test + connect button */}
        <Button
          onClick={handleTestAndList}
          disabled={testing || loadingFiles}
          className="w-full mt-1 rounded-lg h-10 text-[13px] font-semibold bg-background text-foreground border border-border/80 hover:bg-secondary/40 shadow-sm"
          variant="outline"
        >
          {testing || loadingFiles ? (
            <Loader2 className="h-4 w-4 animate-spin mr-2" />
          ) : (
            <Wifi className="h-4 w-4 mr-2 text-muted-foreground" />
          )}
          {testing ? "Testing…" : loadingFiles ? "Listing Files…" : "Test & List Files"}
        </Button>
      </div>

      {/* File list */}
      <AnimatePresence>
        {files.length > 0 && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="border-t border-border/60 overflow-hidden"
          >
            <div className="px-6 pt-3 pb-2 flex items-center justify-between">
              <span className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">
                {files.length} file{files.length !== 1 ? "s" : ""} found
              </span>
              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={toggleAll}
                  className="text-[11px] font-medium text-[#E85C1C] hover:underline"
                >
                  {selectedPaths.size === files.length ? "Deselect all" : "Select all"}
                </button>
                <button
                  type="button"
                  onClick={handleRefreshFiles}
                  disabled={loadingFiles}
                  className="flex items-center gap-1 text-[11px] font-medium text-muted-foreground hover:text-primary"
                >
                  <RefreshCw className={`h-3 w-3 ${loadingFiles ? "animate-spin" : ""}`} />
                  Refresh
                </button>
              </div>
            </div>
            <div className="px-6 pb-4 max-h-52 overflow-y-auto space-y-1">
              {files.map(file => (
                <label
                  key={file.path}
                  className="flex items-center gap-3 rounded-lg border border-border/40 px-3 py-2 cursor-pointer hover:bg-muted/20 transition-colors"
                >
                  <Checkbox
                    checked={selectedPaths.has(file.path)}
                    onCheckedChange={() => toggleFile(file.path)}
                    className="data-[state=checked]:bg-[#E85C1C] data-[state=checked]:border-[#E85C1C]"
                  />
                  <div className="flex items-center gap-2 flex-1 min-w-0">
                    <span className="text-[10px] font-bold bg-muted/50 text-muted-foreground px-1.5 py-0.5 rounded font-mono uppercase shrink-0">
                      {file.format ? formatIconMap[file.format] ?? file.format.toUpperCase() : "?"}
                    </span>
                    <span className="text-[13px] text-foreground truncate font-medium">{file.name}</span>
                  </div>
                  {file.size !== undefined && (
                    <span className="text-[11px] text-muted-foreground shrink-0">{formatBytes(file.size)}</span>
                  )}
                </label>
              ))}
            </div>

            <div className="px-6 pb-5">
              <Button
                onClick={handleConnect}
                disabled={selectedPaths.size === 0 || connected}
                className={`w-full rounded-lg h-10 text-[13px] font-semibold transition-all duration-300 shadow-sm ${
                  connected
                    ? "bg-success/10 text-success border border-success/20"
                    : "bg-[#E85C1C] hover:bg-[#D65116] text-white"
                }`}
              >
                {connected ? (
                  <><Check className="h-4 w-4 mr-2" /> Files Selected</>
                ) : (
                  <><FolderOpen className="h-4 w-4 mr-2" /> Use {selectedPaths.size} Selected File{selectedPaths.size !== 1 ? "s" : ""}</>
                )}
              </Button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
