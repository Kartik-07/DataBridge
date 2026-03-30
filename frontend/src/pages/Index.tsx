import { useState, useCallback, useRef } from "react";
import { Button } from "@/components/ui/button";
import { ConnectionForm } from "@/components/ConnectionForm";
import { FileSourceForm } from "@/components/FileSourceForm";
import { MigrationOptions } from "@/components/MigrationOptions";
import { MigrationLog, type LogEntry } from "@/components/MigrationLog";
import { SchemaMapping } from "@/components/SchemaMapping";
import { type DbType } from "@/components/DatabaseIcon";
import { Play, RotateCcw, ArrowRight, Layers3, Share2, ClipboardCheck, Upload, Download, ShieldCheck, Lightbulb, Activity, Database, File } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { useToast } from "@/hooks/use-toast";
import {
  startMigration,
  startFileMigration,
  fetchSchemas,
  inferFileSchema,
  pauseMigration,
  resumeMigration,
  type ConnectionConfig,
  type FileSourceConfig,
  type FileInfo,
  type MigrationOptions as MigrationOptionsType,
  type LogEvent,
  type TableProgressEntry,
} from "@/services/api";

import { Header } from "@/components/Header";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ValidationReview } from "@/components/ValidationReview";
import { MigrationStatus } from "@/components/MigrationStatus";

const defaultOptions: MigrationOptionsType = {
  migrate_schema: true,
  migrate_data: true,
  migrate_views: true,
  migrate_functions: false,
  migrate_triggers: false,
  migrate_sequences: true,
  drop_existing: false,
  dry_run: false,
  schemas: [],
  selected_tables: {},
  mappings: {},
};

const Index = () => {
  // "database" or "file" source mode
  const [sourceCategory, setSourceCategory] = useState<"database" | "file">("database");

  // Database source state
  const [sourceType, setSourceType] = useState<DbType>("postgresql");
  const [targetType, setTargetType] = useState<DbType>("mysql");
  const [sourceConnected, setSourceConnected] = useState(false);
  const [targetConnected, setTargetConnected] = useState(false);
  const [sourceConfig, setSourceConfig] = useState<ConnectionConfig | null>(null);
  const [targetConfig, setTargetConfig] = useState<ConnectionConfig | null>(null);

  // File source state
  const [fileSourceConfig, setFileSourceConfig] = useState<FileSourceConfig | null>(null);
  const [fileSourceConnected, setFileSourceConnected] = useState(false);
  const [selectedFiles, setSelectedFiles] = useState<FileInfo[]>([]);

  const [migrating, setMigrating] = useState(false);
  const [progress, setProgress] = useState(0);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [options, setOptions] = useState<MigrationOptionsType>(defaultOptions);
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);
  const [tableProgress, setTableProgress] = useState<TableProgressEntry[]>([]);
  const [activeTab, setActiveTab] = useState("source");
  const [isDryRun, setIsDryRun] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const sessionIdRef = useRef<string | null>(null);
  const [isPaused, setIsPaused] = useState(false);
  const { toast } = useToast();

  const addLog = useCallback((message: string, type: LogEntry["type"] = "info") => {
    const now = new Date();
    const time = now.toLocaleTimeString("en-US", { hour12: false });
    setLogs((prev) => [...prev, { time, message, type }]);
  }, []);

  const handleSourceConnect = async (config: ConnectionConfig) => {
    setSourceConfig(config);
    setSourceConnected(true);
    try {
      const schemas = await fetchSchemas(config);
      setAvailableSchemas(schemas);
    } catch {
      // Non-critical — schema picker just won't show
    }
  };

  const handleTargetConnect = (config: ConnectionConfig) => {
    setTargetConfig(config);
    setTargetConnected(true);
  };

  const handleFileSourceConnect = async (config: FileSourceConfig, files: FileInfo[]) => {
    setFileSourceConfig(config);
    setSelectedFiles(files);
    setFileSourceConnected(true);

    // Infer schemas to populate options.mappings and selected_tables
    try {
      const filePaths = files.map(f => f.path);
      const tables = await inferFileSchema(config, filePaths);
      // Set selected_tables using filename stems as table names (no schema prefix)
      const selectedTables: Record<string, string[]> = { "": tables.map(t => t.name) };
      setOptions(prev => ({ ...prev, selected_tables: selectedTables }));
      setAvailableSchemas([]);
    } catch {
      // Non-critical
    }
  };

  const handleMigrate = (dryRun = false) => {
    setMigrating(true);
    setIsDryRun(dryRun);
    setProgress(0);
    setLogs([]);
    setTableProgress([]);
    setActiveTab("status");
    setIsPaused(false);

    const onEvent = (event: LogEvent) => {
      if (event.done) {
        setMigrating(false);
        setIsPaused(false);
        return;
      }
      if (event.message && event.type) {
        addLog(event.message, event.type as LogEntry["type"]);
      }
      if (event.progress !== undefined && event.progress >= 0) {
        setProgress(event.progress);
      }
      if (event.table_progress) {
        setTableProgress(event.table_progress);
      }
    };

    const onError = (error: Error) => {
      setMigrating(false);
      setIsPaused(false);
      toast({ title: "Migration failed", description: error.message, variant: "destructive" });
      addLog(`Error: ${error.message}`, "error");
    };

    if (sourceCategory === "file" && fileSourceConfig && targetConfig) {
      const { controller, sessionId } = startFileMigration(
        { file_source: fileSourceConfig, target: targetConfig, options: { ...options, dry_run: dryRun } },
        onEvent,
        onError,
      );
      abortRef.current = controller;
      sessionIdRef.current = sessionId;
    } else if (sourceConfig && targetConfig) {
      const { controller, sessionId } = startMigration(
        { source: sourceConfig, target: targetConfig, options: { ...options, dry_run: dryRun } },
        onEvent,
        onError,
      );
      abortRef.current = controller;
      sessionIdRef.current = sessionId;
    }
  };

  const handlePauseMigration = async () => {
    const sid = sessionIdRef.current;
    if (sid) {
      await pauseMigration(sid);
      setIsPaused(true);
      addLog("Migration paused by user.", "info");
    }
  };

  const handleResumeMigration = async () => {
    const sid = sessionIdRef.current;
    if (sid) {
      await resumeMigration(sid);
      setIsPaused(false);
      addLog("Migration resumed.", "info");
    }
  };

  const handleCancelMigration = () => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    sessionIdRef.current = null;
    setMigrating(false);
    setIsPaused(false);
    addLog("Migration cancelled by user.", "info");
  };

  const handleReset = () => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    sessionIdRef.current = null;
    setSourceConnected(false);
    setTargetConnected(false);
    setSourceConfig(null);
    setTargetConfig(null);
    setFileSourceConfig(null);
    setFileSourceConnected(false);
    setSelectedFiles([]);
    setMigrating(false);
    setIsDryRun(false);
    setProgress(0);
    setLogs([]);
    setOptions(defaultOptions);
    setAvailableSchemas([]);
    setTableProgress([]);
    setActiveTab("source");
  };

  const sourceReady = sourceCategory === "file" ? (fileSourceConnected && targetConnected) : (sourceConnected && targetConnected);
  const bothConnected = sourceReady;

  return (
    <div className="min-h-screen bg-[#FAFAFA]">
      {/* Header */}
      <Header onReset={handleReset} />

      <main className="container max-w-[1400px] mx-auto px-8 py-8">

        {/* Tabs container */}
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          {/* Stepper Navigation */}
          <TabsList className="flex w-full justify-start gap-10 border-b border-border/60 pb-0 mb-8 bg-transparent rounded-none h-auto">
            <TabsTrigger value="source" className="group rounded-none border-b-2 border-transparent data-[state=active]:border-[#E85C1C] data-[state=active]:bg-transparent data-[state=active]:shadow-none pb-4 pt-2 -mb-[2px]">
              <div className="flex items-center gap-2 text-[14px] font-medium text-muted-foreground group-data-[state=active]:font-bold group-data-[state=active]:text-[#E85C1C]">
                <Layers3 className="h-4 w-4" /> 1. Source Database
              </div>
            </TabsTrigger>
            <TabsTrigger value="scope" disabled={!bothConnected} className="group rounded-none border-b-2 border-transparent data-[state=active]:border-[#E85C1C] data-[state=active]:bg-transparent data-[state=active]:shadow-none pb-4 pt-2 -mb-[2px]">
              <div className="flex items-center gap-2 text-[14px] font-medium text-muted-foreground group-data-[state=active]:font-bold group-data-[state=active]:text-[#E85C1C]">
                <Share2 className="h-4 w-4" /> 2. Scope & Action
              </div>
            </TabsTrigger>
            <TabsTrigger value="mapping" disabled={!bothConnected} className="group rounded-none border-b-2 border-transparent data-[state=active]:border-[#E85C1C] data-[state=active]:bg-transparent data-[state=active]:shadow-none pb-4 pt-2 -mb-[2px]">
              <div className="flex items-center gap-2 text-[14px] font-medium text-muted-foreground group-data-[state=active]:font-bold group-data-[state=active]:text-[#E85C1C]">
                <Share2 className="h-4 w-4" /> 3. Schema Mapping
              </div>
            </TabsTrigger>
            <TabsTrigger value="validation" disabled={!bothConnected} className="group rounded-none border-b-2 border-transparent data-[state=active]:border-[#E85C1C] data-[state=active]:bg-transparent data-[state=active]:shadow-none pb-4 pt-2 -mb-[2px]">
              <div className="flex items-center gap-2 text-[14px] font-medium text-muted-foreground group-data-[state=active]:font-bold group-data-[state=active]:text-[#E85C1C]">
                <ClipboardCheck className="h-4 w-4" /> 4. Validation & Review
              </div>
            </TabsTrigger>
            <TabsTrigger value="status" disabled={!migrating && !logs.length} className="group rounded-none border-b-2 border-transparent data-[state=active]:border-[#E85C1C] data-[state=active]:bg-transparent data-[state=active]:shadow-none pb-4 pt-2 -mb-[2px]">
              <div className="flex items-center gap-2 text-[14px] font-medium text-muted-foreground group-data-[state=active]:font-bold group-data-[state=active]:text-[#E85C1C]">
                <Activity className="h-4 w-4" /> 5. Status
              </div>
            </TabsTrigger>
          </TabsList>

          <TabsContent value="source" className="mt-0 outline-none">
            {/* 3-Column Layout */}
            <div className="grid lg:grid-cols-3 gap-6">
              {/* Column 1: Source — Database or File */}
              <div className="flex flex-col gap-4">
                {/* Source category toggle */}
                <div className="flex gap-2 rounded-xl border border-border/60 bg-card p-1.5 shadow-sm">
                  <button
                    type="button"
                    onClick={() => { setSourceCategory("database"); setFileSourceConnected(false); setFileSourceConfig(null); setSelectedFiles([]); }}
                    className={`flex-1 flex items-center justify-center gap-2 rounded-lg py-2 text-[13px] font-semibold transition-all ${
                      sourceCategory === "database"
                        ? "bg-[#E85C1C] text-white shadow-sm"
                        : "text-muted-foreground hover:bg-muted/30"
                    }`}
                  >
                    <Database className="h-4 w-4" />
                    Database
                  </button>
                  <button
                    type="button"
                    onClick={() => { setSourceCategory("file"); setSourceConnected(false); setSourceConfig(null); }}
                    className={`flex-1 flex items-center justify-center gap-2 rounded-lg py-2 text-[13px] font-semibold transition-all ${
                      sourceCategory === "file"
                        ? "bg-[#E85C1C] text-white shadow-sm"
                        : "text-muted-foreground hover:bg-muted/30"
                    }`}
                  >
                    <File className="h-4 w-4" />
                    File
                  </button>
                </div>

                <AnimatePresence mode="wait">
                  {sourceCategory === "database" ? (
                    <motion.div key="db-form" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                      <ConnectionForm
                        title="Source Database"
                        subtitle="The origin of your data transfer"
                        icon={<Upload className="h-5 w-5 text-[#E85C1C]" />}
                        linkText="URI"
                        linkHref="#"
                        testButtonText="Test Source Connection"
                        dbType={sourceType}
                        onDbTypeChange={setSourceType}
                        connected={sourceConnected}
                        onConnect={handleSourceConnect}
                      />
                    </motion.div>
                  ) : (
                    <motion.div key="file-form" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                      <FileSourceForm
                        connected={fileSourceConnected}
                        onConnect={handleFileSourceConnect}
                      />
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>

              {/* Column 2: Target */}
              <ConnectionForm
                title="Target Database"
                subtitle="The destination for your data"
                icon={<Download className="h-5 w-5" />}
                linkText="URI"
                linkHref="#"
                testButtonText="Test Target Connection"
                dbType={targetType}
                onDbTypeChange={setTargetType}
                connected={targetConnected}
                onConnect={handleTargetConnect}
              />

              {/* Column 3: Info Panel */}
              <div className="flex flex-col gap-6">
                {/* Configuration Tips */}
                <div className="rounded-xl bg-[#FFF6F0] p-6 border border-[#FEE3D4]">
                  <div className="flex items-center gap-2 mb-4 text-[#E85C1C]">
                    <Lightbulb className="h-4 w-4" />
                    <h4 className="text-[13px] font-bold tracking-tight">Configuration Tips</h4>
                  </div>
                  <div className="space-y-4 text-[12px] text-muted-foreground leading-relaxed">
                    <p>Ensure your database allows connections from this machine. For cloud databases (RDS, Cloud SQL, etc.), add your IP to the allowlist or use a VPN.</p>
                    <p>Use a user account with <code className="bg-[#FEE3D4] text-[#E85C1C] px-1.5 py-0.5 rounded text-[11px] font-mono">SELECT</code> on source tables and <code className="bg-[#FEE3D4] text-[#E85C1C] px-1.5 py-0.5 rounded text-[11px] font-mono">INSERT</code>/<code className="bg-[#FEE3D4] text-[#E85C1C] px-1.5 py-0.5 rounded text-[11px] font-mono">CREATE</code> on the target.</p>
                    <p>For production, use SSL/TLS connections when your database supports it.</p>
                  </div>
                  <a href="#" className="inline-block mt-4 text-[11px] font-bold text-[#E85C1C] hover:underline">View Documentation ↗</a>
                </div>

                {/* Live Status */}
                <div className="rounded-xl bg-[#1e2329] text-white p-5 border border-[#1e2329] shadow-apple overflow-hidden font-mono text-[10px] flex-1">
                  <div className="flex items-center justify-between border-b border-slate-700/50 pb-3 mb-4">
                    <div className="flex items-center gap-2">
                      <div className={`h-2 w-2 rounded-full ${migrating ? "bg-amber-500 animate-pulse" : bothConnected ? "bg-emerald-500" : "bg-slate-500"}`} />
                      <span className="font-bold tracking-wider opacity-80 uppercase">Live Status</span>
                    </div>
                    <span className="text-slate-500 opacity-60">v1.2.0-stable</span>
                  </div>
                  <div className="space-y-2 opacity-80 max-h-32 overflow-y-auto">
                    {migrating && logs.length > 0 ? (
                      logs.slice(-5).map((log, i) => (
                        <p key={i} className={`font-medium ${log.type === "error" ? "text-red-400" : log.type === "warning" ? "text-amber-400" : "text-slate-400"}`}>
                          [{log.time}] {log.message}
                        </p>
                      ))
                    ) : (
                      <>
                        <p className="text-slate-400 font-medium">
                          {sourceCategory === "file"
                            ? (fileSourceConnected ? `SOURCE: ${selectedFiles.length} FILE(S) READY` : "WAITING: FILE SOURCE")
                            : (sourceConnected ? "SOURCE CONNECTED" : "WAITING: SOURCE CONNECTION")
                          }
                        </p>
                        <p className="text-slate-400 font-medium">
                          {targetConnected ? "TARGET CONNECTED" : "WAITING: TARGET CONNECTION"}
                        </p>
                        <p className="font-medium">
                          <span className="text-slate-400">STATUS:</span>{" "}
                          <span className={bothConnected ? "text-emerald-400" : "text-amber-400"}>
                            {bothConnected ? "READY FOR SCOPE" : "CONFIGURE CONNECTIONS"}
                          </span>
                        </p>
                      </>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Bottom Bar for source */}
            <div className="mt-8 rounded-xl bg-card border border-border/80 p-4 shadow-sm flex items-center justify-between">
              <div className="flex items-center gap-4">
                <div className="flex-shrink-0 h-10 w-10 rounded-full bg-[#FFF6F0] text-[#E85C1C] flex items-center justify-center">
                  <ShieldCheck className="h-5 w-5" />
                </div>
                <p className="text-[12px] text-muted-foreground leading-snug">
                  All connection details are encrypted using AES-256 and<br />
                  never stored in plain text. <a href="#" className="font-medium text-[#E85C1C] hover:underline">Learn more about security</a>
                </p>
              </div>
              <div className="flex items-center gap-3">
                <Button variant="outline" className="h-10 px-5 rounded-lg text-[13px] font-semibold text-foreground border-border hover:bg-secondary/40 shadow-sm" onClick={handleReset}>
                  Cancel
                </Button>
                <Button
                  className="h-10 px-6 rounded-lg text-[13px] font-semibold bg-[#E85C1C] hover:bg-[#D65116] text-white shadow-sm"
                  disabled={!bothConnected}
                  onClick={() => setActiveTab("scope")}
                >
                  Next: Scope & Action &rarr;
                </Button>
              </div>
            </div>
          </TabsContent>

          <TabsContent value="scope" className="mt-0 outline-none">
            {bothConnected && (
              <MigrationOptions
                options={options}
                onOptionsChange={setOptions}
                availableSchemas={availableSchemas}
                sourceConfig={sourceConfig}
                fileSourceConfig={fileSourceConfig}
                selectedFiles={selectedFiles}
                isFileSource={sourceCategory === "file"}
                onNext={() => setActiveTab("mapping")}
                onCancel={() => setActiveTab("source")}
                migrating={migrating}
              />
            )}
          </TabsContent>

          <TabsContent value="mapping" className="mt-0 outline-none">
            {bothConnected && (
              <SchemaMapping
                options={options}
                onOptionsChange={setOptions}
                sourceConfig={sourceConfig}
                targetConfig={targetConfig}
                fileSourceConfig={fileSourceConfig}
                selectedFiles={selectedFiles}
                isFileSource={sourceCategory === "file"}
                onNext={() => setActiveTab("validation")}
                onBack={() => setActiveTab("scope")}
              />
            )}
          </TabsContent>

          <TabsContent value="validation" className="mt-0 outline-none">
            {bothConnected && (
              <ValidationReview
                options={options}
                sourceConfig={sourceConfig}
                targetConfig={targetConfig}
                onStartMigration={(dryRun) => handleMigrate(dryRun)}
                onBack={() => setActiveTab("mapping")}
              />
            )}
          </TabsContent>

          <TabsContent value="status" className="mt-0 outline-none">
            <MigrationStatus
              migrating={migrating}
              progress={progress}
              logs={logs}
              tableProgress={tableProgress}
              isDryRun={isDryRun}
              isPaused={isPaused}
              onStartMigration={isDryRun ? () => handleMigrate(false) : undefined}
              onPause={handlePauseMigration}
              onResume={handleResumeMigration}
              onCancel={handleCancelMigration}
            />
          </TabsContent>
        </Tabs>

        {/* Progress and Logs have been moved inside the MigrationStatus component */}
      </main>
    </div>
  );
};

export default Index;
