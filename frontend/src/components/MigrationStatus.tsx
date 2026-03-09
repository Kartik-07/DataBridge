import { useState, useEffect, useRef } from "react";
import { motion } from "framer-motion";
import { Button } from "@/components/ui/button";
import { type LogEntry, MigrationLog } from "@/components/MigrationLog";
import { type TableProgressEntry } from "@/services/api";
import { Activity, Clock, Database, Play, Square, Pause } from "lucide-react";

const ETA_INTERVAL_MS = 5000;

function formatEta(seconds: number): string {
    if (!Number.isFinite(seconds) || seconds <= 0) return "0s";
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    const parts: string[] = [];
    if (h > 0) parts.push(`${h}h`);
    if (m > 0) parts.push(`${m}m`);
    parts.push(`${s}s`);
    return parts.join(" ");
}

interface MigrationStatusProps {
    migrating: boolean;
    progress: number;
    logs: LogEntry[];
    tableProgress: TableProgressEntry[];
    isDryRun: boolean;
    isPaused?: boolean;
    onStartMigration?: () => void;
    onPause?: () => void;
    onResume?: () => void;
    onCancel: () => void;
}

export function MigrationStatus({
    migrating,
    progress,
    logs,
    tableProgress,
    isDryRun,
    isPaused = false,
    onStartMigration,
    onPause,
    onResume,
    onCancel
}: MigrationStatusProps) {
    const totalRows = tableProgress.reduce((s, t) => s + t.totalRows, 0);
    const migratedRows = tableProgress.reduce((s, t) => s + t.migratedRows, 0);
    const tablesDone = tableProgress.filter(t => t.status === "done").length;
    const totalTables = tableProgress.length;

    const [speed, setSpeed] = useState(0);
    const [etaSeconds, setEtaSeconds] = useState<number | null>(null);
    const sampleRef = useRef<{ time: number; rows: number } | null>(null);
    const [tick, setTick] = useState(0);

    // Recompute speed and ETA every 5 seconds from rows migrated (speed = rows/sec, ETA = rows left / speed)
    useEffect(() => {
        if (!migrating || totalRows <= 0 || progress >= 100) {
            setSpeed(0);
            setEtaSeconds(null);
            sampleRef.current = null;
            return;
        }

        const now = Date.now();
        const prev = sampleRef.current;

        if (prev === null) {
            sampleRef.current = { time: now, rows: migratedRows };
            return;
        }

        const elapsedSec = (now - prev.time) / 1000;
        if (elapsedSec >= 5) {
            const rowsDelta = Math.max(0, migratedRows - prev.rows);
            const rowsPerSec = elapsedSec > 0 ? rowsDelta / elapsedSec : 0;
            sampleRef.current = { time: now, rows: migratedRows };
            setSpeed(rowsPerSec);

            const rowsLeft = Math.max(0, totalRows - migratedRows);
            if (rowsPerSec > 0 && rowsLeft > 0) {
                setEtaSeconds(rowsLeft / rowsPerSec);
            } else {
                setEtaSeconds(null);
            }
        }
    }, [migrating, totalRows, migratedRows, progress, tick]);

    // Fire a tick every 5 seconds so speed/ETA recalc runs on a fixed interval
    useEffect(() => {
        if (!migrating || totalRows <= 0 || progress >= 100) return;
        const id = setInterval(() => setTick((t) => t + 1), ETA_INTERVAL_MS);
        return () => clearInterval(id);
    }, [migrating, totalRows, progress]);

    const displaySpeed = progress >= 100 ? 0 : speed;
    const displayEta =
        progress >= 100
            ? "0s"
            : etaSeconds != null && Number.isFinite(etaSeconds)
                ? formatEta(etaSeconds)
                : "—";

    return (
        <div className="space-y-6">
            {/* Top Stats Cards */}
            <div className="grid grid-cols-4 gap-4">
                <div className="bg-white rounded-xl border border-border/60 shadow-sm p-5">
                    <div className="flex items-center gap-2 mb-3">
                        <Activity className="h-4 w-4 text-[#E85C1C]" />
                        <h4 className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Rows Migrated</h4>
                    </div>
                    <p className="text-[28px] font-bold text-foreground leading-none mb-1">
                        {migratedRows.toLocaleString()}
                    </p>
                    <p className="text-[12px] text-muted-foreground">of {totalRows.toLocaleString()}</p>
                </div>

                <div className="bg-white rounded-xl border border-border/60 shadow-sm p-5">
                    <div className="flex items-center gap-2 mb-3">
                        <Activity className="h-4 w-4 text-[#E85C1C]" />
                        <h4 className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Speed</h4>
                    </div>
                    <p className="text-[28px] font-bold text-foreground leading-none mb-1">
                        {Math.round(displaySpeed).toLocaleString()}
                    </p>
                    <p className="text-[12px] text-muted-foreground">rows/sec</p>
                </div>

                <div className="bg-white rounded-xl border border-border/60 shadow-sm p-5">
                    <div className="flex items-center gap-2 mb-3">
                        <Clock className="h-4 w-4 text-amber-500" />
                        <h4 className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">ETA</h4>
                    </div>
                    <p className="text-[28px] font-bold text-foreground leading-none mb-1">
                        {displayEta}
                    </p>
                    <p className="text-[12px] text-muted-foreground">remaining</p>
                </div>

                <div className="bg-white rounded-xl border border-border/60 shadow-sm p-5">
                    <div className="flex items-center gap-2 mb-3">
                        <Database className="h-4 w-4 text-emerald-500" />
                        <h4 className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Tables Done</h4>
                    </div>
                    <p className="text-[28px] font-bold text-foreground leading-none mb-1">
                        {tablesDone}
                    </p>
                    <p className="text-[12px] text-muted-foreground">of {totalTables}</p>
                </div>
            </div>

            {/* Main Progress Bar Card */}
            <div className="bg-white rounded-xl border border-border/60 shadow-sm p-6">
                <div className="flex items-center justify-between mb-4">
                    <h3 className="text-[15px] font-bold text-foreground">
                        {isDryRun ? "Dry Run Status" : (progress === 100 ? "Migration Complete" : "Migrating Data...")}
                    </h3>
                    <span className="text-[15px] font-bold text-[#E85C1C] font-mono">{progress}%</span>
                </div>

                <div className="h-3 rounded-full bg-secondary overflow-hidden mb-6">
                    <motion.div
                        className={`h-full rounded-full ${isDryRun ? "bg-gradient-to-r from-blue-400 to-blue-600" : (progress === 100 ? "bg-gradient-to-r from-emerald-400 to-emerald-600" : "bg-gradient-to-r from-orange-400 to-[#E85C1C]")}`}
                        initial={{ width: 0 }}
                        animate={{ width: `${progress}%` }}
                        transition={{ duration: 0.3, ease: "easeOut" }}
                    />
                </div>

                <div className="flex gap-3">
                    {onPause && onResume && (
                        <Button
                            variant="outline"
                            size="sm"
                            className="h-9 px-4 text-[12px] font-bold rounded-lg border-border"
                            disabled={progress === 100}
                            onClick={isPaused ? onResume : onPause}
                        >
                            {isPaused ? (
                                <><Play className="h-3.5 w-3.5 mr-2" /> Resume</>
                            ) : (
                                <><Pause className="h-3.5 w-3.5 mr-2" /> Pause</>
                            )}
                        </Button>
                    )}
                    <Button
                        variant="outline"
                        size="sm"
                        className="h-9 px-4 text-[12px] font-bold rounded-lg border-red-200 text-red-600 hover:bg-red-50 hover:text-red-700"
                        onClick={onCancel}
                        disabled={progress === 100}
                    >
                        <Square className="h-3.5 w-3.5 mr-2" /> Cancel
                    </Button>
                </div>
            </div>

            {/* Table Progress Breakdown */}
            <div className="bg-white rounded-xl border border-border/60 shadow-sm overflow-hidden">
                <div className="grid grid-cols-[1.5fr_1fr_120px] gap-4 px-6 py-3 border-b border-border/60 bg-[#FAFBFC] text-[11px] font-bold text-muted-foreground tracking-wider uppercase">
                    <div>Table</div>
                    <div className="text-right">Progress</div>
                    <div className="text-right">Rows</div>
                </div>

                <div className="divide-y divide-border/30">
                    {tableProgress.length === 0 && (
                        <div className="p-8 text-center text-[13px] text-muted-foreground">Gathering table information...</div>
                    )}
                    {tableProgress.map((table) => {
                        const pct = table.totalRows > 0 ? Math.round((table.migratedRows / table.totalRows) * 100) : 0;
                        return (
                            <div key={`${table.schema}.${table.name}`} className="grid grid-cols-[1.5fr_1fr_120px] gap-4 px-6 py-4 items-center">
                                <div className="flex items-center gap-2">
                                    <div className={`h-2 w-2 rounded-full shrink-0 ${table.status === "done" ? "bg-emerald-500" : table.status === "migrating" ? "bg-[#E85C1C] animate-pulse" : "bg-border"}`} />
                                    <span className="text-[12px] font-medium text-muted-foreground">{table.schema}.<span className="text-foreground font-bold">{table.name}</span></span>
                                </div>
                                <div className="flex items-center justify-end gap-3">
                                    <div className="w-full max-w-[120px] h-1.5 rounded-full bg-secondary overflow-hidden">
                                        <motion.div
                                            className={`h-full rounded-full ${table.status === "done" ? "bg-emerald-500" : table.status === "migrating" ? "bg-[#E85C1C]" : "bg-transparent"}`}
                                            animate={{ width: `${pct}%` }}
                                            transition={{ duration: 0.2 }}
                                        />
                                    </div>
                                    <span className="text-[11px] font-mono font-medium min-w-[32px] text-right">{pct}%</span>
                                </div>
                                <div className="text-right">
                                    <span className="text-[11px] font-mono text-foreground font-medium">{table.migratedRows > 0 ? table.migratedRows.toLocaleString() : "—"}</span>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>

            {/* If Dry Run finished, show Start Migration Button */}
            {isDryRun && progress === 100 && onStartMigration && (
                <div className="bg-[#FFF6F0] rounded-xl border border-[#FEE3D4] p-6 flex flex-col items-center justify-center space-y-4">
                    <h3 className="text-[15px] font-bold text-foreground">Dry Run Completed Successfully</h3>
                    <p className="text-[13px] text-muted-foreground text-center max-w-md">The dry run verified all schema mappings and data extraction without writing to the target. You can now safely begin the actual migration.</p>
                    <Button
                        onClick={onStartMigration}
                        className="bg-[#E85C1C] hover:bg-[#D65116] text-white font-bold h-11 px-8 text-[14px] rounded-lg mt-2"
                    >
                        Start Actual Migration
                    </Button>
                </div>
            )}

            {/* Terminal Output */}
            <div>
                <MigrationLog logs={logs} />
            </div>
        </div>
    );
}
