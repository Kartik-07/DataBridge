import { useMemo } from "react";
import { Button } from "@/components/ui/button";
import { type MigrationOptions, type ConnectionConfig, type ColumnMapping } from "@/services/api";
import { ShieldCheck, AlertTriangle, Link, Upload, Download, CheckCircle2, Bell } from "lucide-react";

interface SchemaCompatibilityWarning {
    type: "data_type_mismatch";
    sourceCol: string;
    sourceType: string;
    targetCol: string;
    targetType: string;
    message: string;
}

function getSchemaCompatibilityWarnings(mappings: Record<string, Record<string, ColumnMapping>>): SchemaCompatibilityWarning[] {
    const warnings: SchemaCompatibilityWarning[] = [];
    const integerTypes = /^(int|integer|bigint|smallint|tinyint|serial|bigserial)(\s|$|\(|\))/i;
    const floatTypes = /^(float|double|real|numeric|decimal)(\s|$|\(|\))/i;

    Object.entries(mappings).forEach(([, tableMapping]) => {
        Object.entries(tableMapping).forEach(([sourceCol, m]) => {
            const sourceType = (m.source_type || "").toLowerCase();
            const targetType = (m.target_type || "").toLowerCase();
            if (!sourceType || !targetType) return;

            // Integer -> Float: precision loss for large integers
            if (integerTypes.test(sourceType) && floatTypes.test(targetType)) {
                warnings.push({
                    type: "data_type_mismatch",
                    sourceCol,
                    sourceType: m.source_type!,
                    targetCol: m.target_name,
                    targetType: m.target_type!,
                    message: "Precision loss might occur.",
                });
            }
            // Float/Decimal -> Integer: truncation
            else if (floatTypes.test(sourceType) && integerTypes.test(targetType)) {
                warnings.push({
                    type: "data_type_mismatch",
                    sourceCol,
                    sourceType: m.source_type!,
                    targetCol: m.target_name,
                    targetType: m.target_type!,
                    message: "Fractional part will be truncated.",
                });
            }
        });
    });
    return warnings;
}

interface ValidationReviewProps {
    options: MigrationOptions;
    sourceConfig: ConnectionConfig | null;
    targetConfig: ConnectionConfig | null;
    onStartMigration: (dryRun: boolean) => void;
    onBack: () => void;
}

export function ValidationReview({ options, sourceConfig, targetConfig, onStartMigration, onBack }: ValidationReviewProps) {
    const fromSelectedTables = Object.values(options.selected_tables).reduce((acc, tables) => acc + tables.length, 0);
    const tablesSelected = fromSelectedTables > 0 ? fromSelectedTables : (options.total_tables_count ?? 0);

    let rulesApplied = 0;
    Object.values(options.mappings).forEach((tableMapping) => {
        Object.values(tableMapping).forEach((colMapping) => {
            if (colMapping.action === "transform") rulesApplied++;
        });
    });

    const schemaCompatibilityWarnings = useMemo(
        () => getSchemaCompatibilityWarnings(options.mappings),
        [options.mappings]
    );
    const schemaWarnings = schemaCompatibilityWarnings.length;
    const connectionOk = Boolean(sourceConfig && targetConfig);

    const noDropAndTablesWithoutPk =
        !options.drop_existing && (options.tables_without_pk?.length ?? 0) > 0;

    return (
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_340px] gap-8">
            {/* Main content */}
            <div className="space-y-6">
            {/* Validation Status */}
            <div className="bg-white rounded-xl border border-border/60 shadow-sm overflow-hidden p-6 relative">
                <div className="flex items-center justify-between mb-6">
                    <div className="flex items-center gap-2">
                        <ShieldCheck className="h-5 w-5 text-[#E85C1C]" />
                        <h3 className="text-[15px] font-bold text-foreground">Validation Status</h3>
                    </div>
                    <div className="bg-emerald-100 text-emerald-700 px-2.5 py-1 rounded text-[11px] font-bold uppercase tracking-wider">
                        Ready to Proceed
                    </div>
                </div>

                <div className="grid grid-cols-3 gap-4">
                    <div className="flex items-center gap-4 bg-[#FAFBFC] border border-border/60 rounded-xl p-4">
                        <div className="h-10 w-10 rounded-full bg-emerald-100 text-emerald-600 flex items-center justify-center">
                            <Link className="h-5 w-5" />
                        </div>
                        <div>
                            <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider mb-0.5">Connection</p>
                            <p className="text-[14px] font-bold text-foreground">{connectionOk ? "Success" : "—"}</p>
                        </div>
                    </div>
                    <div className={`flex items-center gap-4 rounded-xl p-4 border ${schemaWarnings > 0 ? "bg-[#FFF6F0] border-[#FEE3D4]" : "bg-[#FAFBFC] border-border/60"}`}>
                        <div className={`h-10 w-10 rounded-full flex items-center justify-center ${schemaWarnings > 0 ? "bg-orange-100 text-orange-600" : "bg-emerald-100 text-emerald-600"}`}>
                            <AlertTriangle className="h-5 w-5" />
                        </div>
                        <div>
                            <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider mb-0.5">Schema Compatibility</p>
                            <p className={`text-[14px] font-bold ${schemaWarnings > 0 ? "text-orange-800" : "text-foreground"}`}>
                                {schemaWarnings === 0 ? "No warnings" : `${schemaWarnings} Warning${schemaWarnings !== 1 ? "s" : ""}`}
                            </p>
                        </div>
                    </div>
                    <div className="flex items-center gap-4 bg-[#FAFBFC] border border-border/60 rounded-xl p-4">
                        <div className="h-10 w-10 rounded-full bg-emerald-100 text-emerald-600 flex items-center justify-center">
                            <ShieldCheck className="h-5 w-5" />
                        </div>
                        <div>
                            <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider mb-0.5">Permissions</p>
                            <p className="text-[14px] font-bold text-foreground">{connectionOk ? "Success" : "—"}</p>
                        </div>
                    </div>
                </div>
            </div>

            {/* Source & Target Database Summaries */}
            <div className="grid grid-cols-2 gap-6">
                <div className="bg-white rounded-xl border border-border/60 shadow-sm p-6">
                    <div className="flex items-center gap-2 mb-6">
                        <Upload className="h-5 w-5 text-blue-500" />
                        <h3 className="text-[14px] font-bold text-foreground">Source Database</h3>
                    </div>
                    <div className="space-y-4">
                        <div className="flex justify-between items-center pb-3 border-b border-border/40">
                            <span className="text-[12px] text-muted-foreground font-medium">Instance</span>
                            <span className="text-[13px] font-medium font-mono">{sourceConfig?.host || "—"}</span>
                        </div>
                        <div className="flex justify-between items-center pb-3 border-b border-border/40">
                            <span className="text-[12px] text-muted-foreground font-medium">Engine</span>
                            <span className="text-[13px] font-medium">
                                {sourceConfig ? `${sourceConfig.db_type}${sourceConfig.server_version ? ` ${sourceConfig.server_version}` : ""}` : "—"}
                            </span>
                        </div>
                        <div className="flex justify-between items-center">
                            <span className="text-[12px] text-muted-foreground font-medium">Estimated Size</span>
                            <span className="text-[13px] font-bold">
                                {sourceConfig?.tables_count != null ? `~ ${sourceConfig.tables_count} tables` : "—"}
                            </span>
                        </div>
                    </div>
                </div>
                <div className="bg-white rounded-xl border border-border/60 shadow-sm p-6">
                    <div className="flex items-center gap-2 mb-6">
                        <Download className="h-5 w-5 text-[#E85C1C]" />
                        <h3 className="text-[14px] font-bold text-foreground">Target Database</h3>
                    </div>
                    <div className="space-y-4">
                        <div className="flex justify-between items-center pb-3 border-b border-border/40">
                            <span className="text-[12px] text-muted-foreground font-medium">Instance</span>
                            <span className="text-[13px] font-medium font-mono">{targetConfig?.host || "—"}</span>
                        </div>
                        <div className="flex justify-between items-center pb-3 border-b border-border/40">
                            <span className="text-[12px] text-muted-foreground font-medium">Engine</span>
                            <span className="text-[13px] font-medium">
                                {targetConfig ? `${targetConfig.db_type}${targetConfig.server_version ? ` ${targetConfig.server_version}` : ""}` : "—"}
                            </span>
                        </div>
                        <div className="flex justify-between items-center">
                            <span className="text-[12px] text-muted-foreground font-medium">Available Storage</span>
                            <span className="text-[13px] font-bold">
                                {targetConfig?.tables_count != null ? `~ ${targetConfig.tables_count} tables` : "—"}
                            </span>
                        </div>
                    </div>
                </div>
            </div>

            {/* Duplicate warning when table exists + Drop Existing not selected and some tables have no PK */}
            {noDropAndTablesWithoutPk && (
                <div className="rounded-xl border border-amber-300 bg-amber-50 p-4 flex items-start gap-3">
                    <AlertTriangle className="h-5 w-5 text-amber-600 shrink-0 mt-0.5" />
                    <div>
                        <p className="text-[13px] font-semibold text-amber-900">
                            Duplicates may be added on re-run
                        </p>
                        <p className="text-[12px] text-amber-800 mt-1">
                            &quot;Drop existing tables&quot; is not selected and {options.tables_without_pk!.length} table
                            {options.tables_without_pk!.length !== 1 ? "s" : ""} have no primary key. The migration will
                            use plain INSERT for these tables, so re-running the migration will append rows again and
                            create duplicate data. Enable &quot;Drop existing tables&quot; for a clean replace, or add
                            primary keys to these tables for upsert behavior.
                        </p>
                        {options.tables_without_pk!.length <= 10 ? (
                            <p className="text-[11px] text-amber-700 mt-2 font-mono">
                                {options.tables_without_pk!.join(", ")}
                            </p>
                        ) : (
                            <p className="text-[11px] text-amber-700 mt-2">
                                {options.tables_without_pk!.slice(0, 5).join(", ")} and{" "}
                                {options.tables_without_pk!.length - 5} more
                            </p>
                        )}
                    </div>
                </div>
            )}

            {/* Mapping Summary */}
            <div className="bg-white rounded-xl border border-border/60 shadow-sm p-6">
                <div className="flex items-center justify-between mb-6">
                    <div className="flex items-center gap-2">
                        <CheckCircle2 className="h-5 w-5 text-[#E85C1C]" />
                        <h3 className="text-[14px] font-bold text-foreground">Mapping Summary</h3>
                    </div>
                    <span className="text-[12px] text-muted-foreground font-medium">{tablesSelected} tables mapped</span>
                </div>

                <div className="grid grid-cols-4 gap-8">
                    <div className="text-center">
                        <p className="text-[28px] font-bold text-foreground tracking-tight mb-1">{tablesSelected}</p>
                        <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider">Tables Selected</p>
                    </div>
                    <div className="text-center border-l border-border/60">
                        <p className="text-[28px] font-bold text-foreground tracking-tight mb-1">{options.drop_existing ? tablesSelected : 0}</p>
                        <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider">New Tables</p>
                    </div>
                    <div className="text-center border-l border-border/60">
                        <p className="text-[28px] font-bold text-foreground tracking-tight mb-1">{rulesApplied}</p>
                        <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider">Rules Applied</p>
                    </div>
                    <div className="text-center border-l border-border/60">
                        <p className="text-[28px] font-bold text-foreground tracking-tight mb-1">0</p>
                        <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider">Conflicts</p>
                    </div>
                </div>
            </div>

            {/* Resume info */}
            <div className="rounded-xl border border-blue-200 bg-blue-50/50 p-4 flex items-start gap-3">
                <ShieldCheck className="h-5 w-5 text-blue-600 shrink-0 mt-0.5" />
                <div>
                    <p className="text-[13px] font-semibold text-blue-900">Resumable migration</p>
                    <p className="text-[12px] text-blue-800 mt-1">
                        If a migration is interrupted, you can start again — completed batches will be skipped automatically.
                    </p>
                </div>
            </div>

            {/* Actions */}
            <div className="bg-[#F2F4F7] rounded-xl border border-border/40 p-6 flex flex-col items-center justify-center space-y-6">
                <div className="flex items-center justify-between w-full">
                    <h3 className="text-[16px] font-bold text-foreground">Ready to Launch?</h3>
                    <div className="flex gap-4">
                        <Button
                            variant="default"
                            className="bg-[#2563EB] hover:bg-[#1D4ED8] text-white font-bold h-12 px-8 text-[15px] rounded-lg shadow-lg hover:shadow-xl transition-all"
                            onClick={() => onStartMigration(true)}
                        >
                            Dry Run
                        </Button>
                        <Button
                            onClick={() => onStartMigration(false)}
                            className="bg-[#E85C1C] hover:bg-[#D65116] text-white font-bold h-12 px-8 text-[15px] rounded-lg shadow-[0_8px_16px_-4px_rgba(232,92,28,0.4)] hover:shadow-[0_12px_20px_-4px_rgba(232,92,28,0.5)] hover:-translate-y-0.5 transition-all"
                        >
                            Start Migration
                        </Button>
                    </div>
                </div>
            </div>

            <div className="pt-2">
                <Button variant="outline" onClick={onBack} className="text-[13px] font-semibold">
                    Back to Mapping
                </Button>
            </div>
            </div>

            {/* Alerts panel - right side */}
            <div className="lg:order-2">
                <div className="sticky top-6 bg-white rounded-xl border border-border/60 shadow-sm overflow-hidden">
                    <div className="bg-gradient-to-b from-orange-100/80 to-transparent h-1" />
                    <div className="p-5">
                        <div className="flex items-center gap-2 mb-4">
                            <Bell className="h-5 w-5 text-orange-500" />
                            <h3 className="text-[15px] font-bold text-foreground">Alerts</h3>
                        </div>
                        <div className="space-y-3">
                            {schemaCompatibilityWarnings.length === 0 ? (
                                <div className="rounded-xl bg-[#FAFBFC] border border-border/60 p-4">
                                    <p className="text-[13px] text-muted-foreground">No schema compatibility warnings.</p>
                                </div>
                            ) : (
                                schemaCompatibilityWarnings.map((w, i) => (
                                    <div
                                        key={i}
                                        className="rounded-xl border border-border/60 p-4 bg-[#FAFBFC]"
                                    >
                                        <div className="flex items-start gap-3">
                                            <AlertTriangle className="h-5 w-5 text-orange-500 shrink-0 mt-0.5" />
                                            <div>
                                                <p className="text-[10px] font-bold text-orange-600 uppercase tracking-wider mb-1.5">
                                                    Data Type Mismatch
                                                </p>
                                                <p className="text-[12px] text-foreground leading-relaxed">
                                                    <code className="bg-amber-100/80 text-amber-900 px-1.5 py-0.5 rounded text-[11px] font-mono">
                                                        {w.sourceCol}
                                                    </code>{" "}
                                                    is {/^[aeiou]/i.test(w.sourceType) ? "an" : "a"} {w.sourceType}, but{" "}
                                                    <code className="bg-amber-100/80 text-amber-900 px-1.5 py-0.5 rounded text-[11px] font-mono">
                                                        {w.targetCol}
                                                    </code>{" "}
                                                    expects {w.targetType}. {w.message}
                                                </p>
                                                <button
                                                    type="button"
                                                    onClick={onBack}
                                                    className="mt-2 text-[11px] font-bold text-orange-600 hover:text-orange-700 hover:underline"
                                                >
                                                    Fix in Schema Mapping →
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                ))
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
