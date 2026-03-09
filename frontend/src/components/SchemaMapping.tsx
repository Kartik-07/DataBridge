import { useState, useEffect, useMemo, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { CheckCircle2, AlertTriangle, Settings, HelpCircle, AlertCircle, Sparkles, ArrowRight } from "lucide-react";
import { type MigrationOptions, type ConnectionConfig, fetchTables, fetchTableColumns, translateTypes, type ColumnMapping } from "@/services/api";

/** Stable key for caching introspect result by connection (avoids refetch when switching tabs). */
function connectionKey(c: ConnectionConfig | null): string {
  if (!c) return "";
  return `${c.db_type}:${c.host}:${c.port}:${c.database}:${c.schema_name ?? ""}`;
}

interface ColumnInfo {
    name: string;
    data_type: string;
    is_nullable: boolean;
    is_primary_key: boolean;
}

interface TableInfo {
    name: string;
    columns: ColumnInfo[];
}


function tablesWithoutPk(tables: TableInfo[], selectedNames: string[]): string[] {
    const names = selectedNames.length > 0 ? selectedNames : tables.map((t) => t.name);
    return names.filter((name) => {
        const t = tables.find((tbl) => tbl.name === name);
        return t && !t.columns?.some((c) => c.is_primary_key);
    });
}

interface SchemaMappingProps {
    options: MigrationOptions;
    onOptionsChange: (options: MigrationOptions) => void;
    sourceConfig: ConnectionConfig | null;
    targetConfig: ConnectionConfig | null;
    onNext: () => void;
    onBack: () => void;
}

/** True if source type supports date/time transformation (Transform action). */
function isDateOrTimeType(dataType: string): boolean {
    const t = (dataType || "").toLowerCase();
    return /date|time|timestamp|datetime/.test(t);
}

const COMMON_TARGET_TYPES = [
    "VARCHAR", "VARCHAR(255)", "TEXT", "CHAR", "CHAR(36)",
    "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT(1)",
    "BOOLEAN", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL",
    "DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMPTZ", "TIMESTAMP_TZ",
    "JSON", "JSONB", "VARIANT", "BLOB", "BYTEA", "BINARY",
];

export function SchemaMapping({ options, onOptionsChange, sourceConfig, targetConfig, onNext, onBack }: SchemaMappingProps) {
    const [tables, setTables] = useState<TableInfo[]>([]);
    const [loading, setLoading] = useState(false);

    // Flatten selected_tables into a single array of strings like "schema.table"
    const selectedTableList = useMemo(() => {
        const list: string[] = [];
        Object.entries(options.selected_tables).forEach(([schema, tbls]) => {
            tbls.forEach(t => list.push(`${schema}.${t}`));
        });
        return list;
    }, [options.selected_tables]);

    // Table refs from lightweight /tables (all names); when Scope has selection use that for dropdown
    const [tableRefs, setTableRefs] = useState<string[]>([]);
    const tableListForDropdown = useMemo(() => {
        if (selectedTableList.length > 0) return selectedTableList;
        return tableRefs.length > 0 ? tableRefs : tables.map((t) => t.name);
    }, [selectedTableList, tableRefs, tables]);

    const [activeTable, setActiveTable] = useState<string>("");

    // Keep activeTable in sync with available list (e.g. after introspect loads or when switching scope)
    useEffect(() => {
        if (tableListForDropdown.length === 0) return;
        const inList = tableListForDropdown.includes(activeTable);
        if (!activeTable || !inList) {
            setActiveTable(tableListForDropdown[0]);
        }
    }, [tableListForDropdown, activeTable]);

    // Which column is currently showing the transform editor?
    const [editingColumn, setEditingColumn] = useState<string | null>(null);

    // Cache by connection so we don't refetch when switching back to this tab
    const cacheRef = useRef<{ key: string; tableRefs: string[]; tables: TableInfo[] } | null>(null);

    // Cache key includes scope (selected tables or "all") so changing scope refetches
    const scopeKey = selectedTableList.length > 0 ? [...selectedTableList].sort().join("\0") : "all";
    useEffect(() => {
        if (!sourceConfig) return;
        const key = connectionKey(sourceConfig) + "\0" + scopeKey;
        const cached = cacheRef.current;
        if (cached?.key === key) {
            setTableRefs(cached.tableRefs);
            setTables(cached.tables);
            return;
        }
        setLoading(true);
        setTableRefs([]);
        setTables([]);
        // 1) Lightweight table list
        fetchTables(sourceConfig)
            .then((refs) => {
                setTableRefs(refs);
                if (refs.length === 0) {
                    setLoading(false);
                    return;
                }
                const listForColumns = selectedTableList.length > 0 ? selectedTableList : refs;
                const first = listForColumns[0];
                const rest = listForColumns.slice(1);
                cacheRef.current = { key, tableRefs: refs, tables: [] };
                // 2) First table columns (show immediately)
                return fetchTableColumns(sourceConfig, [first]).then((firstTables) => {
                    const firstTable = firstTables[0];
                    if (firstTable) {
                        const nextTables: TableInfo[] = [{ name: firstTable.name, columns: firstTable.columns }];
                        setTables(nextTables);
                        cacheRef.current = { key, tableRefs: refs, tables: nextTables };
                    }
                    setLoading(false);
                    // 3) Rest in background
                    if (rest.length === 0) return;
                    fetchTableColumns(sourceConfig, rest).then((restTables) => {
                        setTables((prev) => {
                            const merged = [...prev];
                            for (const t of restTables) {
                                if (t?.name) merged.push({ name: t.name, columns: t.columns ?? [] });
                            }
                            if (cacheRef.current?.key === key) cacheRef.current.tables = merged;
                            return merged;
                        });
                    }).catch(() => {});
                });
            })
            .catch((err) => {
                console.error("Failed to load tables for mapping", err);
                setLoading(false);
            });
    }, [sourceConfig, scopeKey]);

    const lastTablesWithoutPkRef = useRef<string[] | null>(null);
    // Keep options.tables_without_pk in sync so Validation Review can show duplicate warning when drop_existing is off
    useEffect(() => {
        if (tables.length === 0) return;
        const selected = selectedTableList.length > 0 ? selectedTableList : tables.map((t) => t.name);
        const withoutPk = tablesWithoutPk(tables, selected);
        const prev = lastTablesWithoutPkRef.current;
        if (!prev || prev.length !== withoutPk.length || prev.some((n, i) => n !== withoutPk[i])) {
            lastTablesWithoutPkRef.current = withoutPk;
            onOptionsChange({ ...options, tables_without_pk: withoutPk });
        }
    }, [tables, selectedTableList]);

    // Find columns for the currently active table (normalize schema.table matching)
    const activeTableInfo = tables.find(t => {
        if (t.name === activeTable) return true;
        if (activeTable?.includes(".") && t.name === activeTable) return true;
        const tSchema = t.name.split(".")[0];
        const tName = t.name.split(".").slice(1).join(".") || t.name;
        const aSchema = activeTable?.split(".")[0];
        const aName = activeTable?.split(".").slice(1).join(".") || activeTable;
        return (tSchema === aSchema && tName === aName) || t.name.endsWith(`.${activeTable}`) || t.name === `${aSchema || sourceConfig?.schema_name || "public"}.${aName || activeTable}`;
    });

    const columns = activeTableInfo?.columns || [];
    const columnSignature = useMemo(() => columns.map((c) => `${c.name}:${c.data_type}`).join("|"), [columns]);

    // Auto-fill target types when columns or engine config changes (same engine = same type, cross-engine = mapped)
    useEffect(() => {
        if (!sourceConfig || !targetConfig || columns.length === 0) return;
        const sourceDb = sourceConfig.db_type;
        const targetDb = targetConfig.db_type;
        const currentTableMappings = options.mappings[activeTable] || {};
        const needDefaults = columns.filter((c) => !currentTableMappings[c.name]);
        if (needDefaults.length === 0) return;

        const sourceTypes = columns.map((c) => c.data_type);
        translateTypes(sourceDb, targetDb, sourceTypes)
            .then(({ target_types }) => {
                const next = { ...options.mappings };
                const tableMap = { ...(next[activeTable] || {}) };
                columns.forEach((col, i) => {
                    if (tableMap[col.name]) return;
                    tableMap[col.name] = {
                        action: "direct",
                        target_name: col.name,
                        target_type: target_types[i] || col.data_type || "VARCHAR",
                        source_type: col.data_type,
                    };
                });
                next[activeTable] = tableMap;
                onOptionsChange({ ...options, mappings: next });
            })
            .catch(() => {});
    }, [activeTable, sourceConfig?.db_type, targetConfig?.db_type, columnSignature]);

    const handleUpdateMapping = (columnName: string, mapping: Partial<ColumnMapping>) => {
        const currentTableMappings = options.mappings[activeTable] || {};
        const col = columns.find(c => c.name === columnName);
        const currentColMapping = currentTableMappings[columnName] || {
            action: "direct",
            target_name: columnName,
            target_type: col?.data_type || "VARCHAR",
            source_type: col?.data_type,
        };

        const newColMapping = { ...currentColMapping, ...mapping } as ColumnMapping;
        if (col && !newColMapping.source_type) newColMapping.source_type = col.data_type;

        onOptionsChange({
            ...options,
            mappings: {
                ...options.mappings,
                [activeTable]: {
                    ...currentTableMappings,
                    [columnName]: newColMapping
                }
            }
        });
    };

    const getMapping = (columnName: string): ColumnMapping => {
        const currentTableMappings = options.mappings[activeTable] || {};
        const col = columns.find(c => c.name === columnName);
        return currentTableMappings[columnName] || {
            action: "direct",
            target_name: columnName,
            target_type: col?.data_type || "VARCHAR",
            source_type: col?.data_type,
        };
    };

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <h2 className="text-[16px] font-bold text-foreground">Schema & Mapping</h2>
                <div className="flex items-center gap-3">
                    <span className="text-[13px] text-muted-foreground font-medium">Table:</span>
                    <Select value={activeTable || undefined} onValueChange={setActiveTable}>
                        <SelectTrigger className="w-[280px] h-9 text-[13px] font-medium bg-white">
                            <SelectValue placeholder={loading ? "Loading tables..." : "Select a table..."} />
                        </SelectTrigger>
                        <SelectContent>
                            {tableListForDropdown.map(t => (
                                <SelectItem key={t} value={t} className="text-[13px]">{t}</SelectItem>
                            ))}
                            {!loading && tableListForDropdown.length === 0 && (
                                <SelectItem value="none" disabled>No tables found</SelectItem>
                            )}
                        </SelectContent>
                    </Select>
                </div>
            </div>

            <div className="bg-white rounded-xl border border-border/60 shadow-sm overflow-hidden">
                {/* Header */}
                <div className="grid grid-cols-[1.5fr_1fr_1fr_1.5fr_1fr_60px] gap-4 px-6 py-3 border-b border-border/60 bg-[#FAFBFC] text-[11px] font-bold text-muted-foreground tracking-wider uppercase">
                    <div>Source Field</div>
                    <div>Type</div>
                    <div>Action</div>
                    <div>Target Field</div>
                    <div>Target Type</div>
                    <div className="text-center">Status</div>
                </div>

                {/* Rows */}
                <div className="divide-y divide-border/30">
                    {loading && (
                        <div className="p-8 text-center text-[13px] text-muted-foreground">Loading schema metadata...</div>
                    )}
                    {!loading && columns.length === 0 && (
                        <div className="p-8 text-center text-[13px] text-muted-foreground">No columns found for this table.</div>
                    )}
                    {!loading && columns.map((col, idx) => {
                        const mapping = getMapping(col.name);
                        const isTransforming = editingColumn === col.name;
                        const isTransformOptionSelected = mapping.action === "transform";

                        return (
                            <div key={col.name} className="flex flex-col">
                                <div className="grid grid-cols-[1.5fr_1fr_1fr_1.5fr_1fr_60px] gap-4 px-6 py-4 items-center">
                                    {/* Source Field */}
                                    <div className="flex items-center gap-2">
                                        <span className="text-[#8792A2] font-mono text-[13px]">#</span>
                                        <span className="text-[13px] font-bold text-foreground">{col.name}</span>
                                    </div>

                                    {/* Type */}
                                    <div className="text-[11px] font-mono text-[#8792A2] uppercase">{col.data_type}</div>

                                    {/* Action */}
                                    <div>
                                        <Select
                                            value={mapping.action}
                                            onValueChange={(val: "direct" | "transform") => {
                                                handleUpdateMapping(col.name, { action: val });
                                                if (val === "transform") {
                                                    setEditingColumn(col.name);
                                                } else {
                                                    setEditingColumn(null);
                                                }
                                            }}
                                            disabled={!options.drop_existing && mapping.action !== "transform"}
                                        >
                                            <SelectTrigger className={`h-8 w-[110px] text-[12px] font-semibold border-transparent ${mapping.action === 'transform' ? 'bg-[#FFF6F0] text-[#E85C1C]' : 'bg-[#FAFBFC] text-foreground'} hover:bg-secondary/80 focus:ring-0`}>
                                                <div className="flex items-center gap-1.5">
                                                    {mapping.action === "transform" && <Sparkles className="h-3.5 w-3.5" />}
                                                    <SelectValue />
                                                </div>
                                            </SelectTrigger>
                                            <SelectContent>
                                                <SelectItem value="direct" className="text-[12px] font-medium">Direct</SelectItem>
                                                <SelectItem
                                                    value="transform"
                                                    disabled={!options.drop_existing || !isDateOrTimeType(col.data_type)}
                                                    className="text-[12px] font-medium text-[#E85C1C] focus:text-[#E85C1C] focus:bg-[#FFF6F0]"
                                                    title={!isDateOrTimeType(col.data_type) ? "Transform is only available for date/time types" : undefined}
                                                >
                                                    <div className="flex items-center gap-1.5">
                                                        <Sparkles className="h-3 w-3" />
                                                        Transform
                                                    </div>
                                                </SelectItem>
                                            </SelectContent>
                                        </Select>
                                    </div>

                                    {/* Target Field */}
                                    <div>
                                        {isTransformOptionSelected ? (
                                            <span className="text-[13px] text-muted-foreground/60">Formatting...</span>
                                        ) : (
                                            <span className="text-[13px] font-bold text-foreground">{mapping.target_name}</span>
                                        )}
                                    </div>

                                    {/* Target Type */}
                                    <div>
                                        <Select
                                            value={mapping.target_type || col.data_type}
                                            onValueChange={(val) => handleUpdateMapping(col.name, { target_type: val })}
                                            disabled={!options.drop_existing}
                                        >
                                            <SelectTrigger className="h-8 w-full text-[11px] font-mono uppercase bg-transparent border-transparent hover:bg-secondary/40 focus:ring-0 text-[#8792A2]">
                                                <SelectValue placeholder={col.data_type} />
                                            </SelectTrigger>
                                            <SelectContent>
                                                {COMMON_TARGET_TYPES.map((t) => (
                                                    <SelectItem key={t} value={t} className="text-[11px] font-mono uppercase">
                                                        {t}
                                                    </SelectItem>
                                                ))}
                                                {mapping.target_type && !COMMON_TARGET_TYPES.includes(mapping.target_type) && (
                                                    <SelectItem value={mapping.target_type} className="text-[11px] font-mono">
                                                        {mapping.target_type}
                                                    </SelectItem>
                                                )}
                                            </SelectContent>
                                        </Select>
                                    </div>

                                    {/* Status */}
                                    <div className="flex items-center justify-center gap-3">
                                        {isTransformOptionSelected ? (
                                            <AlertTriangle className="h-4 w-4 text-amber-500" />
                                        ) : (
                                            <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                                        )}
                                        <button className="text-muted-foreground hover:text-foreground transition-colors" onClick={() => {
                                            if (mapping.action === "transform") {
                                                setEditingColumn(isTransforming ? null : col.name);
                                            }
                                        }}>
                                            <Settings className="h-4 w-4" />
                                        </button>
                                    </div>
                                </div>

                                {/* Transform Editor Panel */}
                                {isTransforming && (
                                    <div className="px-6 py-4 bg-[#FFF6F0]/30 border-t border-border/30">
                                        <div className="bg-white rounded-xl border border-[#FEE3D4] p-5 shadow-sm">
                                            <div className="flex items-center justify-between mb-4">
                                                <h4 className="text-[12px] font-bold text-[#E85C1C] uppercase tracking-wider">Date Transformation Rule</h4>
                                                <a href="#" className="flex items-center gap-1.5 text-[12px] font-medium text-muted-foreground hover:text-foreground">
                                                    <HelpCircle className="h-3.5 w-3.5" /> Documentation
                                                </a>
                                            </div>

                                            <div className="grid grid-cols-2 gap-6 mb-5">
                                                <div className="space-y-2">
                                                    <label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Source Format</label>
                                                    <Input
                                                        value={mapping.source_format || "YYYY-MM-DD HH:mm:ss"}
                                                        onChange={(e) => handleUpdateMapping(col.name, { source_format: e.target.value })}
                                                        className="h-10 text-[13px] font-mono bg-[#FAFBFC]"
                                                    />
                                                </div>
                                                <div className="space-y-2">
                                                    <label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider">Target Format</label>
                                                    <Select
                                                        value={mapping.target_format || "ISO 8601"}
                                                        onValueChange={(val) => handleUpdateMapping(col.name, { target_format: val })}
                                                    >
                                                        <SelectTrigger className="h-10 text-[13px] font-mono bg-[#FAFBFC]">
                                                            <SelectValue />
                                                        </SelectTrigger>
                                                        <SelectContent>
                                                            <SelectItem value="ISO 8601" className="text-[13px] font-mono">ISO 8601 (YYYY-MM-DDTHH:mm:ssZ)</SelectItem>
                                                            <SelectItem value="UNIX" className="text-[13px] font-mono">Unix Epoch (Seconds)</SelectItem>
                                                            <SelectItem value="YYYY-MM-DD" className="text-[13px] font-mono">Date Only (YYYY-MM-DD)</SelectItem>
                                                        </SelectContent>
                                                    </Select>
                                                </div>
                                            </div>

                                            <div className="flex items-center gap-2">
                                                <Button
                                                    onClick={() => setEditingColumn(null)}
                                                    className="h-8 px-5 bg-[#E85C1C] hover:bg-[#D65116] text-white text-[13px] font-semibold"
                                                >
                                                    Apply
                                                </Button>
                                                <Button
                                                    variant="ghost"
                                                    onClick={() => {
                                                        handleUpdateMapping(col.name, { action: "direct" });
                                                        setEditingColumn(null);
                                                    }}
                                                    className="h-8 px-4 text-[13px] font-medium text-muted-foreground"
                                                >
                                                    Cancel
                                                </Button>
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            </div>

            <div className="flex items-center justify-between pt-4">
                <Button variant="outline" onClick={onBack} className="text-[13px] font-semibold">
                    Back
                </Button>
                <Button onClick={onNext} className="h-11 px-6 rounded-lg text-[14px] font-bold bg-[#E85C1C] hover:bg-[#D65116] text-white shadow-sm">
                    Next: Validation & Review <ArrowRight className="ml-2 h-4 w-4 inline" />
                </Button>
            </div>
        </div>
    );
}
