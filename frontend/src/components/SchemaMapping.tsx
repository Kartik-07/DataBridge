import { useState, useEffect, useMemo, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { CheckCircle2, AlertTriangle, Settings, HelpCircle, Sparkles, ArrowRight, X, Plus, ChevronDown } from "lucide-react";
import {
  type MigrationOptions,
  type ConnectionConfig,
  type FileSourceConfig,
  type FileInfo,
  type TransformDescriptor,
  fetchTables,
  fetchTableColumns,
  translateTypes,
  fetchTransforms,
  inferFileSchema,
  type ColumnMapping,
} from "@/services/api";

function connectionKey(c: ConnectionConfig | null): string {
  if (!c) return "";
  return `${c.db_type}:${c.host}:${c.port}:${c.database}:${c.schema_name ?? ""}`;
}

function fileSourceKey(config: FileSourceConfig | null | undefined, files: FileInfo[] | undefined): string {
  if (!config) return "";
  const paths = (files ?? []).map(f => f.path).sort().join("|");
  return `${config.source_type}:${paths}`;
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
    fileSourceConfig?: FileSourceConfig | null;
    selectedFiles?: FileInfo[];
    isFileSource?: boolean;
    onNext: () => void;
    onBack: () => void;
}

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

const CATEGORY_LABELS: Record<string, string> = {
    string: "String Formatting",
    null: "Null Handling",
    pii: "Privacy & PII",
    numeric: "Numeric & Math",
    cast: "Type Casting",
};

const CATEGORY_ORDER = ["string", "null", "pii", "numeric", "cast"];

interface ParsedRule {
    name: string;
    params: string[];
}

function parseRules(ruleStr: string): ParsedRule[] {
    if (!ruleStr) return [];
    return ruleStr.split("|").filter(Boolean).map((r) => {
        const parts = r.split(":");
        return { name: parts[0], params: parts.slice(1) };
    });
}

function serializeRules(rules: ParsedRule[]): string {
    return rules.map((r) => [r.name, ...r.params].join(":")).join("|");
}

function TransformPipeline({
    rules,
    catalog,
    onChange,
}: {
    rules: ParsedRule[];
    catalog: Record<string, TransformDescriptor>;
    onChange: (rules: ParsedRule[]) => void;
}) {
    const [addOpen, setAddOpen] = useState(false);
    const dropdownRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!addOpen) return;
        const handler = (e: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
                setAddOpen(false);
            }
        };
        document.addEventListener("mousedown", handler);
        return () => document.removeEventListener("mousedown", handler);
    }, [addOpen]);

    const grouped = useMemo(() => {
        const g: Record<string, [string, TransformDescriptor][]> = {};
        for (const [key, desc] of Object.entries(catalog)) {
            const cat = desc.category;
            if (!g[cat]) g[cat] = [];
            g[cat].push([key, desc]);
        }
        return g;
    }, [catalog]);

    const activeNames = new Set(rules.map((r) => r.name));

    const removeRule = (idx: number) => {
        const next = [...rules];
        next.splice(idx, 1);
        onChange(next);
    };

    const updateParam = (ruleIdx: number, paramIdx: number, value: string) => {
        const next = [...rules];
        const r = { ...next[ruleIdx], params: [...next[ruleIdx].params] };
        r.params[paramIdx] = value;
        next[ruleIdx] = r;
        onChange(next);
    };

    const addRule = (name: string) => {
        const desc = catalog[name];
        const defaultParams = (desc?.params || []).map((p) => {
            if (p === "length") return "255";
            if (p === "decimals") return "2";
            if (p === "factor") return "1";
            if (p === "algorithm") return "sha256";
            if (p === "default_value") return "";
            if (p === "value") return "";
            if (p === "old" || p === "new") return "";
            return "";
        });
        onChange([...rules, { name, params: defaultParams }]);
        setAddOpen(false);
    };

    return (
        <div className="space-y-3">
            {rules.length > 0 && (
                <div className="flex flex-wrap gap-2">
                    {rules.map((rule, idx) => {
                        const desc = catalog[rule.name];
                        const hasParams = desc?.params && desc.params.length > 0;
                        return (
                            <div key={`${rule.name}-${idx}`} className="flex items-center gap-1 bg-[#FFF6F0] border border-[#FEE3D4] rounded-lg px-2.5 py-1.5">
                                <span className="text-[12px] font-semibold text-[#E85C1C]">{desc?.label || rule.name}</span>
                                {hasParams && desc.params.map((p, pi) => (
                                    <Input
                                        key={p}
                                        value={rule.params[pi] ?? ""}
                                        onChange={(e) => updateParam(idx, pi, e.target.value)}
                                        placeholder={p}
                                        className="h-6 w-16 text-[11px] font-mono bg-white border-[#FEE3D4] ml-1 px-1.5"
                                    />
                                ))}
                                <button
                                    onClick={() => removeRule(idx)}
                                    className="ml-1 text-[#E85C1C]/50 hover:text-[#E85C1C] transition-colors"
                                >
                                    <X className="h-3 w-3" />
                                </button>
                                {idx < rules.length - 1 && (
                                    <span className="text-[10px] text-muted-foreground/40 ml-1 mr-0.5">→</span>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}

            <div className="relative" ref={dropdownRef}>
                <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setAddOpen(!addOpen)}
                    className="h-7 text-[11px] font-semibold text-muted-foreground border-dashed gap-1"
                >
                    <Plus className="h-3 w-3" /> Add Transform <ChevronDown className="h-3 w-3 ml-0.5" />
                </Button>

                {addOpen && (
                    <div className="absolute top-full left-0 mt-1 z-50 w-72 bg-white rounded-lg border border-border shadow-lg py-1 max-h-72 overflow-y-auto">
                        {CATEGORY_ORDER.filter((c) => grouped[c]).map((cat) => (
                            <div key={cat}>
                                <div className="px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground/60 bg-[#FAFBFC]">
                                    {CATEGORY_LABELS[cat] || cat}
                                </div>
                                {grouped[cat].map(([key, desc]) => (
                                    <button
                                        key={key}
                                        disabled={activeNames.has(key)}
                                        onClick={() => addRule(key)}
                                        className="w-full text-left px-3 py-1.5 text-[12px] hover:bg-[#FFF6F0] disabled:opacity-30 disabled:cursor-not-allowed flex items-center justify-between"
                                    >
                                        <span className="font-medium text-foreground">{desc.label}</span>
                                        <span className="text-[10px] text-muted-foreground ml-2 truncate max-w-[140px]">{desc.description}</span>
                                    </button>
                                ))}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}

export function SchemaMapping({ options, onOptionsChange, sourceConfig, targetConfig, fileSourceConfig, selectedFiles, isFileSource, onNext, onBack }: SchemaMappingProps) {
    const [tables, setTables] = useState<TableInfo[]>([]);
    const [loading, setLoading] = useState(false);
    const [transformCatalog, setTransformCatalog] = useState<Record<string, TransformDescriptor>>({});

    useEffect(() => {
        fetchTransforms().then(setTransformCatalog).catch(() => {});
    }, []);

    const selectedTableList = useMemo(() => {
        if (isFileSource) {
            // File mode: table names are stored under "" key or as flat keys
            const list: string[] = [];
            Object.entries(options.selected_tables).forEach(([, tbls]) => {
                tbls.forEach(t => list.push(t));
            });
            return list;
        }
        const list: string[] = [];
        Object.entries(options.selected_tables).forEach(([schema, tbls]) => {
            tbls.forEach(t => list.push(`${schema}.${t}`));
        });
        return list;
    }, [options.selected_tables, isFileSource]);

    const [tableRefs, setTableRefs] = useState<string[]>([]);
    const tableListForDropdown = useMemo(() => {
        if (selectedTableList.length > 0) return selectedTableList;
        return tableRefs.length > 0 ? tableRefs : tables.map((t) => t.name);
    }, [selectedTableList, tableRefs, tables]);

    const [activeTable, setActiveTable] = useState<string>("");

    useEffect(() => {
        if (tableListForDropdown.length === 0) return;
        const inList = tableListForDropdown.includes(activeTable);
        if (!activeTable || !inList) {
            setActiveTable(tableListForDropdown[0]);
        }
    }, [tableListForDropdown, activeTable]);

    const [editingColumn, setEditingColumn] = useState<string | null>(null);

    const cacheRef = useRef<{ key: string; tableRefs: string[]; tables: TableInfo[] } | null>(null);

    // ── File source: load schemas from backend inference ──────────────────────
    useEffect(() => {
        if (!isFileSource || !fileSourceConfig || !selectedFiles?.length) return;
        const key = fileSourceKey(fileSourceConfig, selectedFiles);
        const cached = cacheRef.current;
        if (cached?.key === key) {
            setTableRefs(cached.tableRefs);
            setTables(cached.tables);
            return;
        }
        setLoading(true);
        setTableRefs([]);
        setTables([]);
        const filePaths = selectedFiles.map(f => f.path);
        inferFileSchema(fileSourceConfig, filePaths)
            .then((infered) => {
                const mapped: TableInfo[] = infered.map(t => ({
                    name: t.name,
                    columns: t.columns,
                }));
                const refs = mapped.map(t => t.name);
                setTables(mapped);
                setTableRefs(refs);
                cacheRef.current = { key, tableRefs: refs, tables: mapped };
            })
            .catch(err => {
                console.error("Failed to infer file schemas", err);
            })
            .finally(() => setLoading(false));
    }, [isFileSource, fileSourceConfig, selectedFiles]);

    // ── DB source: fetch table refs and columns ───────────────────────────────
    const scopeKey = selectedTableList.length > 0 ? [...selectedTableList].sort().join("\0") : "all";
    useEffect(() => {
        if (isFileSource || !sourceConfig) return;
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
                return fetchTableColumns(sourceConfig, [first]).then((firstTables) => {
                    const firstTable = firstTables[0];
                    if (firstTable) {
                        const nextTables: TableInfo[] = [{ name: firstTable.name, columns: firstTable.columns }];
                        setTables(nextTables);
                        cacheRef.current = { key, tableRefs: refs, tables: nextTables };
                    }
                    setLoading(false);
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
    }, [sourceConfig, scopeKey, isFileSource]);

    const lastTablesWithoutPkRef = useRef<string[] | null>(null);
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

    useEffect(() => {
        if (!targetConfig || columns.length === 0) return;
        if (!isFileSource && !sourceConfig) return;
        const sourceDb = isFileSource ? "postgresql" : sourceConfig!.db_type;
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
                    <span className="text-[13px] text-muted-foreground font-medium">
                        {isFileSource ? "File:" : "Table:"}
                    </span>
                    <Select value={activeTable || undefined} onValueChange={setActiveTable}>
                        <SelectTrigger className="w-[280px] h-9 text-[13px] font-medium bg-white">
                            <SelectValue placeholder={loading ? (isFileSource ? "Inferring schema…" : "Loading tables...") : (isFileSource ? "Select a file…" : "Select a table...")} />
                        </SelectTrigger>
                        <SelectContent>
                            {tableListForDropdown.map(t => (
                                <SelectItem key={t} value={t} className="text-[13px]">{t}</SelectItem>
                            ))}
                            {!loading && tableListForDropdown.length === 0 && (
                                <SelectItem value="none" disabled>{isFileSource ? "No files found" : "No tables found"}</SelectItem>
                            )}
                        </SelectContent>
                    </Select>
                </div>
            </div>

            <div className="bg-white rounded-xl border border-border/60 shadow-sm overflow-hidden">
                {/* Header */}
                <div className="grid grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)_minmax(148px,1fr)_minmax(0,1.5fr)_minmax(0,1fr)_60px] gap-4 px-6 py-3 border-b border-border/60 bg-[#FAFBFC] text-[11px] font-bold text-muted-foreground tracking-wider uppercase">
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
                        <div className="p-8 text-center text-[13px] text-muted-foreground">
                            {isFileSource ? "Inferring schema from files…" : "Loading schema metadata..."}
                        </div>
                    )}
                    {!loading && columns.length === 0 && (
                        <div className="p-8 text-center text-[13px] text-muted-foreground">
                            {isFileSource ? "No columns found. Select a file above." : "No columns found for this table."}
                        </div>
                    )}
                    {!loading && columns.map((col) => {
                        const mapping = getMapping(col.name);
                        const isTransforming = editingColumn === col.name;
                        const isTransformOptionSelected = mapping.action === "transform";
                        const hasDateType = isDateOrTimeType(col.data_type);
                        const hasRules = !!(mapping.transform_rule && mapping.transform_rule.length > 0);
                        const hasDateConfig = !!(mapping.source_format || mapping.target_format);
                        const ruleCount = hasRules ? mapping.transform_rule!.split("|").filter(Boolean).length : 0;

                        return (
                            <div key={col.name} className="flex flex-col">
                                <div className="grid grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)_minmax(148px,1fr)_minmax(0,1.5fr)_minmax(0,1fr)_60px] gap-4 px-6 py-4 items-center">
                                    {/* Source Field */}
                                    <div className="flex items-center gap-2">
                                        <span className="text-[#8792A2] font-mono text-[13px]">#</span>
                                        <span className="text-[13px] font-bold text-foreground">{col.name}</span>
                                    </div>

                                    {/* Type */}
                                    <div className="text-[11px] font-mono text-[#8792A2] uppercase">{col.data_type}</div>

                                    {/* Action — inner wrapper must be a div: SelectTrigger applies line-clamp to direct span children and breaks icon+label flex */}
                                    <div className="min-w-0 w-full">
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
                                        >
                                            <SelectTrigger
                                                className={`h-8 w-full min-h-8 px-2 py-0 text-[12px] font-semibold border-transparent ${mapping.action === "transform" ? "bg-[#FFF6F0] text-[#E85C1C]" : "bg-[#FAFBFC] text-foreground"} hover:bg-secondary/80 focus:ring-0`}
                                            >
                                                <div className="flex min-w-0 flex-1 flex-nowrap items-center justify-start gap-1.5 text-left">
                                                    {mapping.action === "transform" && (
                                                        <Sparkles
                                                            className="size-4 shrink-0 text-[#E85C1C]"
                                                            strokeWidth={2}
                                                            aria-hidden
                                                        />
                                                    )}
                                                    <span className="min-w-0 flex-1 truncate leading-tight">
                                                        <SelectValue placeholder="Direct" />
                                                    </span>
                                                </div>
                                            </SelectTrigger>
                                            <SelectContent>
                                                <SelectItem value="direct" className="text-[12px] font-medium">Direct</SelectItem>
                                                <SelectItem
                                                    value="transform"
                                                    className="text-[12px] font-medium text-[#E85C1C] focus:text-[#E85C1C] focus:bg-[#FFF6F0]"
                                                >
                                                    Transform
                                                </SelectItem>
                                            </SelectContent>
                                        </Select>
                                    </div>

                                    {/* Target Field */}
                                    <div>
                                        {isTransformOptionSelected ? (
                                            <span className="text-[12px] text-muted-foreground/70">
                                                {ruleCount > 0
                                                    ? `${ruleCount} transform${ruleCount > 1 ? "s" : ""} applied`
                                                    : hasDateConfig
                                                    ? "Date formatting"
                                                    : "Configure transforms..."}
                                            </span>
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
                                            (hasRules || hasDateConfig) ? (
                                                <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                                            ) : (
                                                <AlertTriangle className="h-4 w-4 text-amber-500" />
                                            )
                                        ) : (
                                            <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                                        )}
                                        <button className="text-muted-foreground hover:text-foreground transition-colors" onClick={() => {
                                            if (isTransforming) {
                                                setEditingColumn(null);
                                            } else {
                                                if (mapping.action !== "transform") {
                                                    handleUpdateMapping(col.name, { action: "transform" });
                                                }
                                                setEditingColumn(col.name);
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
                                                <h4 className="text-[12px] font-bold text-[#E85C1C] uppercase tracking-wider">Transformation Rules</h4>
                                                <a href="/docs#transformations" target="_blank" rel="noopener noreferrer" className="flex items-center gap-1.5 text-[12px] font-medium text-muted-foreground hover:text-foreground">
                                                    <HelpCircle className="h-3.5 w-3.5" /> Documentation
                                                </a>
                                            </div>

                                            {/* Data transform pipeline */}
                                            <div className="mb-5">
                                                <label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider mb-2 block">Data Transforms</label>
                                                <TransformPipeline
                                                    rules={parseRules(mapping.transform_rule || "")}
                                                    catalog={transformCatalog}
                                                    onChange={(rules) => {
                                                        handleUpdateMapping(col.name, { transform_rule: serializeRules(rules) || undefined });
                                                    }}
                                                />
                                            </div>

                                            {/* Date format section — only for date/time columns */}
                                            {hasDateType && (
                                                <div className="pt-4 border-t border-[#FEE3D4]/50">
                                                    <label className="text-[11px] font-bold text-muted-foreground uppercase tracking-wider mb-3 block">Date Format Conversion</label>
                                                    <div className="grid grid-cols-2 gap-6">
                                                        <div className="space-y-2">
                                                            <label className="text-[11px] font-medium text-muted-foreground">Source Format</label>
                                                            <Input
                                                                value={mapping.source_format || "YYYY-MM-DD HH:mm:ss"}
                                                                onChange={(e) => handleUpdateMapping(col.name, { source_format: e.target.value })}
                                                                className="h-10 text-[13px] font-mono bg-[#FAFBFC]"
                                                            />
                                                        </div>
                                                        <div className="space-y-2">
                                                            <label className="text-[11px] font-medium text-muted-foreground">Target Format</label>
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
                                                </div>
                                            )}

                                            <div className="flex items-center gap-2 mt-5">
                                                <Button
                                                    onClick={() => setEditingColumn(null)}
                                                    className="h-8 px-5 bg-[#E85C1C] hover:bg-[#D65116] text-white text-[13px] font-semibold"
                                                >
                                                    Apply
                                                </Button>
                                                <Button
                                                    variant="ghost"
                                                    onClick={() => {
                                                        handleUpdateMapping(col.name, { action: "direct", transform_rule: undefined, source_format: undefined, target_format: undefined });
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
