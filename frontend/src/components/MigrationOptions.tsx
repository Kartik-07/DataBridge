import { useState, useEffect } from "react";
import { Switch } from "@/components/ui/switch";
import { Checkbox } from "@/components/ui/checkbox";
import { Settings2, ChevronDown, ChevronRight, Database, Table2, Search, Loader2, Workflow, DatabaseBackup, LayoutGrid, Code, ListOrdered, Folder, MoreVertical, ShieldCheck, ArrowRight, Table as TableIcon, File } from "lucide-react";
import { Button } from "@/components/ui/button";
import { motion, AnimatePresence } from "framer-motion";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  type MigrationOptions as MigrationOptionsType,
  type ConnectionConfig,
  type FileSourceConfig,
  type FileInfo,
  fetchSchemaTables,
} from "@/services/api";

interface MigrationOptionsProps {
  options: MigrationOptionsType;
  onOptionsChange: (options: MigrationOptionsType) => void;
  availableSchemas: string[];
  sourceConfig: ConnectionConfig | null;
  fileSourceConfig?: FileSourceConfig | null;
  selectedFiles?: FileInfo[];
  isFileSource?: boolean;
  onNext: () => void;
  onCancel: () => void;
  migrating: boolean;
}

function tableNameFromPath(path: string): string {
  const base = path.split("/").pop() ?? path;
  return base.replace(/\.[^.]+$/, "");
}

export function MigrationOptions({ options, onOptionsChange, availableSchemas, sourceConfig, fileSourceConfig, selectedFiles, isFileSource, onNext, onCancel, migrating }: MigrationOptionsProps) {
  const [schemaTables, setSchemaTables] = useState<Record<string, string[]>>({});
  const [selectedSchemas, setSelectedSchemas] = useState<Set<string>>(new Set());
  const [selectedTables, setSelectedTables] = useState<Record<string, Set<string>>>({});
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [tableSearch, setTableSearch] = useState("");
  const [loading, setLoading] = useState(false);

  const schemas = Object.keys(schemaTables);

  // Build schema/table tree from file list when in file mode
  useEffect(() => {
    if (!isFileSource) return;
    const files = selectedFiles ?? [];
    const tableNames = files.map(f => tableNameFromPath(f.path));
    const grouped: Record<string, string[]> = { "Files": tableNames };
    setSchemaTables(grouped);
    setSelectedSchemas(new Set(["Files"]));
    setSelectedTables({ "Files": new Set(tableNames) });
    setExpandedSchemas(new Set(["Files"]));
    onOptionsChange({
      ...options,
      schemas: [],
      selected_tables: {},
      total_tables_count: tableNames.length,
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isFileSource, selectedFiles]);

  // Fetch tables grouped by schema when sourceConfig is available (DB mode)
  useEffect(() => {
    if (isFileSource || !sourceConfig) return;
    setLoading(true);
    fetchSchemaTables(sourceConfig)
      .then((grouped) => {
        setSchemaTables(grouped);
        const schemaNames = Object.keys(grouped);
        setSelectedSchemas(new Set(schemaNames));
        const allTables: Record<string, Set<string>> = {};
        schemaNames.forEach((s) => {
          allTables[s] = new Set(grouped[s]);
        });
        setSelectedTables(allTables);
        if (schemaNames.includes("public")) {
          setExpandedSchemas(new Set(["public"]));
        }
        const totalCount = Object.values(grouped).reduce((a, b) => a + b.length, 0);
        onOptionsChange({
          ...options,
          schemas: [],
          selected_tables: {},
          total_tables_count: totalCount,
        });
      })
      .catch(() => {
        const fallback: Record<string, string[]> = {};
        availableSchemas.forEach((s) => {
          fallback[s] = [];
        });
        setSchemaTables(fallback);
        setSelectedSchemas(new Set(availableSchemas));
      })
      .finally(() => setLoading(false));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceConfig, isFileSource]);

  const syncOptions = (
    newSelectedSchemas: Set<string>,
    newSelectedTables: Record<string, Set<string>>
  ) => {
    const schemaList = [...newSelectedSchemas];
    const allSchemasSelected = schemaList.length === schemas.length;
    const allTablesSelected = schemaList.every(
      (s) => (newSelectedTables[s]?.size || 0) === (schemaTables[s]?.length || 0)
    );

    const selectedTablesMap: Record<string, string[]> = {};
    if (!allTablesSelected) {
      schemaList.forEach((s) => {
        selectedTablesMap[s] = [...(newSelectedTables[s] || [])];
      });
    }

    const totalTablesCount =
      allTablesSelected && schemaTables
        ? Object.values(schemaTables).reduce((a, b) => a + b.length, 0)
        : undefined;

    onOptionsChange({
      ...options,
      schemas: allSchemasSelected ? [] : schemaList,
      selected_tables: allTablesSelected ? {} : selectedTablesMap,
      total_tables_count: totalTablesCount,
    });
  };

  const toggle = (key: keyof MigrationOptionsType) => {
    onOptionsChange({ ...options, [key]: !options[key] });
  };

  const toggleAllTypes = () => {
    const allSelected = options.migrate_schema && options.migrate_data && options.migrate_views && options.migrate_functions && options.migrate_sequences;
    onOptionsChange({
      ...options,
      migrate_schema: !allSelected,
      migrate_data: !allSelected,
      migrate_views: !allSelected,
      migrate_functions: !allSelected,
      migrate_sequences: !allSelected,
    });
  };

  const toggleSchema = (schema: string) => {
    const next = new Set(selectedSchemas);
    let newTables = { ...selectedTables };
    if (next.has(schema)) {
      next.delete(schema);
      newTables[schema] = new Set();
    } else {
      next.add(schema);
      newTables = { ...newTables, [schema]: new Set(schemaTables[schema]) };
    }
    setSelectedSchemas(next);
    setSelectedTables(newTables);
    syncOptions(next, newTables);
  };

  const toggleTable = (schema: string, table: string) => {
    const current = new Set(selectedTables[schema] || []);
    if (current.has(table)) current.delete(table);
    else current.add(table);
    
    const newTables = { ...selectedTables, [schema]: current };
    const nextSchemas = new Set(selectedSchemas);
    if (current.size === 0) {
      nextSchemas.delete(schema);
    } else {
      nextSchemas.add(schema);
    }
    
    setSelectedTables(newTables);
    setSelectedSchemas(nextSchemas);
    syncOptions(nextSchemas, newTables);
  };

  const toggleExpand = (schema: string) => {
    const next = new Set(expandedSchemas);
    if (next.has(schema)) {
      next.delete(schema);
    } else {
      next.add(schema);
    }
    setExpandedSchemas(next);
  };

  const expandAll = () => {
    if (expandedSchemas.size === schemas.length) {
      setExpandedSchemas(new Set());
    } else {
      setExpandedSchemas(new Set(schemas));
    }
  };

  // Checkbox orange custom classes (simulated)
  const orangeCheckboxClasses = "data-[state=checked]:bg-[#E85C1C] data-[state=checked]:border-[#E85C1C]";

  return (
    <div className="space-y-6">
      {/* Object Types Section */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Workflow className="h-5 w-5 text-[#E85C1C]" />
            <h2 className="text-[16px] font-bold text-foreground">Migration Object Types</h2>
          </div>
          {!isFileSource && (
            <button onClick={toggleAllTypes} className="text-[#E85C1C] text-[13px] font-medium hover:underline">
              Select all types
            </button>
          )}
        </div>
        
        <div className={`grid gap-4 ${isFileSource ? "grid-cols-2" : "grid-cols-5"}`}>
          <ObjectTypeCard
            label="Schema"
            desc={isFileSource ? "Create target tables" : "Table structures & definitions"}
            icon={<Workflow className="h-5 w-5" />}
            checked={options.migrate_schema}
            onToggle={() => toggle("migrate_schema")}
          />
          <ObjectTypeCard
            label="Data"
            desc={isFileSource ? "Import file rows" : "Table records & row data"}
            icon={<DatabaseBackup className="h-5 w-5" />}
            checked={options.migrate_data}
            onToggle={() => toggle("migrate_data")}
          />
          {!isFileSource && (
            <>
              <ObjectTypeCard
                label="Views"
                desc="Virtual tables & queries"
                icon={<LayoutGrid className="h-5 w-5" />}
                checked={options.migrate_views}
                onToggle={() => toggle("migrate_views")}
              />
              <ObjectTypeCard
                label="Functions"
                desc="Stored procedures & code"
                icon={<Code className="h-5 w-5" />}
                checked={options.migrate_functions}
                onToggle={() => toggle("migrate_functions")}
              />
              <ObjectTypeCard
                label="Sequences"
                desc="Auto-incrementing values"
                icon={<ListOrdered className="h-5 w-5" />}
                checked={options.migrate_sequences}
                onToggle={() => toggle("migrate_sequences")}
              />
            </>
          )}
        </div>
      </div>

      {/* Schemas & Tables / Files Section */}
      <div className="bg-white rounded-xl border border-border/60 shadow-sm overflow-hidden">
        <div className="flex items-center justify-between px-6 py-4 border-b border-border/60 bg-[#FAFBFC]">
          <h3 className="text-[15px] font-bold text-foreground">
            {isFileSource ? "Files to Import" : "Schemas & Tables"}
          </h3>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={expandAll} className="h-8 text-[12px] font-semibold text-foreground border-border/80">
              {expandedSchemas.size === schemas.length ? "Collapse All" : "Expand All"}
            </Button>
            <Button variant="outline" size="sm" className="h-8 text-[12px] font-semibold text-foreground border-border/80">
              Filter
            </Button>
          </div>
        </div>

        <ScrollArea className="h-[400px] w-full rounded-b-xl">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-16 text-muted-foreground space-y-3">
              <Loader2 className="h-6 w-6 animate-spin text-primary/60" />
              <span className="text-[13px] font-medium">Fetching objects…</span>
            </div>
          ) : (
            <div className="py-2 min-w-max">
              {schemas.map((schema) => {
                const tables = schemaTables[schema] || [];
                const isSelected = selectedSchemas.has(schema);
                const isExpanded = expandedSchemas.has(schema);
                const tableCount = tables.length;
                
                return (
                  <div key={schema} className="border-b border-border/30 last:border-b-0">
                    {/* Schema Row */}
                    <div className="flex items-center justify-between px-6 py-3 hover:bg-muted/10 transition-colors">
                      <div className="flex items-center gap-3">
                        <Checkbox
                          checked={tableCount > 0 && selectedTables[schema]?.size === tableCount ? true : (selectedTables[schema]?.size > 0 ? "indeterminate" : false)}
                          className={orangeCheckboxClasses}
                          onCheckedChange={() => toggleSchema(schema)}
                        />
                        <button onClick={() => toggleExpand(schema)} className="p-0.5 hover:bg-secondary rounded">
                          {isExpanded ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
                        </button>
                        {isFileSource
                          ? <File className="h-4 w-4 text-[#E85C1C]" />
                          : <Folder className="h-4 w-4 text-[#E85C1C]" fill="#E85C1C" fillOpacity={0.2} strokeWidth={1.5} />
                        }
                        <span className="text-[13px] font-bold text-foreground">{schema}</span>
                        <span className="text-[10px] font-bold text-muted-foreground bg-secondary px-2 py-0.5 rounded-full uppercase tracking-wider">
                          {tableCount} {isFileSource ? "FILES" : "TABLES"}
                        </span>
                      </div>
                      <div className="flex items-center gap-3">
                        <span className="text-[12px] text-muted-foreground font-medium">-</span>
                        <button className="text-muted-foreground hover:text-foreground">
                          <MoreVertical className="h-4 w-4" />
                        </button>
                      </div>
                    </div>

                    {/* Tables List */}
                    <AnimatePresence>
                      {isExpanded && (
                        <motion.div
                          initial={{ height: 0, opacity: 0 }}
                          animate={{ height: "auto", opacity: 1 }}
                          exit={{ height: 0, opacity: 0 }}
                          transition={{ duration: 0.2 }}
                          className="overflow-hidden bg-[#FAFBFC]/50"
                        >
                          <div className="pl-[76px] pr-6 py-1 space-y-1 pb-3">
                            {tables.map(table => {
                              const isTableSelected = selectedTables[schema]?.has(table);
                              return (
                                <div key={table} className="flex items-center justify-between py-1.5 group">
                                  <label className="flex items-center gap-3 cursor-pointer flex-1">
                                    <Checkbox
                                      checked={isTableSelected}
                                      onCheckedChange={() => toggleTable(schema, table)}
                                      className={orangeCheckboxClasses}
                                    />
                                    <TableIcon className="h-3.5 w-3.5 text-[#AAC6D7]" />
                                    <span className="text-[13px] font-semibold text-foreground/80 group-hover:text-foreground transition-colors">{table}</span>
                                  </label>
                                  <span className="text-[11px] text-muted-foreground font-medium">- ROWS</span>
                                </div>
                              );
                            })}
                          </div>
                        </motion.div>
                      )}
                    </AnimatePresence>
                  </div>
                );
              })}
            </div>
          )}
        </ScrollArea>
      </div>

      {/* Drop Existing Tables Toggle */}
      <div className="bg-white rounded-xl border border-border/60 shadow-sm p-6 flex items-center justify-between">
        <div>
          <h3 className="text-[16px] font-bold text-foreground">Drop existing tables</h3>
          <p className="text-[13px] text-muted-foreground mt-1">Drop target tables before migration</p>
        </div>
        <Switch 
          checked={options.drop_existing} 
          onCheckedChange={() => toggle("drop_existing")}
          className="data-[state=checked]:bg-[#E85C1C] scale-110"
        />
      </div>

      {/* Security Info & Bottom Actions */}
      <div className="rounded-xl bg-card border border-border/80 p-4 shadow-sm flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="flex-shrink-0 h-10 w-10 rounded-full bg-[#FFF6F0] text-[#E85C1C] flex items-center justify-center">
            <ShieldCheck className="h-5 w-5" />
          </div>
          <p className="text-[12px] text-muted-foreground leading-snug">
            All connection details are encrypted using AES-256 and<br />
            never stored in plain text. <a href="#" className="font-medium text-[#E85C1C] hover:underline">Learn more about security</a>
          </p>
        </div>
        <div className="flex items-center gap-4">
          <Button variant="ghost" onClick={onCancel} className="text-[14px] font-semibold text-muted-foreground hover:text-foreground">
            Cancel
          </Button>
          <Button
            onClick={onNext}
            disabled={migrating}
            className="h-11 px-6 rounded-lg text-[14px] font-bold bg-[#E85C1C] hover:bg-[#D65116] text-white shadow-sm transition-colors"
          >
            {migrating ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" /> Migration in Progress...</> : <>Next: Schema Mapping <ArrowRight className="ml-2 h-4 w-4" /></>}
          </Button>
        </div>
      </div>
    </div>
  );
}

function ObjectTypeCard({ label, desc, icon, checked, onToggle }: { label: string; desc: string; icon: React.ReactNode; checked: boolean; onToggle: () => void }) {
  return (
    <div onClick={onToggle} className={`relative flex flex-col items-center justify-between px-2 py-5 rounded-2xl border-2 cursor-pointer transition-all duration-200 ${checked ? 'border-[#E85C1C] bg-white shadow-md' : 'border-border/60 bg-white hover:border-border/80 shadow-sm'} text-center h-[160px]`}>
      <div className={`h-[42px] w-[42px] flex items-center justify-center rounded-xl mb-3 transition-colors ${checked ? 'bg-[#FFF6F0] text-[#E85C1C]' : 'bg-[#F1F3F5] text-muted-foreground'}`}>
        {icon}
      </div>
      <div>
        <h4 className="text-[14px] font-bold text-foreground">{label}</h4>
        <p className="text-[11px] text-muted-foreground mt-0.5 leading-tight px-2">{desc}</p>
      </div>
      <div className="mt-3">
        <Switch checked={checked} className="data-[state=checked]:bg-[#E85C1C]" />
      </div>
    </div>
  );
}
