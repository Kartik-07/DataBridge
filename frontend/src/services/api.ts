/**
 * API service layer — communicates with the FastAPI backend.
 */

const API_BASE = "/api";

// ── Types ──────────────────────────────────────────────────────────────────

export type DbType = "postgresql" | "mysql" | "snowflake" | "sqlite" | "sqlserver";

export interface ConnectionConfig {
  db_type: DbType;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  warehouse?: string;
  schema_name?: string;
  /** Snowflake: use browser for MFA (authenticator='externalbrowser') */
  use_browser_login?: boolean;
  /** SQL Server: ODBC driver name override */
  driver?: string;
  /** Set from test-connection response for display (engine version, etc.) */
  server_version?: string;
  tables_count?: number;
}

export interface ConnectionTestResponse {
  success: boolean;
  message: string;
  server_version?: string;
  tables_count?: number;
  available_schemas?: string[];
}

export interface ColumnMapping {
  action: "direct" | "transform";
  target_name: string;
  target_type: string;
  source_type?: string;  // Source column data type, for schema compatibility warnings
  transform_rule?: string;
  source_format?: string;
  target_format?: string;
}

export interface MigrationOptions {
  migrate_schema: boolean;
  migrate_data: boolean;
  migrate_views: boolean;
  migrate_functions: boolean;
  migrate_triggers: boolean;
  migrate_sequences: boolean;
  drop_existing: boolean;
  dry_run: boolean;
  schemas: string[];
  selected_tables: Record<string, string[]>;
  mappings: Record<string, Record<string, ColumnMapping>>;
  /** When "all tables" selected (selected_tables empty), total tables from source for Validation KPIs */
  total_tables_count?: number;
  /** Table names (schema.table) that have no primary key — used to warn about duplicate rows when drop_existing is false */
  tables_without_pk?: string[];
}

export interface MigrationRequest {
  source: ConnectionConfig;
  target: ConnectionConfig;
  options: MigrationOptions;
}

export interface TableProgressEntry {
  name: string;
  schema: string;
  totalRows: number;
  migratedRows: number;
  status: "pending" | "migrating" | "done";
}

export interface LogEvent {
  message?: string;
  type?: "info" | "success" | "warning" | "error";
  progress?: number;
  done?: boolean;
  table_progress?: TableProgressEntry[];
}

export interface HistoryEntry {
  source: DbType;
  target: DbType;
  status: "success" | "failed";
  tables: number;
  rows: number;
  duration: string;
  timestamp: string;
}

// ── API Functions ──────────────────────────────────────────────────────────

export async function testConnection(
  config: ConnectionConfig
): Promise<ConnectionTestResponse> {
  const res = await fetch(`${API_BASE}/test-connection`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Connection test failed (${res.status})`);
  }
  return res.json();
}

export interface IntrospectOptions {
  /** When true, skips row counts and views/indexes/sequences for faster load (Schema Mapping UI). */
  forMapping?: boolean;
}

export async function introspect(config: ConnectionConfig, options?: IntrospectOptions) {
  const params = new URLSearchParams();
  if (options?.forMapping) params.set("for_mapping", "true");
  const url = `${API_BASE}/introspect${params.toString() ? `?${params}` : ""}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!res.ok) throw new Error(`Introspection failed (${res.status})`);
  return res.json();
}

export async function fetchSchemas(
  config: ConnectionConfig
): Promise<string[]> {
  const res = await fetch(`${API_BASE}/schemas`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!res.ok) throw new Error(`Failed to fetch schemas (${res.status})`);
  const data = await res.json();
  return data.schemas;
}

/** Translate source column types to target engine types. Same engine returns as-is. */
export async function translateTypes(
  sourceDb: string,
  targetDb: string,
  sourceTypes: string[]
): Promise<{ target_types: string[] }> {
  const res = await fetch(`${API_BASE}/translate-types`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      source_db: sourceDb,
      target_db: targetDb,
      source_types: sourceTypes,
    }),
  });
  if (!res.ok) throw new Error("Failed to translate types");
  return res.json();
}

/** Lightweight: fetch only table refs (schema.table), no columns. Uses dedicated /tables endpoint. */
export async function fetchTables(
  config: ConnectionConfig
): Promise<string[]> {
  const res = await fetch(`${API_BASE}/tables`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!res.ok) throw new Error(`Failed to fetch tables (${res.status})`);
  const data = await res.json();
  return (data.tables ?? []).map((t: { name: string }) => t.name);
}

/** Fetch column info for specific table refs only (no row counts). For Schema Mapping. */
export async function fetchTableColumns(
  config: ConnectionConfig,
  tableRefs: string[]
): Promise<{ name: string; columns: { name: string; data_type: string; is_nullable: boolean; is_primary_key: boolean }[] }[]> {
  if (tableRefs.length === 0) return [];
  const res = await fetch(`${API_BASE}/tables/columns`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config, table_refs: tableRefs }),
  });
  if (!res.ok) throw new Error(`Failed to fetch columns (${res.status})`);
  const data = await res.json();
  return data.tables ?? [];
}

export async function fetchSchemaTables(
  config: ConnectionConfig
): Promise<Record<string, string[]>> {
  const tableRefs = await fetchTables(config);
  // Group tables by schema: "schema.table" -> { schema: [table, ...] }
  const grouped: Record<string, string[]> = {};
  for (const name of tableRefs) {
    const dotIdx = name.indexOf(".");
    const schema = dotIdx >= 0 ? name.substring(0, dotIdx) : "default";
    const tableName = dotIdx >= 0 ? name.substring(dotIdx + 1) : name;
    if (!grouped[schema]) grouped[schema] = [];
    grouped[schema].push(tableName);
  }
  return grouped;
}

export async function fetchHistory(): Promise<HistoryEntry[]> {
  const res = await fetch(`${API_BASE}/history`);
  if (!res.ok) throw new Error(`Failed to fetch history (${res.status})`);
  return res.json();
}

/**
 * Start a migration via SSE. Calls `onEvent` for each streamed log entry.
 * Returns an AbortController and sessionId for pause/resume.
 * Generates session_id for pause/resume support.
 */
export function startMigration(
  request: MigrationRequest,
  onEvent: (event: LogEvent) => void,
  onError: (error: Error) => void
): { controller: AbortController; sessionId: string } {
  const controller = new AbortController();
  const sessionId = crypto.randomUUID();
  const requestWithSession = { ...request, session_id: sessionId };

  fetch(`${API_BASE}/migrate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestWithSession),
    signal: controller.signal,
  })
    .then(async (res) => {
      if (!res.ok) {
        throw new Error(`Migration request failed (${res.status})`);
      }
      const reader = res.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        // Parse SSE lines
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            try {
              const event: LogEvent = JSON.parse(line.slice(6));
              onEvent(event);
            } catch {
              // Skip malformed lines
            }
          }
        }
      }

      // Process remaining buffer
      if (buffer.startsWith("data: ")) {
        try {
          const event: LogEvent = JSON.parse(buffer.slice(6));
          onEvent(event);
        } catch {
          // Skip
        }
      }
    })
    .catch((err) => {
      if (err.name !== "AbortError") {
        onError(err);
      }
    });

  return { controller, sessionId };
}

/** Pause a running migration. Migration will block until resumed. */
export async function pauseMigration(sessionId: string): Promise<void> {
  await fetch(`${API_BASE}/migration/pause`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId }),
  });
}

/** Resume a paused migration. */
export async function resumeMigration(sessionId: string): Promise<void> {
  await fetch(`${API_BASE}/migration/resume`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId }),
  });
}
