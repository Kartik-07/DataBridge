import { useState, useEffect } from "react";
import { Link, useLocation } from "react-router-dom";
import { Header } from "@/components/Header";
import { BookOpen, Database, Shield, Zap, ArrowRight, Code2, Sparkles, FileStack } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

type SectionId = "getting-started" | "supported-databases" | "file-sources" | "data-safety" | "transformations" | "advanced-features";

const sections: Array<{
  id: SectionId;
  icon: typeof BookOpen;
  title: string;
  description: string;
  bullets: string[];
  exampleTitle?: string;
  exampleContent: React.ReactNode;
}> = [
  {
    id: "getting-started",
    icon: BookOpen,
    title: "Getting Started",
    description: "DataBridge is a 5-step wizard that takes you from source configuration to live migration in minutes. It supports two source modes: a live database connection or a file-based import from your local disk, SSH/SFTP server, or AWS S3 bucket.",
    bullets: [
      "Step 1 — Configure Source: Database or File (Local / SFTP / S3)",
      "Step 2 — Configure Target: any supported database engine",
      "Step 3 — Scope & Action: pick schemas, tables, or files",
      "Step 4 — Schema Mapping: rename columns, override types, add transforms",
      "Step 5 — Validation & Review: dry-run or live migration with real-time progress",
    ],
    exampleTitle: "Wizard Walkthrough",
    exampleContent: (
      <div className="space-y-5 text-sm">
        {/* Step-by-step */}
        <div>
          <p className="font-semibold text-foreground mb-2">The 5-Step Migration Wizard</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold w-8">Step</th>
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Tab</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">What you do</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600 font-bold">1</td><td className="pr-4">Source Database</td><td>Select "Database" or "File", enter credentials, click Test / Test & List Files</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600 font-bold">2</td><td className="pr-4">Source Database</td><td>Configure target engine in the right column, click Test Target Connection</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600 font-bold">3</td><td className="pr-4">Scope & Action</td><td>Choose what to migrate (schema, data, views…), select schemas/tables/files</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600 font-bold">4</td><td className="pr-4">Schema Mapping</td><td>Per-column: rename, change target type, add transform pipeline</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600 font-bold">5</td><td className="pr-4">Validation & Review</td><td>Review summary, run Dry Run or start live migration, watch real-time log</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Two paths */}
        <div>
          <p className="font-semibold text-foreground mb-2">Source Mode Quick Start</p>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <p className="text-orange-600 font-semibold text-xs mb-1.5">Database → Database</p>
              <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40 leading-relaxed">
{`1. Keep "Database" toggle active
2. Select engine, enter host/port/
   user/password, click Test
3. Repeat for target
4. Choose schemas + tables
5. Map columns → Run`}
              </pre>
            </div>
            <div>
              <p className="text-orange-600 font-semibold text-xs mb-1.5">File → Database</p>
              <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40 leading-relaxed">
{`1. Click "File" toggle
2. Choose Local FS / SFTP / S3
3. Enter paths/credentials
4. Click "Test & List Files"
5. Select files → Map → Run`}
              </pre>
            </div>
          </div>
        </div>

        {/* Connection tips */}
        <div>
          <p className="font-semibold text-foreground mb-2">Connection Tips</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Situation</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Recommendation</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Cloud DB (RDS, Cloud SQL)</td><td>Add the server's IP to the DB allowlist or use a VPN / bastion</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Source permissions</td><td>User needs SELECT on all source tables and schemas</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Target permissions</td><td>User needs CREATE TABLE, INSERT, and schema-level CREATE</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Snowflake MFA</td><td>Enable "Sign in with browser" toggle for Duo / SSO authentication</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">URI shortcut</td><td>Click the "URI" link in the header of any connection form to paste and auto-parse a connection string</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* What happens at each stage */}
        <div>
          <p className="font-semibold text-foreground mb-2">What Happens Behind the Scenes</p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`Test Connection  →  verifies credentials, returns engine version + table count
Scope & Action   →  fetches schema/table tree from source
Schema Mapping   →  translates source column types to target engine types
Migration        →  FK-ordered CREATE TABLE → batch INSERT (server-side cursor)
                    streaming SSE progress updates per table / file`}
          </pre>
        </div>
      </div>
    ),
  },
  {
    id: "supported-databases",
    icon: Database,
    title: "Supported Databases",
    description: "DataBridge supports cross-platform migrations between five popular database engines. Any combination of source → target is supported, including same-engine migrations. All engines can also serve as the target for file-based imports.",
    bullets: [
      "PostgreSQL 12+ — schemas, views, sequences, arrays, JSONB, vectors",
      "MySQL 8.0+ — multi-schema, ON DUPLICATE KEY upsert",
      "Snowflake — warehouse, schema, browser MFA, MERGE-based upsert",
      "SQLite 3 — file-path connection, ON CONFLICT upsert",
      "SQL Server 2017+ — dbo schema default, MERGE-based upsert, ODBC driver override",
    ],
    exampleTitle: "Engine Reference",
    exampleContent: (
      <div className="space-y-5 text-sm">
        {/* PostgreSQL */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">PostgreSQL</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600 w-32">Default port</td><td>5432</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">URI format</td><td>postgresql://user:pass@host:5432/database</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Special fields</td><td>None — standard host / port / user / password / database</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Upsert</td><td>INSERT … ON CONFLICT DO UPDATE (requires primary key)</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">Extras</td><td>pgvector extension auto-enabled; JSONB / arrays / sequences migrated</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* MySQL */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">MySQL</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600 w-32">Default port</td><td>3306</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">URI format</td><td>mysql://user:pass@host:3306/database</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Special fields</td><td>None</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Upsert</td><td>INSERT … ON DUPLICATE KEY UPDATE</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">Extras</td><td>Server-side streaming cursor (SSCursor) for large tables</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Snowflake */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Snowflake</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600 w-32">Default port</td><td>443 (HTTPS)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">URI format</td><td>snowflake://user:pass@account.snowflake.com/db?warehouse=WH</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Special fields</td><td>Warehouse (compute resource), Schema (optional default), Browser login (MFA)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Upsert</td><td>Staging table + MERGE (bulk-optimised; avoids row-by-row)</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">Extras</td><td>VARIANT type used for JSON/array columns; enable "Sign in with browser" for Duo / SSO</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* SQLite */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">SQLite</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600 w-32">Connection</td><td>File path instead of host/port (e.g. /data/app.sqlite3 or :memory:)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">URI format</td><td>sqlite:///absolute/path/to/file.sqlite3</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Upsert</td><td>INSERT … ON CONFLICT DO UPDATE</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">Extras</td><td>No host/port/username/password required; enter the file path in "Database File Path"</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* SQL Server */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">SQL Server</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600 w-32">Default port</td><td>1433</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">URI format</td><td>sqlserver://user:pass@host:1433/database</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Special fields</td><td>ODBC Driver override (default: "ODBC Driver 18 for SQL Server")</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Upsert</td><td>MERGE … WHEN MATCHED / WHEN NOT MATCHED</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">Extras</td><td>Schemas auto-created on target; default schema is dbo</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Cross-engine type map snippet */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Cross-Engine Type Examples</p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`PostgreSQL → MySQL
  JSONB          →  JSON
  BOOLEAN        →  TINYINT(1)
  TIMESTAMPTZ    →  DATETIME
  TEXT[]         →  JSON

PostgreSQL → Snowflake
  JSONB          →  VARIANT
  TEXT[]         →  VARIANT
  SERIAL         →  NUMBER(10,0)

MySQL → PostgreSQL
  TINYINT(1)     →  BOOLEAN
  DATETIME       →  TIMESTAMP
  ENUM(...)      →  TEXT  (user-defined → TEXT)`}
          </pre>
        </div>
      </div>
    ),
  },
  {
    id: "file-sources",
    icon: FileStack,
    title: "File Sources",
    description: "Import structured data files directly into any supported database. DataBridge auto-detects the schema, infers column types, and maps columns to the target engine's native types — no manual DDL required.",
    bullets: [
      "Local File System — any path or directory on the server",
      "SSH / SFTP — remote files over password or private-key auth",
      "AWS S3 — objects or prefixes from any S3-compatible bucket (incl. MinIO)",
      "CSV / TSV — headers auto-detected, delimiter inferred",
      "JSON — array of objects or single object",
      "JSONL / NDJSON — one JSON object per line (streaming-friendly)",
      "Excel (XLSX) — first sheet, first row as headers",
      "Parquet — Arrow schema used directly for type mapping",
      "Type inference: bool → integer → float → timestamp → text",
    ],
    exampleTitle: "File Source Reference",
    exampleContent: (
      <div className="space-y-5 text-sm">
        {/* Local */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Local File System</p>
          <p className="text-muted-foreground text-xs mb-2">
            Enter one or more absolute paths — individual files or directories. All supported files inside a directory are discovered recursively (one level).
          </p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`# Examples (one per line in the form)
/data/exports/users.csv
/data/exports/orders.parquet
/reports/                     ← entire directory`}
          </pre>
        </div>

        {/* SFTP */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">SSH / SFTP</p>
          <p className="text-muted-foreground text-xs mb-2">
            Connects with password or RSA/ED25519 private key. Remote paths can be files or directories.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-6 text-muted-foreground font-semibold">Field</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Notes</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Host</td><td>Hostname or IP of the SFTP server</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Port</td><td>Default: 22</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Username</td><td>SSH login user</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Password</td><td>Used when "Use private key" is off</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Private Key Path</td><td>Server-side path to RSA/ED25519 key (e.g. ~/.ssh/id_rsa)</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">Remote Paths</td><td>One file or directory path per line</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* S3 */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">AWS S3 / S3-Compatible</p>
          <p className="text-muted-foreground text-xs mb-2">
            Supports IAM credentials or anonymous access for public buckets. Use the Endpoint URL field for MinIO or other S3-compatible stores.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-6 text-muted-foreground font-semibold">Field</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Notes</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Bucket</td><td>S3 bucket name</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Region</td><td>e.g. us-east-1 (optional for custom endpoints)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Access Key ID</td><td>IAM access key (leave blank to use instance role)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Secret Access Key</td><td>IAM secret key</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Object Keys / Prefixes</td><td>Direct keys (e.g. data/users.csv) or folder prefixes (e.g. exports/2024/)</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">Endpoint URL</td><td>Optional — for MinIO, Cloudflare R2, etc. (e.g. http://localhost:9000)</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Formats */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Supported Formats & Type Inference</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Format</th>
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Extensions</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Schema Source</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">CSV</td><td className="pr-4">.csv, .tsv</td><td>First row as headers; values sampled for types</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">JSON</td><td className="pr-4">.json</td><td>Array of objects or single object; keys become columns</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">JSONL</td><td className="pr-4">.jsonl, .ndjson</td><td>Union of keys across sampled lines</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Excel</td><td className="pr-4">.xlsx, .xls</td><td>First sheet; row 1 as headers</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">Parquet</td><td className="pr-4">.parquet</td><td>Arrow schema mapped to SQL types directly</td></tr>
              </tbody>
            </table>
          </div>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40 mt-3">
{`# Inferred SQL types (most → least specific)
Python bool   →  BOOLEAN
Python int    →  BIGINT
Python float  →  DOUBLE PRECISION
ISO date str  →  TIMESTAMP
Everything else → TEXT`}
          </pre>
        </div>
      </div>
    ),
  },
  {
    id: "data-safety",
    icon: Shield,
    title: "Data Safety",
    description: "DataBridge is built to protect your data at every stage — from preview-only dry runs to crash-safe batch checkpointing, pause/resume control, and retry logic for transient failures.",
    bullets: [
      "Dry-run mode — generates and logs all DDL/DML without executing",
      "Batch-level checkpointing — each committed batch is recorded; restarts skip completed batches",
      "3× automatic retry per batch with exponential back-off",
      "Pause / Resume — instantly halt a running migration and continue later",
      "Drop Existing safeguard — explicit toggle required; off by default",
      "FK-aware ordering — child tables always inserted after parent tables",
      "No credentials stored — connection details live only in memory for the session",
    ],
    exampleTitle: "Safety Features Reference",
    exampleContent: (
      <div className="space-y-5 text-sm">
        {/* Dry run */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Dry Run Mode</p>
          <p className="text-muted-foreground text-xs mb-2">
            Enable "Dry Run" on the Validation & Review tab. The migration runs its full planning phase — creates connections, resolves FK order, validates mappings — but skips all write operations. The log shows exactly what would happen.
          </p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`[DRY RUN] Would CREATE TABLE "public"."orders" (…)
[DRY RUN] Would INSERT 84,231 rows into "orders"
[DRY RUN] Would CREATE VIEW "public"."v_active_orders"
[DRY RUN] Dry run complete — 0 rows written`}
          </pre>
        </div>

        {/* Checkpointing */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Batch Checkpointing & Resume</p>
          <p className="text-muted-foreground text-xs mb-2">
            Every committed batch is recorded in a <code className="bg-muted px-1 py-0.5 rounded">_databridge_batches</code> tracking table on the target. If a migration crashes mid-way, re-running it will automatically skip already-completed batches.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Column</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Description</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">table_name</td><td>Fully-qualified table being migrated</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">batch_index</td><td>Zero-based batch number within that table</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">status</td><td>pending → running → done / failed</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">rows_count</td><td>Number of rows in this batch</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Pause / Resume */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Pause & Resume</p>
          <p className="text-muted-foreground text-xs mb-2">
            Click the Pause button on the Status tab at any time. The migration finishes its current batch (never splits a commit) and then blocks until you click Resume. Safe to leave paused indefinitely.
          </p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`State transitions:
  RUNNING  →  [Pause clicked]  →  PAUSED (after batch boundary)
  PAUSED   →  [Resume clicked] →  RUNNING
  RUNNING  →  [Cancel clicked] →  CANCELLED (connection closed)`}
          </pre>
        </div>

        {/* Retry logic */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Automatic Retry Logic</p>
          <p className="text-muted-foreground text-xs mb-2">
            Each batch is retried up to 3 times on failure (e.g. transient network drops, lock timeouts). Between attempts the connection is rolled back and re-established.
          </p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`Attempt 1: immediate
Attempt 2: +1 s delay
Attempt 3: +2 s delay
After 3 failures: table marked ERROR, migration continues with remaining tables`}
          </pre>
        </div>

        {/* Drop existing */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Drop Existing Tables</p>
          <p className="text-muted-foreground text-xs mb-2">
            The "Drop existing tables" toggle in Scope & Action is <strong className="text-foreground">off by default</strong>. When off, <code className="bg-muted px-1 py-0.5 rounded">CREATE TABLE IF NOT EXISTS</code> is used and existing rows are appended (or upserted if the table has a primary key). Turn it on only for a clean-slate migration.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-6 text-muted-foreground font-semibold">Setting</th>
                  <th className="text-left py-1.5 pr-6 text-muted-foreground font-semibold">DDL used</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Write strategy</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-6 text-orange-600">Off (default)</td><td className="pr-6">CREATE TABLE IF NOT EXISTS</td><td>INSERT or UPSERT (if PK exists)</td></tr>
                <tr><td className="py-1.5 pr-6 text-orange-600">On</td><td className="pr-6">DROP TABLE + CREATE TABLE</td><td>Plain INSERT (faster, no conflict check)</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    ),
  },
  {
    id: "transformations",
    icon: Sparkles,
    title: "Data Transformations",
    description:
      "Apply row-level data transformations during migration. Transforms are chainable — combine multiple rules into a pipeline that processes each cell value left-to-right.",
    bullets: [
      "String formatting & cleansing (trim, case, truncate, replace, strip HTML)",
      "Null handling & defaults (coalesce, nullif)",
      "Privacy & PII masking (mask email, mask credit card, hash, redact)",
      "Numeric & math (round, multiply, divide, absolute value)",
      "Type casting & normalisation (to_boolean, to_string)",
      "Date/time format conversion (ISO 8601, Unix epoch, date-only)",
      "Pipe-separated chaining: trim|uppercase|truncate:255",
    ],
    exampleTitle: "Transform Reference",
    exampleContent: (
      <div className="space-y-5 text-sm">
        {/* Chaining intro */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Chaining Transforms</p>
          <p className="text-muted-foreground mb-2">
            Combine multiple rules with the pipe <code className="bg-muted px-1 py-0.5 rounded text-xs font-mono">|</code> character. They execute left-to-right.
          </p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`# Example pipeline
trim | lowercase | truncate:100

# Input:  "  Hello World  "
# Step 1 (trim):       "Hello World"
# Step 2 (lowercase):  "hello world"
# Step 3 (truncate):   "hello world"  (already under 100)`}
          </pre>
        </div>

        {/* String */}
        <div>
          <p className="font-semibold text-foreground mb-2">String Formatting & Cleansing</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Rule</th>
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Description</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Example</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">uppercase</td><td className="pr-4">Convert to UPPER CASE</td><td>"hello" → "HELLO"</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">lowercase</td><td className="pr-4">Convert to lower case</td><td>"HELLO" → "hello"</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">titlecase</td><td className="pr-4">Convert to Title Case</td><td>"john doe" → "John Doe"</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">trim</td><td className="pr-4">Remove leading & trailing whitespace</td><td>"  hi  " → "hi"</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">ltrim</td><td className="pr-4">Remove leading whitespace</td><td>"  hi  " → "hi  "</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">rtrim</td><td className="pr-4">Remove trailing whitespace</td><td>"  hi  " → "  hi"</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">truncate:<span className="text-foreground/60">N</span></td><td className="pr-4">Cut string to max N characters</td><td>truncate:5 → "Hello" from "Hello World"</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">replace:<span className="text-foreground/60">old</span>:<span className="text-foreground/60">new</span></td><td className="pr-4">Replace substring occurrences</td><td>replace:@old.com:@new.com</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">strip_html</td><td className="pr-4">Remove HTML tags, keep text</td><td>{"\"<b>Hi</b>\" → \"Hi\""}</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Null */}
        <div>
          <p className="font-semibold text-foreground mb-2">Null Handling & Defaults</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Rule</th>
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Description</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Example</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">coalesce:<span className="text-foreground/60">default</span></td><td className="pr-4">Replace NULL/empty with a default value</td><td>coalesce:0 → NULL becomes 0</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">nullif:<span className="text-foreground/60">value</span></td><td className="pr-4">Convert matching value to NULL</td><td>nullif:N/A → "N/A" becomes NULL</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* PII */}
        <div>
          <p className="font-semibold text-foreground mb-2">Privacy, Security & PII</p>
          <p className="text-muted-foreground mb-2 text-xs">
            Anonymise sensitive data when migrating production databases to staging or analytics environments.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Rule</th>
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Description</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Example</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">mask_email</td><td className="pr-4">Mask email address, keep first char + domain</td><td>user@ex.com → u***@ex.com</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">mask_credit_card</td><td className="pr-4">Show only last 4 digits</td><td>4111...1234 → ****-****-****-1234</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">hash:<span className="text-foreground/60">algo</span></td><td className="pr-4">One-way hash (sha256 or md5)</td><td>hash:sha256 → 64-char hex digest</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">redact</td><td className="pr-4">Replace entire value with [REDACTED]</td><td>"secret" → "[REDACTED]"</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Numeric */}
        <div>
          <p className="font-semibold text-foreground mb-2">Numeric & Math</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Rule</th>
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Description</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Example</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">round:<span className="text-foreground/60">N</span></td><td className="pr-4">Round to N decimal places</td><td>round:2 → 3.14159 becomes 3.14</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">multiply:<span className="text-foreground/60">N</span></td><td className="pr-4">Multiply value by factor</td><td>multiply:100 → 1.5 becomes 150.0</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">divide:<span className="text-foreground/60">N</span></td><td className="pr-4">Divide value by factor</td><td>divide:100 → 1500 becomes 15.0</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">abs</td><td className="pr-4">Convert to absolute (positive) value</td><td>-42 → 42.0</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Type Casting */}
        <div>
          <p className="font-semibold text-foreground mb-2">Type Casting & Normalisation</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Rule</th>
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Description</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Example</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">to_boolean</td><td className="pr-4">Convert Y/N/1/0/true/false/t/f to bool</td><td>"Yes" → true, "0" → false</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">to_string</td><td className="pr-4">Force any value to string</td><td>12345 → "12345"</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Date/time */}
        <div>
          <p className="font-semibold text-foreground mb-2">Date/Time Format Conversion</p>
          <p className="text-muted-foreground mb-2 text-xs">
            For date and timestamp columns, a separate date format converter is available alongside the general transforms.
          </p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`Source Format:  YYYY-MM-DD HH:mm:ss
Target Formats: ISO 8601  →  2024-01-15T09:30:00Z
                UNIX      →  1705312200
                YYYY-MM-DD → 2024-01-15`}
          </pre>
        </div>

        {/* Real-world examples */}
        <div>
          <p className="font-semibold text-foreground mb-2">Real-World Pipelines</p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`# Clean user-entered names for CRM migration
trim | titlecase | truncate:100

# Anonymise emails for staging environment
mask_email

# Convert cents to dollars and round
divide:100 | round:2

# Normalise legacy boolean flags
nullif:N/A | to_boolean

# Sanitise HTML content from CMS
strip_html | trim | truncate:500

# Hash SSNs for analytics warehouse
trim | hash:sha256`}
          </pre>
        </div>
      </div>
    ),
  },
  {
    id: "advanced-features",
    icon: Zap,
    title: "Advanced Features",
    description: "A full set of power-user capabilities for complex migration scenarios: granular scope control, per-column mapping, FK-ordered parallel streaming, upsert strategies, views, sequences, and migration history.",
    bullets: [
      "Schema & table tree — multi-schema, per-table selection with search",
      "Column mapping — rename, override target type, add transform pipeline per column",
      "FK-aware parallel streaming — tables within the same dependency level run concurrently",
      "Configurable batch size and parallelism via server environment variables",
      "Views migration — DDL copied from source, applied best-effort on target",
      "Sequences migration — current value restored on PostgreSQL targets",
      "Upsert modes — ON CONFLICT / ON DUPLICATE KEY / MERGE per engine",
      "Migration history — every run logged with status, table count, row count, duration",
    ],
    exampleTitle: "Advanced Reference",
    exampleContent: (
      <div className="space-y-5 text-sm">
        {/* Column mapping */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Column Mapping Options</p>
          <p className="text-muted-foreground text-xs mb-2">
            Available per column in the Schema Mapping tab. Changes only take effect on the target — source data is never modified.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Option</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Description</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Target Field</td><td>Rename the column on the target (default: same as source)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Target Type</td><td>Override the inferred/translated SQL type (editable when Drop Existing is on)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Action: Direct</td><td>Copy value as-is (default)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Action: Transform</td><td>Apply a pipe-chained transform pipeline (trim|hash:sha256 etc.)</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">Date Format</td><td>Convert between date string formats (source pattern → ISO 8601 / UNIX / YYYY-MM-DD)</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* FK ordering */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">FK-Aware Parallel Streaming</p>
          <p className="text-muted-foreground text-xs mb-2">
            DataBridge builds a dependency graph from foreign keys before migrating. Tables at the same dependency level are migrated in parallel; child tables wait for their parents to finish.
          </p>
          <pre className="text-xs font-mono text-muted-foreground bg-muted/50 rounded-md p-3 border border-border/40">
{`Level 0 (no FK deps): users, products, categories  ← parallel
Level 1 (depends on 0): orders, inventory           ← parallel
Level 2 (depends on 1): order_items                 ← after level 1

Server env vars:
  MIGRATION_PARALLELISM=4   (tables per level, default 4)
  MIGRATION_BATCH_SIZE=1000 (rows per commit, default 1000)`}
          </pre>
        </div>

        {/* Upsert */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Upsert Strategies by Engine</p>
          <p className="text-muted-foreground text-xs mb-2">
            When "Drop Existing" is off and a table has a primary key, DataBridge uses an engine-native upsert to avoid duplicate-key errors on re-runs.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Engine</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Strategy</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">PostgreSQL</td><td>INSERT … ON CONFLICT (pk_col) DO UPDATE SET …</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">MySQL</td><td>INSERT INTO … ON DUPLICATE KEY UPDATE …</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">SQLite</td><td>INSERT INTO … ON CONFLICT(pk_col) DO UPDATE SET …</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">SQL Server</td><td>MERGE … USING … WHEN MATCHED / WHEN NOT MATCHED</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">Snowflake</td><td>Temp staging table → bulk INSERT → MERGE (batched for efficiency)</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Views & sequences */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Views & Sequences (PostgreSQL)</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Feature</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Behaviour</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Views</td><td>Source DDL fetched with pg_get_viewdef; executed on target (best-effort, errors logged)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">Sequences</td><td>Current value read with lastval(); setval() applied on target after data load</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">pgvector</td><td>CREATE EXTENSION IF NOT EXISTS vector automatically on PostgreSQL targets</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Migration history */}
        <div>
          <p className="font-semibold text-foreground mb-1.5">Migration History</p>
          <p className="text-muted-foreground text-xs mb-2">
            Every completed run is appended to <code className="bg-muted px-1 py-0.5 rounded">migration_history.json</code> on the server and viewable on the History page.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border/60">
                  <th className="text-left py-1.5 pr-4 text-muted-foreground font-semibold">Field</th>
                  <th className="text-left py-1.5 text-muted-foreground font-semibold">Description</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">source / target</td><td>Engine type (e.g. postgresql → mysql)</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">status</td><td>success or failed</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">tables</td><td>Number of tables migrated</td></tr>
                <tr className="border-b border-border/20"><td className="py-1.5 pr-4 text-orange-600">rows</td><td>Total rows written</td></tr>
                <tr><td className="py-1.5 pr-4 text-orange-600">duration</td><td>Wall-clock time for the full migration</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    ),
  },
];

const sectionIds: SectionId[] = [
  "getting-started",
  "supported-databases",
  "file-sources",
  "data-safety",
  "transformations",
  "advanced-features",
];

function parseSectionFromHash(hash: string): SectionId | null {
  const id = hash.replace(/^#/, "") as SectionId;
  return sectionIds.includes(id) ? id : null;
}

export default function Docs() {
  const location = useLocation();
  const [activeId, setActiveId] = useState<SectionId>(() => {
    const fromHash = parseSectionFromHash(location.hash);
    return fromHash ?? "getting-started";
  });
  const activeSection = sections.find((s) => s.id === activeId) ?? sections[0];

  // Sync hash with active section
  useEffect(() => {
    const fromHash = parseSectionFromHash(location.hash);
    if (fromHash && fromHash !== activeId) setActiveId(fromHash);
  }, [location.hash]);

  const setActiveAndHash = (id: SectionId) => {
    setActiveId(id);
    window.history.replaceState(null, "", `#${id}`);
  };

  return (
    <div className="min-h-screen bg-[#FAFAFA]">
      <Header />

      <main className="container max-w-6xl mx-auto px-4 sm:px-6 py-8">
        <div className="flex flex-col lg:flex-row gap-8 lg:gap-12">
          {/* Sidebar */}
          <aside className="lg:w-56 shrink-0">
            <ScrollArea className="h-[calc(100vh-8rem)] pr-4">
              <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
                Sections
              </p>
              <nav className="space-y-0.5">
                {sections.map((section) => {
                  const Icon = section.icon;
                  const isActive = activeId === section.id;
                  return (
                    <button
                      key={section.id}
                      onClick={() => setActiveAndHash(section.id)}
                      className={cn(
                        "w-full flex items-center gap-3 rounded-lg px-3 py-2.5 text-left text-sm font-medium transition-colors",
                        isActive
                          ? "bg-orange-500/12 text-orange-700 dark:text-orange-400"
                          : "text-muted-foreground hover:bg-muted hover:text-foreground"
                      )}
                    >
                      <Icon
                        className={cn(
                          "h-4 w-4 shrink-0",
                          isActive ? "text-orange-600 dark:text-orange-400" : "text-muted-foreground"
                        )}
                      />
                      {section.title}
                    </button>
                  );
                })}
              </nav>
              <div className="mt-8 pt-6 border-t border-border">
                <Button asChild variant="secondary" className="w-full gap-2">
                  <Link to="/">
                    Start Migration
                    <ArrowRight className="h-4 w-4" />
                  </Link>
                </Button>
              </div>
            </ScrollArea>
          </aside>

          {/* Main content */}
          <div className="flex-1 min-w-0">
            <AnimatePresence mode="wait">
              <motion.article
                key={activeId}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.2 }}
                className="space-y-6"
              >
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-xl bg-orange-500/12 flex items-center justify-center shrink-0">
                    <activeSection.icon className="h-5 w-5 text-orange-600 dark:text-orange-400" />
                  </div>
                  <h1 className="text-2xl font-bold tracking-tight text-foreground">
                    {activeSection.title}
                  </h1>
                </div>

                <p className="text-[15px] text-muted-foreground leading-relaxed">
                  {activeSection.description}
                </p>

                {/* Bullets card */}
                <div className="bg-card rounded-2xl border border-border/60 shadow-sm p-6">
                  <ul className="space-y-3">
                    {activeSection.bullets.map((bullet, i) => (
                      <li
                        key={i}
                        className="flex items-center gap-3 text-[14px] text-foreground"
                      >
                        <span className="w-1.5 h-1.5 rounded-full bg-orange-500 shrink-0" />
                        {bullet}
                      </li>
                    ))}
                  </ul>
                </div>

                {/* Example card */}
                <div className="bg-card rounded-2xl border border-border/60 shadow-sm p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <Code2 className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium text-foreground">Example</span>
                  </div>
                  {activeSection.exampleTitle && (
                    <p className="text-sm font-medium text-foreground mb-3">
                      {activeSection.exampleTitle}
                    </p>
                  )}
                  <div className="bg-muted/50 rounded-lg p-4 border border-border/40">
                    {activeSection.exampleContent}
                  </div>
                </div>
              </motion.article>
            </AnimatePresence>
          </div>
        </div>
      </main>
    </div>
  );
}
