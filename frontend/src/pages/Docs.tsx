import { useState, useEffect } from "react";
import { Link, useLocation } from "react-router-dom";
import { Header } from "@/components/Header";
import { BookOpen, Database, Shield, Zap, ArrowRight, Code2 } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";

type SectionId = "getting-started" | "supported-databases" | "data-safety" | "advanced-features";

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
    description: "Learn the basics of connecting databases and running your first migration.",
    bullets: [
      "Connect source & target databases",
      "Select schemas and tables",
      "Configure migration options",
      "Run and monitor migration",
    ],
    exampleTitle: "Quick Start",
    exampleContent: (
      <ol className="list-decimal list-inside space-y-2 text-sm text-muted-foreground font-mono">
        <li>Select your source and target databases</li>
        <li>Enter connection credentials.</li>
        <li>Choose schemas and tables to migrate</li>
        <li>Review the summary and start migration</li>
      </ol>
    ),
  },
  {
    id: "supported-databases",
    icon: Database,
    title: "Supported Databases",
    description: "DBMigrate supports cross-platform migrations between popular database engines.",
    bullets: [
      "PostgreSQL 12+",
      "MySQL 8.0+",
      "Snowflake (latest)",
      "SQLite 3",
      "SQL Server",
    ],
    exampleTitle: "Connection String Formats",
    exampleContent: (
      <pre className="text-sm text-muted-foreground font-mono whitespace-pre-wrap">
        {`# Connection String Formats
PostgreSQL: postgresql://user:pass@host:5432/db
MySQL: mysql://user:pass@host:3306/db
Snowflake: snowflake://user:pass@account.snowflake.com/db
SQLite: sqlite:///path/to/database.sqlite3
SQL Server: sqlserver://user:pass@host:1433/db`}
      </pre>
    ),
  },
  {
    id: "data-safety",
    icon: Shield,
    title: "Data Safety",
    description: "Your data is handled securely with built-in safeguards.",
    bullets: [
      "Dry-run mode to preview SQL without writing",
      "Batch-level checkpointing for crash-safe resume",
      "Retry logic for transient batch failures",
      "Pause and resume long-running migrations",
    ],
    exampleTitle: "Dry Run Mode",
    exampleContent: (
      <pre className="text-sm text-muted-foreground font-mono whitespace-pre-wrap">
        {`# Dry Run Mode
Enable "Dry Run" in migration options
to preview all SQL statements
without executing them.`}
      </pre>
    ),
  },
  {
    id: "advanced-features",
    icon: Zap,
    title: "Advanced Features",
    description: "Power-user features for complex migration scenarios.",
    bullets: [
      "Schema-level and table-level selection",
      "Custom column mapping (rename, type override)",
      "Date/time transforms (ISO 8601, Unix epoch)",
      "Views and sequences migration (PostgreSQL)",
      "FK-aware parallel table streaming",
    ],
    exampleTitle: "Type Mapping",
    exampleContent: (
      <pre className="text-sm text-muted-foreground font-mono whitespace-pre-wrap">
        {`# Type Mapping
JSONB (PostgreSQL) -> JSON (MySQL)
SERIAL -> AUTO_INCREMENT
TIMESTAMPTZ -> TIMESTAMP`}
      </pre>
    ),
  },
];

const sectionIds: SectionId[] = [
  "getting-started",
  "supported-databases",
  "data-safety",
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
