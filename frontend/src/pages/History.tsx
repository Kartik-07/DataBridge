import { useEffect, useState } from "react";
import { Header } from "@/components/Header";
import { fetchHistory, type HistoryEntry } from "@/services/api";
import { CheckCircle2, XCircle, ArrowRight, Clock } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

export default function History() {
    const [history, setHistory] = useState<HistoryEntry[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetchHistory()
            .then((data) => {
                setHistory(data);
                setLoading(false);
            })
            .catch((err) => {
                console.error("Failed to load history:", err);
                setLoading(false);
            });
    }, []);

    const getDbName = (type: string) => {
        switch (type) {
            case "postgresql": return "PostgreSQL";
            case "mysql": return "MySQL";
            case "snowflake": return "Snowflake";
            case "sqlite": return "SQLite";
            case "sqlserver": return "SQL Server";
            default: return type;
        }
    };

    return (
        <div className="min-h-screen bg-[#FAFAFA]">
            <Header />

            <main className="container max-w-4xl mx-auto px-5 py-8 space-y-6">
                <div className="mb-8">
                    <h1 className="text-3xl font-bold tracking-tight text-foreground mb-2">
                        Migration History
                    </h1>
                    <p className="text-muted-foreground text-[15px]">
                        View past migrations and their results
                    </p>
                </div>

                {loading ? (
                    <div className="flex justify-center py-12">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
                    </div>
                ) : history.length === 0 ? (
                    <div className="text-center py-12 bg-card rounded-2xl border border-border/60 shadow-sm">
                        <p className="text-muted-foreground text-[15px]">No migration history found.</p>
                    </div>
                ) : (
                    <div className="space-y-4">
                        <AnimatePresence>
                            {history.map((entry, i) => (
                                <motion.div
                                    key={i}
                                    initial={{ opacity: 0, y: 10 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    transition={{ delay: i * 0.05 }}
                                    className="bg-card rounded-2xl border border-border/60 shadow-sm p-4 hover:shadow-md transition-shadow"
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-4">
                                            {/* Status Icon */}
                                            <div className={`p-2 rounded-full flex items-center justify-center shrink-0 w-12 h-12 ${entry.status === "success" ? "bg-green-50" : "bg-red-50"
                                                }`}>
                                                {entry.status === "success" ? (
                                                    <CheckCircle2 className="h-6 w-6 text-green-500" />
                                                ) : (
                                                    <XCircle className="h-6 w-6 text-red-500" />
                                                )}
                                            </div>

                                            {/* Details */}
                                            <div className="flex flex-col">
                                                <div className="flex items-center gap-2 mb-1">
                                                    <span className="font-semibold text-[15px]">{getDbName(entry.source)}</span>
                                                    <ArrowRight className="h-4 w-4 text-muted-foreground" />
                                                    <span className="font-semibold text-[15px]">{getDbName(entry.target)}</span>
                                                </div>
                                                <div className="flex items-center gap-3 text-sm text-muted-foreground">
                                                    <span>{entry.tables} {entry.tables === 1 ? 'table' : 'tables'}</span>
                                                    <span className="w-1 h-1 rounded-full bg-border"></span>
                                                    <span>{entry.rows.toLocaleString()} rows</span>
                                                    <span className="w-1 h-1 rounded-full bg-border"></span>
                                                    <span>{entry.duration}</span>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="flex flex-col items-end gap-2 shrink-0">
                                            <span className={`px-2.5 py-1 rounded-full text-xs font-medium uppercase tracking-wider ${entry.status === "success" ? "bg-muted text-foreground" : "bg-red-500 text-white"
                                                }`}>
                                                {entry.status}
                                            </span>
                                            <div className="flex items-center text-sm text-muted-foreground gap-1.5">
                                                <Clock className="h-3.5 w-3.5" />
                                                <span>{entry.timestamp}</span>
                                            </div>
                                        </div>
                                    </div>
                                </motion.div>
                            ))}
                        </AnimatePresence>
                    </div>
                )}
            </main>
        </div>
    );
}
