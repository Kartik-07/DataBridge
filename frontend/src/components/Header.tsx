import React from "react";
import { Link, useLocation } from "react-router-dom";

function NavButton({ label, to, active }: { label: string; to: string; active?: boolean }) {
    return (
        <Link
            to={to}
            className={`flex items-center px-4 h-full text-[13px] font-medium transition-colors border-b-2 ${active
                ? "text-white border-[#E85C1C]"
                : "text-slate-400 border-transparent hover:text-slate-200"
                }`}
        >
            {label}
        </Link>
    );
}

interface HeaderProps {
    onReset?: () => void;
}

export function Header({ onReset }: HeaderProps) {
    const location = useLocation();

    return (
        <header className="bg-[#1e2329] border-b border-[#1e2329] sticky top-0 z-10 text-white">
            <div className="container max-w-[1400px] mx-auto flex items-center h-14 px-8">
                {/* Brand (left) */}
                <div className="flex-1 flex justify-start">
                    <Link to="/" className="flex items-center gap-2.5">
                        <img src="/DataBridge.png" alt="DataBridge Logo" className="h-7 w-auto object-contain" />
                        <span className="text-[14px] font-bold tracking-wide uppercase">
                            <span className="text-white">Data</span><span className="text-[#EC5B13]">Bridge</span>
                        </span>
                    </Link>
                </div>

                {/* Nav (center) */}
                <nav className="hidden md:flex items-center h-full flex-shrink-0">
                    <NavButton to="/" label="Migrate" active={location.pathname === "/"} />
                    <NavButton to="/history" label="History" active={location.pathname === "/history"} />
                    <NavButton to="/docs" label="Docs" active={location.pathname === "/docs"} />
                </nav>

                {/* Spacer (right, for balance) */}
                <div className="flex-1" />
            </div>
        </header>
    );
}
