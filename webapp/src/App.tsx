import { ChevronRight } from "lucide-react";
import { Navigate, NavLink, Route, Routes } from "react-router-dom";

import { Dot, Pill } from "./components/ui/atoms";
import { TopbarIdentityProvider, useTopbarIdentity } from "./lib/topbarIdentity";
import Config from "./routes/Config";
import Dashboards from "./routes/Dashboards";
import Dlq from "./routes/Dlq";

export default function App() {
  return (
    <TopbarIdentityProvider>
      <div className="app-shell">
        <TopBar />
        <main className="content">
          <Routes>
            <Route path="/" element={<Navigate to="/config" replace />} />
            <Route path="/config" element={<Config />} />
            <Route path="/dlq" element={<Dlq />} />
            <Route path="/dashboards" element={<Dashboards />} />
          </Routes>
        </main>
      </div>
    </TopbarIdentityProvider>
  );
}

function TopBar() {
  const { identity } = useTopbarIdentity();

  return (
    <header className="top-bar">
      <div className="brand-mark" aria-label="ComHub">
        c
      </div>
      <strong className="brand-wordmark">ComHub</strong>
      <nav className="top-nav" aria-label="Primary">
        <NavLink className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`} to="/config">
          Config
        </NavLink>
        <NavLink className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`} to="/dlq">
          DLQ
        </NavLink>
        <NavLink className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`} to="/dashboards">
          Dashboards
        </NavLink>
      </nav>
      {identity ? <IdentityBreadcrumb identity={identity} /> : null}
      <div className="utility-area">
        <TelemetryStrip />
        <Pill tone="accent">local</Pill>
        <span className="sync-indicator">
          <Dot tone="success" />
          last config sync: observing
        </span>
      </div>
    </header>
  );
}

function IdentityBreadcrumb({ identity }: { identity: { topic: string; sourceEventType: string; enabled: boolean } }) {
  return (
    <div className="identity-pill" aria-label="Editing source identity">
      <KafkaIcon />
      <span className="identity-pill-topic mono">{identity.topic}</span>
      <ChevronRight className="identity-pill-chevron" size={12} aria-hidden="true" />
      <strong className="identity-pill-event">{identity.sourceEventType}</strong>
      <Dot tone={identity.enabled ? "success" : "neutral"} />
      <span className="identity-pill-state">{identity.enabled ? "enabled" : "disabled"}</span>
    </div>
  );
}

function TelemetryStrip() {
  // Static MVP values; real values come from a metrics endpoint in a later story.
  const cells = [
    { label: "lag", value: "3", tone: "success" as const },
    { label: "thru", value: "142/s", tone: "neutral" as const },
    { label: "retry", value: "0", tone: "neutral" as const },
    { label: "dlq", value: "2", tone: "warning" as const }
  ];
  return (
    <div className="telem-strip" role="group" aria-label="Pipeline telemetry">
      {cells.map((cell, index) => (
        <div key={cell.label} className={`telem-cell ${index === cells.length - 1 ? "telem-cell-last" : ""}`}>
          <span className="telem-cell-label">{cell.label}</span>
          <span className={`telem-cell-value telem-cell-${cell.tone}`}>{cell.value}</span>
        </div>
      ))}
    </div>
  );
}

function KafkaIcon() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="identity-pill-mark"
      aria-hidden="true"
    >
      <circle cx="12" cy="4" r="2" />
      <circle cx="12" cy="12" r="2" />
      <circle cx="12" cy="20" r="2" />
      <path d="M12 6v4M12 14v4" />
    </svg>
  );
}
