import { Navigate, NavLink, Route, Routes } from "react-router-dom";

import Config from "./routes/Config";
import Dashboards from "./routes/Dashboards";
import Dlq from "./routes/Dlq";

export default function App() {
  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="brand-mark" aria-label="ComHub">
          c
        </div>
        <strong>ComHub</strong>
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
        <div className="utility-area">
          <span className="badge">local</span>
          <span className="sync-indicator">
            <span className="status-dot" aria-hidden="true" />
            last config sync: observing
          </span>
        </div>
      </header>
      <main className="content">
        <Routes>
          <Route path="/" element={<Navigate to="/config" replace />} />
          <Route path="/config" element={<Config />} />
          <Route path="/dlq" element={<Dlq />} />
          <Route path="/dashboards" element={<Dashboards />} />
        </Routes>
      </main>
    </div>
  );
}
