import { Layers3, Rss, Settings2, X } from "lucide-react";

export function Sidebar({
  section,
  select,
  mobileOpen,
  close,
}: {
  section: string;
  select: (section: string) => void;
  mobileOpen: boolean;
  close: () => void;
}) {
  return (
    <aside className={`sidebar ${mobileOpen ? "mobile-open" : ""}`}>
      <a className="brand" href="/reader/" aria-label="DANK reader home">
        <span>DANK</span>
      </a>
      <button
        type="button"
        className="icon-button close-menu"
        onClick={close}
        aria-label="Close navigation"
      >
        <X size={20} />
      </button>
      <nav aria-label="Main navigation">
        <button
          type="button"
          className={`nav-item ${section === "all" ? "active" : ""}`}
          aria-current={section === "all" ? "page" : undefined}
          onClick={() => select("all")}
        >
          <Layers3 size={19} /> All stories <span className="active-dot" />
        </button>
        <button
          type="button"
          className={`nav-item ${section === "sources" ? "active" : ""}`}
          aria-current={section === "sources" ? "page" : undefined}
          onClick={() => select("sources")}
        >
          <Rss size={19} /> Sources
        </button>
      </nav>
      <div className="sidebar-bottom">
        <button
          type="button"
          className={`nav-item ${section === "settings" ? "active" : ""}`}
          aria-current={section === "settings" ? "page" : undefined}
          onClick={() => select("settings")}
        >
          <Settings2 size={19} /> Settings
        </button>
      </div>
    </aside>
  );
}
