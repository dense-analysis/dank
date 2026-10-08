import {
  ArrowUpRight,
  Bookmark,
  CircleHelp,
  Layers3,
  Library,
  Plus,
  Rss,
  X,
} from "lucide-react";
import type { SavedFeed } from "../lib/types";

export function Sidebar({
  section,
  feeds,
  savedCount,
  select,
  newFeed,
  mobileOpen,
  close,
}: {
  section: string;
  feeds: SavedFeed[];
  savedCount: number;
  select: (section: string) => void;
  newFeed: () => void;
  mobileOpen: boolean;
  close: () => void;
}) {
  return (
    <aside className={`sidebar ${mobileOpen ? "mobile-open" : ""}`}>
      <a className="brand" href="/reader/" aria-label="DANK reader home">
        <img src="/reader/favicon.svg" alt="" width="34" height="34" />
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
      <div className="workspace-label">
        <span className="workspace-avatar">Y</span>
        <div>
          Your workspace<small>A quieter corner of the internet</small>
        </div>
      </div>
      <nav aria-label="Main navigation">
        <p className="nav-label">LIBRARY</p>
        <button
          type="button"
          className={`nav-item ${section === "all" ? "active" : ""}`}
          onClick={() => select("all")}
        >
          <Layers3 size={19} />
          All stories
          <span className="active-dot" />
        </button>
        <button
          type="button"
          className={`nav-item ${section === "saved" ? "active" : ""}`}
          onClick={() => select("saved")}
        >
          <Bookmark size={19} />
          Saved<span className="nav-count">{savedCount || ""}</span>
        </button>
        <button
          type="button"
          className={`nav-item ${section === "sources" ? "active" : ""}`}
          onClick={() => select("sources")}
        >
          <Rss size={19} />
          Sources
        </button>
        <div className="nav-label nav-label-row">
          <span>YOUR FEEDS</span>
          <button
            type="button"
            className="icon-button"
            aria-label="Create a feed"
            onClick={newFeed}
          >
            <Plus size={15} />
          </button>
        </div>
        {feeds.map((feed) => (
          <button
            type="button"
            className={`nav-item ${section === feed.id ? "active" : ""}`}
            key={feed.id}
            onClick={() => select(feed.id)}
          >
            <span className="feed-dot" />
            {feed.name}
          </button>
        ))}
        <button type="button" className="nav-item new-feed" onClick={newFeed}>
          <Plus size={18} />
          Create a feed
        </button>
      </nav>
      <div className="sidebar-bottom">
        <div className="personal-note">
          <Library size={20} />
          <strong>A library with your point of view.</strong>
          <p>Bring your sources together. Find something worth your time.</p>
          <button type="button" onClick={newFeed}>
            Make your first feed <ArrowUpRight size={14} />
          </button>
        </div>
        <div className="local-note">
          <CircleHelp size={14} />
          Feeds & bookmarks stay in this browser.
        </div>
      </div>
    </aside>
  );
}
