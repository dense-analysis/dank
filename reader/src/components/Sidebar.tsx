import {
  ChevronDown,
  Layers3,
  MessagesSquare,
  Rss,
  Settings2,
  X,
} from "lucide-react";
import { useState } from "react";
import type { RedditCommunity } from "../lib/types";

export function Sidebar({
  section,
  select,
  redditEnabled,
  communities,
  community,
  selectCommunity,
  mobileOpen,
  close,
}: {
  section: string;
  select: (section: string) => void;
  redditEnabled: boolean;
  communities: RedditCommunity[];
  community: string;
  selectCommunity: (community: string) => void;
  mobileOpen: boolean;
  close: () => void;
}) {
  const [communitiesOpen, setCommunitiesOpen] = useState(true);
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
        {redditEnabled && (
          <div className="reddit-navigation">
            <div className="reddit-nav-heading">
              <button
                type="button"
                className={`nav-item ${section === "reddit" && !community ? "active" : ""}`}
                aria-current={
                  section === "reddit" && !community ? "page" : undefined
                }
                onClick={() => select("reddit")}
              >
                <MessagesSquare size={19} /> Reddit
              </button>
              <button
                type="button"
                className="icon-button community-toggle"
                aria-label={
                  communitiesOpen
                    ? "Collapse Reddit communities"
                    : "Expand Reddit communities"
                }
                aria-expanded={communitiesOpen}
                aria-controls="reddit-communities"
                onClick={() => setCommunitiesOpen(!communitiesOpen)}
              >
                <ChevronDown size={16} />
              </button>
            </div>
            <ul
              id="reddit-communities"
              className="reddit-communities"
              hidden={!communitiesOpen}
            >
              {communities.map((item) => (
                <li key={item.name}>
                  <button
                    type="button"
                    className={`nav-item community-item ${section === "reddit" && community.toLowerCase() === item.name.toLowerCase() ? "active" : ""}`}
                    aria-current={
                      section === "reddit" &&
                      community.toLowerCase() === item.name.toLowerCase()
                        ? "page"
                        : undefined
                    }
                    onClick={() => selectCommunity(item.name)}
                    title={`r/${item.name}`}
                    aria-label={`r/${item.name}, ${item.count} collected posts`}
                  >
                    <span className="community-name">r/{item.name}</span>
                    <span className="community-count" aria-hidden="true">
                      {item.count.toLocaleString()}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          </div>
        )}
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
