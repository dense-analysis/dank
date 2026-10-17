import { Layers3, Newspaper, Rss, Settings2, X } from "lucide-react";
import type { RedditCommunity, Section, XAccount } from "../lib/types";
import { CollectionNavGroup } from "./CollectionNavGroup";
import { CommunityAvatar } from "./CommunityAvatar";
import { IdentityAvatar } from "./IdentityAvatar";
import { PlatformIcon } from "./PlatformIcon";

export function Sidebar({
  section,
  select,
  redditEnabled,
  communities,
  community,
  selectCommunity,
  xEnabled,
  accounts,
  account,
  selectAccount,
  mobileOpen,
  close,
}: {
  section: Section;
  select: (section: Section) => void;
  redditEnabled: boolean;
  communities: RedditCommunity[];
  community: string;
  selectCommunity: (community: string) => void;
  xEnabled: boolean;
  accounts: XAccount[];
  account: string;
  selectAccount: (account: string) => void;
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
          <Layers3 size={19} /> All <span className="active-dot" />
        </button>
        <button
          type="button"
          className={`nav-item ${section === "stories" ? "active" : ""}`}
          aria-current={section === "stories" ? "page" : undefined}
          onClick={() => select("stories")}
        >
          <Newspaper size={19} /> Blogs
        </button>
        {xEnabled && (
          <CollectionNavGroup
            id="x-accounts"
            label="X"
            itemsLabel="X accounts"
            icon={<PlatformIcon platform="x" />}
            active={section === "x"}
            selected={account}
            select={() => select("x")}
            selectItem={selectAccount}
            items={accounts.map((item) => ({
              id: item.handle,
              label:
                item.name.trim() &&
                item.name.toLowerCase() !== item.handle.toLowerCase()
                  ? item.name
                  : `@${item.handle}`,
              accessibleLabel:
                item.name.trim() &&
                item.name.toLowerCase() !== item.handle.toLowerCase()
                  ? `${item.name} (@${item.handle})`
                  : `@${item.handle}`,
              title: item.name
                ? `${item.name} (@${item.handle})`
                : `@${item.handle}`,
              count: item.count,
              avatar: (
                <IdentityAvatar
                  name={item.handle}
                  iconUrl={item.icon_url}
                  kind="account"
                />
              ),
            }))}
          />
        )}
        {redditEnabled && (
          <CollectionNavGroup
            id="reddit-communities"
            label="Reddit"
            itemsLabel="Reddit communities"
            icon={<PlatformIcon platform="reddit" />}
            active={section === "reddit"}
            selected={community}
            select={() => select("reddit")}
            selectItem={selectCommunity}
            items={communities.map((item) => ({
              id: item.name,
              label: `r/${item.name}`,
              count: item.count,
              avatar: (
                <CommunityAvatar
                  community={item.name}
                  iconUrl={item.icon_url}
                />
              ),
            }))}
          />
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
