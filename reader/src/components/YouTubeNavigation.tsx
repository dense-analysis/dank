import "../youtube-navigation.css";
import {
  Flame,
  Link2,
  ListVideo,
  Newspaper,
  Play,
  Sparkles,
  TvMinimal,
} from "lucide-react";
import type { YouTubeSources } from "../lib/youtubeCollection";
import { youtubeFeedLabel, youtubeGroupFeeds } from "../lib/youtubeNavigation";
import { CollectionNavGroup } from "./CollectionNavGroup";
import { CountryFlag } from "./CountryFlag";
import { IdentityAvatar } from "./IdentityAvatar";
import { PlatformIcon } from "./PlatformIcon";

export function YouTubeNavigation({
  sources,
  active,
  selected,
  select,
  selectItem,
}: {
  sources?: YouTubeSources;
  active: boolean;
  selected: string;
  select: () => void;
  selectItem: (selection: string) => void;
}) {
  const common = {
    active,
    selected,
    selectItem,
    caseSensitive: true,
    countLabel: "discoveries",
  };
  const groups = [
    { kind: "hype" as const, label: "Hyped", icon: <Flame size={16} /> },
    { kind: "news" as const, label: "News", icon: <Newspaper size={16} /> },
  ];

  return (
    <CollectionNavGroup
      {...common}
      id="youtube-feeds"
      label="YouTube"
      itemsLabel="YouTube feeds"
      icon={<PlatformIcon platform="youtube" />}
      select={select}
      items={
        sources
          ? [
              {
                id: "linked",
                label: "From your feeds",
                count: sources.linked_count,
                avatar: <Link2 size={15} />,
              },
              ...sources.feeds
                .filter(
                  (feed) =>
                    !["hype", "news", "channel", "playlist"].includes(
                      feed.kind,
                    ),
                )
                .map((feed) => ({
                  id: `feed:${feed.id}`,
                  label: youtubeFeedLabel(feed),
                  count: feed.count,
                  avatar:
                    feed.kind === "home" ? (
                      <Sparkles size={15} />
                    ) : (
                      <Play size={15} />
                    ),
                })),
            ]
          : []
      }
    >
      <li>
        <button
          type="button"
          className={`nav-item collection-item ${active && selected === "playlists" ? "active" : ""}`}
          aria-current={active && selected === "playlists" ? "page" : undefined}
          onClick={() => selectItem("playlists")}
        >
          <ListVideo size={15} />
          <span className="collection-name">Playlists</span>
        </button>
      </li>
      {groups.map(({ kind, label, icon }) => {
        const feeds = youtubeGroupFeeds(sources, kind);
        if (!feeds.length) return null;

        return (
          <li key={kind}>
            <CollectionNavGroup
              {...common}
              active={
                active && feeds.some((feed) => selected === `feed:${feed.id}`)
              }
              id={`youtube-${kind}`}
              label={label}
              itemsLabel={`YouTube ${label}`}
              icon={icon}
              select={() => selectItem(`feed:${feeds[0].id}`)}
              items={feeds.map((feed) => ({
                id: `feed:${feed.id}`,
                label: youtubeFeedLabel(feed),
                count: feed.count,
                avatar:
                  kind === "hype" ? (
                    <CountryFlag region={feed.region} />
                  ) : (
                    <Newspaper size={14} />
                  ),
              }))}
            />
          </li>
        );
      })}
      {!!sources?.channels.length && (
        <li>
          <CollectionNavGroup
            {...common}
            active={active && selected.startsWith("channel:")}
            id="youtube-channels"
            label="Channel Feeds"
            itemsLabel="YouTube Channel Feeds"
            icon={<TvMinimal size={16} />}
            countLabel="collected videos"
            items={sources.channels.map((channel) => ({
              id: `channel:${channel.id}`,
              label: channel.name,
              count: channel.count,
              avatar: (
                <IdentityAvatar
                  name={channel.name}
                  iconUrl={channel.avatar_url}
                  kind="community"
                />
              ),
            }))}
          />
        </li>
      )}
    </CollectionNavGroup>
  );
}
