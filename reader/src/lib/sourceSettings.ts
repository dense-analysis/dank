/**
 * Pure rules for editing saved sources. The server owns validation; this
 * only shapes what the screens send and how they describe a source.
 */
import { ReaderRequestError } from "./api";
import type { components } from "./api.gen";
import type { RefreshScope } from "./refresh";
import type { SavedSource } from "./types";

type Schemas = components["schemas"];

/** A whole source without its ID, as every write sends it. */
export type SourceDraft = Schemas["SourceBody"];
export type SourceTarget = Schemas["SourceTargetModel"];
export type SourceAdapterDraft = Schemas["SourceAdapterModel"];
export type SourcePlatform = Schemas["SourceAdapter"];
export type TargetList = "accounts" | "feeds";

export const platforms: readonly SourcePlatform[] = [
  "rss",
  "x",
  "reddit",
  "youtube",
  "hacker_news",
];

export const platformLabels: Record<SourcePlatform, string> = {
  rss: "Blog or feed",
  x: "X",
  reddit: "Reddit",
  youtube: "YouTube",
  hacker_news: "Hacker News",
};

/** The one domain each platform other than a publisher collects. */
export const platformDomains: Record<Exclude<SourcePlatform, "rss">, string> = {
  x: "x.com",
  reddit: "reddit.com",
  youtube: "youtube.com",
  hacker_news: "news.ycombinator.com",
};

/** The platform that collects a canonical domain, as the server decides it. */
export function platformOf(domain: string): SourcePlatform {
  const match = Object.entries(platformDomains).find(
    ([, value]) => value === domain.trim().toLowerCase(),
  );

  return match ? (match[0] as SourcePlatform) : "rss";
}

interface ListCopy {
  label: string;
  /** One entry, for buttons and counts. */
  one: string;
  many: string;
  placeholder: string;
  help: string;
}

/** The lists a platform collects from, in the order the editor shows them. */
export const targetLists: Record<
  SourcePlatform,
  Partial<Record<TargetList, ListCopy>>
> = {
  rss: {
    feeds: {
      label: "Feeds",
      one: "feed",
      many: "feeds",
      placeholder: "https://example.com/feed.xml",
      help: "Leave empty to let DANK find the site's feed.",
    },
  },
  x: {
    accounts: {
      label: "Accounts",
      one: "account",
      many: "accounts",
      placeholder: "@handle",
      help: "With no accounts, this source collects only your For You feed.",
    },
  },
  reddit: {
    feeds: {
      label: "Communities",
      one: "community",
      many: "communities",
      placeholder: "r/community",
      help: "A Reddit source needs at least one community.",
    },
  },
  youtube: {
    accounts: {
      label: "Channels",
      one: "channel",
      many: "channels",
      placeholder: "@channel",
      help: "A channel's handle, or its ID starting with UC.",
    },
    feeds: {
      label: "Channel and playlist links",
      one: "link",
      many: "links",
      placeholder: "https://www.youtube.com/playlist?list=…",
      help: "A channel, playlist or feed page on youtube.com.",
    },
  },
  hacker_news: {
    feeds: {
      label: "RSS feeds",
      one: "feed",
      many: "feeds",
      placeholder: "https://hnrss.org/frontpage",
      help: "Optional. Collected alongside the front page.",
    },
  },
};

export const hackerNewsFilterLabels = {
  top10: "Top 10 each day",
  top20: "Top 20 each day",
  top50: "Top 50 each day",
  all: "Every front-page story",
} as const;

export function newDraft(platform: SourcePlatform): SourceDraft {
  return {
    enabled: true,
    domain: platform === "rss" ? "" : platformDomains[platform],
    accounts: [],
    feeds: [],
    tags: [],
    adapters: null,
    // The defaults config.toml gives a source without these tables.
    reddit: platform === "reddit" ? { sort: "top", time_filter: "day" } : null,
    hacker_news: platform === "hacker_news" ? { filter: "top50" } : null,
  };
}

export function draftOf(source: SavedSource): SourceDraft {
  const { id: _, ...draft } = source;

  return draft;
}

export function sameDraft(left: SourceDraft, right: SourceDraft): boolean {
  return JSON.stringify(ordered(left)) === JSON.stringify(ordered(right));
}

function ordered(draft: SourceDraft): unknown[] {
  return [
    draft.enabled,
    draft.domain,
    draft.accounts.map(pair),
    draft.feeds.map(pair),
    draft.tags,
    draft.adapters?.map((adapter) => [
      adapter.kind,
      adapter.feeds?.map(pair) ?? null,
    ]) ?? null,
    draft.reddit && [draft.reddit.sort, draft.reddit.time_filter],
    draft.hacker_news && [draft.hacker_news.filter],
  ];
}

function pair(target: SourceTarget): [string, boolean] {
  return [target.value, target.enabled];
}

/**
 * What the editor sends. Hacker News feeds only collect through an RSS
 * adapter, which config.toml adds itself and the API wants written out.
 */
export function prepared(source: SourceDraft): SourceDraft {
  // A pasted link stands for its site.
  const domain = siteOf(source.domain).domain;
  // An adapter's feeds are typed a line at a time, so blank lines are noise.
  const draft: SourceDraft = {
    ...source,
    adapters:
      source.adapters?.map((adapter) => {
        const feeds = adapter.feeds
          ?.map((feed) => ({ ...feed, value: feed.value.trim() }))
          .filter((feed) => feed.value);

        return { ...adapter, feeds: feeds?.length ? feeds : null };
      }) ?? null,
  };

  if (
    platformOf(domain) === "hacker_news" &&
    draft.feeds.length > 0 &&
    draft.adapters === null
  ) {
    return {
      ...draft,
      domain,
      adapters: [
        { kind: "hacker_news", feeds: null },
        { kind: "rss", feeds: null },
      ],
    };
  }

  return { ...draft, domain };
}

/** A source's targets as people write them, the first few by name. */
function targetNames(source: SourceDraft): string {
  const platform = platformOf(source.domain);
  const names = (["accounts", "feeds"] as const).flatMap((list) =>
    targetLists[platform][list]
      ? source[list].map((target) => targetLabel(platform, list, target.value))
      : [],
  );
  const more = names.length - 3;

  return `${names.slice(0, 3).join(", ")}${more > 0 ? ` and ${more} more` : ""}`;
}

/** What to call a source: its site, or the accounts and communities in it. */
export function sourceTitle(source: SourceDraft): string {
  const platform = platformOf(source.domain);

  switch (platform) {
    case "rss":
      return source.domain.replace(/^www\./i, "") || "New source";
    case "hacker_news":
      return platformLabels.hacker_news;
    default:
      return (
        targetNames(source) ||
        {
          x: "X For You feed",
          reddit: "Reddit",
          youtube: "Your YouTube feeds",
        }[platform]
      );
  }
}

/** The line under a source's title: its platform, options and what is off. */
export function sourceDetail(source: SourceDraft): string {
  const platform = platformOf(source.domain);
  const off = [...source.accounts, ...source.feeds].filter(
    (target) => !target.enabled,
  ).length;
  const feeds = source.feeds.length;
  const parts = {
    rss: [
      feeds
        ? `${feeds} ${feeds === 1 ? "feed" : "feeds"}`
        : "Feed found automatically",
    ],
    x: ["X"],
    reddit: [
      "Reddit",
      source.reddit &&
        `${source.reddit.sort}, ${source.reddit.time_filter === "all" ? "all time" : `past ${source.reddit.time_filter}`}`,
    ],
    youtube: ["YouTube"],
    hacker_news: [
      source.hacker_news && hackerNewsFilterLabels[source.hacker_news.filter],
      feeds && `${feeds} ${feeds === 1 ? "feed" : "feeds"}`,
    ],
  }[platform];

  return [...parts, off && `${off} off`].filter(Boolean).join(" · ");
}

/** A target as people write it: @handle, r/community, or its link. */
export function targetLabel(
  platform: SourcePlatform,
  list: TargetList,
  value: string,
): string {
  if (list === "accounts") {
    return /^(https?:|UC)/.test(value) ? value : `@${value.replace(/^@/, "")}`;
  }

  if (platform === "reddit") {
    const community = communityName(value);

    return community ? `r/${community}` : value;
  }

  return value.replace(/^https?:\/\/(www\.)?/i, "");
}

/** What a source collects, in a line. */
export function sourceSummary(source: SourceDraft): string {
  const platform = platformOf(source.domain);
  const parts: string[] = [];

  for (const list of ["accounts", "feeds"] as const) {
    const copy = targetLists[platform][list];
    const targets = source[list];

    if (!copy || !targets.length) continue;

    const off = targets.filter((target) => !target.enabled).length;
    const names = targets
      .slice(0, 3)
      .map((target) => targetLabel(platform, list, target.value));
    const more = targets.length - names.length;

    parts.push(
      `${names.join(", ")}${more ? ` and ${more} more` : ""}${off ? ` (${off} off)` : ""}`,
    );
  }

  if (parts.length) return parts.join(" · ");

  return {
    rss: "Feed found automatically",
    x: "For You only",
    reddit: "No communities",
    youtube: "Your YouTube feeds only",
    hacker_news: "Front page",
  }[platform];
}

function communityName(value: string): string {
  return /(?:^|\/)r\/([A-Za-z0-9_]+)\/?$/.exec(value.trim())?.[1] ?? "";
}

function channelKey(value: string): string {
  const path = /^https?:\/\/(?:www\.)?youtube\.com(\/[^?#]*)/i.exec(value)?.[1];
  const key = (path ?? value)
    .replace(/\/(videos|shorts|posts|community|featured)\/?$/, "")
    .replace(/^\/(channel\/)?/, "")
    .replace(/^@/, "")
    .replace(/\/$/, "");

  return key.toLowerCase();
}

/** How a list tells two entries apart, close to the server's own rule. */
export function targetKey(
  platform: SourcePlatform,
  list: TargetList,
  value: string,
): string {
  const text = value.trim();

  if (platform === "x") return text.replace(/^@/, "").toLowerCase();
  if (platform === "reddit")
    return communityName(text).toLowerCase() || text.toLowerCase();
  if (platform === "youtube" && (list === "accounts" || !text.includes("?")))
    return channelKey(text);

  return text.replace(/\/$/, "");
}

/** A captured account, community, channel or site that could be collected. */
export interface SourceIdentity {
  platform: SourcePlatform;
  list: TargetList | null;
  /** The value saved in the list, or the domain for a site. */
  value: string;
  /** What to call it, such as @handle or r/community. */
  label: string;
}

export function accountIdentity(handle: string): SourceIdentity {
  const value = handle.replace(/^@/, "");

  return { platform: "x", list: "accounts", value, label: `@${value}` };
}

export function communityIdentity(name: string): SourceIdentity {
  return {
    platform: "reddit",
    list: "feeds",
    value: `r/${name}`,
    label: `r/${name}`,
  };
}

export function channelIdentity(channel: {
  id: string;
  name: string;
  url: string;
}): SourceIdentity | null {
  // Only a channel's own page can be collected, not a search or a video.
  if (!/^https?:\/\/(www\.)?youtube\.com\/(@|channel\/)/i.test(channel.url))
    return null;

  return {
    platform: "youtube",
    list: "feeds",
    value: channel.url,
    label: channel.name || channel.url,
  };
}

export function siteIdentity(domain: string): SourceIdentity {
  return { platform: "rss", list: null, value: domain, label: domain };
}

export interface SavedMatch {
  source: SavedSource;
  /** The matching entry's place in its list, or null for a whole site. */
  index: number | null;
  /** Whether DANK collects it now: the source and the entry are both on. */
  collecting: boolean;
}

/** Where an identity is already saved, enabled matches first. */
export function savedMatches(
  sources: readonly SavedSource[],
  identity: SourceIdentity,
): SavedMatch[] {
  const matches: SavedMatch[] = [];

  for (const source of sources) {
    if (platformOf(source.domain) !== identity.platform) continue;

    if (identity.list === null) {
      if (source.domain === identity.value.toLowerCase())
        matches.push({ source, index: null, collecting: source.enabled });

      continue;
    }

    const key = targetKey(identity.platform, identity.list, identity.value);
    // A YouTube channel can be saved as a handle or as its link.
    const lists: TargetList[] =
      identity.platform === "youtube" ? ["accounts", "feeds"] : [identity.list];

    for (const list of lists) {
      const index = source[list].findIndex(
        (target) => targetKey(identity.platform, list, target.value) === key,
      );

      if (index >= 0) {
        matches.push({
          source,
          index,
          collecting: source.enabled && source[list][index].enabled,
        });
      }
    }
  }

  return matches.sort(
    (left, right) => Number(right.collecting) - Number(left.collecting),
  );
}

/** Sources an identity could be added to: the same platform's, enabled first. */
export function sourcesAccepting(
  sources: readonly SavedSource[],
  identity: SourceIdentity,
): SavedSource[] {
  if (identity.list === null) return [];

  return sources
    .filter((source) => platformOf(source.domain) === identity.platform)
    .sort((left, right) => Number(right.enabled) - Number(left.enabled));
}

/** The draft with an identity collected: added, or turned back on. */
export function withIdentity(
  draft: SourceDraft,
  identity: SourceIdentity,
): SourceDraft {
  if (identity.list === null)
    return { ...draft, enabled: true, domain: identity.value };

  const { platform, list } = identity;
  const key = targetKey(platform, list, identity.value);
  const matches = (at: TargetList) => (target: SourceTarget) =>
    targetKey(platform, at, target.value) === key;
  // A YouTube channel can already be there as a handle rather than a link.
  const held = (
    platform === "youtube" ? (["accounts", "feeds"] as const) : [list]
  ).find((at) => draft[at].some(matches(at)));

  if (!held) {
    return {
      ...draft,
      enabled: true,
      [list]: [...draft[list], { value: identity.value, enabled: true }],
    };
  }

  return {
    ...draft,
    enabled: true,
    [held]: draft[held].map((target) =>
      matches(held)(target) ? { ...target, enabled: true } : target,
    ),
  };
}

/** Each problem's message by its path in the source, such as feeds.0.value. */
export function fieldMessages(error: unknown): Map<string, string> {
  const messages = new Map<string, string>();

  if (!(error instanceof ReaderRequestError)) return messages;

  for (const field of error.fields) {
    // Shape errors are reported from the request, rule errors from the source.
    const path = field.path.replace(/^body\./, "");

    if (!messages.has(path)) messages.set(path, field.message);
  }

  return messages;
}

/** Whether a save may have reached the server, so only a retry can tell. */
export function unconfirmed(error: unknown): boolean {
  return !(error instanceof ReaderRequestError) || error.status >= 500;
}

/**
 * The narrowest collection that covers a saved source. One entry that this
 * save added or turned on is collected alone; anything else collects the
 * source's platform.
 */
export function collectScope(
  saved: SourceDraft,
  before?: SourceDraft,
): RefreshScope {
  const platform = platformOf(saved.domain);
  const added = (["accounts", "feeds"] as const).flatMap((list) => {
    const was = new Set(
      (before?.enabled ? before[list] : [])
        .filter((target) => target.enabled)
        .map((target) => target.value),
    );

    return saved[list]
      .filter((target) => target.enabled && !was.has(target.value))
      .map((target) => ({ list, value: target.value }));
  });
  const only = added.length === 1 ? added[0] : undefined;

  switch (platform) {
    case "x":
      return only && /^@?[A-Za-z0-9_]{1,15}$/.test(only.value)
        ? { kind: "x", account: only.value.replace(/^@/, "") }
        : { kind: "x" };
    case "reddit": {
      const community = only ? communityName(only.value) : "";

      return community ? { kind: "reddit", community } : { kind: "reddit" };
    }
    case "youtube":
      return { kind: "youtube" };
    case "hacker_news":
      return { kind: "hacker_news" };
    default:
      return { kind: "rss", domains: [saved.domain] };
  }
}

export function parseTags(text: string): string[] {
  return [
    ...new Set(
      text
        .split(",")
        .map((tag) => tag.trim().toLowerCase())
        .filter(Boolean),
    ),
  ];
}

/** Adapters a platform's source can list, with its own first. */
export function adapterKinds(platform: SourcePlatform): SourcePlatform[] {
  return platform === "rss"
    ? ["rss"]
    : platform === "hacker_news"
      ? ["hacker_news", "rss"]
      : [platform];
}

function settingsKey(draft: SourceDraft): string {
  const platform = platformOf(draft.domain);
  const keys = (list: TargetList, targets: readonly SourceTarget[]) =>
    targets.map((target) => targetKey(platform, list, target.value));

  return JSON.stringify([
    draft.domain.trim().toLowerCase(),
    keys("accounts", draft.accounts),
    keys("feeds", draft.feeds),
    [...draft.tags].sort(),
    draft.adapters?.map((adapter) => [
      adapter.kind,
      adapter.feeds ? keys("feeds", adapter.feeds) : null,
    ]) ?? null,
    draft.reddit,
    draft.hacker_news,
  ]);
}

/**
 * The saved source a refused duplicate collided with. The server compares
 * normalized values, so this can miss; callers then say so without a link.
 */
export function duplicateOf(
  sources: readonly SavedSource[],
  draft: SourceDraft,
  except?: string,
): SavedSource | undefined {
  const key = settingsKey(draft);

  return sources.find(
    (source) => source.id !== except && settingsKey(draftOf(source)) === key,
  );
}

/** What pasted text asks DANK to collect. */
export interface ParsedSource {
  identity: SourceIdentity;
  /** The site's feed to collect, when the link was to one. */
  feed?: string;
  /** Another reading of the same text: a bare @handle is on X or YouTube. */
  other?: SourceIdentity;
}

const xHosts = new Set(["x.com", "twitter.com", "mobile.twitter.com"]);
const redditHosts = new Set(["reddit.com", "old.reddit.com", "new.reddit.com"]);
const youtubeHosts = new Set(["youtube.com", "m.youtube.com"]);
const hackerNewsHosts = new Set(["news.ycombinator.com", "hckrnews.com"]);
// The first part of an x.com path that is a page rather than an account.
const xPages = new Set([
  "home",
  "explore",
  "search",
  "i",
  "settings",
  "notifications",
  "messages",
  "compose",
  "hashtag",
  "login",
]);
// A heuristic for links that are feeds rather than pages of the site.
const feedPath =
  /\.(xml|rss|atom)$|(^|\/)(feeds?|rss|atom)(\/|\.|$)|[?&](feed|format|alt)=(rss|atom)/i;

function parsedUrl(text: string): URL | null {
  try {
    const url = new URL(
      /^[a-z][a-z0-9+.-]*:\/\//i.test(text) ? text : `https://${text}`,
    );

    return /^https?:$/.test(url.protocol) && url.hostname.includes(".")
      ? url
      : null;
  } catch {
    return null;
  }
}

/**
 * A site's domain from what was typed or pasted, with its feed when the
 * link was to one. Text that isn't a link is left for the server to judge.
 */
export function siteOf(text: string): { domain: string; feed?: string } {
  const entry = text.trim();
  const url = /[/:]/.test(entry) && !/\s/.test(entry) ? parsedUrl(entry) : null;

  if (!url) return { domain: entry };

  return {
    domain: url.host.toLowerCase().replace(/^www\./, ""),
    feed: feedPath.test(url.pathname + url.search) ? url.href : undefined,
  };
}

function channelLink(path: string): SourceIdentity {
  return {
    platform: "youtube",
    list: "feeds",
    value: `https://www.youtube.com/${path}`,
    label: path.startsWith("@") ? path : `youtube.com/${path}`,
  };
}

/**
 * Reads one pasted link, handle, community or domain. Returns null for
 * nothing, and a problem to show for text that names no source.
 */
export function parseSource(
  text: string,
): ParsedSource | { problem: string } | null {
  const entry = text.trim();

  if (!entry) return null;
  if (/\s/.test(entry)) return { problem: "Add one source at a time." };

  const handle = /^@([A-Za-z0-9_.-]+)$/.exec(entry)?.[1];

  if (handle) {
    // X handles are at most 15 letters, digits and underscores.
    return /^[A-Za-z0-9_]{1,15}$/.test(handle)
      ? { identity: accountIdentity(handle), other: channelLink(`@${handle}`) }
      : { identity: channelLink(`@${handle}`) };
  }

  const community = /^\/?r\/([A-Za-z0-9_]+)\/?$/.exec(entry)?.[1];

  if (community) return { identity: communityIdentity(community) };

  const url = parsedUrl(entry);

  if (!url) {
    return { problem: "Paste a link, a domain, @handle or r/community." };
  }

  const host = url.hostname.toLowerCase().replace(/^www\./, "");
  const path = url.pathname.replace(/\/+$/, "");

  if (xHosts.has(host)) {
    const name = path.split("/")[1] ?? "";

    if (!name) {
      return {
        identity: {
          platform: "x",
          list: null,
          value: platformDomains.x,
          label: "the X For You feed",
        },
      };
    }

    return /^[A-Za-z0-9_]{1,15}$/.test(name) && !xPages.has(name.toLowerCase())
      ? { identity: accountIdentity(name) }
      : { problem: "Paste an account's link, such as x.com/handle." };
  }

  if (redditHosts.has(host)) {
    const name = /^\/r\/([A-Za-z0-9_]+)/.exec(path)?.[1];

    return name
      ? { identity: communityIdentity(name) }
      : { problem: "Paste a community's link, such as reddit.com/r/name." };
  }

  if (youtubeHosts.has(host) || host === "youtu.be") {
    const playlist = path === "/playlist" && url.searchParams.get("list");
    const channel = /^\/(@[^/]+|(?:channel|c|user)\/[^/]+)/.exec(path)?.[1];

    if (playlist) {
      return {
        identity: {
          platform: "youtube",
          list: "feeds",
          value: `https://www.youtube.com/playlist?list=${playlist}`,
          label: "this YouTube playlist",
        },
      };
    }

    if (channel) return { identity: channelLink(channel) };

    return path || host === "youtu.be"
      ? { problem: "Paste a channel's or playlist's link, not a video's." }
      : {
          identity: {
            platform: "youtube",
            list: null,
            value: platformDomains.youtube,
            label: "your YouTube feeds",
          },
        };
  }

  if (hackerNewsHosts.has(host)) {
    return {
      identity: {
        platform: "hacker_news",
        list: null,
        value: platformDomains.hacker_news,
        label: "Hacker News",
      },
    };
  }

  const site = siteOf(url.href);

  return { identity: siteIdentity(site.domain), feed: site.feed };
}

/** How saving a parsed source changes what is saved. */
export interface AddPlan {
  /** The saved source to change, or null to add one. */
  id: string | null;
  draft: SourceDraft;
  /** The source as it was, when it is already saved. */
  before?: SourceDraft;
  /** The source that collects this already, leaving nothing to save. */
  collected?: SavedSource;
}

/**
 * Where a parsed source goes: turned back on where it is saved, added to a
 * source of its platform that is on, or saved as a new source.
 */
export function addPlan(
  sources: readonly SavedSource[],
  { identity, feed }: ParsedSource,
): AddPlan {
  const matches = savedMatches(sources, identity);
  const target =
    matches[0]?.source ??
    sourcesAccepting(sources, identity).find((source) => source.enabled);
  const before = target && draftOf(target);
  let draft = withIdentity(before ?? newDraft(identity.platform), identity);

  if (feed) {
    draft = withIdentity(draft, {
      platform: "rss",
      list: "feeds",
      value: feed,
      label: feed,
    });
  }

  return {
    id: target?.id ?? null,
    draft,
    before,
    collected:
      target && before && sameDraft(draft, before) ? target : undefined,
  };
}
