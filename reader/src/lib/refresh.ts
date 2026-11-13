import { apiGet, apiPost } from "./api";
import type { components } from "./api.gen";
import { feedLabels, sectionFromView } from "./collections";
import { parseFilters } from "./navigation";

type Schemas = components["schemas"];

export type RefreshScope = Schemas["RefreshRequest"]["scope"];
export type RefreshPlatform = NonNullable<
  Schemas["AllScope"]["platforms"]
>[number];
export type RefreshJob = Schemas["RefreshJobOut"];

type KeysOf<U> = U extends unknown ? keyof U : never;
type ValueOf<U, K extends PropertyKey> = U extends unknown
  ? K extends keyof U
    ? U[K]
    : never
  : never;

/**
 * Every kind's fields at once, derived from the schema's scope variants, for
 * code that builds a scope a field at a time or reads any kind. The server
 * checks which fields go with which kind.
 */
export type RefreshScopeFields = {
  [K in KeysOf<RefreshScope>]?: ValueOf<RefreshScope, K>;
} & { kind: RefreshScope["kind"] };

export function refreshContext(params: URLSearchParams) {
  const domain = params.get("article_source");
  const post = params.get("article");
  const section = sectionFromView(params.get("view"));
  const youtubeItem = params.get("youtube_item");
  const filters = parseFilters(params);
  let scope: RefreshScopeFields;
  let label: string;

  if (domain && post) {
    scope = { kind: "post", domain, post_id: post };
    label = "this post";
  } else if (youtubeItem) {
    scope = { kind: "post", domain: "youtube.com", post_id: youtubeItem };
    label = "this YouTube item";
  } else if (section === "settings" || section === "sources") {
    return null;
  } else if (section === "youtube") {
    const channel = params.get("channel");
    const feed = params.get("feed");
    scope = { kind: "youtube" };
    label = "YouTube";

    if (params.get("youtube_view") === "playlists") {
      const playlist = params.get("youtube_playlist");
      if (playlist) scope.playlist_id = playlist;
      else scope.feed_id = "playlists";
      label = playlist ? "this YouTube playlist" : "YouTube playlists";
    } else if (channel && feed !== "https://www.youtube.com/") {
      scope.channel_id = channel;
      label = "this YouTube channel";
    } else if (feed) {
      scope.feed_id = feed;
      label =
        feed === "https://www.youtube.com/"
          ? "YouTube recommendations"
          : "this YouTube feed";
    }

    if (feed === "linked" && !channel) {
      return {
        scope: scope as RefreshScope,
        label: "linked videos",
        disabled: "Refresh the original sources to update linked videos",
      };
    }
  } else {
    scope = { kind: section === "stories" ? "rss" : section };
    label = {
      all: "all sources",
      stories: "blogs",
      reddit: "Reddit",
      x: "X",
      hacker_news: "Hacker News",
    }[section];

    if (section === "stories" && filters.blogFeed) {
      scope.blog_feed = filters.blogFeed;
      label =
        filters.blogFeed === "external"
          ? "external article sources"
          : "source feeds";
    }

    if (section === "all" && filters.platforms.length) {
      scope.platforms = filters.platforms
        .map((platform) => (platform === "stories" ? "rss" : platform))
        .sort();
      label = filters.platforms
        .map((platform) => feedLabels[platform])
        .join(" + ");
    }

    if (filters.community) {
      scope.community = filters.community;
      label = `r/${filters.community}`;
    }

    if (filters.account && !filters.xFeed) {
      scope.account = filters.account;
      label = `@${filters.account}`;
    }

    if (filters.xFeed === "for_you") {
      scope.feed_id = "for_you";
      label = "X For You recommendations";
    }
  }

  if (
    scope.kind === "post" &&
    scope.domain === "youtube.com" &&
    !scope.post_id?.startsWith("video:")
  ) {
    return {
      scope: scope as RefreshScope,
      label,
      disabled:
        "Individual YouTube community posts cannot be refreshed yet. Refresh the channel feed instead.",
    };
  }

  // Publisher filters do not identify the feeds that discovered external articles.
  if (scope.kind !== "post" && scope.blog_feed !== "external") {
    if (filters.domains.length) {
      scope.domains = [...filters.domains].sort();
      label =
        filters.domains.length === 1
          ? `${label} · ${filters.domains[0]}`
          : `${label} · selected sources`;
    }

    if (filters.tags.length) {
      scope.tags = [...filters.tags].sort();
      label += ` · ${filters.tags.join(", ")}`;
    }
  }

  return { scope: scope as RefreshScope, label, disabled: undefined };
}

export function refreshDestination(sent: RefreshScope): URLSearchParams {
  const scope: RefreshScopeFields = sent;
  const params = new URLSearchParams();

  if (scope.kind === "post") {
    if (scope.domain === "youtube.com") {
      params.set("view", "youtube");
      params.set("youtube_item", scope.post_id ?? "");
    } else {
      params.set("article", scope.post_id ?? "");
      params.set("article_source", scope.domain ?? "");
    }

    return params;
  }

  if (scope.kind !== "all") {
    params.set("view", scope.kind === "rss" ? "stories" : scope.kind);
  }
  if (scope.kind === "rss" && scope.blog_feed)
    params.set("blog_feed", scope.blog_feed);

  for (const platform of scope.platforms ?? []) {
    params.append("platform", platform === "rss" ? "stories" : platform);
  }

  for (const domain of scope.domains ?? []) params.append("domain", domain);
  for (const tag of scope.tags ?? []) params.append("tag", tag);
  if (scope.community) params.set("community", scope.community);
  if (scope.account) params.set("account", scope.account);
  if (scope.channel_id) params.set("channel", scope.channel_id);
  if (
    scope.playlist_id ||
    (scope.kind === "youtube" && scope.feed_id === "playlists")
  ) {
    params.set("youtube_view", "playlists");
    if (scope.playlist_id) params.set("youtube_playlist", scope.playlist_id);
  } else if (scope.feed_id)
    params.set(scope.kind === "x" ? "x_feed" : "feed", scope.feed_id);

  return params;
}

export function refreshScopeKey(sent: RefreshScope): string {
  const scope: RefreshScopeFields = sent;

  return JSON.stringify([
    scope.kind,
    scope.blog_feed ?? "",
    [...(scope.platforms ?? [])].sort(),
    [...(scope.domains ?? [])].sort(),
    [...(scope.tags ?? [])].sort(),
    scope.community?.toLowerCase() ?? "",
    scope.account?.toLowerCase() ?? "",
    scope.channel_id ?? "",
    scope.playlist_id ?? "",
    scope.feed_id ?? "",
    scope.domain ?? "",
    scope.post_id ?? "",
  ]);
}

export function refreshActive(job: RefreshJob): boolean {
  return ["queued", "running", "processing"].includes(job.status);
}

export async function fetchRefreshJobs(
  signal?: AbortSignal,
): Promise<RefreshJob[]> {
  const body = await apiGet("/api/reader/refresh", { signal });

  return body.jobs;
}

export async function startRefresh(scope: RefreshScope): Promise<RefreshJob> {
  const body = await apiPost("/api/reader/refresh", { scope });

  return body.job;
}
