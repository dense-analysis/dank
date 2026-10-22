import { request } from "./api";
import { feedLabels, sectionFromView } from "./collections";
import { parseFilters } from "./navigation";
import { isRecord, isStringArray } from "./validation";

export type RefreshPlatform =
  | "rss"
  | "x"
  | "reddit"
  | "hacker_news"
  | "youtube";

export interface RefreshScope {
  kind: "all" | "rss" | "reddit" | "x" | "hacker_news" | "youtube" | "post";
  platforms?: RefreshPlatform[];
  domains?: string[];
  tags?: string[];
  community?: string;
  account?: string;
  channel_id?: string;
  feed_id?: string;
  domain?: string;
  post_id?: string;
}

export interface RefreshJob {
  id: string;
  scope: RefreshScope;
  label: string;
  status:
    | "queued"
    | "running"
    | "processing"
    | "completed"
    | "partial"
    | "failed"
    | "interrupted";
  message: string;
  created_at: string;
  finished_at: string | null;
  posts_saved: number;
  run_id?: string | null;
}

export function refreshContext(params: URLSearchParams) {
  const domain = params.get("article_source");
  const post = params.get("article");
  const section = sectionFromView(params.get("view"));
  const youtubeItem = params.get("youtube_item");
  const filters = parseFilters(params);
  let scope: RefreshScope;
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

    if (channel) {
      scope.channel_id = channel;
      label = "this YouTube channel";
    } else if (feed) {
      scope.feed_id = feed;
      label = "this YouTube feed";
    }

    if (feed === "linked" && !channel) {
      return {
        scope,
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

    if (filters.account) {
      scope.account = filters.account;
      label = `@${filters.account}`;
    }
  }

  if (
    scope.kind === "post" &&
    scope.domain === "youtube.com" &&
    !scope.post_id?.startsWith("video:")
  ) {
    return {
      scope,
      label,
      disabled:
        "Individual YouTube community posts cannot be refreshed yet. Refresh the channel feed instead.",
    };
  }

  if (scope.kind !== "post") {
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

  return { scope, label, disabled: undefined };
}

export function refreshDestination(scope: RefreshScope): URLSearchParams {
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

  for (const platform of scope.platforms ?? []) {
    params.append("platform", platform === "rss" ? "stories" : platform);
  }

  for (const domain of scope.domains ?? []) params.append("domain", domain);
  for (const tag of scope.tags ?? []) params.append("tag", tag);
  if (scope.community) params.set("community", scope.community);
  if (scope.account) params.set("account", scope.account);
  if (scope.channel_id) params.set("channel", scope.channel_id);
  if (scope.feed_id) params.set("feed", scope.feed_id);

  return params;
}

export function refreshScopeKey(scope: RefreshScope): string {
  return JSON.stringify([
    scope.kind,
    [...(scope.platforms ?? [])].sort(),
    [...(scope.domains ?? [])].sort(),
    [...(scope.tags ?? [])].sort(),
    scope.community?.toLowerCase() ?? "",
    scope.account?.toLowerCase() ?? "",
    scope.channel_id ?? "",
    scope.feed_id ?? "",
    scope.domain ?? "",
    scope.post_id ?? "",
  ]);
}

export function refreshActive(job: RefreshJob): boolean {
  return ["queued", "running", "processing"].includes(job.status);
}

function isScope(value: unknown): value is RefreshScope {
  return (
    isRecord(value) &&
    ["all", "rss", "reddit", "x", "hacker_news", "youtube", "post"].includes(
      String(value.kind),
    ) &&
    (value.platforms === undefined ||
      (value.kind === "all" &&
        isStringArray(value.platforms) &&
        value.platforms.every((platform) =>
          ["rss", "x", "reddit", "hacker_news", "youtube"].includes(platform),
        ))) &&
    ["domains", "tags"].every(
      (key) => value[key] === undefined || isStringArray(value[key]),
    ) &&
    [
      "community",
      "account",
      "channel_id",
      "feed_id",
      "domain",
      "post_id",
    ].every((key) => value[key] === undefined || typeof value[key] === "string")
  );
}

export function isRefreshJob(value: unknown): value is RefreshJob {
  return (
    isRecord(value) &&
    ["id", "label", "message", "created_at"].every(
      (key) => typeof value[key] === "string",
    ) &&
    isScope(value.scope) &&
    [
      "queued",
      "running",
      "processing",
      "completed",
      "partial",
      "failed",
      "interrupted",
    ].includes(String(value.status)) &&
    (value.finished_at === null || typeof value.finished_at === "string") &&
    typeof value.posts_saved === "number" &&
    Number.isSafeInteger(value.posts_saved) &&
    value.posts_saved >= 0 &&
    (value.run_id === undefined ||
      value.run_id === null ||
      typeof value.run_id === "string")
  );
}

export async function fetchRefreshJobs(
  signal?: AbortSignal,
): Promise<RefreshJob[]> {
  const body = await request("/refresh", signal);

  if (
    !isRecord(body) ||
    !Array.isArray(body.jobs) ||
    !body.jobs.every(isRefreshJob)
  ) {
    throw new Error(
      "The reader received an unexpected refresh status. Please try again.",
    );
  }

  return body.jobs;
}

export async function startRefresh(scope: RefreshScope): Promise<RefreshJob> {
  const body = await request("/refresh", undefined, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scope }),
  });

  if (!isRecord(body) || !isRefreshJob(body.job)) {
    throw new Error(
      "The reader could not confirm the refresh. Check its status before trying again.",
    );
  }

  return body.job;
}
