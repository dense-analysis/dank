import type { Post, RedditFlair, RedditPost, Source, XProfile } from "./types";

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isStringArray(value: unknown): value is string[] {
  return (
    Array.isArray(value) && value.every((item) => typeof item === "string")
  );
}

export function isDateFilter(value: unknown): value is string {
  if (value === "") return true;
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value))
    return false;

  const date = new Date(`${value}T00:00:00Z`);
  return (
    !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 10) === value
  );
}

function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isOptionalIcon(value: unknown): boolean {
  return value === undefined || value === null || typeof value === "string";
}

function isFlair(value: unknown): value is RedditFlair {
  return (
    isRecord(value) && typeof value.name === "string" && isCount(value.count)
  );
}

function isOptionalFlairs(value: unknown): boolean {
  return value === undefined || (Array.isArray(value) && value.every(isFlair));
}

function isRedditVideo(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.url === "string" &&
    (value.poster_url === null || typeof value.poster_url === "string") &&
    typeof value.remote === "boolean" &&
    isOptionalIcon(value.hls_url) &&
    isOptionalIcon(value.fallback_url)
  );
}

function isRedditComment(value: unknown): boolean {
  return (
    isRecord(value) &&
    ["id", "parent_id", "author", "body_html", "body", "permalink"].every(
      (key) => typeof value[key] === "string",
    ) &&
    (value.score === null ||
      (typeof value.score === "number" && Number.isSafeInteger(value.score))) &&
    (value.created_utc === null ||
      (typeof value.created_utc === "number" &&
        Number.isFinite(value.created_utc) &&
        value.created_utc >= 0 &&
        value.created_utc <= 8640000000000)) &&
    typeof value.is_op === "boolean" &&
    isCount(value.depth)
  );
}

function isCommentCapture(value: unknown): boolean {
  return (
    isRecord(value) &&
    ["captured", "disabled", "failed", "not_captured"].includes(
      String(value.status),
    ) &&
    typeof value.sort === "string" &&
    isCount(value.limit) &&
    (value.total_available === null || isCount(value.total_available)) &&
    typeof value.has_more === "boolean" &&
    Array.isArray(value.comments) &&
    value.comments.every(isRedditComment) &&
    (value.captured_at === undefined ||
      typeof value.captured_at === "string") &&
    (value.latest_attempt_status === undefined ||
      value.latest_attempt_status === "disabled" ||
      value.latest_attempt_status === "failed")
  );
}

function isRedditPost(value: unknown): value is RedditPost {
  return (
    isRecord(value) &&
    typeof value.community === "string" &&
    (value.score === null ||
      (typeof value.score === "number" && Number.isSafeInteger(value.score))) &&
    (value.comments === null || isCount(value.comments)) &&
    (value.flair === null || typeof value.flair === "string") &&
    isOptionalIcon(value.icon_url) &&
    (value.video === undefined ||
      value.video === null ||
      isRedditVideo(value.video)) &&
    (value.videos === undefined ||
      (Array.isArray(value.videos) && value.videos.every(isRedditVideo))) &&
    (value.comment_capture === undefined ||
      value.comment_capture === null ||
      isCommentCapture(value.comment_capture))
  );
}

function isXProfile(value: unknown): value is XProfile {
  return (
    isRecord(value) &&
    typeof value.handle === "string" &&
    /^[a-zA-Z0-9_]{1,15}$/.test(value.handle) &&
    typeof value.name === "string" &&
    isOptionalIcon(value.icon_url)
  );
}

export function isPost(value: unknown): value is Post {
  return (
    isRecord(value) &&
    [
      "id",
      "domain",
      "url",
      "author",
      "title",
      "excerpt",
      "html",
      "created_at",
      "source",
    ].every((key) => typeof value[key] === "string") &&
    (value.reddit === undefined ||
      value.reddit === null ||
      isRedditPost(value.reddit)) &&
    (value.x === undefined || value.x === null || isXPost(value.x)) &&
    (value.collection_incomplete === undefined ||
      typeof value.collection_incomplete === "boolean") &&
    (value.thumbnail === null || typeof value.thumbnail === "string") &&
    Array.isArray(value.media) &&
    value.media.every(
      (item) =>
        isRecord(item) &&
        typeof item.url === "string" &&
        typeof item.content_type === "string",
    )
  );
}

function isXPost(value: unknown): boolean {
  return (
    isRecord(value) &&
    (isXProfile(value) ||
      (value.handle === "" &&
        typeof value.name === "string" &&
        isOptionalIcon(value.icon_url))) &&
    (value.links === undefined ||
      (Array.isArray(value.links) &&
        value.links.every(
          (link) =>
            isRecord(link) &&
            typeof link.url === "string" &&
            typeof link.title === "string" &&
            typeof link.description === "string" &&
            (link.image_url === null || typeof link.image_url === "string"),
        ))) &&
    (value.media === undefined ||
      (Array.isArray(value.media) &&
        value.media.every(
          (media) =>
            isRecord(media) &&
            ["image", "video", "gif"].includes(String(media.type)) &&
            typeof media.url === "string" &&
            typeof media.alt === "string" &&
            typeof media.remote === "boolean" &&
            (media.poster_url === null || typeof media.poster_url === "string"),
        ))) &&
    (value.reply_count === undefined ||
      value.reply_count === null ||
      isCount(value.reply_count)) &&
    (value.replies_status === undefined ||
      value.replies_status === "not_collected") &&
    isOptionalIcon(value.captured_at)
  );
}

export function isSource(value: unknown): value is Source {
  return (
    isRecord(value) &&
    typeof value.domain === "string" &&
    typeof value.name === "string" &&
    isStringArray(value.tags) &&
    isCount(value.count) &&
    isOptionalFlairs(value.flairs) &&
    (value.kind === undefined ||
      ["rss", "x", "reddit"].includes(String(value.kind))) &&
    (value.accounts === undefined ||
      (Array.isArray(value.accounts) &&
        value.accounts.every(
          (account) =>
            isXProfile(account) && isRecord(account) && isCount(account.count),
        ))) &&
    (value.communities === undefined ||
      (Array.isArray(value.communities) &&
        value.communities.every(
          (community) =>
            isRecord(community) &&
            typeof community.name === "string" &&
            isCount(community.count) &&
            isOptionalIcon(community.icon_url) &&
            isOptionalFlairs(community.flairs),
        )))
  );
}
