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

function isTimestamp(value: unknown): boolean {
  return (
    typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:\d{2})$/.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}

function isOptionalPostTotal(value: unknown): boolean {
  return (
    value === undefined ||
    value === null ||
    (isRecord(value) &&
      isCount(value.count) &&
      value.source === "arctic_shift" &&
      isTimestamp(value.updated_at) &&
      isTimestamp(value.captured_at))
  );
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
    isOptionalIcon(value.fallback_url) &&
    isOptionalIcon(value.source_url)
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
    ["captured", "disabled", "failed", "deferred", "not_captured"].includes(
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
      value.latest_attempt_status === "failed" ||
      value.latest_attempt_status === "deferred")
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
    (value.images === undefined ||
      (Array.isArray(value.images) &&
        value.images.every(
          (image) =>
            isRecord(image) &&
            typeof image.url === "string" &&
            typeof image.source_url === "string",
        ))) &&
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
  return validPost(value, true);
}

function validPost(value: unknown, allowConversation: boolean): value is Post {
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
    (value.x === undefined ||
      value.x === null ||
      isXPost(value.x, allowConversation)) &&
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

function isConversationStatus(value: unknown): boolean {
  return (
    typeof value === "string" &&
    ["captured", "partial", "failed", "disabled", "not_collected"].includes(
      value,
    )
  );
}

function isConversationNode(value: unknown): boolean {
  return (
    validPost(value, false) &&
    isRecord(value) &&
    value.source === "x" &&
    ["x.com", "twitter.com"].includes(value.domain) &&
    (value.parent_id === null || typeof value.parent_id === "string") &&
    isCount(value.depth) &&
    value.depth <= 20 &&
    typeof value.is_author === "boolean" &&
    typeof value.missing_parent === "boolean"
  );
}

function isConversation(value: unknown): boolean {
  return (
    isRecord(value) &&
    isConversationStatus(value.status) &&
    (value.captured_at === null || typeof value.captured_at === "string") &&
    typeof value.has_more === "boolean" &&
    (value.reason === null || typeof value.reason === "string") &&
    (value.reported_replies === null || isCount(value.reported_replies)) &&
    isCount(value.limit) &&
    Array.isArray(value.context) &&
    value.context.length <= 100 &&
    value.context.every(isConversationNode) &&
    Array.isArray(value.replies) &&
    value.replies.length <= 500 &&
    value.replies.every(isConversationNode) &&
    (value.latest_attempt_status === undefined ||
      isConversationStatus(value.latest_attempt_status)) &&
    isOptionalIcon(value.latest_attempt_at)
  );
}

function isXPost(value: unknown, allowConversation: boolean): boolean {
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
            (media.source_url === undefined ||
              typeof media.source_url === "string") &&
            (media.aspect_ratio === undefined ||
              (typeof media.aspect_ratio === "number" &&
                Number.isFinite(media.aspect_ratio) &&
                media.aspect_ratio > 0)) &&
            (media.poster_url === null || typeof media.poster_url === "string"),
        ))) &&
    ["reply_count", "repost_count", "like_count", "view_count"].every(
      (key) =>
        value[key] === undefined || value[key] === null || isCount(value[key]),
    ) &&
    (value.replies_status === undefined ||
      isConversationStatus(value.replies_status)) &&
    (value.replies_captured_count === undefined ||
      isCount(value.replies_captured_count)) &&
    isOptionalIcon(value.captured_at) &&
    (value.conversation === undefined ||
      (allowConversation && isConversation(value.conversation)))
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
      ["rss", "x", "reddit", "youtube"].includes(String(value.kind))) &&
    (value.accounts === undefined ||
      (Array.isArray(value.accounts) &&
        value.accounts.every(
          (account) =>
            isXProfile(account) &&
            isRecord(account) &&
            isCount(account.count) &&
            (account.total_posts === undefined ||
              account.total_posts === null ||
              isCount(account.total_posts)),
        ))) &&
    (value.communities === undefined ||
      (Array.isArray(value.communities) &&
        value.communities.every(
          (community) =>
            isRecord(community) &&
            typeof community.name === "string" &&
            isCount(community.count) &&
            isOptionalIcon(community.icon_url) &&
            isOptionalFlairs(community.flairs) &&
            isOptionalPostTotal(community.post_total),
        )))
  );
}
