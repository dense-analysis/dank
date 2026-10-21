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

function isHackerNewsPost(value: unknown): boolean {
  if (!isRecord(value)) return false;
  const capture = value.comment_capture;

  return (
    (value.submitted_at === undefined ||
      value.submitted_at === null ||
      isTimestamp(value.submitted_at)) &&
    (value.points === null || isCount(value.points)) &&
    (value.comments === null || isCount(value.comments)) &&
    typeof value.discussion_url === "string" &&
    (value.external_url === null || typeof value.external_url === "string") &&
    typeof value.day === "string" &&
    value.day !== "" &&
    isDateFilter(value.day) &&
    (value.rank === null || (isCount(value.rank) && value.rank > 0)) &&
    (capture === undefined ||
      (isRecord(capture) &&
        ["captured", "partial", "disabled", "failed", "not_captured"].includes(
          String(capture.status),
        ) &&
        isCount(capture.captured) &&
        (capture.total === null || isCount(capture.total)) &&
        (capture.reason === null || typeof capture.reason === "string"))) &&
    (value.comments_items === undefined ||
      (Array.isArray(value.comments_items) &&
        value.comments_items.every(
          (comment) =>
            isRecord(comment) &&
            ["id", "parent_id", "author", "html"].every(
              (key) => typeof comment[key] === "string",
            ) &&
            (comment.created_at === null || isTimestamp(comment.created_at)) &&
            isCount(comment.depth),
        )))
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

function isYouTubePost(value: unknown): boolean {
  return (
    isRecord(value) &&
    ["video", "short", "post"].includes(String(value.kind)) &&
    (value.video_id === null || typeof value.video_id === "string") &&
    isRecord(value.channel) &&
    typeof value.channel.id === "string" &&
    typeof value.channel.name === "string" &&
    typeof value.channel.url === "string" &&
    typeof value.duration_text === "string" &&
    typeof value.views_text === "string" &&
    (value.thumbnail_url === null || typeof value.thumbnail_url === "string") &&
    (value.video_url === null || typeof value.video_url === "string")
  );
}

export function isPost(value: unknown): value is Post {
  return validPost(value, true);
}

function isPostReference(value: unknown): boolean {
  return (
    isRecord(value) &&
    ["id", "domain", "url", "title", "source"].every(
      (key) => typeof value[key] === "string",
    ) &&
    (value.excerpt === undefined || typeof value.excerpt === "string")
  );
}

function isCollectionOrigin(value: unknown): boolean {
  return (
    isRecord(value) &&
    ["domain", "id", "source", "url", "label"].every(
      (key) => typeof value[key] === "string",
    ) &&
    isTimestamp(value.observed_at) &&
    (value.kind === "reference" || value.kind === "article")
  );
}

function isCollectionInfo(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.article_url === "string" &&
    (value.article === null || isPostReference(value.article)) &&
    ["collected", "incomplete", "unavailable"].includes(
      String(value.article_status),
    ) &&
    [
      "post_first_seen_at",
      "link_first_seen_at",
      "first_seen_at",
      "article_collected_at",
    ].every((key) => value[key] === null || isTimestamp(value[key])) &&
    Array.isArray(value.first_seen_from) &&
    value.first_seen_from.length <= 5 &&
    value.first_seen_from.every(isCollectionOrigin) &&
    (value.collected_via === null || isCollectionOrigin(value.collected_via)) &&
    (value.seen_before_post === null ||
      typeof value.seen_before_post === "boolean") &&
    ["post_published_at", "earlier_discovered_at"].every(
      (key) =>
        value[key] === undefined ||
        value[key] === null ||
        isTimestamp(value[key]),
    ) &&
    (value.discovered_before_publication === undefined ||
      value.discovered_before_publication === null ||
      typeof value.discovered_before_publication === "boolean") &&
    (value.earlier_discovered_from === undefined ||
      (Array.isArray(value.earlier_discovered_from) &&
        value.earlier_discovered_from.length <= 5 &&
        value.earlier_discovered_from.every(isCollectionOrigin))) &&
    value.coverage === "saved_history"
  );
}

function validPost(
  value: unknown,
  allowConversation: boolean,
  allowQuote = true,
): value is Post {
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
    (value.published_at === undefined ||
      value.published_at === null ||
      isTimestamp(value.published_at)) &&
    (value.linked_post === undefined ||
      value.linked_post === null ||
      isPostReference(value.linked_post)) &&
    (value.conversation_id === undefined ||
      (typeof value.conversation_id === "string" &&
        /^[0-9]{1,20}$/.test(value.conversation_id))) &&
    (value.reply_to === undefined ||
      (isRecord(value.reply_to) &&
        typeof value.reply_to.id === "string" &&
        /^[0-9]{1,20}$/.test(value.reply_to.id) &&
        ["x.com", "twitter.com"].includes(String(value.reply_to.domain)) &&
        typeof value.reply_to.url === "string" &&
        typeof value.reply_to.collected === "boolean" &&
        (value.reply_to.is_self === null ||
          typeof value.reply_to.is_self === "boolean"))) &&
    (value.links === undefined ||
      (Array.isArray(value.links) && value.links.every(isPostReference))) &&
    (value.links_count === undefined || isCount(value.links_count)) &&
    (value.reddit === undefined ||
      value.reddit === null ||
      isRedditPost(value.reddit)) &&
    (value.hacker_news === undefined ||
      value.hacker_news === null ||
      isHackerNewsPost(value.hacker_news)) &&
    (value.youtube === undefined ||
      value.youtube === null ||
      isYouTubePost(value.youtube)) &&
    (value.x === undefined ||
      value.x === null ||
      isXPost(value.x, allowConversation, allowQuote)) &&
    (value.collection_info === undefined ||
      value.collection_info === null ||
      isCollectionInfo(value.collection_info)) &&
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
    typeof value.missing_parent === "boolean" &&
    (value.collected === undefined || typeof value.collected === "boolean")
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
    (value.local_posts_count === undefined ||
      isCount(value.local_posts_count)) &&
    (value.local_posts_limited === undefined ||
      typeof value.local_posts_limited === "boolean") &&
    isOptionalIcon(value.latest_attempt_at)
  );
}

function isXQuote(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    /^[0-9]{1,20}$/.test(value.id) &&
    ["x.com", "twitter.com"].includes(String(value.domain)) &&
    typeof value.url === "string" &&
    typeof value.collected === "boolean" &&
    (value.post === undefined ||
      (validPost(value.post, false, false) &&
        value.post.id === value.id &&
        value.post.domain === value.domain &&
        value.post.source === "x"))
  );
}

function isXPost(
  value: unknown,
  allowConversation: boolean,
  allowQuote: boolean,
): boolean {
  return (
    isRecord(value) &&
    (isXProfile(value) ||
      (value.handle === "" &&
        typeof value.name === "string" &&
        isOptionalIcon(value.icon_url))) &&
    (value.conversation_id === undefined ||
      (typeof value.conversation_id === "string" &&
        /^[0-9]{1,20}$/.test(value.conversation_id))) &&
    (value.reply_to === undefined ||
      (isRecord(value.reply_to) &&
        typeof value.reply_to.id === "string" &&
        /^[0-9]{1,20}$/.test(value.reply_to.id) &&
        ["x.com", "twitter.com"].includes(String(value.reply_to.domain)) &&
        typeof value.reply_to.url === "string" &&
        typeof value.reply_to.collected === "boolean" &&
        (value.reply_to.is_self === null ||
          typeof value.reply_to.is_self === "boolean"))) &&
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
    (value.quote === undefined || (allowQuote && isXQuote(value.quote))) &&
    (value.conversation === undefined ||
      (allowConversation && isConversation(value.conversation)))
  );
}

export function isSource(value: unknown): value is Source {
  return (
    isRecord(value) &&
    (value.configured === undefined || typeof value.configured === "boolean") &&
    typeof value.domain === "string" &&
    typeof value.name === "string" &&
    isStringArray(value.tags) &&
    isCount(value.count) &&
    isOptionalFlairs(value.flairs) &&
    (value.kind === undefined ||
      ["rss", "x", "reddit", "youtube", "hacker_news"].includes(
        String(value.kind),
      )) &&
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
