import type { Post, RedditFlair, RedditPost, Source } from "./types";

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

function isRedditPost(value: unknown): value is RedditPost {
  return (
    isRecord(value) &&
    typeof value.community === "string" &&
    (value.score === null ||
      (typeof value.score === "number" && Number.isSafeInteger(value.score))) &&
    (value.comments === null || isCount(value.comments)) &&
    (value.flair === null || typeof value.flair === "string") &&
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
