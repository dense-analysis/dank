import { describe, expect, it } from "vitest";
import { collectionInfoTitle } from "../components/CollectionInfo";
import type { CollectionInfo, Post } from "./types";
import { isPost } from "./validation";

const origin = {
  domain: "reddit.com",
  id: "first",
  source: "reddit",
  url: "https://reddit.com/r/science/comments/first/",
  label: "r/science",
  observed_at: "2026-10-19T08:00:00Z",
  kind: "reference" as const,
};
const info: CollectionInfo = {
  article_url: "https://publisher.test/article",
  article: null,
  article_status: "unavailable",
  post_first_seen_at: "2026-10-19T09:00:00Z",
  link_first_seen_at: "2026-10-19T09:00:00Z",
  first_seen_at: origin.observed_at,
  first_seen_from: [origin],
  article_collected_at: null,
  collected_via: null,
  seen_before_post: true,
  coverage: "saved_history",
};
const post: Post = {
  id: "later",
  domain: "reddit.com",
  url: "https://reddit.com/r/news/comments/later/",
  source: "reddit",
  author: "reader",
  title: "A story",
  excerpt: "",
  html: "",
  created_at: "2026-10-19T07:00:00Z",
  thumbnail: null,
  media: [],
  collection_info: info,
};

describe("collection information contract", () => {
  it("accepts unknown historical values and all article capture statuses", () => {
    expect(isPost(post)).toBe(true);
    expect(isPost({ ...post, collection_info: undefined })).toBe(true);
    expect(isPost({ ...post, collection_info: null })).toBe(true);
    for (const article_status of ["collected", "incomplete", "unavailable"])
      expect(
        isPost({
          ...post,
          collection_info: {
            ...info,
            article_status,
            post_first_seen_at: null,
            link_first_seen_at: null,
            first_seen_at: null,
            seen_before_post: null,
            first_seen_from: [],
          },
        }),
      ).toBe(true);
  });

  it("rejects invalid dates, routes, status, coverage and unbounded origins", () => {
    for (const invalid of [
      { first_seen_at: "not-a-date" },
      { post_first_seen_at: "2026-10-19" },
      { link_first_seen_at: "not-a-date" },
      { article_collected_at: false },
      { seen_before_post: "true" },
      { article_status: "success" },
      { coverage: "complete_history" },
      { first_seen_from: Array(6).fill(origin) },
      { first_seen_from: [{ ...origin, observed_at: "yesterday" }] },
      { collected_via: { ...origin, kind: "guessed" } },
      { article: { id: "article" } },
    ])
      expect(
        isPost({ ...post, collection_info: { ...info, ...invalid } }),
      ).toBe(false);
  });

  it("keeps tied earliest sources in the tooltip and labels unavailable times", () => {
    expect(
      collectionInfoTitle({
        ...info,
        first_seen_from: [
          origin,
          { ...origin, id: "hn", source: "hacker_news", label: "Hacker News" },
        ],
      }),
    ).toMatch(/^First seen from r\/science and Hacker News at /);
    expect(collectionInfoTitle({ ...info, first_seen_at: null })).toContain(
      "unavailable",
    );
  });
});
