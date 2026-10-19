import { describe, expect, it } from "vitest";
import { earlierDiscoveryTitle } from "../components/CollectionInfo";
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
  post_published_at: "2026-10-19T08:30:00Z",
  discovered_before_publication: true,
  earlier_discovered_at: origin.observed_at,
  earlier_discovered_from: [origin],
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
  created_at: "2026-10-19T08:30:00Z",
  thumbnail: null,
  media: [],
  collection_info: info,
};

describe("collection information contract", () => {
  it("accepts unknown historical values and all article capture statuses", () => {
    expect(isPost(post)).toBe(true);
    expect(
      isPost({
        ...post,
        collection_info: {
          ...info,
          post_published_at: undefined,
          discovered_before_publication: undefined,
          earlier_discovered_at: undefined,
          earlier_discovered_from: undefined,
        },
      }),
    ).toBe(true);
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
      { post_published_at: "2026-10-19" },
      { earlier_discovered_at: "yesterday" },
      { discovered_before_publication: "true" },
      { earlier_discovered_from: Array(6).fill(origin) },
      { earlier_discovered_from: [{ ...origin, observed_at: "yesterday" }] },
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

  it("keeps tied independent sources in the tooltip and omits unavailable times", () => {
    expect(
      earlierDiscoveryTitle({
        ...info,
        earlier_discovered_from: [
          origin,
          { ...origin, id: "hn", source: "hacker_news", label: "Hacker News" },
        ],
      }),
    ).toMatch(/^Discovered at r\/science and Hacker News on /);
    expect(
      earlierDiscoveryTitle({ ...info, earlier_discovered_at: null }),
    ).toBeNull();
  });

  it("uses the discovery shown in the tooltip and publication, not collection time, for the gap", () => {
    expect(
      earlierDiscoveryTitle({
        ...info,
        first_seen_at: "2026-10-19T07:00:00Z",
        post_first_seen_at: "2026-10-20T09:00:00Z",
        article_collected_at: "2026-10-21T09:00:00Z",
      }),
    ).toMatch(/ · Post published 30m after first discovery$/);
  });

  it("never turns collection order or incomplete history into an earlier-discovery eye", () => {
    for (const unknown of [
      { post_published_at: null },
      { post_published_at: undefined },
      { discovered_before_publication: false },
      { discovered_before_publication: null },
      { discovered_before_publication: undefined },
      { earlier_discovered_at: undefined },
      { earlier_discovered_from: [] },
      { earlier_discovered_from: undefined },
      { earlier_discovered_at: info.post_published_at },
      { earlier_discovered_at: "2026-10-19T08:45:00Z" },
    ])
      expect(earlierDiscoveryTitle({ ...info, ...unknown })).toBeNull();
  });
});
