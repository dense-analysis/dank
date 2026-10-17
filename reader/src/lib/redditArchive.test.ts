import { describe, expect, it } from "vitest";
import { isPost } from "./validation";

const comment = {
  id: "one",
  parent_id: "t3_post",
  author: "reader",
  body_html: "<p>Hello</p>",
  body: "Hello",
  score: 2,
  created_utc: 1792180200,
  permalink: "https://www.reddit.com/comments/post/one",
  is_op: false,
  depth: 0,
};
const capture = {
  status: "captured",
  sort: "best",
  limit: 50,
  total_available: 100,
  has_more: true,
  comments: [comment],
};
const video = {
  url: "https://v.redd.it/video/HLSPlaylist.m3u8",
  poster_url: null,
  remote: true,
  hls_url: "https://v.redd.it/video/HLSPlaylist.m3u8",
};
const post = {
  id: "one",
  domain: "reddit.com",
  url: "https://www.reddit.com/comments/one",
  author: "reader",
  title: "A post",
  excerpt: "Text",
  html: "<p>Text</p>",
  created_at: "2026-10-16T17:45:00Z",
  source: "reddit",
  thumbnail: null,
  media: [],
  reddit: { community: "example", score: 3, comments: 100, flair: null },
};

describe("Reddit archive response validation", () => {
  it("accepts compact, historical, remote, archived and retained snapshots", () => {
    expect(isPost(post)).toBe(true);
    expect(
      isPost({
        ...post,
        reddit: { ...post.reddit, video, comment_capture: capture },
      }),
    ).toBe(true);
    expect(
      isPost({
        ...post,
        reddit: {
          ...post.reddit,
          video: { ...video, url: "/assets/video.mp4", remote: false },
          comment_capture: {
            ...capture,
            captured_at: "2026-10-16T17:45:00Z",
            latest_attempt_status: "deferred",
          },
        },
      }),
    ).toBe(true);
    expect(
      isPost({
        ...post,
        reddit: { ...post.reddit, video: null, comment_capture: null },
      }),
    ).toBe(true);
  });

  it.each([
    { ...capture, status: "unknown" },
    { ...capture, limit: -1 },
    { ...capture, has_more: "true" },
    { ...capture, total_available: -1 },
    { ...capture, latest_attempt_status: "unknown" },
    { ...capture, comments: [{ ...comment, depth: -1 }] },
    { ...capture, comments: [{ ...comment, created_utc: Infinity }] },
    { ...capture, comments: [{ ...comment, created_utc: 8640000000001 }] },
    { ...capture, comments: [{ ...comment, body_html: {} }] },
  ])(
    "rejects malformed comment snapshots without crashing the reader",
    (comment_capture) => {
      expect(
        isPost({ ...post, reddit: { ...post.reddit, comment_capture } }),
      ).toBe(false);
    },
  );

  it.each([
    { ...video, remote: "true" },
    { ...video, url: null },
    { ...video, hls_url: 42 },
    { ...video, poster_url: {} },
    { ...video, source_url: 42 },
  ])("rejects malformed video metadata", (video) => {
    expect(isPost({ ...post, reddit: { ...post.reddit, video } })).toBe(false);
  });
});
