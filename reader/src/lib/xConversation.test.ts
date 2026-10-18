import { describe, expect, it } from "vitest";
import { isPost } from "./validation";

const post = {
  id: "1",
  domain: "x.com",
  url: "https://x.com/account/status/1",
  author: "Account",
  title: "Post",
  excerpt: "Post",
  html: "<p>Post</p>",
  created_at: "2026-10-17T12:00:00Z",
  source: "x",
  thumbnail: null,
  media: [],
  x: { handle: "account", name: "Account", icon_url: null },
};
const reply = {
  ...post,
  id: "2",
  parent_id: "1",
  depth: 0,
  is_author: false,
  missing_parent: false,
};
const conversation = {
  status: "partial",
  captured_at: "2026-10-17T13:00:00Z",
  has_more: true,
  reason: "limit",
  reported_replies: 24,
  limit: 50,
  context: [],
  replies: [reply],
};

describe("X conversation response boundary", () => {
  it("accepts saved replies and retained archives independently of reported totals", () => {
    expect(
      isPost({
        ...post,
        x: {
          ...post.x,
          replies_status: "partial",
          replies_captured_count: 1,
          conversation,
        },
      }),
    ).toBe(true);
    expect(
      isPost({
        ...post,
        x: {
          ...post.x,
          conversation: {
            ...conversation,
            latest_attempt_status: "failed",
            latest_attempt_at: "2026-10-17T14:00:00Z",
          },
        },
      }),
    ).toBe(true);
  });

  it.each(["captured", "partial", "failed", "disabled", "not_collected"])(
    "accepts the %s collection state",
    (status) => {
      expect(
        isPost({
          ...post,
          x: {
            ...post.x,
            replies_status: status,
            conversation: { ...conversation, status },
          },
        }),
      ).toBe(true);
    },
  );

  it("rejects recursive conversations and malformed nodes before rendering", () => {
    for (const bad of [
      { depth: -1 },
      { depth: 21 },
      { parent_id: 1 },
      { is_author: "false" },
      { missing_parent: null },
      { x: { ...reply.x, conversation } },
      { source: "reddit" },
    ]) {
      expect(
        isPost({
          ...post,
          x: {
            ...post.x,
            conversation: { ...conversation, replies: [{ ...reply, ...bad }] },
          },
        }),
      ).toBe(false);
    }
  });

  it("rejects invalid capture status, counts and unbounded arrays", () => {
    for (const bad of [
      { status: "live" },
      { reported_replies: -1 },
      { has_more: "true" },
      { limit: -1 },
      { replies: Array.from({ length: 501 }, () => reply) },
      { context: Array.from({ length: 101 }, () => reply) },
    ]) {
      expect(
        isPost({
          ...post,
          x: { ...post.x, conversation: { ...conversation, ...bad } },
        }),
      ).toBe(false);
    }
  });
});

it("validates quoted identities and rejects recursive quotes and discussions", () => {
  const quote = {
    id: "2",
    domain: "x.com",
    url: "https://x.com/quoted/status/2",
    collected: true,
    post: { ...post, id: "2" },
  };
  const parent = (value: unknown) => ({
    ...post,
    x: { ...post.x, quote: value },
  });
  expect(isPost(parent(quote))).toBe(true);
  expect(isPost(parent({ ...quote, post: undefined, collected: false }))).toBe(
    true,
  );
  for (const bad of [
    { id: "bad" },
    { domain: "example.com" },
    { collected: "yes" },
    { post },
    { post: { ...quote.post, domain: "twitter.com" } },
    { post: { ...quote.post, x: { ...post.x, quote } } },
    { post: { ...quote.post, x: { ...post.x, conversation } } },
  ])
    expect(isPost(parent({ ...quote, ...bad }))).toBe(false);
});
