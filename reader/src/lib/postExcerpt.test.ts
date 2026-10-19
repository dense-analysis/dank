import { expect, it } from "vitest";
import { savedPublisherExcerpt } from "./postExcerpt";
import type { Post } from "./types";

const url = "https://publisher.test/article";
const post: Post = {
  id: "123",
  domain: "reddit.com",
  source: "reddit",
  url: "https://reddit.com/comments/123",
  title: "A linked article",
  author: "submitter",
  html: `<p><img src="https://image.test/thumbnail.png"></p><p><a href="${url}">${url}</a></p>`,
  excerpt: url,
  created_at: "2026-10-19T09:00:00Z",
  thumbnail: null,
  media: [],
  linked_post: {
    id: "article",
    domain: "publisher.test",
    source: "rss",
    url,
    title: "Publisher title",
    excerpt: "Saved publisher text.",
  },
};

it("uses saved publisher text for a bare link with a thumbnail", () => {
  expect(savedPublisherExcerpt(post, "https://publisher…")).toBe(
    "Saved publisher text.",
  );
});

it("keeps meaningful post text and does not mistake a URL followed by text for a bare link", () => {
  for (const text of ["My explanation.", `${url} My explanation.`]) {
    expect(
      savedPublisherExcerpt({ ...post, html: `<p>${text}</p>` }, text),
    ).toBe(text);
  }
});

it("keeps the current preview when no publisher text is available", () => {
  expect(savedPublisherExcerpt({ ...post, linked_post: undefined }, url)).toBe(
    url,
  );
});

it("fills an empty aggregator preview without replacing legacy excerpt text", () => {
  expect(savedPublisherExcerpt({ ...post, html: "", excerpt: "" }, "")).toBe(
    "Saved publisher text.",
  );
  expect(
    savedPublisherExcerpt(
      { ...post, html: "", excerpt: "Legacy text." },
      "Legacy text.",
    ),
  ).toBe("Legacy text.");
});
