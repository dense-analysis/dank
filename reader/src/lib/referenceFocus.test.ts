import { describe, expect, it } from "vitest";
import { renderArticle } from "./article";
import {
  clearReferenceParams,
  highlightLinkReferences,
  highlightVideoReferences,
  parseReferenceTarget,
  referenceParams,
  videoReferenceId,
} from "./referenceFocus";
import type { YouTubeReference } from "./youtubeCollection";

const video = "NnV_cWeoo5Q";
const reference: YouTubeReference = {
  domain: "reddit.com",
  post_id: "t3_story",
  kind: "comment",
  reference_id: "t1_target",
  matched_url: `https://youtu.be/${video}?t=1468`,
  observed_at: "2026-10-17T12:00:00Z",
  url: "https://reddit.com/r/example/comments/story",
  title: "Discussion",
  source: "reddit",
  reader_url: "/reader/",
  reference_url: "https://reddit.com/comments/story/comment/target/",
  author: "Reader",
  excerpt: "Seen it happen in real time",
};

describe("video reference navigation", () => {
  it("round trips exact reference identity while preserving collection context", () => {
    const origin = new URLSearchParams({
      view: "youtube",
      feed: "linked",
      youtube_item: `video:${video}`,
      q: "security",
    });
    const params = referenceParams(origin, reference);
    expect(parseReferenceTarget(params)).toEqual({
      kind: "comment",
      id: "t1_target",
      videoId: video,
      url: reference.matched_url,
      saved: reference.observed_at,
    });
    expect(params.get("article_source")).toBe("reddit.com");
    expect(params.get("article")).toBe("t3_story");
    params.delete("article");
    params.delete("article_source");
    clearReferenceParams(params);
    expect(params.toString()).toBe(origin.toString());
    expect(origin.has("article")).toBe(false);
  });

  it.each([
    `https://youtube.com.evil.example/watch?v=${video}`,
    `https://youtube.com@evil.example/watch?v=${video}`,
    `javascript:alert('${video}')`,
    `https://example.com/?url=https://youtube.com/watch?v=${video}`,
    `https://youtube.com/watch?v=${video}x`,
  ])("rejects lookalike video destinations: %s", (url) => {
    expect(videoReferenceId(url)).toBeUndefined();
  });

  it("requires article and comment identities and a valid video ID", () => {
    const params = referenceParams(new URLSearchParams(), reference);
    params.delete("reference_id");
    expect(parseReferenceTarget(params)).toBeUndefined();
    params.set("reference_id", "t1_target");
    params.set("reference_video", "javascript:alert(1)");
    expect(parseReferenceTarget(params)).toBeUndefined();
  });

  it("highlights exact destinations and descriptive labels, not wrong video labels", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      `<p><a href="https://youtu.be/${video}?t=1468">seen <em>it happen</em> in real time</a> <a href="https://youtube.com/watch?v=aaaaaaaaaaa">${video}</a> https://youtube.com/watch?v=${video}. <a href="javascript:alert(1)">unsafe</a></p>`,
    );
    const anchor = body.querySelector("a");
    highlightVideoReferences(body, video);
    expect(body.querySelectorAll('[data-reference-match="true"]')).toHaveLength(
      2,
    );
    expect(anchor?.classList.contains("reference-match")).toBe(true);
    expect(anchor?.querySelector("em")?.textContent).toBe("it happen");
    expect(
      body.querySelectorAll("a")[1].classList.contains("reference-match"),
    ).toBe(false);
    expect(body.querySelector("mark")?.textContent).toBe(
      `https://youtube.com/watch?v=${video}`,
    );
    expect(body.querySelector('[href^="javascript:"]')).toBeNull();
  });

  it("scopes captured RSS comments by source identity without retaining source IDs or spoofed markers", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      `<div data-reader-comment-target="true" data-reference-match="true"><a href="https://youtu.be/${video}">Wrong comment</a></div><div id="comment-target"><a href="https://youtu.be/${video}">Right comment</a><script>evil()</script></div>`,
      "",
      "comment-target",
    );
    const scope = body.querySelector<HTMLElement>(
      '[data-reader-comment-target="true"]',
    );
    expect(scope).not.toBeNull();
    highlightVideoReferences(scope as HTMLElement, video);
    expect(body.querySelectorAll('[data-reference-match="true"]')).toHaveLength(
      1,
    );
    expect(
      body.querySelector('[data-reference-match="true"]')?.textContent,
    ).toBe("Right comment");
    expect(body.querySelector("[id],script")).toBeNull();
  });
});

it("does not include a nested RSS reply in its parent's reference", () => {
  const body = document.createElement("div");
  renderArticle(
    body,
    `<div id="comment-1">Parent has no link<div id="comment-2"><a href="https://youtu.be/${video}">Nested reply</a></div></div>`,
    "",
    "comment-1",
  );
  const scope = body.querySelector<HTMLElement>(
    '[data-reader-comment-target="true"]',
  );
  highlightVideoReferences(scope as HTMLElement, video);
  expect(body.querySelector('[data-reference-match="true"]')).toBeNull();
});

it("does not satisfy a post link with a hidden link, source comment, or recommendation", () => {
  const body = document.createElement("div");
  const link = `<a href="https://youtu.be/${video}">A source link</a>`;
  renderArticle(
    body,
    `<p hidden>${link}</p><div id="comment-1">${link}</div><div class="recommended-items">${link}</div><p>${link}</p>`,
    "",
    undefined,
    true,
  );
  highlightVideoReferences(body, video);
  expect(body.querySelectorAll('[data-reference-match="true"]')).toHaveLength(
    1,
  );
  expect(
    body.lastElementChild?.querySelector('[data-reference-match="true"]'),
  ).not.toBeNull();
});

describe("article reference navigation", () => {
  const url = "https://publisher.test/article?edition=2#security";

  it("round trips the matched destination separately from its source permalink", () => {
    const params = referenceParams(new URLSearchParams("connections=graph"), {
      ...reference,
      matched_url: url,
    });
    expect(params.get("reference_url")).toBe(url);
    expect(params.has("reference_video")).toBe(false);
    expect(params.get("connections")).toBe("graph");
    expect(parseReferenceTarget(params)).toMatchObject({
      url,
      videoId: "",
      id: "t1_target",
    });
  });

  it("retains old video-only deep links and rejects conflicting destinations", () => {
    const params = referenceParams(new URLSearchParams(), reference);
    params.delete("reference_url");
    expect(parseReferenceTarget(params)?.url).toBe(
      `https://www.youtube.com/watch?v=${video}`,
    );
    params.set("reference_url", url);
    expect(parseReferenceTarget(params)).toBeUndefined();
  });

  it.each([
    "javascript:alert(1)",
    "/reader/",
    "https://user:password@example.com/post",
  ])("rejects malicious destination parameters: %s", (input) => {
    const params = referenceParams(new URLSearchParams(), {
      ...reference,
      matched_url: url,
    });
    params.set("reference_url", input);
    expect(parseReferenceTarget(params)).toBeUndefined();
  });

  it("highlights the descriptive anchor and plain URL only in the selected body", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      `<p><a href="${url}">Read <em>the source</em></a> ${url}. <a href="https://publisher.test/article?edition=1#security">Wrong edition</a><a href="https://publisher.test/article?edition=2#other">Wrong section</a><a href="https://publisher.test.evil.example/article?edition=2#security">Wrong host</a></p>`,
    );
    highlightLinkReferences(body, url);
    expect(body.querySelectorAll('[data-reference-match="true"]')).toHaveLength(
      2,
    );
    expect(body.querySelector("a em")?.textContent).toBe("the source");
    expect(body.querySelector("mark")?.textContent).toBe(url);
  });

  it("uses the publisher base for relative anchors without trusting source match markers", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      '<div data-reference-match="true" class="reference-match">Spoofed</div><a href="../article?edition=2#security">Relative source</a>',
    );
    highlightLinkReferences(body, url, "https://publisher.test/news/story");
    expect(body.querySelectorAll('[data-reference-match="true"]')).toHaveLength(
      1,
    );
    expect(
      body.querySelector('[data-reference-match="true"]')?.textContent,
    ).toBe("Relative source");
  });
});
