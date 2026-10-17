import { expect, it } from "vitest";
import {
  previewReferenceKey,
  videoReferenceKey,
  withoutDisplayedLinks,
  xTextParagraphs,
} from "./xText";

it("keeps source links separate from navigable text and preserves paragraph breaks", () => {
  const paragraphs = xTextParagraphs(
    '<p>Read <a href="https://example.com">the study</a> today.<br>Next line.</p><p>Second paragraph.</p>',
  );
  expect(
    paragraphs.map((segments) =>
      segments.map(({ text, href }) => ({ text, href })),
    ),
  ).toEqual([
    [
      { text: "Read ", href: undefined },
      { text: "the study", href: "https://example.com/" },
      { text: " today.\nNext line.", href: undefined },
    ],
    [{ text: "Second paragraph.", href: undefined }],
  ]);
  const starts = paragraphs.flatMap((segments) =>
    segments.map((segment) => segment.start),
  );
  expect(new Set(starts).size).toBe(starts.length);
});

it("retains literal text while removing unsafe markup and unsafe source links", () => {
  const paragraphs = xTextParagraphs(
    '<p>&lt;script&gt; &amp; math <a href="javascript:attack()">unsafe</a></p><script>attack()</script><iframe src="https://example.com"></iframe>',
  );
  expect(paragraphs).toHaveLength(1);
  expect(paragraphs[0].map((segment) => segment.text).join("")).toBe(
    "<script> & math unsafe",
  );
  expect(paragraphs.flat().every((segment) => !segment.href)).toBe(true);
});

it("hides only a paired raw video reference and preserves surrounding text and other destinations", () => {
  const video = "https://x.com/author/status/123/video/1";
  const other = "https://x.com/author/status/999/video/1";
  const html = `<p>Some text.<br><a href="${video}">${video}</a></p><p><a href="${video}">Watch the explanation</a></p><p><a href="${other}">${other}</a></p><p><a href="https://youtu.be/example">YouTube</a></p>`;
  const visible = withoutDisplayedLinks(
    html,
    new Set([videoReferenceKey(video) ?? ""]),
  );
  const target = document.createElement("div");
  target.innerHTML = visible;
  expect(target.querySelectorAll(`a[href="${video}"]`)).toHaveLength(1);
  expect(target.querySelector(`a[href="${video}"]`)?.textContent).toBe(
    "Watch the explanation",
  );
  expect(target.querySelector("p")?.innerHTML).toBe("Some text.");
  expect(target.querySelector(`a[href="${other}"]`)).not.toBeNull();
  expect(
    target.querySelector('a[href="https://youtu.be/example"]'),
  ).not.toBeNull();
  expect(withoutDisplayedLinks(html, new Set())).toBe(html);
});

it("matches equivalent X video aliases without matching arbitrary links or empty paragraphs", () => {
  const key = videoReferenceKey("https://x.com/author/status/123/video/1");
  expect(
    videoReferenceKey("https://www.twitter.com/author/status/123/video/1?s=20"),
  ).toBe(key);
  expect(
    videoReferenceKey("https://example.com/author/status/123/video/1"),
  ).toBeUndefined();
  expect(
    videoReferenceKey("https://mobile.twitter.com/author/status/123/video/1"),
  ).toBe(key);
  expect(videoReferenceKey("javascript:alert(1)")).toBeUndefined();
  expect(
    withoutDisplayedLinks(
      '<p><a href="https://x.com/author/status/123/video/1">x.com/author/status/123/video/1</a></p>',
      new Set([key ?? ""]),
    ),
  ).toBe("");
});

it.each([
  "https://youtu.be/math-video",
  "https://www.youtube.com/watch?v=math-video&t=30",
  "https://example.com/research?topic=math#proof",
])("hides only raw links represented by a clickable preview: %s", (url) => {
  const otherUrl = new URL(url);
  otherUrl.searchParams.set("other", "1");
  const other = otherUrl.href;
  const html = `<p>Commentary.</p><p><a href="${url}">${url}</a></p><p><a href="${url}">${url.replace(/^https?:\/\//, "")}</a></p><p>See <a href="${url}">the full explanation</a>.</p><p><a href="${other}">${other}</a></p>`;
  const body = document.createElement("div");
  body.innerHTML = withoutDisplayedLinks(
    html,
    new Set(),
    new Set([previewReferenceKey(url) ?? ""]),
  );
  const anchors = [...body.querySelectorAll("a")];
  const represented = anchors.filter(
    (anchor) => anchor.getAttribute("href") === url,
  );
  expect(represented).toHaveLength(1);
  expect(represented[0].textContent).toBe("the full explanation");
  expect(anchors.some((anchor) => anchor.getAttribute("href") === other)).toBe(
    true,
  );
  expect(body.querySelectorAll("p")).toHaveLength(3);
  expect(withoutDisplayedLinks(html, new Set(), new Set())).toBe(html);
});

it("keeps preview destinations distinct and rejects unsafe URLs", () => {
  expect(
    previewReferenceKey("https://youtube.com/watch?v=one&t=30#part"),
  ).not.toBe(previewReferenceKey("https://youtube.com/watch?v=one&t=60#part"));
  expect(previewReferenceKey("https://example.com/#one")).not.toBe(
    previewReferenceKey("https://example.com/#two"),
  );
  expect(previewReferenceKey("javascript:alert(1)")).toBeUndefined();
  expect(
    previewReferenceKey("https://user:secret@example.com"),
  ).toBeUndefined();
});
