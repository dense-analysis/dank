import { expect, it } from "vitest";
import { xTextParagraphs } from "./xText";

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
