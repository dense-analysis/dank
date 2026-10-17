import { expect, it } from "vitest";
import { truncateXText, xPostLength } from "./xPostPreview";
import { xTextParagraphs } from "./xText";

const text = (paragraphs: ReturnType<typeof xTextParagraphs>) =>
  paragraphs
    .map((parts) => parts.map((part) => part.text).join(""))
    .join("\n\n");

it("keeps paragraph structure and complete link destinations while clipping a linked label", () => {
  const source = xTextParagraphs(
    '<p>Intro.</p><p>Read <a href="https://example.com/study?q=math">the study</a> and its conclusion.</p>',
  );
  const original = structuredClone(source);
  const preview = truncateXText(source, 20);
  expect(preview).toHaveLength(2);
  expect(text(preview)).toBe("Intro.\n\nRead the…");
  expect(preview[1].find((segment) => segment.href)?.href).toBe(
    "https://example.com/study?q=math",
  );
  expect(preview[1].at(-1)?.href).toBeUndefined();
  expect(source).toEqual(original);
  const keys = preview.flat().map((segment) => segment.start);
  expect(new Set(keys).size).toBe(keys.length);
});

it("prefers a nearby complete word and never leaves a trailing empty paragraph", () => {
  const source = xTextParagraphs(
    "<p>One two three four five six.</p><p>Another paragraph.</p>",
  );
  expect(text(truncateXText(source, 20))).toBe("One two three four…");
  expect(truncateXText(source, 28)).toHaveLength(1);
  expect(text(truncateXText(source, 28))).toBe("One two three four five six.…");
});

it("preserves complete emoji and combining characters at a cut boundary", () => {
  const prefix = "a".repeat(297);
  const emoji = "👩🏽‍🔬";
  expect(
    text(truncateXText(xTextParagraphs(`<p>${prefix}${emoji}after</p>`), 300)),
  ).toBe(`${prefix}…`);
  expect(
    text(truncateXText(xTextParagraphs(`<p>${prefix}éafter</p>`), 298)),
  ).toBe(`${prefix}…`);
});

it("leaves short content unchanged and handles empty content", () => {
  const source = xTextParagraphs(
    '<p>Short <a href="https://example.com">link</a>.</p>',
  );
  expect(truncateXText(source, 300)).toBe(source);
  expect(truncateXText(source, 0)).toEqual([]);
  expect(truncateXText([], 300)).toEqual([]);
  expect(xPostLength([]).articleLength).toBe(false);
});

it("keeps sanitized source text safe through truncation", () => {
  const source = xTextParagraphs(
    '<p>&lt;script&gt; Literal text <a href="javascript:alert(1)">unsafe</a> remains readable.</p><script>secret()</script>',
  );
  const preview = truncateXText(source, 35);
  expect(text(preview)).toContain("<script> Literal text");
  expect(text(preview)).not.toContain("secret()");
  expect(preview.flat().every((segment) => !segment.href)).toBe(true);
});

it("estimates a long read even without spaces and bases length on visible text", () => {
  const long = xTextParagraphs(`<p>${"文".repeat(6000)}</p>`);
  expect(xPostLength(long)).toMatchObject({ articleLength: true, minutes: 5 });
  expect(
    xPostLength(
      xTextParagraphs(
        `<p><a href="https://example.com/${"x".repeat(4000)}">Short label</a></p>`,
      ),
    ),
  ).toMatchObject({ articleLength: false, minutes: 1 });
});

it("measures and clips the same text when outer whitespace is present", () => {
  const short = xTextParagraphs(`<p>${" ".repeat(100)}${"a".repeat(250)}</p>`);
  expect(xPostLength(short).characters).toBe(250);
  expect(truncateXText(short, 300)).toBe(short);
  const long = xTextParagraphs(
    `<p>${" ".repeat(500)}${"a".repeat(400)}   </p>`,
  );
  expect(text(truncateXText(long, 300))).toBe(`${"a".repeat(300)}…`);
});
