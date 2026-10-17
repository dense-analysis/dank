import type { XTextSegment } from "./xText";

// Local reading-density heuristics, independent of X's publishing limits.
export const X_COLLAPSED_CHARACTERS = 300;
export const X_EXPANDED_CHARACTERS = 1200;
const LONG_POST_WORDS = 500;
const CHARACTERS_PER_WORD = 6;
const WORDS_PER_MINUTE = 230; // Matches the reader's article estimate.

function paragraphText(paragraphs: XTextSegment[][]) {
  return paragraphs
    .map((segments) => segments.map(({ text }) => text).join(""))
    .join("\n\n");
}

export function xPostLength(paragraphs: XTextSegment[][]) {
  const text = paragraphText(paragraphs).trim();
  const words = text.match(/\S+/gu)?.length ?? 0;
  // A character fallback also bounds posts without spaces or with very long tokens.
  const readingWords = Math.max(words, text.length / CHARACTERS_PER_WORD);

  return {
    characters: text.length,
    articleLength: readingWords >= LONG_POST_WORDS,
    minutes: Math.max(1, Math.ceil(readingWords / WORDS_PER_MINUTE)),
  };
}

/** Slice sanitized text segments while keeping paragraphs, links and stable keys. */
export function truncateXText(
  paragraphs: XTextSegment[][],
  limit: number,
): XTextSegment[][] {
  const raw = paragraphText(paragraphs);
  const text = raw.trim();
  if (text.length <= limit) return paragraphs;
  if (limit <= 0) return [];

  let end = 0;
  const graphemes = new Intl.Segmenter(undefined, { granularity: "grapheme" });

  for (const { segment, index } of graphemes.segment(text)) {
    if (index + segment.length > limit) break;

    end = index + segment.length;
  }

  // Prefer a nearby word boundary without discarding most of a short preview.
  const prefix = text.slice(0, end);
  const boundary = prefix.search(/\s+\S*$/u);
  const insideWord =
    /\S/u.test(text[end - 1] ?? "") && /\S/u.test(text[end] ?? "");
  if (insideWord && boundary >= end * 0.75) end = boundary;

  const start = raw.length - raw.trimStart().length;
  end = start + text.slice(0, end).trimEnd().length;
  let offset = 0;
  const result: XTextSegment[][] = [];

  for (const paragraph of paragraphs) {
    const segments: XTextSegment[] = [];

    for (const segment of paragraph) {
      if (offset >= end) break;

      const from = Math.max(0, start - offset);
      const to = Math.min(segment.text.length, end - offset);

      if (to > from)
        segments.push({
          ...segment,
          start: segment.start + from,
          text: segment.text.slice(from, to),
        });

      offset += segment.text.length;
    }

    if (segments.some((segment) => segment.text.trim())) result.push(segments);
    if (offset >= end) break;

    offset += 2; // Paragraph separator in paragraphText.
  }

  const lastParagraph = result.at(-1);
  const last = lastParagraph?.at(-1);

  if (last && lastParagraph) {
    if (last.href)
      lastParagraph.push({ text: "…", start: last.start + last.text.length });
    else last.text += "…";
  }

  return result;
}
