export function highlightMatches(text: string, query: string) {
  const terms = [...new Set(query.trim().split(/\s+/).filter(Boolean))].sort(
    (a, b) => b.length - a.length,
  );
  if (!terms.length) return [{ text, highlighted: false, start: 0 }];
  const pattern = new RegExp(
    terms.map((term) => term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|"),
    "giu",
  );
  const parts: { text: string; highlighted: boolean; start: number }[] = [];
  let offset = 0;
  for (const match of text.matchAll(pattern)) {
    if (match.index > offset)
      parts.push({
        text: text.slice(offset, match.index),
        highlighted: false,
        start: offset,
      });
    parts.push({ text: match[0], highlighted: true, start: match.index });
    offset = match.index + match[0].length;
  }
  if (offset < text.length)
    parts.push({ text: text.slice(offset), highlighted: false, start: offset });
  return parts;
}
