import { sanitizeArticle } from "./article";
import type { HackerNewsFilter } from "./types";

export const hackerNewsFilters: { value: HackerNewsFilter; label: string }[] = [
  { value: "top10", label: "Top 10" },
  { value: "top20", label: "Top 20" },
  { value: "top50", label: "Top 50%" },
  { value: "all", label: "All" },
];

export function parseHackerNewsFilter(value: string | null): HackerNewsFilter {
  return (
    hackerNewsFilters.find((filter) => filter.value === value)?.value ?? "top50"
  );
}

export function hackerNewsCaptureReason(reason: string | null): string {
  const retainedNotice = "Comments retained from an earlier detailed scrape.";
  const retained = reason?.startsWith(retainedNotice) ?? false;
  const detail = (
    retained ? reason?.slice(retainedNotice.length) : reason
  )?.trim();
  let message: string;

  switch (detail) {
    case "comment_limit":
    case "Comment limit reached.":
      message = "The configured comment limit was reached.";
      break;
    case "timeout":
      message =
        "Comment collection timed out before the discussion was complete.";
      break;
    case undefined:
    case "":
      message = retained ? "" : "Some comments or replies were not captured.";
      break;
    default:
      message = "Comment collection could not complete.";
  }

  return [retained ? retainedNotice : "", message].filter(Boolean).join(" ");
}

export function hackerNewsExcerpt(
  text: string,
  externalUrl: string | null,
  html = "",
): string {
  // Feed excerpts can truncate URLs; the full saved body identifies link-only posts.
  if (html.trim() && !hasHackerNewsBody(html, externalUrl)) return "";

  return isOriginalLink(text, externalUrl) ? "" : text;
}

export function hasHackerNewsBody(
  html: string,
  externalUrl: string | null,
): boolean {
  const content = sanitizeArticle(html);
  const text = content.textContent?.trim() ?? "";

  return Boolean(
    (text && !isOriginalLink(text, externalUrl)) ||
      content.querySelector("img[src], video[src], audio[src], source[src]"),
  );
}

function isOriginalLink(text: string, externalUrl: string | null): boolean {
  if (!externalUrl || !/^https?:\/\/\S+$/.test(text.trim())) return false;

  try {
    return new URL(text.trim()).href === new URL(externalUrl).href;
  } catch {
    return false;
  }
}
