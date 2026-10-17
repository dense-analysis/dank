import { sanitizeArticle } from "./article";
import type { RedditVideo } from "./types";
import { safeUrl } from "./urls";

// A signed manifest, packaged MP4 and public v.redd.it link identify the same clip.
export function redditVideoKey(value: string): string | undefined {
  try {
    const url = new URL(
      /^(?:v|packaged-media)\.redd\.it\//i.test(value)
        ? `https://${value}`
        : value,
      window.location.origin,
    );
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.username ||
      url.password
    )
      return undefined;

    const id = url.pathname.split("/")[1];
    if (
      ["v.redd.it", "packaged-media.redd.it"].includes(url.hostname) &&
      /^[a-z0-9]+$/i.test(id)
    )
      return `reddit-video:${id}`;

    return url.href;
  } catch {
    return undefined;
  }
}

export function displayedRedditVideos(
  videos: RedditVideo[],
  failed: Set<string>,
): Set<string> {
  return new Set(
    videos.flatMap((video) => {
      if (
        failed.has(video.url) ||
        !safeUrl(video.remote ? video.hls_url || video.url : video.url)
      )
        return [];

      return [
        video.url,
        video.source_url,
        video.hls_url,
        video.fallback_url,
      ].flatMap((url) => {
        const key = url ? redditVideoKey(url) : undefined;

        return key ? [key] : [];
      });
    }),
  );
}

export function withoutRedditVideoUrls(
  text: string,
  displayed: Set<string>,
): string {
  if (!displayed.size) return text;

  return text
    .replace(
      /(?:https?:\/\/|(?:v|packaged-media)\.redd\.it\/|\/assets\/)[^\s<>"']+/gi,
      (token) => {
        const url = token.replace(/[.,!?;:)\]]+$/, "");

        return displayed.has(redditVideoKey(url) ?? "")
          ? token.slice(url.length)
          : token;
      },
    )
    .trim();
}

export function withoutRedditVideoLinks(
  html: string,
  displayed: Set<string>,
): string {
  if (!displayed.size) return html;

  const content = sanitizeArticle(html);
  for (const anchor of content.querySelectorAll("a[href]")) {
    const key = redditVideoKey(anchor.getAttribute("href") ?? "");
    const label = anchor.textContent?.trim() ?? "";

    // Descriptive links and image links remain useful even with a player.
    if (
      key &&
      displayed.has(key) &&
      label &&
      !anchor.querySelector("img") &&
      withoutRedditVideoUrls(label, displayed) === ""
    )
      anchor.remove();
  }

  const walker = document.createTreeWalker(content, NodeFilter.SHOW_TEXT);
  while (walker.nextNode()) {
    const node = walker.currentNode;
    if (!node.parentElement?.closest("a, code, pre")) {
      // Preserve spacing around inline markup; only strip represented URL tokens.
      const text = node.textContent ?? "";
      const trimmed = withoutRedditVideoUrls(text, displayed);
      if (trimmed !== text.trim())
        node.textContent = text.replace(text.trim(), trimmed);
    }
  }

  for (const block of content.querySelectorAll("p, div")) {
    if (!block.textContent?.trim() && !block.querySelector("img, video, audio"))
      block.remove();
  }

  const container = document.createElement("div");
  container.append(content);

  return container.innerHTML;
}
