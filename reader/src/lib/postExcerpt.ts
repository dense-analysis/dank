import { sanitizeArticle } from "./article";
import { normalizedLinkUrl } from "./linkIdentity";
import type { Post } from "./types";

export function savedPublisherExcerpt(post: Post, excerpt: string): string {
  const publisher = post.linked_post?.excerpt;
  if (!publisher) return excerpt;

  // Inspect the full saved body: a card's excerpt may truncate a bare URL.
  const text = post.html.trim()
    ? (sanitizeArticle(post.html).textContent?.trim() ?? "")
    : post.excerpt.trim();

  return !text || (!/\s/.test(text) && normalizedLinkUrl(text))
    ? publisher
    : excerpt;
}
