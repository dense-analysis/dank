import { hasArticleContent } from "./article";
import { sourceKind } from "./collections";
import type { Post, XMedia, XProfile } from "./types";

export function isXPost(post: Pick<Post, "domain" | "source">): boolean {
  return post.source === "x" || sourceKind(post.domain) === "x";
}

export function xProfile(post: Post): XProfile | undefined {
  if (post.x) return post.x.handle ? post.x : undefined;

  try {
    const url = new URL(post.url);
    const handle = /^\/([a-z0-9_]{1,15})\/status\//i.exec(url.pathname)?.[1];

    if (
      sourceKind(url.hostname) === "x" &&
      handle &&
      handle.toLowerCase() !== "i"
    ) {
      return { handle, name: post.author || handle, icon_url: null };
    }
  } catch {
    return undefined;
  }

  return undefined;
}

export function xMedia(post: Post): XMedia[] {
  if (post.x?.media) return post.x.media;

  return post.media.flatMap((asset): XMedia[] => {
    const type = asset.content_type.startsWith("image/")
      ? "image"
      : asset.content_type.startsWith("video/")
        ? "video"
        : null;

    return type
      ? [
          {
            type,
            url: asset.url,
            poster_url: null,
            alt: "",
            remote: !asset.url.startsWith("/assets/"),
          },
        ]
      : [];
  });
}

export function hasXContent(post: Post): boolean {
  return (
    hasArticleContent(post.html) ||
    xMedia(post).length > 0 ||
    (post.x?.links?.length ?? 0) > 0
  );
}

export function xMediaUrl(
  value: string | null | undefined,
): string | undefined {
  if (!value) return undefined;

  try {
    const url = new URL(value, window.location.origin);
    const local =
      url.origin === window.location.origin &&
      url.pathname.startsWith("/assets/");
    const remote =
      url.protocol === "https:" &&
      ["pbs.twimg.com", "video.twimg.com"].includes(url.hostname) &&
      !url.port;

    if (!url.username && !url.password && (local || remote)) return url.href;
  } catch {
    return undefined;
  }

  return undefined;
}
