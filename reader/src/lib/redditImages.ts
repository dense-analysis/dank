import { prepareRedditLinkVisibility } from "./redditLinkVisibility";
import type { RedditImage } from "./types";

const rawImageUrls =
  /(?:https?:\/\/|(?:(?:i|preview|external-preview)\.redd\.it|(?:(?:www|old)\.)?reddit\.com)\/|\/assets\/)[^\s<>"']+/gi;

export function redditImageKey(value: string): string | undefined {
  try {
    const url = new URL(
      /^(?:(?:i|preview|external-preview)\.redd\.it|(?:(?:www|old)\.)?reddit\.com)\//i.test(
        value,
      )
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

    if (!url.port && ["i.redd.it", "preview.redd.it"].includes(url.hostname))
      return `reddit-image:${url.pathname}`;

    const gallery = /^\/gallery\/([a-z0-9]+)\/?$/i.exec(url.pathname);

    if (
      !url.port &&
      ["reddit.com", "www.reddit.com", "old.reddit.com"].includes(
        url.hostname,
      ) &&
      gallery
    )
      return `reddit-gallery:${gallery[1].toLowerCase()}`;

    return url.href;
  } catch {
    return undefined;
  }
}

export function displayedRedditImages(
  images: RedditImage[],
  shown: Set<string>,
  postId?: string,
): Set<string> {
  const displayed = new Set<string>();

  for (const image of images) {
    const key = redditImageKey(image.url);

    if (!key || !shown.has(key)) continue;

    displayed.add(key);
    const source = redditImageKey(image.source_url);

    if (source) displayed.add(source);
  }

  const id = postId?.replace(/^t3_/, "");

  if (displayed.size && id && /^[a-z0-9]+$/i.test(id))
    displayed.add(`reddit-gallery:${id.toLowerCase()}`);

  return displayed;
}

export function withoutRedditImageUrls(
  text: string,
  displayed: Set<string>,
): string {
  if (!displayed.size) return text;

  return text
    .replace(rawImageUrls, (token) => {
      const url = token.replace(/[.,!?;:)\]]+$/, "");

      return displayed.has(redditImageKey(url) ?? "")
        ? token.slice(url.length)
        : token;
    })
    .trim();
}

// Synchronize presentation on imported HTML without replacing images or their controls.
export function enhanceRedditImageLinks(
  element: HTMLElement,
  images: RedditImage[],
  postId?: string,
) {
  const possible = displayedRedditImages(
    images,
    new Set(images.map((image) => redditImageKey(image.url) ?? "")),
    postId,
  );
  const visibility = prepareRedditLinkVisibility(
    element,
    possible,
    redditImageKey,
    withoutRedditImageUrls,
  );
  let hidden = true;
  const failed = new Set<HTMLImageElement>();
  const elements = [...element.querySelectorAll("img")];
  const refresh = () => {
    const shown = new Set(
      elements.flatMap((image) => {
        const key = redditImageKey(image.src);
        const href = image.closest("a")?.getAttribute("href");
        const links = displayedRedditImages(
          images,
          new Set(key ? [key] : []),
          postId,
        );

        return key &&
          href &&
          (redditImageKey(href) === key ||
            links.has(redditImageKey(href) ?? "")) &&
          !failed.has(image) &&
          !(image.complete && !image.naturalWidth)
          ? [key]
          : [];
      }),
    );
    const complete =
      images.length > 0 &&
      images.every((image) => shown.has(redditImageKey(image.url) ?? ""));
    const displayed = displayedRedditImages(
      images,
      shown,
      complete ? postId : undefined,
    );

    visibility.update(displayed, hidden);
  };
  const update = (event: Event) => {
    const image = event.currentTarget as HTMLImageElement;

    if (event.type === "error") failed.add(image);
    else failed.delete(image);

    refresh();
  };

  for (const image of elements) {
    image.addEventListener("load", update);
    image.addEventListener("error", update);
  }

  refresh();

  return {
    setHidden(value: boolean) {
      hidden = value;
      refresh();
    },
    dispose() {
      for (const image of elements) {
        image.removeEventListener("load", update);
        image.removeEventListener("error", update);
      }

      visibility.dispose();
    },
  };
}
