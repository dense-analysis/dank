/** Browser counterpart of dank.links.normalized_url; queries retain publisher meaning. */
export function normalizedLinkUrl(
  value: string,
  base?: string,
): string | undefined {
  if (!value || /[\s\\]/.test(value)) return undefined;

  let url: URL;

  try {
    url = base ? new URL(value, base) : new URL(value);
  } catch {
    return undefined;
  }

  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password
  )
    return undefined;

  // URL validates the authority, but rewrites dot segments and Unicode paths.
  // Preserve absolute publisher paths exactly as the backend identity does.
  const absolute = /^[a-z][a-z0-9+.-]*:\/\//i.test(value) ? value : url.href;
  const raw = /^https?:\/\/([^/?#]*)([^?#]*)(\?[^#]*)?/i.exec(absolute);

  if (!raw) return undefined;

  const host = raw[1].replace(/:\d*$/, "").toLowerCase();
  const path = raw[2] || "/";
  const query = raw[3] && raw[3] !== "?" ? raw[3] : "";

  if (!url.port) {
    if (
      ["x.com", "www.x.com", "twitter.com", "www.twitter.com"].includes(host)
    ) {
      const post = /^\/[^/]+\/status\/(\d+)\/?$/.exec(path);

      if (post) return `https://x.com/i/status/${post[1]}`;
    }

    if (
      [
        "reddit.com",
        "www.reddit.com",
        "old.reddit.com",
        "new.reddit.com",
        "m.reddit.com",
      ].includes(host)
    ) {
      const post =
        /^\/(?:r\/[^/]+\/)?comments\/([a-zA-Z0-9]+)(?:\/[^/]+)?\/?$/.exec(path);

      if (post)
        return `https://www.reddit.com/comments/${post[1].toLowerCase()}`;
    }

    const parts = path.replace(/^\/+|\/+$/g, "").split("/");
    let video: string | undefined;

    if (["youtu.be", "www.youtu.be"].includes(host) && parts.length === 1) {
      video = parts[0];
    } else if (
      [
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "youtube-nocookie.com",
        "www.youtube-nocookie.com",
      ].includes(host)
    ) {
      if (parts.length === 1 && parts[0] === "watch") {
        const values = url.searchParams.getAll("v").filter(Boolean);
        video = values.length === 1 ? values[0] : undefined;
      } else if (parts.length === 2 && ["shorts", "embed"].includes(parts[0])) {
        video = parts[1];
      }
    }

    if (video && /^[A-Za-z0-9_-]{11}$/.test(video))
      return `https://www.youtube.com/watch?v=${video}`;
  }

  return `${url.protocol}//${host}${url.port ? `:${url.port}` : ""}${path}${query}`;
}

export function matchesLinkUrl(
  value: string,
  target: string,
  base?: string,
): boolean {
  const identity = normalizedLinkUrl(target);

  if (!identity || normalizedLinkUrl(value, base) !== identity) return false;

  // Identity groups fragments together; focus still honours the exact cited section.
  const fragment = new URL(target).hash;
  const platform =
    /^https:\/\/(?:www\.youtube\.com\/watch\?|x\.com\/i\/status\/|www\.reddit\.com\/comments\/)/.test(
      identity,
    );

  return platform || !fragment || new URL(value, base).hash === fragment;
}
