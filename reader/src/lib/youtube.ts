const VIDEO_ID = /^[A-Za-z0-9_-]{11}$/;

/** Build a trusted player URL; never embed a collected URL directly. */
export function youtubeEmbedUrl(value: string): string | undefined {
  let url: URL;

  try {
    url = new URL(value);
  } catch {
    return undefined;
  }

  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.port
  )
    return undefined;

  const host = url.hostname.replace(/^www\./, "");
  const path = url.pathname.split("/").filter(Boolean);
  let id: string | null | undefined;

  if (host === "youtu.be" && path.length === 1) {
    id = path[0];
  } else if (
    ["youtube.com", "m.youtube.com", "youtube-nocookie.com"].includes(host)
  ) {
    if (url.pathname === "/watch") {
      id = url.searchParams.get("v");
    } else if (
      path.length === 2 &&
      ["embed", "shorts", "live"].includes(path[0])
    ) {
      id = path[1];
    }
  }

  if (!id || !VIDEO_ID.test(id)) return undefined;

  const embed = new URL(`https://www.youtube-nocookie.com/embed/${id}`);
  embed.searchParams.set("autoplay", "1");
  embed.searchParams.set("playsinline", "1");
  const start = startSeconds(
    url.searchParams.get("start") ??
      url.searchParams.get("t") ??
      new URLSearchParams(url.hash.slice(1)).get("t"),
  );

  if (start) embed.searchParams.set("start", String(start));

  return embed.href;
}

function startSeconds(value: string | null): number | undefined {
  if (!value) return undefined;

  const units = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/.exec(value);
  const seconds = /^\d+$/.test(value)
    ? Number(value)
    : units
      ? Number(units[1] ?? 0) * 3600 +
        Number(units[2] ?? 0) * 60 +
        Number(units[3] ?? 0)
      : NaN;

  return Number.isSafeInteger(seconds) && seconds > 0 ? seconds : undefined;
}
