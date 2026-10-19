import type { CollectionInfo } from "./types";

/** Compare publication with observed discovery; collection dates are not inputs. */
export function discoveryTiming(
  info: Pick<CollectionInfo, "post_published_at" | "first_seen_at">,
) {
  const publishedAt =
    info.post_published_at &&
    Number.isFinite(Date.parse(info.post_published_at))
      ? info.post_published_at
      : null;
  const discoveredAt =
    info.first_seen_at && Number.isFinite(Date.parse(info.first_seen_at))
      ? info.first_seen_at
      : null;
  const gapMs =
    publishedAt && discoveredAt
      ? Date.parse(publishedAt) - Date.parse(discoveredAt)
      : null;
  let gapLabel = "Gap unavailable";

  if (gapMs === 0) {
    gapLabel = "Same time as first discovery";
  } else if (gapMs !== null) {
    const minutes = Math.floor(Math.abs(gapMs) / 60_000);
    // Show up to two nonzero units, rounding down to minutes. Preserve the
    // direction of sub-minute gaps instead of calling them simultaneous.
    const units = [
      [Math.floor(minutes / 1440), "d"],
      [Math.floor((minutes % 1440) / 60), "h"],
      [minutes % 60, "m"],
    ] as const;
    const duration =
      minutes < 1
        ? "<1m"
        : units
            .filter(([value]) => value > 0)
            .slice(0, 2)
            .map(([value, unit]) => `${value}${unit}`)
            .join(" ");
    gapLabel = `${duration} ${gapMs > 0 ? "after" : "before"} first discovery`;
  }

  return { publishedAt, discoveredAt, gapMs, gapLabel };
}
