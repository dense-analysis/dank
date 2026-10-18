export function dateLabel(date: string) {
  const value = new Date(date);

  return Number.isNaN(value.getTime())
    ? "Date unavailable"
    : new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
      }).format(value);
}

// Social and article dates switch at local midnight, matching the reader's dates.
export function postDateLabel(date: string, now = new Date()) {
  const value = new Date(date);
  const elapsed = now.getTime() - value.getTime();

  if (
    !Number.isFinite(elapsed) ||
    elapsed < 0 ||
    value.toDateString() !== now.toDateString()
  )
    return dateLabel(date);

  const minutes = Math.floor(elapsed / 60_000);

  if (minutes === 0) return "Just now";

  return new Intl.RelativeTimeFormat(undefined, { numeric: "always" }).format(
    minutes < 60 ? -minutes : -Math.floor(minutes / 60),
    minutes < 60 ? "minute" : "hour",
  );
}
