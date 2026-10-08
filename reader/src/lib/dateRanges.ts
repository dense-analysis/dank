export function recentDates(days: 1 | 7 | 30, now = new Date()) {
  // Date filters use inclusive UTC calendar days in the reader API.
  const end = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()),
  );
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - days + 1);
  return {
    after: start.toISOString().slice(0, 10),
    before: end.toISOString().slice(0, 10),
  };
}

export function calendarDate(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(new Date(`${value}T00:00:00Z`));
}
