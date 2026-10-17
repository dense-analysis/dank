// Navigation state for this reader tab; keep long browsing sessions bounded.
const MAX_VIDEO_POSITIONS = 500;
const positions = new Map<string, number>();

export function videoPositionKey(
  scope: string,
  kind: string,
  media: string | number,
): string {
  return JSON.stringify([scope, kind, media]);
}

export function videoPosition(key?: string): number | undefined {
  return key ? positions.get(key) : undefined;
}

export function rememberVideoPosition(
  key: string | undefined,
  time: number,
): void {
  if (!key || !Number.isFinite(time) || time < 0 || positions.get(key) === time)
    return;

  positions.delete(key);
  positions.set(key, time);
  while (positions.size > MAX_VIDEO_POSITIONS) {
    const oldest = positions.keys().next().value;
    if (oldest) positions.delete(oldest);
  }
}
