import { isRecord } from "./validation";

export interface ReadingPosition {
  scroll: number;
  block: number | null;
  offset: number;
}

const storageKey = "dank-reader:positions:v1";
const maxEntries = 200;

function positions(): Map<string, ReadingPosition> {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(storageKey) ?? "[]");
    if (!Array.isArray(value)) return new Map();
    const valid = value.filter((entry): entry is [string, ReadingPosition] => {
      if (
        !Array.isArray(entry) ||
        typeof entry[0] !== "string" ||
        !isRecord(entry[1])
      )
        return false;
      const position = entry[1];
      return (
        typeof position.scroll === "number" &&
        Number.isFinite(position.scroll) &&
        position.scroll >= 0 &&
        position.scroll <= 10_000_000 &&
        typeof position.offset === "number" &&
        Number.isFinite(position.offset) &&
        Math.abs(position.offset) <= 10_000_000 &&
        (position.block === null ||
          (typeof position.block === "number" &&
            Number.isInteger(position.block) &&
            position.block >= 0 &&
            position.block < 100_000))
      );
    });
    return new Map(valid.slice(-maxEntries));
  } catch {
    return new Map();
  }
}

export function loadReadingPosition(key: string): ReadingPosition | undefined {
  return positions().get(key);
}

export function saveReadingPosition(
  key: string,
  position: ReadingPosition,
): boolean {
  try {
    const stored = positions();
    stored.delete(key);
    stored.set(key, position);
    localStorage.setItem(
      storageKey,
      JSON.stringify([...stored].slice(-maxEntries)),
    );
    return true;
  } catch {
    return false;
  }
}

export function captureReadingPosition(body: Element): ReadingPosition {
  const edge =
    document.querySelector(".reader-toolbar")?.getBoundingClientRect().height ??
    0;
  const block =
    window.scrollY > 0 && body.getBoundingClientRect().top <= edge
      ? [...body.children].findIndex(
          (element) => element.getBoundingClientRect().bottom > edge,
        )
      : -1;
  return {
    scroll: window.scrollY,
    block: block >= 0 ? block : null,
    offset:
      block >= 0 ? edge - body.children[block].getBoundingClientRect().top : 0,
  };
}

export function readingPositionTop(
  body: Element,
  position: ReadingPosition,
): number {
  const anchor = position.block === null ? null : body.children[position.block];
  const edge =
    document.querySelector(".reader-toolbar")?.getBoundingClientRect().height ??
    0;
  return anchor
    ? Math.max(
        0,
        window.scrollY +
          anchor.getBoundingClientRect().top +
          position.offset -
          edge,
      )
    : position.scroll;
}
