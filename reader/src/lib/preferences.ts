import { isRecord } from "./validation";

export const readingFonts = {
  "dm-sans": {
    name: "DM Sans",
    description: "Clean & familiar",
    family:
      '"DM Sans Variable", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  },
  newsreader: {
    name: "Newsreader",
    description: "An editorial touch",
    family: '"Newsreader Variable", Georgia, serif',
  },
  georgia: {
    name: "Georgia",
    description: "The original article serif",
    family: 'Georgia, "Times New Roman", serif',
  },
} as const;

export type ReadingFont = keyof typeof readingFonts;
export interface ReaderPreferences {
  font: ReadingFont;
  fontSize: number;
}

export const defaultPreferences: ReaderPreferences = {
  font: "dm-sans",
  fontSize: 19,
};
const preferencesKey = "dank-reader:preferences:v1";

export function loadPreferences(): ReaderPreferences {
  try {
    const value: unknown = JSON.parse(
      globalThis.localStorage.getItem(preferencesKey) ?? "null",
    );
    if (!isRecord(value)) return { ...defaultPreferences };
    return {
      font:
        value.font === "dm-sans" ||
        value.font === "newsreader" ||
        value.font === "georgia"
          ? value.font
          : defaultPreferences.font,
      fontSize:
        typeof value.fontSize === "number" &&
        Number.isInteger(value.fontSize) &&
        value.fontSize >= 16 &&
        value.fontSize <= 26
          ? value.fontSize
          : defaultPreferences.fontSize,
    };
  } catch {
    return { ...defaultPreferences };
  }
}

export function savePreferences(preferences: ReaderPreferences): boolean {
  try {
    globalThis.localStorage.setItem(
      preferencesKey,
      JSON.stringify(preferences),
    );
    return true;
  } catch {
    return false;
  }
}
