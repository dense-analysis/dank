import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  ReaderPreferencesProvider,
  useReaderPreferences,
} from "../hooks/useReaderPreferences";
import { ArticleBody } from "./ArticleBody";

// Pixel classification is covered in the browser; these tests exercise portal lifetime.
vi.mock("../lib/imageAnalysis", () => ({
  analyzeImage: () => ({ transparent: true, preferLight: true }),
}));

let container: HTMLDivElement;
let root: Root;
const writeText = vi.fn<(text: string) => Promise<void>>();

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  vi.stubGlobal("localStorage", {
    getItem: () => JSON.stringify({ autoImageBackground: true }),
  });
  writeText.mockReset().mockResolvedValue(undefined);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

async function render(html: string) {
  await act(() => {
    root.render(
      <StrictMode>
        <ReaderPreferencesProvider>
          <ArticleBody html={html} />
        </ReaderPreferencesProvider>
      </StrictMode>,
    );
  });

  await act(() => {
    for (const image of container.querySelectorAll("img")) {
      image.dispatchEvent(new Event("load"));
    }
  });
}

it("replaces article HTML without stale portal state or late clipboard updates", async () => {
  let finishCopy: () => void = () => undefined;
  writeText.mockImplementation(
    () =>
      new Promise<void>((resolve) => {
        finishCopy = resolve;
      }),
  );

  await render('<img src="/old.svg" alt="Old chart"><pre>old code</pre>');
  const oldImage = container.querySelector("img");
  const oldToggle = container.querySelector<HTMLButtonElement>(
    ".image-background-toggle",
  );
  const oldCopy = container.querySelector<HTMLButtonElement>(".code-copy");
  await act(() => {
    oldToggle?.click();
    oldCopy?.click();
  });
  expect(oldToggle?.getAttribute("aria-pressed")).toBe("false");
  expect(oldCopy?.disabled).toBe(true);
  expect(writeText).toHaveBeenCalledWith("old code");

  await render(
    '<img src="/new.svg" alt="New chart"><pre>new code</pre><button onclick="attack()">Untrusted button</button>',
  );
  expect(oldImage?.isConnected).toBe(false);
  expect(oldCopy?.isConnected).toBe(false);
  expect(container.querySelectorAll("button")).toHaveLength(2);
  expect(container.querySelector("a button, [onclick]")).toBeNull();
  expect(
    container
      .querySelector(".image-background-toggle")
      ?.getAttribute("aria-pressed"),
  ).toBe("true");

  await act(async () => finishCopy());
  const newCopy = container.querySelector<HTMLButtonElement>(".code-copy");
  expect(newCopy?.textContent).toBe("Copy");
  expect(newCopy?.disabled).toBe(false);
  expect(container.querySelector("pre")?.textContent).toBe("new code");
});

it("resets copy feedback and cancels its timer when the article unmounts", async () => {
  vi.useFakeTimers();
  await render("<pre>code to copy</pre>");
  const button = container.querySelector<HTMLButtonElement>(".code-copy");

  await act(() => button?.click());
  expect(button?.textContent).toBe("Copied");
  expect(container.querySelector('[role="status"]')?.textContent).toBe(
    "Code copied.",
  );

  await act(() => {
    vi.advanceTimersByTime(3000);
  });
  expect(button?.textContent).toBe("Copy");
  expect(container.querySelector('[role="status"]')?.textContent).toBe("");

  await act(() => button?.click());
  expect(vi.getTimerCount()).toBe(1);

  await act(() => root.render(null));
  expect(vi.getTimerCount()).toBe(0);
  expect(container.querySelector("button")).toBeNull();
});

it("changes raw image and video visibility without replacing images or their manual backgrounds", async () => {
  const original = "https://i.redd.it/chart.svg";
  const videoUrl = "https://v.redd.it/clip";
  const images = [{ url: "/assets/chart.svg", source_url: original }];
  const videos = [
    { url: videoUrl, source_url: videoUrl, poster_url: null, remote: true },
  ];
  const videoKeys = new Set(["reddit-video:clip"]);

  function Controls() {
    const { preferences, updatePreferences } = useReaderPreferences();

    return (
      <>
        <button
          type="button"
          className="image-choice"
          onClick={() =>
            updatePreferences({
              ...preferences,
              hideRawImageLinks: !preferences.hideRawImageLinks,
            })
          }
        >
          Images
        </button>
        <button
          type="button"
          className="video-choice"
          onClick={() =>
            updatePreferences({
              ...preferences,
              hideRawVideoLinks: !preferences.hideRawVideoLinks,
            })
          }
        >
          Videos
        </button>
      </>
    );
  }

  await act(() =>
    root.render(
      <StrictMode>
        <ReaderPreferencesProvider>
          <Controls />
          <ArticleBody
            html={`<img src="/assets/chart.svg"><p><a href="${original}">${original}</a> <a href="${videoUrl}">${videoUrl}</a></p>`}
            redditImages={images}
            redditVideos={videos}
            redditVideoKeys={videoKeys}
            redditPostId="t3_chart"
          />
        </ReaderPreferencesProvider>
      </StrictMode>,
    ),
  );
  await act(() => {
    for (const image of container.querySelectorAll("img"))
      image.dispatchEvent(new Event("load"));
  });
  const image = container.querySelector("img");
  const imageLink = container.querySelector<HTMLAnchorElement>(
    `a[href="${original}"]`,
  );
  const videoLink = container.querySelector<HTMLAnchorElement>(
    `a[href="${videoUrl}"]`,
  );
  const background = container.querySelector<HTMLButtonElement>(
    ".image-background-toggle",
  );

  expect(imageLink?.hidden).toBe(true);
  expect(videoLink?.hidden).toBe(true);
  await act(() => background?.click());
  expect(background?.getAttribute("aria-pressed")).toBe("false");

  await act(() =>
    container.querySelector<HTMLButtonElement>(".image-choice")?.click(),
  );
  expect(imageLink?.hidden).toBe(false);
  expect(videoLink?.hidden).toBe(true);
  await act(() =>
    container.querySelector<HTMLButtonElement>(".video-choice")?.click(),
  );
  expect(videoLink?.hidden).toBe(false);
  expect(container.querySelector("img")).toBe(image);
  expect(container.querySelector(".image-background-toggle")).toBe(background);
  expect(background?.getAttribute("aria-pressed")).toBe("false");

  await act(() =>
    container.querySelector<HTMLButtonElement>(".image-choice")?.click(),
  );
  expect(imageLink?.hidden).toBe(true);
  expect(videoLink?.hidden).toBe(false);
  expect(container.querySelector("img")).toBe(image);
});
