import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { AttachedVideo } from "./AttachedVideo";

vi.mock("../hooks/useVideoPlayback", () => ({
  useVideoPlayback: () => undefined,
}));
vi.mock("./YouTubePlayer", () => ({
  YouTubePlayer: ({ src }: { src: string }) => (
    <iframe src={src} title="Remote player" />
  ),
}));

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

it("retired YouTube attachments embed only after the user presses Play", async () => {
  await act(() =>
    root.render(
      <AttachedVideo
        url="https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        positionKey="article:video"
      />,
    ),
  );
  expect(container.querySelector("iframe, video")).toBeNull();
  expect(container.textContent).toBe("Play from YouTube");
  await act(() => container.querySelector("button")?.click());
  expect(container.querySelector("iframe")?.src).toContain(
    "https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ?",
  );
});

it("available downloads still use the local video player", async () => {
  await act(() =>
    root.render(
      <AttachedVideo url="/assets/saved.mp4" positionKey="article:video" />,
    ),
  );
  expect(container.querySelector("video")?.getAttribute("src")).toBe(
    new URL("/assets/saved.mp4", window.location.origin).href,
  );
  expect(container.querySelector("iframe, button")).toBeNull();
});

it("lookalike YouTube hosts never become embeds", async () => {
  await act(() =>
    root.render(
      <AttachedVideo
        url="https://youtube.com.evil.test/watch?v=dQw4w9WgXcQ"
        positionKey="article:video"
      />,
    ),
  );
  expect(container.querySelector("iframe, button")).toBeNull();
});
