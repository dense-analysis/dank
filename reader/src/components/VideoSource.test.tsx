import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, expect, it } from "vitest";
import { ReaderPreferencesProvider } from "../hooks/useReaderPreferences";
import { defaultPreferences, savePreferences } from "../lib/preferences";
import { VideoSource } from "./VideoSource";

beforeEach(() => localStorage.clear());

function caption(url: string, postUrl: string, remote = true) {
  return renderToStaticMarkup(
    <ReaderPreferencesProvider>
      <VideoSource url={url} postUrl={postUrl} remote={remote} />
    </ReaderPreferencesProvider>,
  );
}

it.each([
  [
    "https://v.redd.it/clip/HLSPlaylist.m3u8",
    "https://www.reddit.com/r/videos/comments/clip",
    "Reddit",
  ],
  [
    "https://video.twimg.com/clip.mp4",
    "https://x.com/author/status/1",
    "X.com",
  ],
])(
  "keeps the globe and source label without a duplicate link for %s",
  (url, post, name) => {
    const html = caption(url, post);
    expect(html).toContain(`Remote video - ${name}`);
    expect(html).toContain("<svg");
    expect(html).not.toContain("<a ");
  },
);

it("retains a separate accessible destination for an embedded video from another site", () => {
  const html = caption(
    "https://youtu.be/dQw4w9WgXcQ?t=90",
    "https://x.com/author/status/1",
  );
  expect(html).toContain("Remote video - YouTube");
  expect(html).toContain('href="https://youtu.be/dQw4w9WgXcQ?t=90"');
  expect(html).toContain('aria-label="Watch on YouTube"');
});

it("hides remote labels without removing a distinct video's destination", () => {
  savePreferences({ ...defaultPreferences, showRemoteContentLabels: false });
  expect(
    caption(
      "https://v.redd.it/clip",
      "https://reddit.com/r/videos/comments/clip",
    ),
  ).toBe("");
  const html = caption(
    "https://media.example.com/clip.mp4",
    "https://reddit.com/r/videos/comments/clip",
  );
  expect(html).not.toContain("Remote video");
  expect(html).toContain('href="https://media.example.com/clip.mp4"');
});

it("does not call downloaded video remote or add a redundant caption", () => {
  expect(
    caption(
      "https://v.redd.it/clip",
      "https://reddit.com/r/videos/comments/clip",
      false,
    ),
  ).toBe("");
  expect(
    caption("/assets/clip.mp4", "https://x.com/author/status/1", false),
  ).toBe("");
});
