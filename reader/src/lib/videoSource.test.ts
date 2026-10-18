import { expect, it } from "vitest";
import { videoSource } from "./videoSource";

it.each([
  ["https://v.redd.it/clip/HLSPlaylist.m3u8", "Reddit"],
  ["https://old.reddit.com/r/videos/comments/clip", "Reddit"],
  ["https://www.redditmedia.com/clip", "Reddit"],
  ["https://video.twimg.com/ext_tw_video/clip.mp4", "X.com"],
  ["https://twitter.com/author/status/1", "X.com"],
  ["https://x.com/author/status/1", "X.com"],
  ["https://youtu.be/dQw4w9WgXcQ?t=90", "YouTube"],
  ["https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ", "YouTube"],
  ["https://rr1.googlevideo.com/videoplayback", "YouTube"],
  ["https://www.example.com/clip.mp4", "example.com"],
  ["https://v.redd.it.example.com/clip.mp4", "v.redd.it.example.com"],
])(
  "identifies the source of %s without mistaking delivery hosts for another platform",
  (url, name) => {
    expect(videoSource(url)).toEqual({ name, url });
  },
);

it.each([
  undefined,
  null,
  "",
  "/assets/clip.mp4",
  "javascript:alert(1)",
  "data:video/mp4,abc",
  "https://user:password@reddit.com/clip",
])("does not produce an external destination from %s", (url) =>
  expect(videoSource(url)).toBeUndefined(),
);
