import { describe, expect, it } from "vitest";
import {
  displayedRedditVideos,
  redditVideoKey,
  withoutRedditVideoLinks,
  withoutRedditVideoUrls,
} from "./redditText";
import type { RedditVideo } from "./types";

const url = "https://v.redd.it/portrait";
const video: RedditVideo = {
  url: "/assets/saved.mp4",
  source_url: `${url}/HLSPlaylist.m3u8?a=old`,
  poster_url: null,
  remote: false,
};
const displayed = displayedRedditVideos([video], new Set());

describe("represented Reddit video links", () => {
  it("matches the public link, rotating manifest, packaged video and local archive", () => {
    for (const value of [
      url,
      `${url}/HLSPlaylist.m3u8?a=new`,
      "https://packaged-media.redd.it/portrait/pb/video.mp4?signature=new",
      "v.redd.it/portrait",
      video.url,
    ]) {
      expect(displayed.has(redditVideoKey(value) ?? "")).toBe(true);
      expect(withoutRedditVideoUrls(value, displayed)).toBe("");
    }
  });
  it("keeps other clips, deceptive hosts and descriptive links", () => {
    for (const value of [
      "https://v.redd.it/other",
      "https://v.redd.it.evil.test/portrait",
      "https://example.com/portrait",
    ]) {
      expect(withoutRedditVideoUrls(value, displayed)).toBe(value);
    }
    const html = withoutRedditVideoLinks(
      `<p>A clip <a href="${url}">${url}</a></p><p><a href="${url}">the discussion</a></p><p>Plain: ${url}</p><p><a href="https://v.redd.it/other">https://v.redd.it/other</a></p>`,
      displayed,
    );
    expect(html).toContain("the discussion");
    expect(html).toContain("https://v.redd.it/other");
    expect(html).not.toContain(`>${url}<`);
    expect(html).not.toContain(`Plain: ${url}`);
  });
  it("retains code and image links, removes empty URL paragraphs, and sanitizes imported HTML", () => {
    const html = withoutRedditVideoLinks(
      `<p><a href="${url}">${url}</a><br></p><p><a href="${url}"><img src="/assets/poster.jpg"></a></p><pre>${url}</pre><script>alert(1)</script>`,
      displayed,
    );
    expect(html).not.toContain("<br>");
    expect(html).toContain("<img");
    expect(html).toContain(`<pre>${url}</pre>`);
    expect(html).not.toContain("script");
  });
  it("restores failed references and leaves links alone when no usable player represents them", () => {
    for (const assets of [[], [{ ...video, url: "javascript:alert(1)" }]]) {
      const keys = displayedRedditVideos(assets, new Set());
      expect(withoutRedditVideoUrls(url, keys)).toBe(url);
    }
    const keys = displayedRedditVideos(
      [
        video,
        {
          ...video,
          url: "/assets/other.mp4",
          source_url: "https://v.redd.it/other/HLSPlaylist.m3u8",
        },
      ],
      new Set([video.url]),
    );
    expect(withoutRedditVideoUrls(url, keys)).toBe(url);
    expect(withoutRedditVideoUrls("https://v.redd.it/other", keys)).toBe("");
  });
});
