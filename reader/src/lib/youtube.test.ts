import { describe, expect, it } from "vitest";
import { youtubeEmbedUrl } from "./youtube";

const id = "LKrwhjOI8Nc";

describe("YouTube embeds", () => {
  it.each([
    `https://youtu.be/${id}?si=tracking`,
    `https://www.youtube.com/watch?v=${id}&list=playlist`,
    `https://m.youtube.com/watch?v=${id}`,
    `https://youtube.com/shorts/${id}`,
    `https://youtube.com/live/${id}`,
    `https://www.youtube-nocookie.com/embed/${id}`,
  ])(
    "recognizes video URL %s without forwarding arbitrary parameters",
    (url) => {
      expect(youtubeEmbedUrl(url)).toBe(
        `https://www.youtube-nocookie.com/embed/${id}?autoplay=1&playsinline=1`,
      );
    },
  );

  it.each([
    ["?t=90", "90"],
    ["?t=1m30s", "90"],
    ["?t=1h2m3s", "3723"],
    ["?start=40&t=90", "40"],
    ["#t=30s", "30"],
    ["?t=-1", null],
    ["?t=Infinity", null],
    ["?t=9007199254740992", null],
  ])("preserves a valid start time in %s", (suffix, start) => {
    const embed = youtubeEmbedUrl(`https://youtu.be/${id}${suffix}`);
    expect(new URL(embed ?? "").searchParams.get("start")).toBe(start);
  });

  it.each([
    `https://youtube.com.evil.test/watch?v=${id}`,
    `https://evil.test/youtube.com/watch?v=${id}`,
    `https://www.youtube.com@evil.test/watch?v=${id}`,
    `https://user:password@youtube.com/watch?v=${id}`,
    `https://youtube.com:8080/watch?v=${id}`,
    `javascript:alert(1)`,
    `//youtube.com/watch?v=${id}`,
    `https://youtube.com/watch?v=bad-id`,
    `https://youtu.be/${id}/extra`,
    `https://youtube.com/playlist?list=playlist`,
    `https://youtube.com/@3blue1brown`,
    `https://youtube.com/clip/${id}`,
  ])("leaves unsupported or untrusted URL %s as an ordinary link", (url) => {
    expect(youtubeEmbedUrl(url)).toBeUndefined();
  });
});
