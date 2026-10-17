import { describe, expect, it } from "vitest";
import { matchesLinkUrl, normalizedLinkUrl } from "./linkIdentity";

describe("collected link identities", () => {
  it.each([
    ["https://example.com/a/../b", "https://example.com/a/../b"],
    ["https://example.com/世界?q=消息", "https://example.com/世界?q=消息"],
    [
      "https://EXAMPLE.com:443/article?a=1&b=2#part",
      "https://example.com/article?a=1&b=2",
    ],
    ["http://example.com:80/article", "http://example.com/article"],
    ["https://example.com:8443/article", "https://example.com:8443/article"],
    [
      "https://twitter.com/writer/status/123?s=20",
      "https://x.com/i/status/123",
    ],
    ["http://www.x.com:80/writer/status/123/", "https://x.com/i/status/123"],
    [
      "https://old.reddit.com/r/topic/comments/ABC/title/?context=3",
      "https://www.reddit.com/comments/abc",
    ],
    [
      "https://reddit.com/r/topic/comments/abc/title/def/?context=3",
      "https://reddit.com/r/topic/comments/abc/title/def/?context=3",
    ],
    [
      "https://music.youtube.com/watch?v=NnV_cWeoo5Q&t=12",
      "https://www.youtube.com/watch?v=NnV_cWeoo5Q",
    ],
    [
      "https://youtu.be/NnV_cWeoo5Q#t=12",
      "https://www.youtube.com/watch?v=NnV_cWeoo5Q",
    ],
    [
      "http://youtube.com:80/shorts/NnV_cWeoo5Q",
      "https://www.youtube.com/watch?v=NnV_cWeoo5Q",
    ],
    [
      "https://youtube.com:80/watch?v=NnV_cWeoo5Q",
      "https://youtube.com:80/watch?v=NnV_cWeoo5Q",
    ],
    [
      "https://youtube.com/watch?v=NnV_cWeoo5Q&v=aaaaaaaaaaa",
      "https://youtube.com/watch?v=NnV_cWeoo5Q&v=aaaaaaaaaaa",
    ],
  ])(
    "normalizes %s consistently with the evidence index",
    (input, expected) => {
      expect(normalizedLinkUrl(input)).toBe(expected);
    },
  );

  it.each([
    "javascript:alert(1)",
    "data:text/html,unsafe",
    "https://writer:password@example.com/a",
    "https://example.com/a b",
    "https://example.com\\@evil.test/a",
    "/relative/without/base",
    "https://example.com:99999/a",
    "https://example.com/a\n",
  ])("rejects unsafe or ambiguous URLs: %s", (url) => {
    expect(normalizedLinkUrl(url)).toBeUndefined();
  });

  it("resolves relative destinations against the source, preserving path and query meaning", () => {
    expect(
      normalizedLinkUrl(
        "../article?p=1#part",
        "https://example.com/stories/intro",
      ),
    ).toBe("https://example.com/article?p=1");
    expect(
      matchesLinkUrl(
        "../article?p=1#part",
        "https://example.com/article?p=1#part",
        "https://example.com/stories/intro",
      ),
    ).toBe(true);
    expect(
      matchesLinkUrl(
        "https://example.com/article?p=2",
        "https://example.com/article?p=1",
      ),
    ).toBe(false);
    expect(
      matchesLinkUrl(
        "https://example.com/article?utm_source=feed",
        "https://example.com/article",
      ),
    ).toBe(false);
    expect(
      matchesLinkUrl(
        "https://example.com/article#other",
        "https://example.com/article#part",
      ),
    ).toBe(false);
    expect(
      matchesLinkUrl(
        "https://example.com/article#part",
        "https://example.com/article",
      ),
    ).toBe(true);
    expect(
      matchesLinkUrl(
        "https://example.com.evil.test/article",
        "https://example.com/article",
      ),
    ).toBe(false);
  });
});
