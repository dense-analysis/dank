import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchPost, fetchPosts, fetchSources } from "./api";
import { defaultFilters } from "./storage";

function respond(body: unknown, status = 200): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation(
      async () =>
        new Response(JSON.stringify(body), {
          status,
          headers: { "Content-Type": "application/json" },
        }),
    ),
  );
}

afterEach(() => vi.unstubAllGlobals());

describe("reader API", () => {
  it("sends all filters, an opaque cursor, and the cancellation signal", async () => {
    respond({ posts: [], next_cursor: null, limited: false });
    const signal = new AbortController().signal;
    await fetchPosts(
      {
        ...defaultFilters,
        q: "politics & science",
        domains: ["example.com", "example.org"],
        tags: ["news", "science & technology"],
        author: "Ada",
        authorExact: true,
      },
      "a+b/=?",
      signal,
    );

    const [path, options] = vi.mocked(fetch).mock.calls[0];
    const url = new URL(String(path), "https://reader.test");
    expect(url.pathname).toBe("/api/reader/posts");
    expect(url.searchParams.getAll("domain")).toEqual([
      "example.com",
      "example.org",
    ]);
    expect(url.searchParams.getAll("tag")).toEqual([
      "news",
      "science & technology",
    ]);
    expect(url.searchParams.get("q")).toBe("politics & science");
    expect(url.searchParams.get("mode")).toBe("combined");
    expect(url.searchParams.get("author_match")).toBe("exact");
    expect(url.searchParams.get("cursor")).toBe("a+b/=?");
    expect(url.searchParams.get("limit")).toBe("30");
    expect(options?.signal).toBe(signal);
  });

  it("requests combined relevance search and keeps ordinary browsing free of search modes", async () => {
    respond({ posts: [], next_cursor: null, limited: false });
    await fetchPosts({
      ...defaultFilters,
      q: "domain driven design",
      sort: "relevance",
    });
    const search = new URL(
      String(vi.mocked(fetch).mock.calls[0][0]),
      "https://reader.test",
    );
    expect(search.searchParams.get("mode")).toBe("combined");
    expect(search.searchParams.get("sort")).toBe("relevance");
    await fetchPosts(defaultFilters);
    const browse = new URL(
      String(vi.mocked(fetch).mock.calls[1][0]),
      "https://reader.test",
    );
    expect(browse.searchParams.has("mode")).toBe(false);
    expect(browse.searchParams.has("q")).toBe(false);
  });

  it("validates source counts before returning them", async () => {
    const sources = [
      { domain: "example.com", name: "Example", tags: ["news"], count: 12 },
    ];
    respond({ sources, total_posts: 12 });
    await expect(fetchSources()).resolves.toEqual({ sources, total_posts: 12 });
    respond({ sources, total_posts: "12" });
    await expect(fetchSources()).rejects.toThrow("unexpected response");
  });

  it("rejects malformed posts and invalid pagination instead of passing them to the UI", async () => {
    respond({
      posts: [{ id: "missing everything else" }],
      next_cursor: null,
      limited: false,
    });
    await expect(fetchPosts(defaultFilters)).rejects.toThrow(
      "unexpected response",
    );
    respond({ posts: [], next_cursor: 1, limited: false });
    await expect(fetchPosts(defaultFilters)).rejects.toThrow(
      "unexpected response",
    );
    respond(null);
    await expect(fetchPost("example.com", "123")).rejects.toThrow(
      "unexpected response",
    );
  });

  it("encodes both parts of article identity", async () => {
    respond(null);
    await expect(fetchPost("a/b.example", "id?x=y&z")).rejects.toThrow();
    const [path] = vi.mocked(fetch).mock.calls[0];
    const url = new URL(String(path), "https://reader.test");
    expect(url.searchParams.get("domain")).toBe("a/b.example");
    expect(url.searchParams.get("id")).toBe("id?x=y&z");
  });

  it("returns API error messages and a readable fallback for proxy errors", async () => {
    respond({ detail: "Meaning search is not available yet." }, 503);
    await expect(fetchPosts(defaultFilters)).rejects.toThrow(
      "Meaning search is not available yet.",
    );
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response("<html>Bad gateway</html>", { status: 502 }),
        ),
    );
    await expect(fetchSources()).rejects.toThrow("502");
  });

  it("preserves cancellation rather than converting it into a misleading response error", async () => {
    const error = new DOMException("Cancelled", "AbortError");
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(error));
    await expect(fetchSources()).rejects.toBe(error);
  });
});
