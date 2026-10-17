import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchPost, fetchPosts, fetchSources } from "./api";
import { defaultFilters } from "./storage";
import { isPost } from "./validation";

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
  it("validates optional X post identities while accepting missing profile images and older posts", () => {
    const post = {
      id: "1",
      domain: "x.com",
      url: "https://x.com/reporter/status/1",
      author: "Display name",
      title: "A dispatch",
      excerpt: "A dispatch",
      html: "<p>A dispatch</p>",
      created_at: "2026-10-17T12:00:00Z",
      source: "x",
      thumbnail: null,
      media: [],
    };
    const profile = {
      handle: "reporter",
      name: "Display name",
      icon_url: null,
    };
    expect(isPost(post)).toBe(true);
    expect(isPost({ ...post, x: null })).toBe(true);
    expect(isPost({ ...post, x: profile })).toBe(true);
    expect(isPost({ ...post, x: { ...profile, handle: "", name: "" } })).toBe(
      true,
    );

    for (const bad of [
      { handle: "../wrong" },
      { handle: "x".repeat(16) },
      { name: null },
      { icon_url: 42 },
    ]) {
      expect(isPost({ ...post, x: { ...profile, ...bad } })).toBe(false);
    }
  });

  it("validates X attachment and reply snapshots without claiming saved replies", () => {
    const post = {
      id: "1",
      domain: "x.com",
      url: "https://x.com/reporter/status/1",
      author: "Reporter",
      title: "Post",
      excerpt: "Post",
      html: "<p>Post</p>",
      created_at: "2026-10-17T12:00:00Z",
      source: "x",
      thumbnail: null,
      media: [],
    };
    const x = {
      handle: "reporter",
      name: "Reporter",
      links: [
        {
          url: "https://example.com",
          title: "Research",
          description: "Details",
          image_url: null,
        },
      ],
      media: [
        {
          type: "image",
          url: "/assets/photo.png",
          poster_url: null,
          alt: "A diagram",
          remote: false,
        },
      ],
      reply_count: 0,
      replies_status: "not_collected",
      captured_at: null,
    };
    expect(isPost({ ...post, x })).toBe(true);

    for (const bad of [
      { reply_count: -1 },
      { replies_status: "invented" },
      { repost_count: -1 },
      { like_count: "5" },
      { view_count: Number.MAX_SAFE_INTEGER + 1 },
      { links: [{ ...x.links[0], image_url: 7 }] },
      { media: [{ ...x.media[0], remote: "false" }] },
      { media: [{ ...x.media[0], type: "iframe" }] },
      { media: [{ ...x.media[0], aspect_ratio: 0 }] },
      { media: [{ ...x.media[0], aspect_ratio: -1 }] },
      { media: [{ ...x.media[0], aspect_ratio: "9/16" }] },
      { media: [{ ...x.media[0], source_url: 42 }] },
    ]) {
      expect(isPost({ ...post, x: { ...x, ...bad } })).toBe(false);
    }
  });

  it.each(["all", "stories", "x"] as const)(
    "sends the %s collection without the UI view parameter",
    async (kind) => {
      respond({ posts: [], next_cursor: null, limited: false });
      await fetchPosts({ ...defaultFilters, kind, account: "Reporter" });
      const params = new URL(
        String(vi.mocked(fetch).mock.calls[0][0]),
        "https://reader.test",
      ).searchParams;
      expect(params.get("kind")).toBe(kind);
      expect(params.get("account")).toBe(kind === "x" ? "reporter" : null);
      expect(params.has("view")).toBe(false);
    },
  );

  it("validates account counts, handles and images before rendering source navigation", async () => {
    const account = {
      handle: "reporter",
      name: "City Reporter",
      count: 0,
      icon_url: null,
    };
    const source = {
      domain: "x.com",
      name: "X",
      kind: "x",
      count: 0,
      tags: [],
      accounts: [account],
    };
    respond({ sources: [source], total_posts: 0 });
    await expect(fetchSources()).resolves.toEqual({
      sources: [source],
      total_posts: 0,
    });

    for (const bad of [
      { count: -1 },
      { total_posts: -1 },
      { total_posts: 1.5 },
      { total_posts: "10161" },
      { total_posts: true },
      { total_posts: Number.MAX_SAFE_INTEGER + 1 },
      { handle: "../bad" },
      { name: null },
      { icon_url: 42 },
    ]) {
      respond({
        sources: [{ ...source, accounts: [{ ...account, ...bad }] }],
        total_posts: 0,
      });
      await expect(fetchSources()).rejects.toThrow("unexpected response");
    }

    for (const total_posts of [null, 0, 10161]) {
      const withTotal = { ...source, accounts: [{ ...account, total_posts }] };
      respond({ sources: [withTotal], total_posts: 0 });
      await expect(fetchSources()).resolves.toEqual({
        sources: [withTotal],
        total_posts: 0,
      });
    }
  });

  it("keeps Reddit queries separate from stories and sends the community without a UI view parameter", async () => {
    respond({ posts: [], next_cursor: null, limited: false });
    await fetchPosts({
      ...defaultFilters,
      kind: "reddit",
      community: "LocalLLaMA",
      flairs: ["New Model", "Research & News"],
    });
    const reddit = new URL(
      String(vi.mocked(fetch).mock.calls[0][0]),
      "https://reader.test",
    );
    expect(reddit.searchParams.get("kind")).toBe("reddit");
    expect(reddit.searchParams.get("community")).toBe("LocalLLaMA");
    expect(reddit.searchParams.getAll("flair")).toEqual([
      "New Model",
      "Research & News",
    ]);
    expect(reddit.searchParams.has("view")).toBe(false);
    await fetchPosts({
      ...defaultFilters,
      kind: "stories",
      flairs: ["New Model"],
    });
    const stories = new URL(
      String(vi.mocked(fetch).mock.calls[1][0]),
      "https://reader.test",
    );
    expect(stories.searchParams.get("kind")).toBe("stories");
    expect(stories.searchParams.has("community")).toBe(false);
    expect(stories.searchParams.has("flair")).toBe(false);
  });

  it("validates configured communities including those with no posts", async () => {
    const source = {
      domain: "reddit.com",
      name: "Reddit",
      kind: "reddit",
      tags: [],
      count: 0,
      communities: [{ name: "LocalLLaMA", count: 0 }],
    };
    respond({ sources: [source], total_posts: 0 });
    await expect(fetchSources()).resolves.toEqual({
      sources: [source],
      total_posts: 0,
    });
    respond({
      sources: [
        { ...source, communities: [{ name: "LocalLLaMA", count: "bad" }] },
      ],
      total_posts: 0,
    });
    await expect(fetchSources()).rejects.toThrow("unexpected response");
  });

  it("validates optional community icons and collected flair counts", async () => {
    const source = {
      domain: "reddit.com",
      name: "Reddit",
      kind: "reddit",
      count: 1,
      tags: [],
      flairs: [{ name: "New Model", count: 1 }],
      communities: [
        {
          name: "LocalLLaMA",
          count: 1,
          icon_url: "/assets/local.png",
          flairs: [{ name: "New Model", count: 1 }],
        },
      ],
    };
    respond({ sources: [source], total_posts: 1 });
    await expect(fetchSources()).resolves.toEqual({
      sources: [source],
      total_posts: 1,
    });

    for (const bad of [
      { ...source, flairs: [{ name: "New Model", count: -1 }] },
      { ...source, communities: [{ ...source.communities[0], icon_url: 7 }] },
      {
        ...source,
        communities: [
          {
            ...source.communities[0],
            flairs: [{ name: "New Model", count: "1" }],
          },
        ],
      },
    ]) {
      respond({ sources: [bad], total_posts: 1 });
      await expect(fetchSources()).rejects.toThrow("unexpected response");
    }
  });

  it("validates incomplete collection flags while accepting older saved posts", () => {
    const post = {
      id: "1",
      domain: "example.com",
      url: "https://example.com/1",
      title: "Feed text",
      author: "Writer",
      excerpt: "Still readable",
      html: "Still readable",
      created_at: "2026-10-15T12:00:00Z",
      source: "rss",
      thumbnail: null,
      media: [],
    };

    expect(isPost(post)).toBe(true);
    expect(isPost({ ...post, collection_incomplete: true })).toBe(true);
    expect(isPost({ ...post, collection_incomplete: false })).toBe(true);
    expect(isPost({ ...post, collection_incomplete: "false" })).toBe(false);
  });

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
