import { afterEach, describe, expect, it, vi } from "vitest";
import {
  apiGet,
  apiUrl,
  fetchCategories,
  fetchPost,
  fetchPosts,
  fetchSources,
  ReaderRequestError,
} from "./api";
import { post } from "./api.fixture";
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

function respondText(body: string, status: number): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation(async () => new Response(body, { status })),
  );
}

async function requestError(
  request: Promise<unknown>,
): Promise<ReaderRequestError> {
  const error = await request.then(
    () => undefined,
    (reason: unknown) => reason,
  );
  if (!(error instanceof ReaderRequestError))
    throw new Error("Expected a ReaderRequestError");

  return error;
}

afterEach(() => vi.unstubAllGlobals());

describe("reader API", () => {
  it("fills path values and appends the query to the schema path", async () => {
    respond({ playlist: null, items: [], next_cursor: null });
    const signal = new AbortController().signal;
    await apiGet("/api/reader/youtube/playlists/{playlist_id}", {
      params: { playlist_id: "PL a/b?c" },
      query: new URLSearchParams({ q: "talks & demos", limit: "10" }),
      signal,
    });

    const [path, options] = vi.mocked(fetch).mock.calls[0];
    expect(path).toBe(
      "/api/reader/youtube/playlists/PL%20a%2Fb%3Fc?q=talks+%26+demos&limit=10",
    );
    expect(options?.method).toBe("GET");
    expect(options?.signal).toBe(signal);
    expect(new Headers(options?.headers).get("Accept")).toBe(
      "application/json",
    );
    expect(options?.body).toBeUndefined();
  });

  it("leaves a path without a query unchanged and refuses a missing path value", () => {
    expect(apiUrl("/api/reader/sources")).toBe("/api/reader/sources");
    expect(apiUrl("/api/reader/sources", {}, new URLSearchParams())).toBe(
      "/api/reader/sources",
    );
    expect(() => apiUrl("/api/reader/youtube/playlists/{playlist_id}")).toThrow(
      "Missing path value: playlist_id",
    );
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

  it("encodes both parts of article identity", async () => {
    const article = post({ id: "id?x=y&z", domain: "a/b.example" });
    respond(article);
    await expect(fetchPost("a/b.example", "id?x=y&z")).resolves.toEqual(
      article,
    );
    const [path] = vi.mocked(fetch).mock.calls[0];
    const url = new URL(String(path), "https://reader.test");
    expect(url.pathname).toBe("/api/reader/post");
    expect(url.searchParams.get("domain")).toBe("a/b.example");
    expect(url.searchParams.get("id")).toBe("id?x=y&z");
  });

  it("carries the error envelope's code and field messages", async () => {
    respond(
      {
        error: "Check the highlighted values.",
        code: "invalid_request",
        fields: [
          {
            path: "query.limit",
            code: "too_large",
            message: "Ask for 100 posts or fewer.",
          },
          { path: "query.cursor", code: "invalid" },
        ],
      },
      422,
    );
    const error = await requestError(fetchPosts(defaultFilters));
    expect(error.message).toBe("Check the highlighted values.");
    expect(error.status).toBe(422);
    expect(error.code).toBe("invalid_request");
    expect(error.fields).toEqual([
      {
        path: "query.limit",
        code: "too_large",
        message: "Ask for 100 posts or fewer.",
      },
    ]);
    expect(error.fieldMessage("query.limit")).toBe(
      "Ask for 100 posts or fewer.",
    );
    expect(error.fieldMessage("query.cursor")).toBeUndefined();
  });

  it("explains an error the API did not describe", async () => {
    const fallback =
      "The reader could not load this request (502). Please try again.";
    respondText("<html>Bad gateway</html>", 502);
    const proxy = await requestError(fetchSources());
    expect(proxy.message).toBe(fallback);
    expect(proxy.status).toBe(502);
    expect(proxy.code).toBe("");
    expect(proxy.fields).toEqual([]);

    respond({ detail: "Meaning search is not available yet." }, 502);
    await expect(fetchSources()).rejects.toThrow(fallback);

    respond({ error: "", code: "unavailable" }, 502);
    await expect(fetchSources()).rejects.toThrow(fallback);
  });

  it("reports a success body it cannot read", async () => {
    respondText("<html>Signed out</html>", 200);
    await expect(fetchSources()).rejects.toThrow(
      "The reader received an unreadable response. Please try again.",
    );
  });

  it("preserves cancellation rather than converting it into a misleading response error", async () => {
    const error = new DOMException("Cancelled", "AbortError");
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(error));
    await expect(fetchSources()).rejects.toBe(error);
  });
});

describe("category catalog", () => {
  it("returns the configured names and keeps errors explicit", async () => {
    respond({ categories: ["Software", "Science"] });
    await expect(fetchCategories()).resolves.toEqual(["Software", "Science"]);
    respond({ categories: [] });
    await expect(fetchCategories()).resolves.toEqual([]);

    respond({ error: "Collection unavailable" }, 503);
    await expect(fetchCategories()).rejects.toThrow("Collection unavailable");
  });
});
