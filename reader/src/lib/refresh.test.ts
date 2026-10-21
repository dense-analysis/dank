import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchRefreshJobs,
  isRefreshJob,
  type RefreshJob,
  refreshContext,
  refreshScopeKey,
  startRefresh,
} from "./refresh";

const job: RefreshJob = {
  id: "one",
  scope: { kind: "reddit", community: "LocalLLaMA" },
  label: "r/LocalLLaMA",
  status: "queued",
  message: "Waiting to collect",
  created_at: "2026-10-22T10:00:00Z",
  finished_at: null,
  posts_saved: 0,
};

afterEach(() => vi.unstubAllGlobals());

describe("contextual collection scope", () => {
  it.each([
    ["", { kind: "all" }],
    ["platform=reddit", { kind: "all", platforms: ["reddit"] }],
    [
      "platform=youtube&platform=stories&platform=reddit",
      { kind: "all", platforms: ["reddit", "rss", "youtube"] },
    ],
    [
      "platform=reddit&domain=reddit.com&tag=news&q=llama",
      {
        kind: "all",
        platforms: ["reddit"],
        domains: ["reddit.com"],
        tags: ["news"],
      },
    ],
    [
      "platform=youtube&youtube_item=video%3AdQw4w9WgXcQ",
      { kind: "post", domain: "youtube.com", post_id: "video:dQw4w9WgXcQ" },
    ],
    [
      "view=stories&domain=example.com",
      { kind: "rss", domains: ["example.com"] },
    ],
    ["view=reddit", { kind: "reddit" }],
    [
      "view=reddit&community=LocalLLaMA",
      { kind: "reddit", community: "LocalLLaMA" },
    ],
    ["view=x&account=%40Reporter", { kind: "x", account: "reporter" }],
    ["view=hacker_news&hn_filter=top10", { kind: "hacker_news" }],
    ["view=youtube", { kind: "youtube" }],
    ["view=youtube&channel=UC123", { kind: "youtube", channel_id: "UC123" }],
    [
      "view=youtube&feed=https%3A%2F%2Fwww.youtube.com%2Ffeed%2Fhype",
      { kind: "youtube", feed_id: "https://www.youtube.com/feed/hype" },
    ],
    [
      "view=youtube&channel=UC123&youtube_item=video%3AdQw4w9WgXcQ",
      { kind: "post", domain: "youtube.com", post_id: "video:dQw4w9WgXcQ" },
    ],
    [
      "view=reddit&community=LocalLLaMA&article=abc&article_source=reddit.com&tag=tech",
      { kind: "post", domain: "reddit.com", post_id: "abc" },
    ],
    [
      "domain=b.com&domain=a.com&tag=tech&tag=news",
      { kind: "all", domains: ["a.com", "b.com"], tags: ["news", "tech"] },
    ],
  ])("maps %s to the narrow collection context", (url, scope) => {
    expect(refreshContext(new URLSearchParams(url))?.scope).toEqual(scope);
  });

  it("does not turn display filters into collection limits", () => {
    expect(
      refreshContext(
        new URLSearchParams(
          "view=reddit&community=LocalLLaMA&q=embeddings&author=Ada&after=2026-10-01&before=2026-10-22&flair=News&sort=oldest&unread=1",
        ),
      )?.scope,
    ).toEqual({ kind: "reddit", community: "LocalLLaMA" });
  });

  it("explains unavailable derived feeds without collecting every source", () => {
    expect(
      refreshContext(new URLSearchParams("view=youtube&feed=linked")),
    ).toMatchObject({
      scope: { kind: "youtube", feed_id: "linked" },
      disabled: "Refresh the original sources to update linked videos",
    });
    expect(
      refreshContext(
        new URLSearchParams("view=youtube&youtube_item=post%3AUgkx123"),
      ),
    ).toMatchObject({
      scope: { kind: "post", domain: "youtube.com", post_id: "post:Ugkx123" },
      disabled: expect.stringContaining("community posts cannot be refreshed"),
    });
    expect(
      refreshContext(
        new URLSearchParams(
          "article=post%3AUgkx123&article_source=youtube.com",
        ),
      )?.disabled,
    ).toContain("community posts cannot be refreshed");
    expect(
      refreshContext(new URLSearchParams("youtube_item=post%3AUgkx123"))
        ?.disabled,
    ).toContain("community posts cannot be refreshed");
    expect(refreshContext(new URLSearchParams("view=settings"))).toBeNull();
    expect(refreshContext(new URLSearchParams("view=sources"))).toBeNull();
  });

  it("matches normalized server scopes for duplicate prevention", () => {
    expect(
      refreshScopeKey({ kind: "all", platforms: ["youtube", "reddit"] }),
    ).toBe(refreshScopeKey({ kind: "all", platforms: ["reddit", "youtube"] }));
    expect(refreshScopeKey({ kind: "all", platforms: ["reddit"] })).not.toBe(
      refreshScopeKey({ kind: "all", platforms: ["youtube"] }),
    );
    expect(
      refreshScopeKey({
        kind: "reddit",
        community: "LocalLLaMA",
        tags: ["b", "a"],
      }),
    ).toBe(
      refreshScopeKey({
        kind: "reddit",
        community: "localllama",
        tags: ["a", "b"],
        domains: [],
      }),
    );
  });
});

describe("refresh API boundary", () => {
  it("validates status before displaying server jobs", () => {
    expect(isRefreshJob(job)).toBe(true);
    expect(
      isRefreshJob({
        ...job,
        scope: { kind: "all", platforms: ["rss", "youtube"] },
      }),
    ).toBe(true);
    expect(
      isRefreshJob({ ...job, scope: { kind: "all", platforms: ["stories"] } }),
    ).toBe(false);
    expect(
      isRefreshJob({
        ...job,
        scope: { kind: "reddit", platforms: ["youtube"] },
      }),
    ).toBe(false);
    expect(isRefreshJob({ ...job, status: "unknown" })).toBe(false);
    expect(isRefreshJob({ ...job, posts_saved: -1 })).toBe(false);
    expect(isRefreshJob({ ...job, scope: { kind: "post", post_id: 22 } })).toBe(
      false,
    );
  });

  it("starts only the selected scope using a JSON POST", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify({ job }), { status: 202 }),
      );
    vi.stubGlobal("fetch", fetcher);
    expect(await startRefresh(job.scope)).toEqual(job);
    expect(fetcher).toHaveBeenCalledWith(
      "/api/reader/refresh",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ scope: job.scope }),
      }),
    );
    expect(fetcher.mock.calls[0][1].headers.get("Content-Type")).toBe(
      "application/json",
    );
  });

  it("retains meaningful server rejection details", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            detail:
              "The refresh queue is full. Try again after a collection finishes.",
          }),
          { status: 409 },
        ),
      ),
    );
    await expect(startRefresh({ kind: "all" })).rejects.toThrow(
      "The refresh queue is full",
    );
  });

  it("rejects malformed job history", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response(JSON.stringify({ jobs: [{ ...job, status: "done" }] })),
        ),
    );
    await expect(fetchRefreshJobs()).rejects.toThrow(
      "unexpected refresh status",
    );
  });
});
