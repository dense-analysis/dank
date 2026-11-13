import { describe, expect, it } from "vitest";
import { ReaderRequestError } from "./api";
import {
  accountIdentity,
  addPlan,
  channelIdentity,
  collectScope,
  communityIdentity,
  draftOf,
  duplicateOf,
  fieldMessages,
  newDraft,
  type ParsedSource,
  parseSource,
  parseTags,
  platformOf,
  prepared,
  type SourceDraft,
  sameDraft,
  savedMatches,
  siteIdentity,
  siteOf,
  sourceDetail,
  sourceSummary,
  sourcesAccepting,
  sourceTitle,
  targetLabel,
  unconfirmed,
  withIdentity,
} from "./sourceSettings";
import type { SavedSource } from "./types";

function saved(id: string, fields: Partial<SourceDraft>): SavedSource {
  return {
    ...newDraft(platformOf(fields.domain ?? "example.com")),
    domain: "example.com",
    ...fields,
    id,
  };
}

const on = (value: string) => ({ value, enabled: true });
const off = (value: string) => ({ value, enabled: false });

describe("platforms", () => {
  it("collect a publisher's domain with RSS and the others natively", () => {
    expect(platformOf("example.com")).toBe("rss");
    expect(platformOf("x.com")).toBe("x");
    expect(platformOf(" Reddit.com ")).toBe("reddit");
    expect(platformOf("news.ycombinator.com")).toBe("hacker_news");
  });

  it("start a new source with the options its platform requires", () => {
    expect(newDraft("reddit").reddit).toEqual({
      sort: "top",
      time_filter: "day",
    });
    expect(newDraft("hacker_news").hacker_news).toEqual({ filter: "top50" });
    expect(newDraft("x")).toMatchObject({ domain: "x.com", reddit: null });
    expect(newDraft("rss").domain).toBe("");
  });
});

describe("what is sent", () => {
  it("writes out the RSS adapter Hacker News feeds need", () => {
    const draft = {
      ...newDraft("hacker_news"),
      feeds: [on("https://hnrss.org/frontpage")],
    };

    expect(prepared(draft).adapters).toEqual([
      { kind: "hacker_news", feeds: null },
      { kind: "rss", feeds: null },
    ]);
    expect(prepared(newDraft("hacker_news")).adapters).toBeNull();
  });

  it("drops the blank lines of an adapter's feeds", () => {
    const draft = {
      ...newDraft("rss"),
      domain: " example.com ",
      adapters: [
        {
          kind: "rss" as const,
          feeds: [on(" https://a.example/feed "), on("")],
        },
        { kind: "rss" as const, feeds: [on("  ")] },
      ],
    };

    expect(prepared(draft)).toMatchObject({
      domain: "example.com",
      adapters: [
        { kind: "rss", feeds: [on("https://a.example/feed")] },
        { kind: "rss", feeds: null },
      ],
    });
  });

  it("notices only real edits", () => {
    const source = saved("1", { domain: "x.com", accounts: [on("one")] });

    expect(sameDraft(draftOf(source), { ...draftOf(source) })).toBe(true);
    expect(
      sameDraft(draftOf(source), {
        ...draftOf(source),
        accounts: [off("one")],
      }),
    ).toBe(false);
  });

  it("reads tags as lowercase, without repeats or blanks", () => {
    expect(parseTags(" AI, news,,ai ")).toEqual(["ai", "news"]);
  });
});

describe("describing a source", () => {
  it("names entries the way people write them", () => {
    expect(targetLabel("x", "accounts", "handle")).toBe("@handle");
    expect(
      targetLabel("reddit", "feeds", "https://www.reddit.com/r/LocalLLaMA/"),
    ).toBe("r/LocalLLaMA");
    expect(
      targetLabel("rss", "feeds", "https://www.example.com/feed.xml"),
    ).toBe("example.com/feed.xml");
  });

  it("summarizes what is collected and what is off", () => {
    expect(
      sourceSummary(
        saved("1", {
          domain: "x.com",
          accounts: [on("a"), off("b"), on("c"), on("d")],
        }),
      ),
    ).toBe("@a, @b, @c and 1 more (1 off)");
    expect(sourceSummary(saved("2", { domain: "x.com" }))).toBe("For You only");
    expect(sourceSummary(saved("3", {}))).toBe("Feed found automatically");
  });

  it("titles a source by what it collects, with its platform beneath", () => {
    const reddit = saved("1", {
      domain: "reddit.com",
      feeds: [on("https://www.reddit.com/r/rust/"), off("r/go")],
    });

    expect(sourceTitle(reddit)).toBe("r/rust, r/go");
    expect(sourceDetail(reddit)).toBe("Reddit · top, past day · 1 off");
    expect(sourceTitle(saved("2", { domain: "x.com" }))).toBe("X For You feed");
    expect(sourceDetail(saved("2", { domain: "x.com" }))).toBe("X");
    expect(sourceTitle(saved("3", { domain: "www.example.com" }))).toBe(
      "example.com",
    );
    expect(
      sourceDetail(saved("3", { feeds: [on("https://a.example/f")] })),
    ).toBe("1 feed");
    expect(sourceDetail(saved("4", { domain: "news.ycombinator.com" }))).toBe(
      "Top 50 each day",
    );
  });
});

describe("reading pasted text", () => {
  const read = (text: string) => parseSource(text) as ParsedSource;
  const problem = (text: string) =>
    (parseSource(text) as { problem: string }).problem;

  it("reads handles, communities and platform links", () => {
    expect(parseSource("  ")).toBeNull();
    expect(read("r/LocalLLaMA").identity).toEqual(
      communityIdentity("LocalLLaMA"),
    );
    expect(
      read("https://old.reddit.com/r/rust/comments/abc/title/").identity,
    ).toEqual(communityIdentity("rust"));
    expect(read("https://twitter.com/Jack?lang=en").identity).toEqual(
      accountIdentity("Jack"),
    );
    expect(read("x.com").identity).toMatchObject({
      platform: "x",
      list: null,
      value: "x.com",
    });
    expect(read("https://m.youtube.com/@Veritasium/videos").identity).toEqual({
      platform: "youtube",
      list: "feeds",
      value: "https://www.youtube.com/@Veritasium",
      label: "@Veritasium",
    });
    expect(
      read("https://www.youtube.com/playlist?list=PL123&si=x").identity.value,
    ).toBe("https://www.youtube.com/playlist?list=PL123");
    expect(read("https://news.ycombinator.com/news").identity.platform).toBe(
      "hacker_news",
    );
  });

  it("reads a bare handle as an X account, or a YouTube channel", () => {
    const parsed = read("@jack");

    expect(parsed.identity).toEqual(accountIdentity("jack"));
    expect(parsed.other?.value).toBe("https://www.youtube.com/@jack");
    // Too long for X, so it can only be a channel.
    expect(read("@a-very-long-channel-name").identity.platform).toBe("youtube");
  });

  it("reads a site from a domain or any link, and a feed from its link", () => {
    expect(read("Example.com")).toEqual({
      identity: siteIdentity("example.com"),
      feed: undefined,
    });
    expect(read("https://www.example.com/2024/a-post/")).toEqual({
      identity: siteIdentity("example.com"),
      feed: undefined,
    });
    expect(read("https://blog.example.com/feed.xml")).toEqual({
      identity: siteIdentity("blog.example.com"),
      feed: "https://blog.example.com/feed.xml",
    });
    expect(read("example.com/?feed=rss2").feed).toBe(
      "https://example.com/?feed=rss2",
    );
    expect(siteOf("www.example.com")).toEqual({ domain: "www.example.com" });
    expect(siteOf(" https://www.example.com/about ")).toEqual({
      domain: "example.com",
      feed: undefined,
    });
    expect(
      prepared({ ...newDraft("rss"), domain: "https://a.example/x" }).domain,
    ).toBe("a.example");
  });

  it("says what to paste when the text names no source", () => {
    expect(problem("just words")).toBe("Add one source at a time.");
    expect(problem("nonsense")).toContain("Paste a link");
    expect(problem("https://x.com/home")).toContain("x.com/handle");
    expect(problem("https://www.reddit.com/")).toContain("reddit.com/r/name");
    expect(problem("https://youtu.be/dQw4w9WgXcQ")).toContain("not a video");
    expect(problem("https://www.youtube.com/watch?v=abc")).toContain(
      "not a video",
    );
  });
});

describe("planning an add", () => {
  const plan = (sources: SavedSource[], text: string) =>
    addPlan(sources, parseSource(text) as ParsedSource);

  it("saves a new source when nothing collects that platform", () => {
    expect(plan([], "@jack")).toMatchObject({
      id: null,
      draft: { domain: "x.com", accounts: [on("jack")], enabled: true },
    });
    expect(plan([], "https://example.com/feed.xml")).toMatchObject({
      id: null,
      draft: {
        domain: "example.com",
        feeds: [on("https://example.com/feed.xml")],
      },
    });
    expect(plan([], "r/rust").draft.reddit).toEqual({
      sort: "top",
      time_filter: "day",
    });
  });

  it("adds to a source of the platform that is on, never one that is off", () => {
    const sources = [
      saved("1", { domain: "x.com", enabled: false, accounts: [on("a")] }),
      saved("2", { domain: "x.com", accounts: [on("b")], tags: ["ai"] }),
    ];

    expect(plan(sources, "@jack")).toMatchObject({
      id: "2",
      draft: { accounts: [on("b"), on("jack")], tags: ["ai"] },
      before: { accounts: [on("b")] },
    });
    expect(plan(sources.slice(0, 1), "@jack").id).toBeNull();
  });

  it("turns a saved entry back on, and saves nothing for a collected one", () => {
    const sources = [
      saved("1", { domain: "x.com", accounts: [off("Jack"), on("b")] }),
      saved("2", { domain: "example.com" }),
    ];

    expect(plan(sources, "@jack")).toMatchObject({
      id: "1",
      draft: { accounts: [on("Jack"), on("b")] },
      collected: undefined,
    });
    expect(plan(sources, "https://x.com/B").collected?.id).toBe("1");
    expect(plan(sources, "example.com").collected?.id).toBe("2");
    // A feed link adds the feed to the site that is already saved.
    expect(plan(sources, "example.com/rss")).toMatchObject({
      id: "2",
      draft: { feeds: [on("https://example.com/rss")] },
      collected: undefined,
    });
  });
});

describe("a captured identity", () => {
  const sources = [
    saved("x-off", {
      domain: "x.com",
      enabled: false,
      accounts: [on("Alice")],
    }),
    saved("x-on", { domain: "x.com", accounts: [off("bob"), on("carol")] }),
    saved("reddit", {
      domain: "reddit.com",
      feeds: [on("https://www.reddit.com/r/LocalLLaMA/")],
    }),
    saved("youtube", { domain: "youtube.com", accounts: [on("@Channel")] }),
    saved("site", { domain: "example.com" }),
  ];

  it("is found where it is saved, whatever its case or spelling", () => {
    expect(savedMatches(sources, accountIdentity("@CAROL"))).toMatchObject([
      { source: { id: "x-on" }, index: 1, collecting: true },
    ]);
    expect(
      savedMatches(sources, communityIdentity("localllama")),
    ).toMatchObject([{ source: { id: "reddit" }, collecting: true }]);
    expect(savedMatches(sources, siteIdentity("example.com"))).toMatchObject([
      { source: { id: "site" }, index: null, collecting: true },
    ]);
    expect(savedMatches(sources, accountIdentity("nobody"))).toEqual([]);
  });

  it("isn't collected while it or its source is off", () => {
    expect(savedMatches(sources, accountIdentity("bob"))[0].collecting).toBe(
      false,
    );
    expect(savedMatches(sources, accountIdentity("alice"))[0].collecting).toBe(
      false,
    );
  });

  it("matches a YouTube channel saved as a handle", () => {
    const channel = channelIdentity({
      id: "UC1",
      name: "Channel",
      url: "https://www.youtube.com/@channel",
    });

    expect(channel && savedMatches(sources, channel)).toMatchObject([
      { source: { id: "youtube" }, index: 0 },
    ]);
    expect(
      channelIdentity({
        id: "",
        name: "",
        url: "https://youtube.com/watch?v=1",
      }),
    ).toBeNull();
  });

  it("can join any source of its platform, enabled ones first", () => {
    expect(
      sourcesAccepting(sources, accountIdentity("dave")).map(({ id }) => id),
    ).toEqual(["x-on", "x-off"]);
    expect(sourcesAccepting(sources, siteIdentity("new.example"))).toEqual([]);
  });

  it("is added once, or switched back on along with its source", () => {
    const [disabled, enabled] = sources;

    expect(
      withIdentity(draftOf(enabled), accountIdentity("dave")).accounts,
    ).toEqual([off("bob"), on("carol"), on("dave")]);
    expect(
      withIdentity(draftOf(enabled), accountIdentity("Bob")).accounts,
    ).toEqual([on("bob"), on("carol")]);
    expect(
      withIdentity(draftOf(disabled), accountIdentity("alice")),
    ).toMatchObject({ enabled: true, accounts: [on("Alice")] });
    expect(
      withIdentity(newDraft("rss"), siteIdentity("new.example")).domain,
    ).toBe("new.example");
  });
});

describe("after a save", () => {
  it("collects one new entry alone, and otherwise the platform", () => {
    const before = draftOf(
      saved("1", { domain: "x.com", accounts: [on("a")] }),
    );

    expect(
      collectScope({ ...before, accounts: [on("a"), on("b")] }, before),
    ).toEqual({ kind: "x", account: "b" });
    expect(collectScope(before, before)).toEqual({ kind: "x" });
    expect(
      collectScope({
        ...newDraft("reddit"),
        feeds: [on("https://www.reddit.com/r/rust/")],
      }),
    ).toEqual({ kind: "reddit", community: "rust" });
    expect(collectScope(draftOf(saved("2", {})))).toEqual({
      kind: "rss",
      domains: ["example.com"],
    });
  });

  it("counts an entry turned back on as new", () => {
    const before = draftOf(
      saved("1", { domain: "x.com", accounts: [off("a")] }),
    );

    expect(collectScope({ ...before, accounts: [on("a")] }, before)).toEqual({
      kind: "x",
      account: "a",
    });
  });

  it("finds the source a duplicate collided with, however it was typed", () => {
    const sources = [
      saved("1", {
        domain: "reddit.com",
        feeds: [on("https://www.reddit.com/r/Rust/")],
      }),
    ];
    const typed = { ...newDraft("reddit"), feeds: [on("r/rust")] };

    expect(duplicateOf(sources, typed)?.id).toBe("1");
    expect(duplicateOf(sources, typed, "1")).toBeUndefined();
  });

  it("puts each problem beside its field", () => {
    const error = new ReaderRequestError("No", 400, "invalid_source", [
      { path: "feeds.1.value", code: "invalid_url", message: "Use r/name." },
      { path: "body.domain", code: "missing", message: "Required." },
      { path: "feeds.1.value", code: "too_long", message: "Shorter." },
    ]);

    expect([...fieldMessages(error)]).toEqual([
      ["feeds.1.value", "Use r/name."],
      ["domain", "Required."],
    ]);
    expect(fieldMessages(new Error("offline")).size).toBe(0);
  });

  it("treats outages and network errors as unconfirmed, not refusals", () => {
    expect(unconfirmed(new TypeError("Failed to fetch"))).toBe(true);
    expect(unconfirmed(new ReaderRequestError("No", 503, "unavailable"))).toBe(
      true,
    );
    expect(
      unconfirmed(new ReaderRequestError("No", 409, "duplicate_source")),
    ).toBe(false);
  });
});
