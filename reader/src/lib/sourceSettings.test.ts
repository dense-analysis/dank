import { describe, expect, it } from "vitest";
import { ReaderRequestError } from "./api";
import {
  accountIdentity,
  channelIdentity,
  collectScope,
  communityIdentity,
  draftOf,
  duplicateOf,
  fieldMessages,
  newDraft,
  parseTags,
  platformOf,
  prepared,
  type SourceDraft,
  sameDraft,
  savedMatches,
  siteIdentity,
  sourceSummary,
  sourcesAccepting,
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
