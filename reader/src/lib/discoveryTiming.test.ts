import { expect, it } from "vitest";
import { discoveryTiming } from "./discoveryTiming";

it("compares actual Reddit publication with first discovery, preserving direction", () => {
  expect(
    discoveryTiming({
      post_published_at: "2026-10-17T07:09:59.892Z",
      first_seen_at: "2026-10-16T21:25:30.953Z",
    }),
  ).toMatchObject({
    gapMs: 35_068_939,
    gapLabel: "9h 44m after first discovery",
  });
  expect(
    discoveryTiming({
      post_published_at: "2026-10-17T08:18:10.825Z",
      first_seen_at: "2026-10-17T13:54:58.736+01:00",
    }),
  ).toMatchObject({
    gapMs: -16_607_911,
    gapLabel: "4h 36m before first discovery",
  });
});

it.each([null, undefined, "", "not a date"])(
  "does not invent publication from missing or invalid dates: %s",
  (value) => {
    expect(
      discoveryTiming({
        post_published_at: value,
        first_seen_at: "2026-10-16T21:25:00Z",
      }),
    ).toMatchObject({
      publishedAt: null,
      gapMs: null,
      gapLabel: "Gap unavailable",
    });
  },
);

it.each([null, "", "not a date"])(
  "keeps publication known when discovery is unavailable: %s",
  (value) => {
    expect(
      discoveryTiming({
        post_published_at: "2026-10-17T07:09:00Z",
        first_seen_at: value,
      }),
    ).toMatchObject({
      publishedAt: "2026-10-17T07:09:00Z",
      discoveredAt: null,
      gapMs: null,
      gapLabel: "Gap unavailable",
    });
  },
);

it.each([
  ["2026-10-17T10:00:00Z", "Same time as first discovery"],
  ["2026-10-17T10:00:00.001Z", "<1m after first discovery"],
  ["2026-10-17T09:59:59.999Z", "<1m before first discovery"],
  ["2026-10-19T11:02:00Z", "2d 1h after first discovery"],
])(
  "handles ties and small or large gaps without rounding away their direction: %s",
  (published, gapLabel) => {
    expect(
      discoveryTiming({
        post_published_at: published,
        first_seen_at: "2026-10-17T10:00:00Z",
      }).gapLabel,
    ).toBe(gapLabel);
  },
);

it("compares instants across a daylight-saving clock change", () => {
  expect(
    discoveryTiming({
      post_published_at: "2026-10-25T01:30:00+00:00",
      first_seen_at: "2026-10-25T01:30:00+01:00",
    }).gapLabel,
  ).toBe("1h after first discovery");
});
