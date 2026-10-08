import { describe, expect, it } from "vitest";
import { recentDates } from "./dateRanges";

describe("recent date ranges", () => {
  it("includes today in the seven-day window", () => {
    expect(recentDates(7, new Date("2026-10-08T12:00:00Z"))).toEqual({
      after: "2026-10-02",
      before: "2026-10-08",
    });
  });
  it("handles year and leap-month boundaries", () => {
    expect(recentDates(7, new Date("2025-01-02T12:00:00Z"))).toEqual({
      after: "2024-12-27",
      before: "2025-01-02",
    });
    expect(recentDates(30, new Date("2024-03-01T12:00:00Z"))).toEqual({
      after: "2024-02-01",
      before: "2024-03-01",
    });
  });
  it("uses UTC days even when local midnight is on another date", () => {
    expect(recentDates(1, new Date("2026-10-09T00:30:00+01:00"))).toEqual({
      after: "2026-10-08",
      before: "2026-10-08",
    });
  });
});
