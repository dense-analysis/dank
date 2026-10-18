import { describe, expect, it } from "vitest";
import { dateLabel, postDateLabel } from "./timestamps";

const now = new Date(2026, 9, 18, 12, 0);
const ago = (minutes: number) =>
  new Date(now.getTime() - minutes * 60_000).toISOString();

describe("post dates", () => {
  it.each([
    [0, "Just now"],
    [0.9, "Just now"],
    [1, "1 minute ago"],
    [59, "59 minutes ago"],
    [60, "1 hour ago"],
    [180, "3 hours ago"],
  ])("shows today's age at %s minutes", (minutes, label) => {
    expect(postDateLabel(ago(minutes), now)).toBe(label);
  });

  it("switches at local midnight even when yesterday was less than an hour ago", () => {
    const midnight = new Date(2026, 9, 18, 0, 10);
    const yesterday = new Date(2026, 9, 17, 23, 50).toISOString();
    expect(postDateLabel(yesterday, midnight)).toBe(dateLabel(yesterday));
  });

  it("keeps full dates for older and future posts instead of inventing relative ages", () => {
    for (const date of [ago(1440), ago(525600), ago(-30)]) {
      expect(postDateLabel(date, now)).toBe(dateLabel(date));
    }
  });

  it("labels invalid dates explicitly", () => {
    expect(postDateLabel("unavailable", now)).toBe("Date unavailable");
  });
});
