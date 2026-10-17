import { describe, expect, it } from "vitest";
import {
  rememberVideoPosition,
  videoPosition,
  videoPositionKey,
} from "./videoPositions";

describe("video navigation positions", () => {
  it("keeps posts, providers and separate attachments independent", () => {
    const one = videoPositionKey('["x.com","1"]', "native", 0);
    const other = videoPositionKey('["x.com","2"]', "native", 0);
    const second = videoPositionKey('["x.com","1"]', "native", 1);
    const youtube = videoPositionKey('["x.com","1"]', "youtube", 0);
    rememberVideoPosition(one, 12.5);
    expect(videoPosition(one)).toBe(12.5);
    for (const key of [other, second, youtube])
      expect(videoPosition(key)).toBeUndefined();
    rememberVideoPosition(one, 2);
    expect(videoPosition(one)).toBe(2);
    rememberVideoPosition(one, 0);
    expect(videoPosition(one)).toBe(0);
  });
  it("ignores invalid samples and bounds retained browsing history", () => {
    const key = "valid-position";
    rememberVideoPosition(key, 5);
    for (const time of [NaN, Infinity, -1]) rememberVideoPosition(key, time);
    expect(videoPosition(key)).toBe(5);
    for (let i = 0; i < 500; i++) rememberVideoPosition(`bounded-${i}`, i);
    expect(videoPosition(key)).toBeUndefined();
    expect(videoPosition("bounded-499")).toBe(499);
  });
});
