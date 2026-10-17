import { expect, it } from "vitest";
import { formatCount } from "./counts";

it.each([
  [null, "—"],
  [undefined, "—"],
  [0, "0"],
  [999, "999"],
  [1_000, "1K"],
  [1_250, "1.2K"],
  [10_161, "10.1K"],
  [999_999, "999.9K"],
  [1_000_000, "1M"],
  [1_866_004, "1.8M"],
  [4_800_000, "4.8M"],
  [1_000_000_000, "1B"],
  [-1_250, "-1.2K"],
])("formats %s as %s without overstating the count", (value, expected) => {
  expect(formatCount(value)).toBe(expected);
});
