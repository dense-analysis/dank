const compactCount = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
  // Keep collected totals from rounding up to a count we have not reached.
  roundingMode: "trunc",
});

export function formatCount(value: number | null | undefined): string {
  return value == null ? "—" : compactCount.format(value);
}
