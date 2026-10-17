import type { HackerNewsFilter } from "./types";

export const hackerNewsFilters: { value: HackerNewsFilter; label: string }[] = [
  { value: "top10", label: "Top 10" },
  { value: "top20", label: "Top 20" },
  { value: "top50", label: "Top 50%" },
  { value: "all", label: "All" },
];

export function parseHackerNewsFilter(value: string | null): HackerNewsFilter {
  return (
    hackerNewsFilters.find((filter) => filter.value === value)?.value ?? "top50"
  );
}
