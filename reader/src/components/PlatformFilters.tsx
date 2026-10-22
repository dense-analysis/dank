import { Layers3, Newspaper } from "lucide-react";
import { feedLabels } from "../lib/collections";
import type { FeedPlatform, Filters } from "../lib/types";
import { PlatformIcon } from "./PlatformIcon";

const platforms: FeedPlatform[] = ["x", "reddit", "youtube", "stories"];

export function PlatformFilters({
  filters,
  change,
}: {
  filters: Filters;
  change: (filters: Filters) => void;
}) {
  return (
    <fieldset className="platform-filters" aria-label="Filter by platform">
      <legend className="visually-hidden">Filter by platform</legend>
      <button
        type="button"
        aria-pressed={filters.platforms.length === 0}
        onClick={() => change({ ...filters, platforms: [] })}
      >
        <Layers3 size={15} aria-hidden="true" />
        All sources
      </button>
      {platforms.map((platform) => (
        <button
          key={platform}
          type="button"
          aria-pressed={filters.platforms.includes(platform)}
          onClick={() =>
            change({
              ...filters,
              platforms: filters.platforms.includes(platform)
                ? filters.platforms.filter((value) => value !== platform)
                : [...filters.platforms, platform],
            })
          }
        >
          {platform === "stories" ? (
            <Newspaper size={15} aria-hidden="true" />
          ) : (
            <PlatformIcon platform={platform} size={15} monochrome />
          )}
          {feedLabels[platform]}
        </button>
      ))}
    </fieldset>
  );
}
