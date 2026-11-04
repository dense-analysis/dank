import { useQuery } from "@tanstack/react-query";
import { fetchCategories } from "../lib/api";
import type { Filters } from "../lib/types";

export function CategoryFilters({
  filters,
  change,
}: {
  filters: Filters;
  change: (filters: Filters) => void;
}) {
  const categories = useQuery({
    queryKey: ["categories"],
    queryFn: ({ signal }) => fetchCategories(signal),
  });
  const names = [
    ...new Set([...(categories.data ?? []), ...filters.categories]),
  ].sort();

  return (
    <div className="category-controls">
      <fieldset className="platform-filters" aria-label="Filter by category">
        <legend>Categories</legend>
        <button
          type="button"
          aria-pressed={filters.categories.length === 0}
          onClick={() => change({ ...filters, categories: [] })}
        >
          All categories
        </button>
        {names.map((name) => (
          <button
            key={name}
            type="button"
            aria-pressed={filters.categories.includes(name)}
            onClick={() =>
              change({
                ...filters,
                categories: [name],
              })
            }
          >
            {name}
          </button>
        ))}
      </fieldset>
      {categories.isPending && <p role="status">Loading categories…</p>}
      {categories.isError && (
        <p role="alert">
          Could not load categories.{" "}
          <button type="button" onClick={() => void categories.refetch()}>
            Retry
          </button>
        </p>
      )}
      {categories.isSuccess && !names.length && (
        <p>No categories configured.</p>
      )}
    </div>
  );
}
