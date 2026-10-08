import { Search, SlidersHorizontal, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { calendarDate, recentDates } from "../lib/dateRanges";
import type { Filters, Source } from "../lib/types";

export function FeedToolbar({
  filters,
  sources,
  change,
}: {
  filters: Filters;
  sources: Source[];
  change: (filters: Filters) => void;
}) {
  const [draft, setDraft] = useState(filters.q);
  const [authorDraft, setAuthorDraft] = useState(filters.author);
  const [expanded, setExpanded] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => {
    setDraft(filters.q);
  }, [filters.q]);
  useEffect(() => {
    setAuthorDraft(filters.author);
  }, [filters.author]);
  useEffect(() => {
    function key(event: KeyboardEvent) {
      if (
        event.key === "/" &&
        !(
          event.target instanceof HTMLElement &&
          (event.target.closest("input, textarea, select, dialog") ||
            event.target.isContentEditable)
        )
      ) {
        event.preventDefault();
        input.current?.focus();
      }
    }
    window.addEventListener("keydown", key);
    return () => window.removeEventListener("keydown", key);
  }, []);
  const activeCount =
    filters.domains.length +
    filters.tags.length +
    Number(!!filters.author) +
    Number(!!filters.after) +
    Number(!!filters.before);
  const tags = [...new Set(sources.flatMap((source) => source.tags))].sort();
  return (
    <div className="feed-tools">
      <form
        className="search-bar"
        onSubmit={(event) => {
          event.preventDefault();
          change({
            ...filters,
            q: draft.trim(),
            sort: draft.trim() ? "relevance" : "newest",
          });
        }}
      >
        <Search size={18} />
        <input
          ref={input}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          placeholder="Search stories"
          aria-label="Search stories"
        />
        {draft && (
          <button
            className="icon-button"
            type="button"
            aria-label="Clear search"
            onClick={() => {
              setDraft("");
              change({ ...filters, q: "", sort: "newest" });
            }}
          >
            <X size={15} />
          </button>
        )}
        <kbd>/</kbd>
        <button type="submit" className="search-submit">
          Search
        </button>
      </form>
      <div className="filter-row">
        <button
          type="button"
          className={`filter-button ${activeCount ? "has-filters" : ""}`}
          onClick={() => setExpanded(!expanded)}
          aria-expanded={expanded}
        >
          <SlidersHorizontal size={15} />
          Filters{activeCount > 0 && <span>{activeCount}</span>}
        </button>
        <select
          className="sort-select"
          aria-label="Order stories"
          value={filters.sort}
          onChange={(event) =>
            change({ ...filters, sort: event.target.value as Filters["sort"] })
          }
        >
          <option value="newest">Latest first</option>
          <option value="oldest">Oldest first</option>
          {filters.q && <option value="relevance">Most relevant</option>}
        </select>
      </div>
      {expanded && (
        <div className="filter-panel">
          <div className="filter-panel-head">
            <strong>Narrow your view</strong>
            <button
              className="text-button"
              type="button"
              onClick={() =>
                change({
                  ...filters,
                  domains: [],
                  tags: [],
                  author: "",
                  authorExact: false,
                  after: "",
                  before: "",
                })
              }
            >
              Reset filters
            </button>
          </div>
          <label>
            Source
            <select
              value={filters.domains.length === 1 ? filters.domains[0] : ""}
              onChange={(event) =>
                change({
                  ...filters,
                  domains: event.target.value ? [event.target.value] : [],
                })
              }
            >
              <option value="">
                {filters.domains.length > 1
                  ? `${filters.domains.length} selected sources`
                  : "All sources"}
              </option>
              {sources.map((source) => (
                <option key={source.domain} value={source.domain}>
                  {source.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            Author
            <input
              value={authorDraft}
              placeholder="Name or account"
              onChange={(event) => {
                setAuthorDraft(event.target.value);
                change({
                  ...filters,
                  author: event.target.value,
                  authorExact: false,
                });
              }}
            />
          </label>
          <fieldset className="date-shortcuts">
            <legend>
              Date range <span>UTC</span>
            </legend>
            <div className="tag-options">
              {([1, 7, 30] as const).map((days) => {
                const range = recentDates(days);
                const selected =
                  filters.after === range.after &&
                  filters.before === range.before;
                return (
                  <button
                    key={days}
                    type="button"
                    className={`tag ${selected ? "chosen" : ""}`}
                    aria-pressed={selected}
                    onClick={() => change({ ...filters, ...recentDates(days) })}
                  >
                    {days === 1 ? "Today" : `Past ${days} days`}
                  </button>
                );
              })}
            </div>
          </fieldset>
          <label>
            From
            <input
              type="date"
              aria-label="From date"
              value={filters.after}
              max={filters.before || undefined}
              onChange={(event) =>
                change({ ...filters, after: event.target.value })
              }
            />
          </label>
          <label>
            Through
            <input
              type="date"
              aria-label="Through date"
              value={filters.before}
              min={filters.after}
              onChange={(event) =>
                change({ ...filters, before: event.target.value })
              }
            />
          </label>
          {tags.length > 0 && (
            <fieldset className="tag-field">
              <legend>Source tags</legend>
              <div className="tag-options">
                {tags.map((tag) => (
                  <button
                    type="button"
                    key={tag}
                    className={`tag ${filters.tags.includes(tag) ? "chosen" : ""}`}
                    aria-pressed={filters.tags.includes(tag)}
                    onClick={() =>
                      change({
                        ...filters,
                        tags: filters.tags.includes(tag)
                          ? filters.tags.filter((value) => value !== tag)
                          : [...filters.tags, tag],
                      })
                    }
                  >
                    {tag}
                  </button>
                ))}
              </div>
              <p>Tags filter sources, not story topics.</p>
            </fieldset>
          )}
        </div>
      )}
      {(filters.q || activeCount > 0) && (
        <fieldset className="active-filters" aria-label="Active filters">
          <span>{filters.q ? `Search: “${filters.q}”` : "Filtered view"}</span>
          {filters.author && (
            <button
              type="button"
              aria-label="Remove author filter"
              title={filters.author}
              onClick={() =>
                change({ ...filters, author: "", authorExact: false })
              }
            >
              <span className="filter-chip-label">
                Author: {filters.author}
              </span>
              <X size={12} />
            </button>
          )}
          {filters.after && (
            <button
              type="button"
              aria-label="Remove start date filter"
              onClick={() => change({ ...filters, after: "" })}
            >
              <span className="filter-chip-label">
                From: {calendarDate(filters.after)} UTC
              </span>
              <X size={12} />
            </button>
          )}
          {filters.before && (
            <button
              type="button"
              aria-label="Remove end date filter"
              onClick={() => change({ ...filters, before: "" })}
            >
              <span className="filter-chip-label">
                Through: {calendarDate(filters.before)} UTC
              </span>
              <X size={12} />
            </button>
          )}
          {filters.domains.map((domain) => (
            <button
              type="button"
              key={domain}
              aria-label={`Remove source filter: ${domain}`}
              onClick={() =>
                change({
                  ...filters,
                  domains: filters.domains.filter((value) => value !== domain),
                })
              }
            >
              <span className="filter-chip-label">Source: {domain}</span>
              <X size={12} />
            </button>
          ))}
          {filters.tags.map((tag) => (
            <button
              type="button"
              key={tag}
              aria-label={`Remove tag filter: ${tag}`}
              onClick={() =>
                change({
                  ...filters,
                  tags: filters.tags.filter((value) => value !== tag),
                })
              }
            >
              <span className="filter-chip-label">Tag: {tag}</span>
              <X size={12} />
            </button>
          ))}
        </fieldset>
      )}
    </div>
  );
}
