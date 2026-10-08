import { Search, SlidersHorizontal, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { Filters, Source } from "../lib/types";

export function FeedToolbar({
  filters,
  sources,
  change,
  saveFeed,
}: {
  filters: Filters;
  sources: Source[];
  change: (filters: Filters) => void;
  saveFeed: () => void;
}) {
  const [draft, setDraft] = useState(filters.q);
  const [expanded, setExpanded] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => {
    setDraft(filters.q);
  }, [filters.q]);
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
            sort:
              draft.trim() && filters.mode === "meaning"
                ? "relevance"
                : filters.sort,
          });
        }}
      >
        <Search size={18} />
        <input
          ref={input}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          placeholder="Find your next good read"
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
        <fieldset className="segmented" aria-label="Search method">
          <button
            type="button"
            className={filters.mode === "words" ? "selected" : ""}
            aria-pressed={filters.mode === "words"}
            onClick={() =>
              change({ ...filters, mode: "words", sort: "newest" })
            }
          >
            Words
          </button>
          <button
            type="button"
            className={filters.mode === "meaning" ? "selected" : ""}
            aria-pressed={filters.mode === "meaning"}
            onClick={() =>
              change({
                ...filters,
                mode: "meaning",
                sort: filters.q ? "relevance" : "newest",
              })
            }
          >
            Meaning
          </button>
        </fieldset>
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
              value={filters.author}
              placeholder="Name or account"
              onChange={(event) =>
                change({ ...filters, author: event.target.value })
              }
            />
          </label>
          <label>
            From
            <input
              type="date"
              aria-label="From date"
              value={filters.after}
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
              <p>
                Tags select sources. Search finds words or meaning in their
                stories.
              </p>
            </fieldset>
          )}
          <button
            type="button"
            className="text-button filter-save"
            onClick={saveFeed}
          >
            Save these filters as a feed →
          </button>
        </div>
      )}
      {(filters.q || activeCount > 0) && (
        <div className="active-filters">
          <span>
            {filters.q
              ? `${filters.mode === "words" ? "Words containing" : "Meaning similar to"} “${filters.q}”`
              : "Filtered view"}
          </span>
          {filters.domains.map((domain) => (
            <button
              type="button"
              key={domain}
              onClick={() =>
                change({
                  ...filters,
                  domains: filters.domains.filter((value) => value !== domain),
                })
              }
            >
              {domain}
              <X size={12} />
            </button>
          ))}
          {filters.tags.map((tag) => (
            <button
              type="button"
              key={tag}
              onClick={() =>
                change({
                  ...filters,
                  tags: filters.tags.filter((value) => value !== tag),
                })
              }
            >
              Source: {tag}
              <X size={12} />
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
