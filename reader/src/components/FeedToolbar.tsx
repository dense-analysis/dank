import { SlidersHorizontal, X } from "lucide-react";
import { useEffect, useState } from "react";
import { sourceLabel } from "../lib/collections";
import { formatCount } from "../lib/counts";
import { calendarDate, recentDates } from "../lib/dateRanges";
import { availableFlairs } from "../lib/reddit";
import type { Filters, Source } from "../lib/types";
import { Count } from "./Count";
import { SearchField } from "./SearchField";

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
  useEffect(() => {
    setDraft(filters.q);
  }, [filters.q]);
  useEffect(() => {
    setAuthorDraft(filters.author);
  }, [filters.author]);
  const searchLabel =
    filters.kind === "hacker_news"
      ? "Search Hacker News"
      : filters.kind === "reddit"
        ? filters.community
          ? `Search r/${filters.community}`
          : "Search Reddit"
        : filters.kind === "x"
          ? filters.account
            ? `Search @${filters.account}`
            : filters.xFeed === "for_you"
              ? "Search For You"
              : "Search X"
          : filters.domains.length === 1
            ? `Search ${sourceLabel(filters.domains[0])}`
            : filters.kind === "stories"
              ? filters.blogFeed === "external"
                ? "Search external articles"
                : filters.blogFeed === "sources"
                  ? "Search source feeds"
                  : "Search blogs"
              : "Search everything";
  const activeCount =
    filters.domains.length +
    filters.tags.length +
    filters.flairs.length +
    Number(!!filters.author) +
    Number(!!filters.xFeed && !!filters.account) +
    Number(!!filters.after) +
    Number(!!filters.before);
  const tags = [...new Set(sources.flatMap((source) => source.tags))].sort();
  const flairs =
    filters.kind === "reddit"
      ? availableFlairs(sources, filters.community)
      : [];
  return (
    <div className="feed-tools">
      {filters.xFeed === "for_you" && (
        <fieldset className="x-feed-selection" aria-label="For You view">
          <legend className="visually-hidden">For You view</legend>
          {(
            [
              ["posts", "Posts"],
              ["authors", "Authors"],
            ] as const
          ).map(([xTab, label]) => (
            <button
              key={xTab}
              type="button"
              aria-pressed={filters.xTab === xTab}
              onClick={() =>
                change({
                  ...filters,
                  xTab,
                  account: xTab === "authors" ? "" : filters.account,
                })
              }
            >
              {label}
            </button>
          ))}
        </fieldset>
      )}
      <SearchField
        label={searchLabel}
        value={draft}
        onChange={setDraft}
        onSubmit={() =>
          change({
            ...filters,
            q: draft.trim(),
            sort: draft.trim() ? "relevance" : "newest",
          })
        }
        onClear={() => {
          setDraft("");
          change({ ...filters, q: "", sort: "newest" });
        }}
      />
      {filters.kind === "x" && !filters.xFeed && (
        <fieldset className="x-feed-selection" aria-label="X post selection">
          <legend className="visually-hidden">X post selection</legend>
          {(
            [
              ["posts", "Posts"],
              ["replies", "Posts & replies"],
            ] as const
          ).map(([value, label]) => (
            <button
              key={value}
              type="button"
              aria-pressed={filters.xView === value}
              onClick={() => change({ ...filters, xView: value })}
            >
              {label}
            </button>
          ))}
        </fieldset>
      )}
      <div className="filter-row">
        <button
          type="button"
          className={`filter-button ${activeCount ? "has-filters" : ""}`}
          onClick={() => setExpanded(!expanded)}
          aria-expanded={expanded}
        >
          <SlidersHorizontal size={15} />
          Filters
          {activeCount > 0 && <Count value={activeCount} />}
        </button>
        {filters.xFeed && filters.xTab === "authors" ? (
          <span className="result-note x-author-order">
            Most recommended first
          </span>
        ) : (
          <select
            className="sort-select"
            aria-label="Order stories"
            value={filters.sort}
            onChange={(event) =>
              change({
                ...filters,
                sort: event.target.value as Filters["sort"],
              })
            }
          >
            <option value="newest">
              {filters.xFeed ? "Recent first" : "Latest first"}
            </option>
            <option value="oldest">
              {filters.xFeed ? "Earliest first" : "Oldest first"}
            </option>
            {filters.q && <option value="relevance">Most relevant</option>}
          </select>
        )}
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
                  platforms: [],
                  domains: [],
                  tags: [],
                  flairs: [],
                  author: "",
                  authorExact: false,
                  after: "",
                  before: "",
                  account: filters.xFeed ? "" : filters.account,
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
                  ? `${formatCount(filters.domains.length)} selected sources`
                  : "All sources"}
              </option>
              {sources.map((source) => (
                <option key={source.domain} value={source.domain}>
                  {sourceLabel(source.name)}
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
          {flairs.length > 0 && (
            <fieldset className="tag-field flair-field">
              <legend>Flair</legend>
              <div className="tag-options">
                {flairs.map((flair) => (
                  <button
                    type="button"
                    key={flair.name}
                    className={`tag ${filters.flairs.includes(flair.name) ? "chosen" : ""}`}
                    aria-pressed={filters.flairs.includes(flair.name)}
                    title={`${flair.count.toLocaleString()} collected posts`}
                    onClick={() =>
                      change({
                        ...filters,
                        flairs: filters.flairs.includes(flair.name)
                          ? filters.flairs.filter(
                              (value) => value !== flair.name,
                            )
                          : [...filters.flairs, flair.name],
                      })
                    }
                  >
                    {flair.name}{" "}
                    <span className="flair-count" aria-hidden="true">
                      <Count value={flair.count} />
                    </span>
                  </button>
                ))}
              </div>
              <p>Match any selected flair in collected posts.</p>
            </fieldset>
          )}
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
          {filters.xFeed && filters.account && (
            <button
              type="button"
              aria-label="Remove recommended author filter"
              onClick={() => change({ ...filters, account: "" })}
            >
              @{filters.account}
              <X size={12} />
            </button>
          )}
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
              aria-label={`Remove source filter: ${sourceLabel(domain)}`}
              onClick={() =>
                change({
                  ...filters,
                  domains: filters.domains.filter((value) => value !== domain),
                })
              }
            >
              <span className="filter-chip-label">
                Source: {sourceLabel(domain)}
              </span>
              <X size={12} />
            </button>
          ))}
          {filters.flairs.map((flair) => (
            <button
              type="button"
              key={flair}
              aria-label={`Remove flair filter: ${flair}`}
              onClick={() =>
                change({
                  ...filters,
                  flairs: filters.flairs.filter((value) => value !== flair),
                })
              }
            >
              <span className="filter-chip-label">Flair: {flair}</span>
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
