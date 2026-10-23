import { ChevronRight } from "lucide-react";
import { useState } from "react";
import { sourceKind, sourceLabel } from "../lib/collections";
import type { Source } from "../lib/types";
import { Count } from "./Count";
import { PlatformIcon } from "./PlatformIcon";
import { SourceMark } from "./PostCard";
import { SearchField } from "./SearchField";

export function SourcesView({
  sources,
  select,
}: {
  sources: Source[];
  select: (source: Source) => void;
}) {
  const [query, setQuery] = useState("");
  const matches = sources.filter((source) =>
    `${source.name} ${source.domain} ${source.tags.join(" ")}`
      .toLowerCase()
      .includes(query.trim().toLowerCase()),
  );
  return (
    <div className="sources-view">
      <SearchField
        label="Search sources"
        placeholder="Find a source or tag"
        value={query}
        onChange={setQuery}
        onClear={() => setQuery("")}
      />
      <p className="sources-intro">
        Select a source to view its collected posts.
      </p>
      {matches.map((source) => {
        const kind = sourceKind(source.domain);

        return (
          <button
            type="button"
            className="source-row"
            key={source.domain}
            onClick={() => select(source)}
          >
            {kind === "stories" ? (
              <SourceMark domain={source.domain} />
            ) : (
              <span className="source-mark source-platform">
                <PlatformIcon platform={kind} size={22} />
              </span>
            )}
            <span>
              <strong>{sourceLabel(source.name)}</strong>
              {sourceLabel(source.name) !== sourceLabel(source.domain) && (
                <small>{sourceLabel(source.domain)}</small>
              )}
              {source.tags.length > 0 && (
                <span className="source-tags">
                  {source.tags.map((tag) => (
                    <span key={tag}>{tag}</span>
                  ))}
                </span>
              )}
            </span>
            <span className="source-total">
              <span>
                <Count value={source.count} />{" "}
                {source.count === 1 ? "post" : "posts"}
              </span>
              <ChevronRight size={16} aria-hidden="true" />
            </span>
          </button>
        );
      })}
      {!matches.length && (
        <p className="empty-state">No sources match this search.</p>
      )}
      <p className="field-help sources-footnote">
        Collection is managed in DANK’s source configuration. Selecting a source
        here filters your reading.
      </p>
    </div>
  );
}
