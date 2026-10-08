import { ArrowUpRight, Search } from "lucide-react";
import { useState } from "react";
import type { Source } from "../lib/types";
import { SourceMark } from "./PostCard";

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
      .includes(query.toLowerCase()),
  );
  return (
    <div className="sources-view">
      <div className="search-bar">
        <Search size={18} />
        <input
          placeholder="Find a source or tag"
          aria-label="Search sources"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </div>
      <p className="sources-intro">
        The voices in your library. Open a source to explore its stories.
      </p>
      {matches.map((source) => (
        <button
          type="button"
          className="source-row"
          key={source.domain}
          onClick={() => select(source)}
        >
          <SourceMark domain={source.domain} />
          <span>
            <strong>{source.name}</strong>
            <small>{source.domain}</small>
            <span className="source-tags">
              {source.tags.map((tag) => (
                <span key={tag}>{tag}</span>
              ))}
            </span>
          </span>
          <span className="source-total">
            {source.count} stories
            <ArrowUpRight size={16} />
          </span>
        </button>
      ))}
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
