import { Check, Search, X } from "lucide-react";
import { useState } from "react";
import type { Filters, SavedFeed, Source } from "../lib/types";
import { Modal } from "./Modal";

export function FeedDialog({
  initial,
  filters,
  sources,
  close,
  save,
  remove,
}: {
  initial?: SavedFeed;
  filters: Filters;
  sources: Source[];
  close: () => void;
  save: (feed: SavedFeed) => void;
  remove: (id: string) => void;
}) {
  const [name, setName] = useState(initial?.name ?? "");
  const [draft, setDraft] = useState(initial?.filters ?? filters);
  const [sourceQuery, setSourceQuery] = useState("");
  const [confirmDelete, setConfirmDelete] = useState(false);
  return (
    <Modal
      label={initial ? "Edit feed" : "Create a feed"}
      onClose={close}
      className="feed-dialog"
    >
      <div className="dialog-heading">
        <div>
          <p className="eyebrow">YOUR POINT OF VIEW</p>
          <h2>
            {initial ? "Make it your own." : "A feed worth coming back to."}
          </h2>
        </div>
        <button
          type="button"
          className="icon-button"
          onClick={close}
          aria-label="Close feed editor"
        >
          <X size={20} />
        </button>
      </div>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (name.trim())
            save({
              id: initial?.id ?? crypto.randomUUID(),
              name: name.trim(),
              filters: draft,
            });
        }}
      >
        <label>
          Feed name
          <input
            required
            maxLength={60}
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="e.g. Politics, Design, The long read"
          />
        </label>
        <div className="source-selector-heading">
          <strong>Choose existing sources</strong>
          <span>
            {draft.domains.length
              ? `${draft.domains.length} selected`
              : "All sources"}
          </span>
        </div>
        <div className="source-search">
          <Search size={15} />
          <input
            value={sourceQuery}
            onChange={(event) => setSourceQuery(event.target.value)}
            aria-label="Find a source"
            placeholder="Find a source…"
          />
        </div>
        <div className="source-checklist">
          {sources
            .filter((source) =>
              `${source.name} ${source.domain}`
                .toLowerCase()
                .includes(sourceQuery.toLowerCase()),
            )
            .map((source) => (
              <label key={source.domain}>
                <input
                  type="checkbox"
                  checked={draft.domains.includes(source.domain)}
                  onChange={(event) =>
                    setDraft({
                      ...draft,
                      domains: event.target.checked
                        ? [...draft.domains, source.domain]
                        : draft.domains.filter(
                            (value) => value !== source.domain,
                          ),
                    })
                  }
                />
                <span>
                  {source.name}
                  <small>{source.domain}</small>
                </span>
                <span className="source-count">{source.count}</span>
              </label>
            ))}
        </div>
        <p className="field-help">
          Leave all unchecked to include every source. This selects collected
          sources; it doesn’t add a source for collection.
        </p>
        <div className="feed-rules">
          <label>
            Content rule <span className="optional">optional</span>
            <input
              value={draft.q}
              onChange={(event) =>
                setDraft({ ...draft, q: event.target.value })
              }
              placeholder="e.g. housing policy"
            />
          </label>
          <label>
            Match by
            <select
              value={draft.mode}
              onChange={(event) =>
                setDraft({
                  ...draft,
                  mode: event.target.value as Filters["mode"],
                })
              }
            >
              <option value="words">Words</option>
              <option value="meaning">Meaning</option>
            </select>
          </label>
        </div>
        {(draft.tags.length > 0 ||
          draft.author ||
          draft.after ||
          draft.before) && (
          <div className="inherited-rules">
            <span>
              Also includes:{" "}
              {[
                ...draft.tags.map((tag) => `source tag: ${tag}`),
                draft.author && `author: ${draft.author}`,
                draft.after && `from ${draft.after}`,
                draft.before && `through ${draft.before}`,
              ]
                .filter(Boolean)
                .join(" · ")}
            </span>
            <button
              type="button"
              className="text-button"
              onClick={() =>
                setDraft({
                  ...draft,
                  tags: [],
                  author: "",
                  after: "",
                  before: "",
                })
              }
            >
              Clear additional rules
            </button>
          </div>
        )}
        <div className="dialog-footer">
          <small>Saved in this browser.</small>
          <button type="submit" className="primary-button">
            <Check size={16} />
            {initial ? "Save changes" : "Create feed"}
          </button>
        </div>
        {initial && (
          <button
            type="button"
            className="delete-feed"
            onClick={() =>
              confirmDelete ? remove(initial.id) : setConfirmDelete(true)
            }
          >
            {confirmDelete ? "Confirm delete feed" : "Delete feed"}
          </button>
        )}
      </form>
    </Modal>
  );
}
