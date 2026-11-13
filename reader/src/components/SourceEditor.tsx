import { ArrowLeft, Check, CircleAlert, RotateCcw } from "lucide-react";
import { type FormEvent, useEffect, useId, useState } from "react";
import { useSaveSource, useSourceDraft } from "../hooks/useSourceSettings";
import type { RefreshScope } from "../lib/refresh";
import {
  adapterKinds,
  collectScope,
  draftOf,
  fieldMessages,
  newDraft,
  parseTags,
  platformLabels,
  platformOf,
  type SourceAdapterDraft,
  type SourceDraft,
  type SourcePlatform,
  sameDraft,
  sourceTitle,
  type TargetList,
  targetLabel,
  targetLists,
} from "../lib/sourceSettings";
import type { SavedSource } from "../lib/types";
import { SourceTargets } from "./SourceTargets";

export type SourceNotice =
  | { kind: "saved"; text: string }
  | { kind: "problem"; text: string; existing?: SavedSource }
  | { kind: "unconfirmed"; text: string; collect: boolean };

const redditSorts = ["top", "best", "hot", "new", "rising"] as const;
const redditTimes = ["hour", "day", "week", "month", "year", "all"] as const;
const hackerNewsFilters = {
  top10: "Top 10 each day",
  top20: "Top 20 each day",
  top50: "Top 50 each day",
  all: "Every front-page story",
} as const;

/**
 * Adds or edits one source. Every save sends the whole source, and edits
 * in progress are kept if the saved source changes or the editor closes.
 */
export function SourceEditor({
  source,
  platform,
  inDatabase,
  close,
  opened,
  collect,
  initialNotice,
}: {
  /** The saved source, or undefined for a new one. */
  source?: SavedSource;
  /** A new source's platform. */
  platform: SourcePlatform;
  inDatabase: boolean;
  close: () => void;
  /** Shows another source: the one just added, or a duplicate. */
  opened: (id: string, notice?: SourceNotice) => void;
  /** What to say on opening, such as that this source was just added. */
  initialNotice?: SourceNotice;
  collect: (scope: RefreshScope) => Promise<unknown>;
}) {
  const id = useId();
  const saved = source ? draftOf(source) : newDraft(platform);
  const { draft, baseline, edited, change, discard } = useSourceDraft(
    source?.id ?? `new:${platform}`,
    saved,
  );
  const { save, saving } = useSaveSource();
  const [notice, setNotice] = useState(initialNotice);
  const [messages, setMessages] = useState(new Map<string, string>());
  const kind = source ? platformOf(source.domain) : platform;
  const dirty = edited && !sameDraft(draft, saved);
  // Someone saved this source while it was being edited here.
  const changedElsewhere = dirty && !sameDraft(baseline, saved);
  const [tags, setTags] = useState(draft.tags.join(", "));

  useEffect(() => {
    if (!dirty) return;

    const warn = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", warn);

    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);

  function update(fields: Partial<SourceDraft>) {
    change({ ...draft, ...fields });
  }

  async function submit(andCollect: boolean) {
    setNotice(undefined);
    setMessages(new Map());
    const outcome = await save(
      source?.id ?? null,
      draft,
      notice?.kind === "unconfirmed",
    );

    switch (outcome.status) {
      case "saved": {
        discard();
        let result: SourceNotice = { kind: "saved", text: "Saved." };

        if (andCollect) {
          try {
            await collect(
              collectScope(draftOf(outcome.source), source && baseline),
            );
            result = { kind: "saved", text: "Saved. Collection has started." };
          } catch (error) {
            // The source stays saved; only the collection needs another go.
            result = {
              kind: "problem",
              text: `Saved, but collection couldn't start: ${error instanceof Error ? error.message : "try Refresh."}`,
            };
          }
        }

        if (source) setNotice(result);
        else opened(outcome.source.id, result);

        break;
      }
      case "duplicate":
        setNotice({
          kind: "problem",
          text: "Another source already has exactly these settings.",
          existing: outcome.existing,
        });

        break;
      case "invalid":
        setMessages(fieldMessages(outcome.error));
        setNotice({ kind: "problem", text: outcome.error.message });

        break;
      case "unconfirmed":
        setNotice({
          kind: "unconfirmed",
          text: `${outcome.message} The save isn't confirmed. Trying again is safe.`,
          collect: andCollect,
        });
    }
  }

  function reset() {
    discard();
    setTags(saved.tags.join(", "));
    setMessages(new Map());
    setNotice(undefined);
  }

  const lists = Object.entries(targetLists[kind]) as [
    TargetList,
    NonNullable<(typeof targetLists)[SourcePlatform][TargetList]>,
  ][];

  return (
    <form
      className="source-editor"
      aria-label={source ? `Edit ${sourceTitle(draft)}` : "Add a source"}
      noValidate
      onSubmit={(event: FormEvent) => {
        event.preventDefault();
        void submit(false);
      }}
    >
      <button type="button" className="text-button" onClick={close}>
        <ArrowLeft size={15} aria-hidden="true" /> All sources
      </button>
      <h2>
        {source ? sourceTitle(draft) : `New ${platformLabels[kind]} source`}
      </h2>
      {!inDatabase && <NotInUse />}
      {changedElsewhere && (
        <p className="source-notice" role="status">
          This source was saved somewhere else while you were editing. Your
          edits are kept, and saving replaces that version.
        </p>
      )}
      <div className="image-background-setting">
        <label htmlFor={`${id}-enabled`}>
          <input
            id={`${id}-enabled`}
            type="checkbox"
            checked={draft.enabled}
            aria-describedby={`${id}-enabled-help`}
            onChange={(event) => update({ enabled: event.target.checked })}
          />
          Collect this source
        </label>
        <p className="settings-help" id={`${id}-enabled-help`}>
          Turning it off stops new collection. Everything already saved stays
          readable.
        </p>
      </div>
      {kind === "rss" && (
        <div className="source-field">
          <label htmlFor={`${id}-domain`}>Site</label>
          <input
            id={`${id}-domain`}
            type="text"
            inputMode="url"
            autoCapitalize="none"
            autoCorrect="off"
            spellCheck={false}
            placeholder="example.com"
            value={draft.domain}
            aria-invalid={messages.has("domain") || undefined}
            aria-describedby={`${id}-domain-help`}
            onChange={(event) => update({ domain: event.target.value })}
          />
          <p
            className={messages.has("domain") ? "field-error" : "settings-help"}
            id={`${id}-domain-help`}
          >
            {messages.get("domain") ??
              "The site's domain, without https:// or a path."}
          </p>
        </div>
      )}
      {lists.map(([list, copy]) => (
        <SourceTargets
          key={list}
          {...copy}
          targets={draft[list]}
          change={(targets) => update({ [list]: targets })}
          display={(value) => targetLabel(kind, list, value)}
          message={messages.get(list)}
          entryMessage={(index) => messages.get(`${list}.${index}.value`)}
        />
      ))}
      {draft.reddit && (
        <fieldset className="source-options">
          <legend>Posts to collect</legend>
          {messages.has("reddit") && (
            <p className="field-error">{messages.get("reddit")}</p>
          )}
          <label>
            Sorted by
            <select
              value={draft.reddit.sort}
              onChange={(event) =>
                draft.reddit &&
                update({
                  reddit: {
                    ...draft.reddit,
                    sort: event.target.value as (typeof redditSorts)[number],
                  },
                })
              }
            >
              {redditSorts.map((sort) => (
                <option key={sort} value={sort}>
                  {sort[0].toUpperCase() + sort.slice(1)}
                </option>
              ))}
            </select>
          </label>
          <label>
            From the past
            <select
              value={draft.reddit.time_filter}
              onChange={(event) =>
                draft.reddit &&
                update({
                  reddit: {
                    ...draft.reddit,
                    time_filter: event.target
                      .value as (typeof redditTimes)[number],
                  },
                })
              }
            >
              {redditTimes.map((time) => (
                <option key={time} value={time}>
                  {time === "all"
                    ? "All time"
                    : time[0].toUpperCase() + time.slice(1)}
                </option>
              ))}
            </select>
          </label>
        </fieldset>
      )}
      {draft.hacker_news && (
        <fieldset className="source-options">
          <legend>Stories to collect</legend>
          {messages.has("hacker_news") && (
            <p className="field-error">{messages.get("hacker_news")}</p>
          )}
          <label>
            Front page
            <select
              value={draft.hacker_news.filter}
              onChange={(event) =>
                update({
                  hacker_news: {
                    filter: event.target
                      .value as keyof typeof hackerNewsFilters,
                  },
                })
              }
            >
              {Object.entries(hackerNewsFilters).map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </fieldset>
      )}
      <div className="source-field">
        <label htmlFor={`${id}-tags`}>Tags</label>
        <input
          id={`${id}-tags`}
          type="text"
          autoCapitalize="none"
          placeholder="ai, news"
          value={tags}
          aria-describedby={`${id}-tags-help`}
          onChange={(event) => {
            setTags(event.target.value);
            update({ tags: parseTags(event.target.value) });
          }}
        />
        <p
          className={
            [...messages.keys()].some((path) => path.startsWith("tags"))
              ? "field-error"
              : "settings-help"
          }
          id={`${id}-tags-help`}
        >
          {[...messages].find(([path]) => path.startsWith("tags"))?.[1] ??
            "Separate tags with commas. They filter your reading."}
        </p>
      </div>
      {(adapterKinds(kind).length > 1 ||
        kind === "rss" ||
        draft.adapters !== null) && (
        <SourceAdapters
          kind={kind}
          adapters={draft.adapters}
          change={(adapters) => update({ adapters })}
          messages={messages}
        />
      )}
      {notice && (
        <div
          className={`source-notice ${notice.kind}`}
          role={notice.kind === "saved" ? "status" : "alert"}
        >
          {notice.kind === "saved" ? (
            <Check size={16} aria-hidden="true" />
          ) : (
            <CircleAlert size={16} aria-hidden="true" />
          )}
          <span>{notice.text}</span>
          {notice.kind === "problem" && notice.existing && (
            <button
              type="button"
              className="text-button"
              onClick={() => notice.existing && opened(notice.existing.id)}
            >
              Open it{notice.existing.enabled ? "" : " (it's turned off)"}
            </button>
          )}
          {notice.kind === "unconfirmed" && (
            <button
              type="button"
              className="text-button"
              disabled={saving}
              onClick={() => void submit(notice.collect)}
            >
              Try again
            </button>
          )}
        </div>
      )}
      <div className="source-actions">
        <button
          type="submit"
          className="primary-button"
          disabled={saving || (!!source && !dirty)}
        >
          {saving ? "Saving…" : "Save"}
        </button>
        {draft.enabled && inDatabase && (
          <button
            type="button"
            className="secondary-button"
            disabled={saving}
            title="Save, then collect this source now. A failed collection keeps the saved source."
            onClick={() => void submit(true)}
          >
            Save and collect
          </button>
        )}
        {dirty && (
          <button type="button" className="text-button" onClick={reset}>
            <RotateCcw size={14} aria-hidden="true" /> Discard changes
          </button>
        )}
      </div>
    </form>
  );
}

export function NotInUse() {
  return (
    <p className="source-notice" role="note">
      These sources aren’t in use yet. DANK still collects from{" "}
      <code>config.toml</code> until <code>sources_in_database = true</code> is
      set there and DANK restarts.
    </p>
  );
}

function feedLines(adapter: SourceAdapterDraft): string {
  return (adapter.feeds ?? []).map((feed) => feed.value).join("\n");
}

/**
 * How a source is collected, for the few that combine adapters: a publisher
 * with RSS adapters reading different feeds, or Hacker News with RSS.
 */
function SourceAdapters({
  kind,
  adapters,
  change,
  messages,
}: {
  kind: SourcePlatform;
  adapters: SourceDraft["adapters"];
  change: (adapters: SourceDraft["adapters"]) => void;
  messages: Map<string, string>;
}) {
  const kinds = adapterKinds(kind);
  const problem = [...messages].find(([path]) => path.startsWith("adapters"));

  function replace(index: number, adapter: SourceAdapterDraft) {
    change((adapters ?? []).map((item, at) => (at === index ? adapter : item)));
  }

  return (
    <details className="source-advanced" open={!!problem || undefined}>
      <summary>Advanced: adapters</summary>
      {problem && (
        <p className="field-error" role="alert">
          {problem[1]}
        </p>
      )}
      {adapters === null ? (
        <>
          <p className="settings-help">
            DANK collects this source with its default adapter,{" "}
            {platformLabels[kind]}.
          </p>
          <button
            type="button"
            className="secondary-button"
            onClick={() => change([{ kind, feeds: null }])}
          >
            Choose adapters
          </button>
        </>
      ) : (
        <>
          <ol>
            {adapters.map((adapter, index) => (
              // Adapters have no identity beyond their place in the list.
              // biome-ignore lint/suspicious/noArrayIndexKey: see above
              <li key={index}>
                <label>
                  Adapter {index + 1}
                  <select
                    value={adapter.kind}
                    onChange={(event) =>
                      replace(index, {
                        kind: event.target.value as SourcePlatform,
                        feeds: null,
                      })
                    }
                  >
                    {[...new Set([...kinds, adapter.kind])].map((option) => (
                      <option key={option} value={option}>
                        {option === "rss" ? "RSS" : platformLabels[option]}
                      </option>
                    ))}
                  </select>
                </label>
                {adapter.kind === "rss" && (
                  <label>
                    <span>
                      Its own feeds, one per line. Leave empty to use the
                      source’s feeds.
                    </span>
                    <textarea
                      rows={2}
                      spellCheck={false}
                      autoCapitalize="none"
                      value={feedLines(adapter)}
                      onChange={(event) => {
                        const was = new Map(
                          (adapter.feeds ?? []).map((feed) => [
                            feed.value,
                            feed.enabled,
                          ]),
                        );
                        const values = event.target.value.split("\n");

                        replace(index, {
                          kind: "rss",
                          feeds: event.target.value.trim()
                            ? values.map((value) => ({
                                value,
                                enabled: was.get(value) ?? true,
                              }))
                            : null,
                        });
                      }}
                    />
                  </label>
                )}
                <button
                  type="button"
                  className="text-button"
                  onClick={() =>
                    change(adapters.filter((_, at) => at !== index))
                  }
                >
                  Remove
                </button>
              </li>
            ))}
          </ol>
          <div className="source-actions">
            <button
              type="button"
              className="secondary-button"
              onClick={() =>
                change([
                  ...adapters,
                  { kind: kinds.at(-1) ?? kind, feeds: null },
                ])
              }
            >
              Add an adapter
            </button>
            <button
              type="button"
              className="text-button"
              onClick={() => change(null)}
            >
              Use the default
            </button>
          </div>
        </>
      )}
    </details>
  );
}
