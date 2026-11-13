import { Check, ChevronRight, CircleAlert } from "lucide-react";
import { useState } from "react";
import {
  useEditedSources,
  useSaveSource,
  useSourceSettings,
} from "../hooks/useSourceSettings";
import type { RefreshScope } from "../lib/refresh";
import {
  draftOf,
  platformLabels,
  platformOf,
  platforms,
  type SourcePlatform,
  sourceDetail,
  sourceTitle,
} from "../lib/sourceSettings";
import type { SavedSource } from "../lib/types";
import { PlatformIcon } from "./PlatformIcon";
import { SourceMark } from "./PostCard";
import { SearchField } from "./SearchField";
import { NotInUse, SourceEditor, type SourceNotice } from "./SourceEditor";
import { SourceQuickAdd } from "./SourceQuickAdd";

type Collecting = "all" | "on" | "off";

// A short list is read at a glance, so search and filters wait until the
// list is longer than a phone screen shows at once.
const FILTERS_FROM = 6;

function platformParam(value: string | null): SourcePlatform {
  return platforms.find((platform) => platform === value) ?? "rss";
}

export function SourceIcon({ domain }: { domain: string }) {
  const platform = platformOf(domain);

  return platform === "rss" ? (
    <SourceMark domain={domain} />
  ) : (
    <span className="source-mark source-platform">
      <PlatformIcon platform={platform} size={22} />
    </span>
  );
}

/**
 * The saved sources and their editor. `source` in the URL names the one
 * being edited, or `new` with a `platform`, so each has a link.
 */
export function SourceManager({
  params,
  navigate,
  collect,
}: {
  params: URLSearchParams;
  navigate: (params: URLSearchParams, replace?: boolean) => void;
  collect: (scope: RefreshScope) => Promise<unknown>;
}) {
  const settings = useSourceSettings();
  const edited = useEditedSources();
  const { save } = useSaveSource();
  const [query, setQuery] = useState("");
  const [platform, setPlatform] = useState<SourcePlatform | "all">("all");
  const [collecting, setCollecting] = useState<Collecting>("all");
  // What the editor said as it closed, shown above the list.
  const [notice, setNotice] = useState<SourceNotice>();
  // The source just added or saved, pointed out in the list.
  const [recent, setRecent] = useState<string>();
  const [toggle, setToggle] = useState<{ id: string; problem?: string }>();
  const editing = params.get("source");

  function open(id: string | null, fields: Record<string, string> = {}) {
    const next = new URLSearchParams({ view: "sources", manage: "1" });

    if (id) next.set("source", id);
    for (const [name, value] of Object.entries(fields)) next.set(name, value);

    return next;
  }

  if (settings.isPending) {
    return (
      <p className="empty-state" role="status">
        Loading sources…
      </p>
    );
  }

  if (settings.isError) {
    return (
      <div className="empty-state" role="alert">
        <p>{settings.error.message}</p>
        <button
          type="button"
          className="secondary-button"
          onClick={() => void settings.refetch()}
        >
          Try again
        </button>
      </div>
    );
  }

  const { sources, in_database: inDatabase } = settings.data;

  function edit(id: string) {
    setNotice(undefined);
    navigate(open(id));
  }

  if (editing) {
    const source = sources.find((item) => item.id === editing);

    if (editing !== "new" && !source) {
      return (
        <div className="empty-state" role="alert">
          <p>This source isn’t saved here any more.</p>
          <button
            type="button"
            className="secondary-button"
            onClick={() => navigate(open(null), true)}
          >
            All sources
          </button>
        </div>
      );
    }

    return (
      <SourceEditor
        // A different source is a different form, with its own edits.
        key={source?.id ?? `new:${params.get("platform")}`}
        source={source}
        platform={platformParam(params.get("platform"))}
        inDatabase={inDatabase}
        close={() => navigate(open(null))}
        open={(id) => navigate(open(id), true)}
        saved={(id, said) => {
          setNotice(said);
          setRecent(id);
          navigate(open(null), true);
        }}
        collect={collect}
      />
    );
  }

  async function switchSource(source: SavedSource) {
    setToggle({ id: source.id });
    const outcome = await save(source.id, {
      ...draftOf(source),
      enabled: !source.enabled,
    });

    setToggle(
      outcome.status === "saved"
        ? undefined
        : {
            id: source.id,
            problem:
              outcome.status === "invalid"
                ? outcome.error.message
                : outcome.message,
          },
    );
  }

  const filtering = sources.length >= FILTERS_FROM;
  const text = filtering ? query.trim().toLowerCase() : "";
  const present = platforms.filter((item) =>
    sources.some((source) => platformOf(source.domain) === item),
  );
  const matches = sources.filter(
    (source) =>
      (!filtering ||
        ((platform === "all" || platformOf(source.domain) === platform) &&
          (collecting === "all" ||
            source.enabled === (collecting === "on")))) &&
      [
        sourceTitle(source),
        platformLabels[platformOf(source.domain)],
        source.domain,
        ...source.tags,
        ...source.accounts.map((target) => target.value),
        ...source.feeds.map((target) => target.value),
      ]
        .join(" ")
        .toLowerCase()
        .includes(text),
  );

  return (
    <div className="source-manager">
      {!inDatabase && <NotInUse />}
      <SourceQuickAdd
        sources={sources}
        inDatabase={inDatabase}
        collect={collect}
        added={(source) => {
          setNotice(undefined);
          setRecent(source.id);
        }}
        edit={edit}
        setUp={(item) => {
          setNotice(undefined);
          navigate(open("new", { platform: item }));
        }}
      />
      {notice && (
        <p
          className={`source-notice ${notice.kind}`}
          role={notice.kind === "saved" ? "status" : "alert"}
        >
          {notice.kind === "saved" ? (
            <Check size={16} aria-hidden="true" />
          ) : (
            <CircleAlert size={16} aria-hidden="true" />
          )}
          <span>{notice.text}</span>
        </p>
      )}
      {filtering && (
        <>
          <SearchField
            label="Search saved sources"
            placeholder="Find a source, account or tag"
            value={query}
            onChange={setQuery}
            onClear={() => setQuery("")}
          />
          <div className="source-filters">
            {present.length > 1 && (
              <fieldset className="platform-filters">
                <legend className="visually-hidden">Platform</legend>
                {(["all", ...present] as const).map((item) => (
                  <button
                    type="button"
                    key={item}
                    aria-pressed={platform === item}
                    onClick={() => setPlatform(item)}
                  >
                    {item === "all" ? "All" : platformLabels[item]}
                  </button>
                ))}
              </fieldset>
            )}
            {sources.some((source) => !source.enabled) && (
              <fieldset className="platform-filters">
                <legend className="visually-hidden">Collection</legend>
                {(
                  [
                    ["all", "On and off"],
                    ["on", "On"],
                    ["off", "Off"],
                  ] as const
                ).map(([value, label]) => (
                  <button
                    type="button"
                    key={value}
                    aria-pressed={collecting === value}
                    onClick={() => setCollecting(value)}
                  >
                    {label}
                  </button>
                ))}
              </fieldset>
            )}
          </div>
        </>
      )}
      <ul className="saved-sources">
        {matches.map((source) => (
          <li
            key={source.id}
            className={[
              source.enabled ? "" : "off",
              recent === source.id ? "recent" : "",
            ].join(" ")}
          >
            <a
              className="source-row"
              href={`/?${open(source.id)}`}
              onClick={(event) => {
                if (event.metaKey || event.ctrlKey || event.shiftKey) return;

                event.preventDefault();
                edit(source.id);
              }}
            >
              <SourceIcon domain={source.domain} />
              <span>
                <strong>{sourceTitle(source)}</strong>
                <small>{sourceDetail(source)}</small>
                {(source.tags.length > 0 || edited.has(source.id)) && (
                  <span className="source-tags">
                    {edited.has(source.id) && (
                      <span className="unsaved">Unsaved changes</span>
                    )}
                    {source.tags.map((tag) => (
                      <span key={tag}>{tag}</span>
                    ))}
                  </span>
                )}
              </span>
              <span className="source-total">
                <span>Edit</span> <ChevronRight size={16} aria-hidden="true" />
              </span>
            </a>
            <label className="source-switch">
              <input
                type="checkbox"
                role="switch"
                aria-checked={source.enabled}
                checked={source.enabled}
                disabled={toggle?.id === source.id && !toggle.problem}
                aria-label={`Collect ${sourceTitle(source)}`}
                onChange={() => void switchSource(source)}
              />
              <span aria-hidden="true">{source.enabled ? "On" : "Off"}</span>
            </label>
            {toggle?.id === source.id && toggle.problem && (
              <p className="field-error" role="alert">
                {toggle.problem}
              </p>
            )}
          </li>
        ))}
      </ul>
      {!sources.length ? (
        <div className="empty-state">
          <h2>No sources yet</h2>
          <p>Paste something above and DANK starts collecting it.</p>
        </div>
      ) : (
        !matches.length && (
          <p className="empty-state">No saved sources match.</p>
        )
      )}
    </div>
  );
}
