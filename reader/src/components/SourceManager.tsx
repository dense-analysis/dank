import { ChevronRight, Plus, Rss } from "lucide-react";
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
  sourceSummary,
  sourceTitle,
} from "../lib/sourceSettings";
import type { SavedSource } from "../lib/types";
import { PlatformIcon } from "./PlatformIcon";
import { SourceMark } from "./PostCard";
import { SearchField } from "./SearchField";
import { NotInUse, SourceEditor, type SourceNotice } from "./SourceEditor";

type Collecting = "all" | "on" | "off";

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
  const [adding, setAdding] = useState(false);
  const [notice, setNotice] = useState<{ id: string; notice: SourceNotice }>();
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
        initialNotice={notice?.id === source?.id ? notice?.notice : undefined}
        close={() => navigate(open(null))}
        opened={(id, said) => {
          setNotice(said && { id, notice: said });
          navigate(open(id), true);
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

  const text = query.trim().toLowerCase();
  const present = platforms.filter((item) =>
    sources.some((source) => platformOf(source.domain) === item),
  );
  const matches = sources.filter(
    (source) =>
      (platform === "all" || platformOf(source.domain) === platform) &&
      (collecting === "all" || source.enabled === (collecting === "on")) &&
      [
        sourceTitle(source),
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
      <div className="source-manager-bar">
        <SearchField
          label="Search saved sources"
          placeholder="Find a source, account or tag"
          value={query}
          onChange={setQuery}
          onClear={() => setQuery("")}
        />
        <button
          type="button"
          className="primary-button"
          aria-expanded={adding}
          onClick={() => setAdding(!adding)}
        >
          <Plus size={15} aria-hidden="true" /> Add source
        </button>
      </div>
      {adding && (
        <fieldset className="source-platforms">
          <legend>What do you want to collect?</legend>
          {platforms.map((item) => (
            <button
              type="button"
              className="secondary-button"
              key={item}
              onClick={() => {
                setAdding(false);
                navigate(open("new", { platform: item }));
              }}
            >
              {item === "rss" ? (
                <Rss size={20} aria-hidden="true" />
              ) : (
                <PlatformIcon platform={item} size={20} />
              )}
              {platformLabels[item]}
            </button>
          ))}
        </fieldset>
      )}
      {sources.length > 0 && (
        <div className="source-filters">
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
        </div>
      )}
      <ul className="saved-sources">
        {matches.map((source) => (
          <li key={source.id} className={source.enabled ? "" : "off"}>
            <a
              className="source-row"
              href={`/?${open(source.id)}`}
              onClick={(event) => {
                if (event.metaKey || event.ctrlKey || event.shiftKey) return;

                event.preventDefault();
                navigate(open(source.id));
              }}
            >
              <SourceIcon domain={source.domain} />
              <span>
                <strong>{sourceTitle(source)}</strong>
                <small>{sourceSummary(source)}</small>
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
                Edit <ChevronRight size={16} aria-hidden="true" />
              </span>
            </a>
            <label className="source-switch">
              <input
                type="checkbox"
                role="switch"
                aria-checked={source.enabled}
                checked={source.enabled}
                disabled={toggle?.id === source.id && !toggle.problem}
                aria-label={`Collect ${sourceTitle(source)}: ${sourceSummary(source)}`}
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
          <p>
            Add a blog, an X account, a Reddit community, a YouTube channel or
            Hacker News, and DANK starts collecting it.
          </p>
        </div>
      ) : (
        !matches.length && (
          <p className="empty-state">No saved sources match.</p>
        )
      )}
    </div>
  );
}
