import { Check, CircleAlert, Plus, Rss } from "lucide-react";
import { type FormEvent, useId, useRef, useState } from "react";
import { useSaveSource } from "../hooks/useSourceSettings";
import type { RefreshScope } from "../lib/refresh";
import {
  addPlan,
  collectScope,
  draftOf,
  fieldMessages,
  parseSource,
  platformLabels,
  platforms,
  type SourceIdentity,
  type SourcePlatform,
} from "../lib/sourceSettings";
import type { SavedSource } from "../lib/types";
import { PlatformIcon } from "./PlatformIcon";

interface Said {
  tone: "saved" | "problem" | "plain";
  text: string;
  /** A source to offer opening: the one changed, or one in the way. */
  source?: SavedSource;
  /** The source is the one this add changed. */
  added?: boolean;
  /** The save went unconfirmed, so it can be sent again. */
  retry?: boolean;
}

function reading(identity: SourceIdentity, feed?: string): string {
  if (identity.platform === "rss")
    return feed ? `${identity.label} and this feed` : identity.label;

  return identity.list
    ? `${platformLabels[identity.platform]} · ${identity.label}`
    : identity.label;
}

export function PlatformMark({
  platform,
  size,
}: {
  platform: SourcePlatform;
  size: number;
}) {
  return platform === "rss" ? (
    <Rss size={size} aria-hidden="true" />
  ) : (
    <PlatformIcon platform={platform} size={size} />
  );
}

/**
 * Adds a source from one pasted link, handle, community or domain. The
 * platform is read from the text, and the source is collected once saved.
 */
export function SourceQuickAdd({
  sources,
  inDatabase,
  collect,
  added,
  edit,
  setUp,
}: {
  sources: readonly SavedSource[];
  inDatabase: boolean;
  collect: (scope: RefreshScope) => Promise<unknown>;
  /** Called with the source that now collects what was added. */
  added: (source: SavedSource) => void;
  edit: (id: string) => void;
  /** Opens the full form for a new source. */
  setUp: (platform: SourcePlatform) => void;
}) {
  const id = useId();
  const field = useRef<HTMLInputElement>(null);
  const { save } = useSaveSource();
  const [text, setText] = useState("");
  // A bare @handle is read as an X account unless YouTube is chosen.
  const [onYouTube, setOnYouTube] = useState(false);
  const [busy, setBusy] = useState(false);
  const [said, setSaid] = useState<Said>();
  const [choosing, setChoosing] = useState(false);
  const parsed = parseSource(text);
  const found = parsed && "identity" in parsed ? parsed : null;
  const chosen =
    found?.other && onYouTube ? { identity: found.other } : (found ?? null);

  async function add(event: FormEvent) {
    event.preventDefault();

    if (!parsed || busy) return;

    if (!chosen) {
      setSaid({
        tone: "problem",
        text: "problem" in parsed ? parsed.problem : "",
      });

      return;
    }

    const { label } = chosen.identity;
    const plan = addPlan(sources, chosen);

    if (plan.collected) {
      setSaid({
        tone: "plain",
        text: `DANK already collects ${label}.`,
        source: plan.collected,
      });

      return;
    }

    setBusy(true);
    const outcome = await save(plan.id, plan.draft, said?.retry);

    switch (outcome.status) {
      case "saved": {
        let result: Said = {
          tone: "saved",
          text: `Added ${label}.`,
          source: outcome.source,
          added: true,
        };

        if (inDatabase) {
          try {
            await collect(collectScope(draftOf(outcome.source), plan.before));
            result = { ...result, text: `Added ${label}. Collecting it now.` };
          } catch (error) {
            // Added all the same: only collecting needs another go.
            result = {
              ...result,
              tone: "problem",
              text: `Added ${label}, but collection couldn't start: ${error instanceof Error ? error.message : "try Refresh."}`,
            };
          }
        }

        setText("");
        setOnYouTube(false);
        setSaid(result);
        added(outcome.source);
        field.current?.focus();

        break;
      }
      case "duplicate":
        setSaid({
          tone: "problem",
          text: "A source already collects exactly this.",
          source: outcome.existing,
        });

        break;
      case "invalid":
        setSaid({
          tone: "problem",
          text:
            [...fieldMessages(outcome.error).values()][0] ??
            outcome.error.message,
        });

        break;
      case "unconfirmed":
        setSaid({
          tone: "problem",
          text: `${outcome.message} The save isn't confirmed. Trying again is safe.`,
          retry: true,
        });
    }

    setBusy(false);
  }

  return (
    <form className="quick-add" aria-label="Add a source" onSubmit={add}>
      <label htmlFor={`${id}-text`}>Add a source</label>
      <div className="quick-add-row">
        <input
          id={`${id}-text`}
          ref={field}
          type="text"
          inputMode="url"
          autoCapitalize="none"
          autoCorrect="off"
          spellCheck={false}
          enterKeyHint="done"
          placeholder="Link, @handle or r/community"
          value={text}
          aria-describedby={`${id}-help`}
          onChange={(event) => {
            setText(event.target.value);
            setSaid(undefined);
          }}
        />
        <button
          type="submit"
          className="primary-button"
          disabled={!text.trim() || busy}
        >
          <Plus size={15} aria-hidden="true" /> {busy ? "Adding…" : "Add"}
        </button>
      </div>
      <div className="quick-add-reading" id={`${id}-help`}>
        {chosen ? (
          <>
            <span>
              <PlatformMark platform={chosen.identity.platform} size={15} />
              {reading(chosen.identity, found?.feed)}
            </span>
            {found?.other && (
              <fieldset className="platform-filters">
                <legend className="visually-hidden">This handle is on</legend>
                {(["X", "YouTube"] as const).map((name) => (
                  <button
                    type="button"
                    key={name}
                    aria-pressed={onYouTube === (name === "YouTube")}
                    onClick={() => setOnYouTube(name === "YouTube")}
                  >
                    {name}
                  </button>
                ))}
              </fieldset>
            )}
          </>
        ) : (
          <>
            <span>
              A site or feed, an X account, a Reddit community, a YouTube
              channel or Hacker News.
            </span>
            <button
              type="button"
              className="text-button"
              aria-expanded={choosing}
              onClick={() => setChoosing(!choosing)}
            >
              More options
            </button>
          </>
        )}
      </div>
      {choosing && !chosen && (
        <fieldset className="source-platforms">
          <legend>Set up a source yourself</legend>
          {platforms.map((item) => (
            <button
              type="button"
              className="secondary-button"
              key={item}
              onClick={() => setUp(item)}
            >
              <PlatformMark platform={item} size={20} />
              {platformLabels[item]}
            </button>
          ))}
        </fieldset>
      )}
      {said && (
        <p
          className={`source-notice ${said.tone}`}
          role={said.tone === "problem" ? "alert" : "status"}
        >
          {said.tone === "saved" ? (
            <Check size={16} aria-hidden="true" />
          ) : (
            <CircleAlert size={16} aria-hidden="true" />
          )}
          <span>{said.text}</span>
          {said.source && (
            <button
              type="button"
              className="text-button"
              onClick={() => said.source && edit(said.source.id)}
            >
              {said.added ? "Edit" : "Open it"}
            </button>
          )}
          {said.retry && (
            <button type="submit" className="text-button" disabled={busy}>
              Try again
            </button>
          )}
        </p>
      )}
    </form>
  );
}
