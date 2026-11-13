import { BookmarkCheck, BookmarkPlus, CircleAlert } from "lucide-react";
import { useId, useState } from "react";
import { useSaveSource, useSourceSettings } from "../hooks/useSourceSettings";
import type { RefreshScope } from "../lib/refresh";
import {
  collectScope,
  draftOf,
  fieldMessages,
  newDraft,
  platformLabels,
  type SourceDraft,
  type SourceIdentity,
  savedMatches,
  sourceSummary,
  sourcesAccepting,
  withIdentity,
} from "../lib/sourceSettings";
import type { SavedSource } from "../lib/types";
import { Modal } from "./Modal";

const nouns = {
  rss: "source",
  x: "account",
  reddit: "community",
  youtube: "channel",
  hacker_news: "source",
} as const;

/**
 * Saves the account, community, channel or site on screen as something DANK
 * collects, reusing the identity already captured with its posts.
 */
export function SaveSourceAction({
  identity,
  collect,
  edit,
}: {
  identity: SourceIdentity;
  collect: (scope: RefreshScope) => Promise<unknown>;
  /** Opens a saved source in the editor. */
  edit: (id: string) => void;
}) {
  const settings = useSourceSettings();
  const [open, setOpen] = useState(false);

  // Saving does nothing while DANK still collects from config.toml.
  if (!settings.data?.in_database) return null;

  const matches = savedMatches(settings.data.sources, identity);
  const noun = nouns[identity.platform];

  if (matches[0]?.collecting) {
    return (
      <button
        type="button"
        className="save-source-button saved"
        title={`DANK collects ${identity.label}. Edit its source.`}
        onClick={() => edit(matches[0].source.id)}
      >
        <BookmarkCheck size={17} aria-hidden="true" />
        <span>Saved</span>
      </button>
    );
  }

  return (
    <>
      <button
        type="button"
        className="save-source-button"
        aria-haspopup="dialog"
        title={
          matches.length
            ? `${identity.label} is saved but turned off`
            : `Collect ${identity.label} from now on`
        }
        onClick={() => setOpen(true)}
      >
        <BookmarkPlus size={17} aria-hidden="true" />
        <span>{matches.length ? "Turn on" : `Save ${noun}`}</span>
      </button>
      {open && (
        <SaveSourceDialog
          identity={identity}
          sources={settings.data.sources}
          off={matches[0]?.source}
          collect={collect}
          edit={edit}
          close={() => setOpen(false)}
        />
      )}
    </>
  );
}

function SaveSourceDialog({
  identity,
  sources,
  off,
  collect,
  edit,
  close,
}: {
  identity: SourceIdentity;
  sources: readonly SavedSource[];
  /** The source that already has this identity, turned off. */
  off?: SavedSource;
  collect: (scope: RefreshScope) => Promise<unknown>;
  edit: (id: string) => void;
  close: () => void;
}) {
  const id = useId();
  const { save, saving } = useSaveSource();
  // Two sources on one platform differ in their options, so adding to one
  // is a choice rather than something to guess from the domain.
  const candidates = off ? [off] : sourcesAccepting(sources, identity);
  const [chosen, setChosen] = useState(candidates[0]?.id ?? "new");
  const [problem, setProblem] = useState<{
    text: string;
    existing?: SavedSource;
    retry?: boolean;
  }>();
  const [savedOnly, setSavedOnly] = useState(false);
  const noun = nouns[identity.platform];

  async function submit(andCollect: boolean) {
    const target = candidates.find((source) => source.id === chosen);
    const before: SourceDraft | undefined = target && draftOf(target);
    const draft = withIdentity(before ?? newDraft(identity.platform), identity);
    const outcome = await save(target?.id ?? null, draft, problem?.retry);

    switch (outcome.status) {
      case "saved":
        if (andCollect) {
          try {
            await collect(collectScope(draftOf(outcome.source), before));
          } catch (error) {
            // Saved all the same: only collecting needs another go.
            setSavedOnly(true);
            setProblem({
              text: `Saved, but collection couldn't start: ${error instanceof Error ? error.message : "try Refresh."}`,
            });

            return;
          }
        }

        close();

        break;
      case "duplicate":
        setProblem({
          text: `A source already collects exactly this.`,
          existing: outcome.existing,
        });

        break;
      case "invalid":
        setProblem({
          text:
            [...fieldMessages(outcome.error).values()][0] ??
            outcome.error.message,
        });

        break;
      case "unconfirmed":
        setProblem({
          text: `${outcome.message} The save isn't confirmed. Trying again is safe.`,
          retry: true,
        });
    }
  }

  return (
    <Modal
      label={`Save ${identity.label}`}
      className="save-source-dialog"
      onClose={close}
      dismissOnBackdrop
    >
      <h2>
        {off ? "Turn on" : "Save"} {identity.label}
      </h2>
      <p className="settings-help">
        {off
          ? `This ${noun} is saved but turned off. Turning it on collects it again.`
          : `DANK collects this ${noun} from now on. Posts already saved stay as they are.`}
      </p>
      {!off && candidates.length > 1 && (
        <fieldset className="save-source-choice">
          <legend>Add it to</legend>
          {[...candidates, undefined].map((source) => (
            <label key={source?.id ?? "new"}>
              <input
                type="radio"
                name={`${id}-source`}
                checked={chosen === (source?.id ?? "new")}
                onChange={() => setChosen(source?.id ?? "new")}
              />
              <span>
                {source ? (
                  <>
                    <strong>{sourceSummary(source)}</strong>
                    <small>
                      {[
                        source.enabled ? "" : "Turned off",
                        source.reddit &&
                          `${source.reddit.sort}, past ${source.reddit.time_filter}`,
                        ...source.tags,
                      ]
                        .filter(Boolean)
                        .join(" · ")}
                    </small>
                  </>
                ) : (
                  <strong>
                    A new {platformLabels[identity.platform]} source
                  </strong>
                )}
              </span>
            </label>
          ))}
        </fieldset>
      )}
      {problem && (
        <p className="source-notice problem" role="alert">
          <CircleAlert size={16} aria-hidden="true" />
          <span>{problem.text}</span>
          {problem.existing && (
            <button
              type="button"
              className="text-button"
              onClick={() => {
                close();
                if (problem.existing) edit(problem.existing.id);
              }}
            >
              Open it
            </button>
          )}
        </p>
      )}
      <div className="source-actions">
        {savedOnly ? (
          <button type="button" className="primary-button" onClick={close}>
            Done
          </button>
        ) : (
          <>
            <button
              type="button"
              className="primary-button"
              disabled={saving}
              onClick={() => void submit(true)}
            >
              {saving ? "Saving…" : "Save and collect"}
            </button>
            <button
              type="button"
              className="secondary-button"
              disabled={saving}
              onClick={() => void submit(false)}
            >
              Save
            </button>
            <button type="button" className="text-button" onClick={close}>
              Cancel
            </button>
          </>
        )}
      </div>
    </Modal>
  );
}
