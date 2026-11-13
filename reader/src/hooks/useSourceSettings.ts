import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useState, useSyncExternalStore } from "react";
import {
  addSource,
  fetchSourceSettings,
  ReaderRequestError,
  saveSource,
} from "../lib/api";
import {
  duplicateOf,
  prepared,
  type SourceDraft,
  unconfirmed,
} from "../lib/sourceSettings";
import type { SavedSource, SourceSettings } from "../lib/types";

// Shared with useSourceChanges, which refetches the sidebar when it changes.
const SETTINGS_KEY = ["source-settings"];

/** Every saved source, for the screens that edit them. */
export function useSourceSettings() {
  return useQuery({
    queryKey: SETTINGS_KEY,
    queryFn: ({ signal }) => fetchSourceSettings(signal),
  });
}

export type SaveOutcome =
  | { status: "saved"; source: SavedSource }
  /** Another source has these settings; `existing` when it could be found. */
  | { status: "duplicate"; existing?: SavedSource; message: string }
  /** The server refused it; its fields say where. */
  | { status: "invalid"; error: ReaderRequestError }
  /** The save may or may not have happened. Sending it again is safe. */
  | { status: "unconfirmed"; message: string };

/**
 * Saves whole sources and keeps the shared list current. `id` is null for a
 * new source, which the server names.
 */
export function useSaveSource() {
  const client = useQueryClient();
  const [saving, setSaving] = useState(false);
  const save = useCallback(
    async (
      id: string | null,
      draft: SourceDraft,
      // An earlier attempt at this same save went unconfirmed.
      retrying = false,
    ): Promise<SaveOutcome> => {
      const body = prepared(draft);
      setSaving(true);

      try {
        const source = id ? await saveSource(id, body) : await addSource(body);
        store(client, source);

        return { status: "saved", source };
      } catch (error) {
        if (unconfirmed(error)) {
          return {
            status: "unconfirmed",
            message:
              error instanceof ReaderRequestError
                ? error.message
                : "The reader couldn't reach DANK.",
          };
        }

        const refused = error as ReaderRequestError;

        if (refused.code !== "duplicate_source")
          return { status: "invalid", error: refused };

        const existing = await client
          .fetchQuery({
            queryKey: SETTINGS_KEY,
            queryFn: ({ signal }) => fetchSourceSettings(signal),
            staleTime: 0,
          })
          .then(
            (list) => duplicateOf(list.sources, body, id ?? undefined),
            () => undefined,
          );

        // A repeated add is refused as a duplicate once the first one worked.
        if (!id && retrying && existing)
          return { status: "saved", source: existing };

        return { status: "duplicate", existing, message: refused.message };
      } finally {
        setSaving(false);
      }
    },
    [client],
  );

  return { save, saving };
}

function store(
  client: ReturnType<typeof useQueryClient>,
  source: SavedSource,
): void {
  const current = client.getQueryData<SourceSettings>(SETTINGS_KEY);

  if (!current) {
    void client.invalidateQueries({ queryKey: SETTINGS_KEY });

    return;
  }

  client.setQueryData<SourceSettings>(SETTINGS_KEY, {
    ...current,
    sources: current.sources.some((item) => item.id === source.id)
      ? current.sources.map((item) => (item.id === source.id ? source : item))
      : [...current.sources, source],
  });
}

interface Editing {
  draft: SourceDraft;
  /** What the server had when editing began. */
  baseline: SourceDraft;
}

// Unsaved edits outlive the editor, so leaving it to check something and
// coming back loses nothing. They last until the page reloads.
const editing = new Map<string, Editing>();
const listeners = new Set<() => void>();
let version = 0;

function changed(): void {
  version += 1;
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);

  return () => listeners.delete(listener);
}

/**
 * The edits in progress for one source, keyed by its ID, or a name of the
 * caller's for a source that isn't saved yet.
 */
export function useSourceDraft(key: string, saved: SourceDraft) {
  useSyncExternalStore(subscribe, () => version);
  const current = editing.get(key);

  return {
    draft: current?.draft ?? saved,
    baseline: current?.baseline ?? saved,
    edited: current !== undefined,
    change: (draft: SourceDraft) => {
      editing.set(key, { draft, baseline: current?.baseline ?? saved });
      changed();
    },
    discard: () => {
      if (editing.delete(key)) changed();
    },
  };
}

/** The keys of sources with unsaved edits. */
export function useEditedSources(): ReadonlySet<string> {
  useSyncExternalStore(subscribe, () => version);

  return new Set(editing.keys());
}
