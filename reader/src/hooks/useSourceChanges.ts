import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { fetchSourceSettings } from "../lib/api";
import type { SourceSettings } from "../lib/types";

// How soon a source saved in another tab or browser reaches this one. The
// list is small (200 sources took a median 2.46 ms in DEN-22's prototype),
// unlike the sidebar's counts, so this polls the list and refetches the
// sidebar only when the list changes.
export const SOURCE_POLL_MS = 5_000;

// Queries built from the sources, refetched after an edit.
const NAVIGATION_QUERIES = ["sources", "youtube-sources"];

// A failed check before any list arrived, so the next one can't be compared.
const MISSED = Symbol("missed");

function watching(state: { data?: SourceSettings }): boolean {
  // A failure, such as a passing Postgres outage, is tried again at the
  // next check.
  return state.data?.in_database !== false;
}

/**
 * Watch the saved sources while they are the ones in use.
 *
 * Checks only while the tab is in the foreground, plus once on returning
 * to it, and refetches the sidebar when the list changes.
 */
export function useSourceChanges(inDatabase: boolean): void {
  const client = useQueryClient();
  const { data, isError } = useQuery({
    queryKey: ["source-settings"],
    queryFn: ({ signal }) => fetchSourceSettings(signal),
    enabled: inDatabase,
    retry: false,
    refetchInterval: (query) =>
      watching(query.state) ? SOURCE_POLL_MS : false,
    refetchOnWindowFocus: (query) => (watching(query.state) ? "always" : false),
  });
  // Structural sharing keeps the same object while nothing changes.
  const seen = useRef<SourceSettings | typeof MISSED | undefined>(undefined);

  useEffect(() => {
    if (data === undefined) {
      if (isError) seen.current = MISSED;

      return;
    }

    const previous = seen.current;
    seen.current = data;

    // The sidebar loaded alongside the first list, so it's already current.
    if (previous === undefined || previous === data) return;

    void client.invalidateQueries({
      predicate: (query) =>
        NAVIGATION_QUERIES.includes(String(query.queryKey[0])),
    });
  }, [client, data, isError]);
}
