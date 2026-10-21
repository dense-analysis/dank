import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import {
  fetchRefreshJobs,
  type RefreshJob,
  refreshActive,
  startRefresh,
} from "../lib/refresh";

const dismissedKey = "dank-reader:refresh-dismissed:v1";
const contentQueries = new Set([
  "posts",
  "post",
  "sources",
  "youtube",
  "youtube-item",
  "youtube-sources",
  "youtube-transcript",
  "post-connections",
  "reference-evidence",
]);

function loadDismissed(): string[] {
  try {
    const value: unknown = JSON.parse(
      localStorage.getItem(dismissedKey) ?? "[]",
    );

    return Array.isArray(value)
      ? value.filter((id): id is string => typeof id === "string").slice(-100)
      : [];
  } catch {
    return [];
  }
}

export function useRefresh() {
  const client = useQueryClient();
  const [dismissed, setDismissed] = useState(loadDismissed);
  const [showingUpdates, setShowingUpdates] = useState(false);
  const [updatesError, setUpdatesError] = useState<string>();
  const query = useQuery({
    queryKey: ["refresh-jobs"],
    queryFn: ({ signal }) => fetchRefreshJobs(signal),
    refetchInterval: (current) =>
      current.state.data?.some(refreshActive) ? 1500 : 15_000,
    refetchIntervalInBackground: true,
  });
  const mutation = useMutation({
    mutationFn: startRefresh,
    onSuccess: (job) => {
      client.setQueryData<RefreshJob[]>(["refresh-jobs"], (jobs = []) => [
        job,
        ...jobs.filter((existing) => existing.id !== job.id),
      ]);
      void query.refetch();
    },
  });
  const jobs = query.data ?? [];
  const visible = jobs.filter(
    (job) => refreshActive(job) || !dismissed.includes(job.id),
  );

  function dismiss(ids: string[]) {
    setDismissed((current) => {
      const next = [...new Set([...current, ...ids])].slice(-100);

      try {
        localStorage.setItem(dismissedKey, JSON.stringify(next));
      } catch {
        // Dismissing still works for this visit when browser storage is unavailable.
      }

      return next;
    });
  }

  async function showUpdates() {
    setShowingUpdates(true);
    setUpdatesError(undefined);

    try {
      await client.invalidateQueries(
        { predicate: (item) => contentQueries.has(String(item.queryKey[0])) },
        { throwOnError: true },
      );
      dismiss(
        visible
          .filter(
            (job) =>
              !refreshActive(job) &&
              (job.status === "completed" ||
                job.status === "partial" ||
                job.posts_saved > 0),
          )
          .map((job) => job.id),
      );
    } catch {
      setUpdatesError(
        "Updates could not be loaded. Your saved content is still available; try again.",
      );
    } finally {
      setShowingUpdates(false);
    }
  }

  return {
    jobs,
    visible,
    start: mutation.mutate,
    starting: mutation.isPending,
    startingScope: mutation.variables,
    startError: mutation.error?.message,
    clearError: mutation.reset,
    statusError: query.error?.message,
    retryStatus: () => void query.refetch(),
    dismiss,
    showUpdates,
    showingUpdates,
    updatesError,
  };
}
