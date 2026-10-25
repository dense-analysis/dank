import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { fetchPost, fetchPosts } from "../lib/api";
import { parseFilters } from "../lib/navigation";
import {
  fetchRefreshJobs,
  type RefreshJob,
  refreshActive,
  refreshDestination,
  startRefresh,
} from "../lib/refresh";
import type { PostPage } from "../lib/types";
import {
  fetchYouTubeItems,
  type YouTubePage,
  youtubeFilters,
} from "../lib/youtubeCollection";

const dismissedKey = "dank-reader:refresh-dismissed:v1";
const contentQueries = new Set([
  "posts",
  "x-authors",
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

export function useRefresh(navigate: (params: URLSearchParams) => void) {
  const client = useQueryClient();
  const [dismissed, setDismissed] = useState(loadDismissed);
  const [showingJob, setShowingJob] = useState<string>();
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

  async function showUpdates(job: RefreshJob) {
    if (showingJob) return;

    setShowingJob(job.id);
    setUpdatesError(undefined);
    const destination = refreshDestination(job.scope);

    try {
      await client.invalidateQueries({
        predicate: (item) => contentQueries.has(String(item.queryKey[0])),
        refetchType: "none",
      });
      // Load the chosen destination before navigating, including when its URL
      // is already open. Other visible feeds keep their current content.
      if (destination.get("view") === "youtube") {
        const itemId = destination.get("youtube_item");
        const filters = youtubeFilters(destination);

        if (itemId) {
          await client.fetchQuery({
            queryKey: ["youtube-item", itemId],
            queryFn: ({ signal }) =>
              fetchYouTubeItems(filters, undefined, signal, itemId),
          });
        } else {
          await client.fetchInfiniteQuery({
            queryKey: ["youtube", filters],
            queryFn: ({ pageParam, signal }) =>
              fetchYouTubeItems(filters, pageParam, signal),
            initialPageParam: undefined as string | undefined,
            getNextPageParam: (last: YouTubePage) =>
              last.next_cursor ?? undefined,
          });
        }
      } else if (job.scope.kind === "post") {
        const domain = job.scope.domain ?? "";
        const postId = job.scope.post_id ?? "";
        await client.fetchQuery({
          queryKey: ["post", domain, postId],
          queryFn: ({ signal }) => fetchPost(domain, postId, signal),
        });
      } else {
        const filters = parseFilters(destination);
        await client.fetchInfiniteQuery({
          queryKey: ["posts", filters],
          queryFn: ({ pageParam, signal }) =>
            fetchPosts(filters, pageParam, signal),
          initialPageParam: undefined as string | undefined,
          getNextPageParam: (last: PostPage) => last.next_cursor ?? undefined,
        });
      }

      if (job.scope.kind === "post") {
        const { domain, post_id: postId } = job.scope;
        await client.refetchQueries({
          type: "active",
          predicate: ({ queryKey }) => {
            if (
              queryKey[0] === "youtube-transcript" &&
              domain === "youtube.com" &&
              postId?.startsWith("video:")
            ) {
              return queryKey[1] === postId.slice("video:".length);
            }

            return (
              ["post-connections", "reference-evidence"].includes(
                String(queryKey[0]),
              ) &&
              queryKey[1] === domain &&
              queryKey[2] === postId
            );
          },
        });
      }

      navigate(destination);
      dismiss([job.id]);
      // Sidebar counts may recover independently of the refreshed content.
      void client.refetchQueries({
        predicate: (item) =>
          ["sources", "youtube-sources"].includes(String(item.queryKey[0])),
        type: "active",
      });
    } catch {
      setUpdatesError(
        "Updates could not be loaded. Your saved content is still available; try again.",
      );
    } finally {
      setShowingJob(undefined);
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
    showingUpdates: !!showingJob,
    showingJob,
    updatesError,
  };
}
