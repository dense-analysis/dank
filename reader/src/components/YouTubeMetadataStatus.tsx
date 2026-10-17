import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Check, CircleAlert, LoaderCircle, RefreshCw } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { YouTubeItem } from "../lib/youtubeCollection";
import { collectYouTubeMetadata } from "../lib/youtubeMetadata";

export function YouTubeMetadataStatus({ item }: { item: YouTubeItem }) {
  const client = useQueryClient();
  const attempted = useRef(false);
  const capture = item.metadata_capture;
  const [now, setNow] = useState(Date.now);
  const mutation = useMutation({
    mutationKey: ["youtube-metadata", item.video_id],
    mutationFn: () => collectYouTubeMetadata(item.video_id ?? ""),
    retry: false,
    onSuccess: async () => {
      await Promise.all([
        client.invalidateQueries({ queryKey: ["youtube"] }),
        client.invalidateQueries({ queryKey: ["youtube-item", item.id] }),
      ]);
    },
  });
  const { mutate } = mutation;
  const status =
    mutation.data &&
    Date.parse(mutation.data.observed_at) >=
      Date.parse(capture?.observed_at || "1970-01-01T00:00:00Z")
      ? mutation.data
      : capture;

  useEffect(() => {
    if (!status?.retry_after) return;
    const delay = Date.parse(status.retry_after) - now;
    if (delay <= 0) return;
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.min(delay + 100, 24 * 60 * 60 * 1000),
    );

    return () => window.clearTimeout(timer);
  }, [status?.retry_after, now]);

  useEffect(() => {
    if (
      item.video_id &&
      !item.discoveries.length &&
      capture?.status === "disabled" &&
      !attempted.current
    ) {
      attempted.current = true;
      mutate();
    }
  }, [item.video_id, item.discoveries.length, capture?.status, mutate]);

  if (!item.video_id || !status) return null;

  const waiting = !!status.retry_after && Date.parse(status.retry_after) > now;
  const failed = status.status === "failed" || status.status === "unavailable";
  const available = status.available;
  const date = status.captured_at
    ? new Date(status.captured_at).toLocaleString(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      })
    : "";

  return (
    <section className="yt-metadata-status" aria-label="YouTube video details">
      {mutation.isPending ? (
        <p className="yt-capture-note" role="status">
          <LoaderCircle size={15} className="yt-metadata-spinner" />
          Getting the title, channel and thumbnail from YouTube…
        </p>
      ) : (
        <>
          {available && (
            <p className="yt-capture-note">
              <Check size={15} />
              <span>
                Video preview details saved from YouTube.
                {date && <small>Collected {date}</small>}
              </span>
            </p>
          )}
          {(failed || mutation.isError) && (
            <p className="yt-capture-note" role="status">
              <CircleAlert size={15} />
              <span>
                {mutation.isError
                  ? mutation.error.message
                  : available
                    ? "The latest refresh couldn't complete. Previously saved details are still shown."
                    : "Video details aren't available from YouTube right now."}
                {waiting && (
                  <small>
                    Retry after {new Date(status.retry_after).toLocaleString()}.
                  </small>
                )}
              </span>
            </p>
          )}
        </>
      )}
      <button
        type="button"
        className="text-button"
        onClick={() => mutate()}
        disabled={mutation.isPending || waiting}
        title={
          waiting
            ? "The saved result is cached until the next refresh."
            : undefined
        }
      >
        <RefreshCw size={13} />
        {available ? "Refresh video details" : "Get video details"}
      </button>
    </section>
  );
}
