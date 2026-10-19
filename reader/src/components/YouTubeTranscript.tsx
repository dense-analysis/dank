import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { FileText, LoaderCircle, RefreshCw } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { fetchYouTubeTranscript } from "../lib/youtubeTranscript";

export function YouTubeTranscript({ videoId }: { videoId: string }) {
  const client = useQueryClient();
  const attempted = useRef(false);
  const [now, setNow] = useState(Date.now);
  const queryKey = ["youtube-transcript", videoId];
  const query = useQuery({
    queryKey,
    queryFn: ({ signal }) => fetchYouTubeTranscript(videoId, false, signal),
    retry: false,
    refetchOnWindowFocus: false,
  });
  const mutation = useMutation({
    mutationKey: ["youtube-transcript", videoId],
    mutationFn: () => fetchYouTubeTranscript(videoId, true),
    retry: false,
    onSuccess: (result) => client.setQueryData(queryKey, result),
  });
  const transcript = query.data;
  const { mutate } = mutation;
  const retryAt = Date.parse(transcript?.retry_after || "");
  const waiting = Number.isFinite(retryAt) && retryAt > now;
  const busy = query.isPending || mutation.isPending;

  useEffect(() => {
    if (!transcript || attempted.current) return;
    attempted.current = true;
    if (
      transcript.status !== "captured" &&
      (!transcript.retry_after ||
        Date.parse(transcript.retry_after) <= Date.now())
    )
      mutate();
  }, [transcript, mutate]);

  useEffect(() => {
    if (!waiting) return;
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.min(retryAt - now + 100, 24 * 60 * 60 * 1000),
    );

    return () => window.clearTimeout(timer);
  }, [waiting, retryAt, now]);

  const error = mutation.error ?? query.error;
  const captured = transcript?.status === "captured";

  return (
    <section className="yt-transcript" aria-label="Video transcript">
      <div className="yt-shelf-heading">
        <h2>
          <FileText size={18} /> Transcript
        </h2>
        {captured && (
          <span className="yt-transcript-language">
            {transcript.language_name || transcript.language} ·{" "}
            {transcript.generated ? "Automatic captions" : "Creator captions"}
          </span>
        )}
      </div>
      {busy && (
        <p className="yt-capture-note" role="status">
          <LoaderCircle size={15} className="yt-metadata-spinner" />
          {mutation.isPending
            ? "Checking YouTube for available captions…"
            : "Loading saved transcript…"}
        </p>
      )}
      {captured && (
        <>
          <p className="yt-transcript-note">
            Saved {new Date(transcript.observed_at).toLocaleString()}.
            Timestamps open that moment on YouTube.
          </p>
          <section
            className="yt-transcript-segments"
            aria-label="Saved transcript text"
          >
            {transcript.segments.map((segment) => (
              <p key={`${segment.start}:${segment.duration}:${segment.text}`}>
                <a
                  href={`https://www.youtube.com/watch?v=${videoId}&t=${Math.floor(segment.start)}s`}
                  target="_blank"
                  rel="noreferrer"
                  aria-label={`${timestamp(segment.start)} on YouTube`}
                >
                  {timestamp(segment.start)}
                </a>
                <span>{segment.text}</span>
              </p>
            ))}
          </section>
        </>
      )}
      {!busy && !captured && (
        <>
          <p role="status">
            {error
              ? error.message
              : transcript?.status === "unavailable"
                ? "No supported captions were available when this video was checked."
                : transcript?.status === "failed"
                  ? "Couldn't fetch captions from YouTube. This doesn't mean the video has no transcript."
                  : "A transcript hasn't been collected for this video yet."}
          </p>
          {waiting && (
            <p className="yt-transcript-note">
              Checked {new Date(transcript?.observed_at ?? "").toLocaleString()}
              . You can check again after {new Date(retryAt).toLocaleString()}.
            </p>
          )}
          <button
            className="text-button"
            type="button"
            disabled={waiting}
            onClick={() => (query.isError ? void query.refetch() : mutate())}
          >
            <RefreshCw size={14} /> Try transcript again
          </button>
        </>
      )}
    </section>
  );
}

function timestamp(seconds: number): string {
  const whole = Math.floor(seconds);
  const minutes = Math.floor(whole / 60);

  return `${minutes}:${String(whole % 60).padStart(2, "0")}`;
}
