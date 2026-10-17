import { useQuery } from "@tanstack/react-query";
import type { ReferenceTarget } from "../lib/referenceFocus";
import { safeUrl } from "../lib/urls";
import { fetchYouTubeItems } from "../lib/youtubeCollection";

export function ReferenceFocusNotice({
  target,
  domain,
  postId,
  matched,
  close,
}: {
  target: ReferenceTarget;
  domain: string;
  postId: string;
  matched: boolean;
  close: () => void;
}) {
  const itemId = `video:${target.videoId}`;
  const evidence = useQuery({
    queryKey: ["youtube-item", itemId],
    queryFn: ({ signal }) =>
      fetchYouTubeItems(
        { feed: "", channel: "", kind: "", q: "" },
        undefined,
        signal,
        itemId,
      ),
    enabled: !matched,
  });
  const reference = evidence.data?.items
    .find((item) => item.id === itemId)
    ?.references.find(
      (entry) =>
        entry.domain === domain &&
        entry.post_id === postId &&
        entry.kind === target.kind &&
        entry.reference_id === target.id,
    );
  const sameCapture =
    reference &&
    (!target.saved ||
      Date.parse(reference.observed_at) === Date.parse(target.saved));
  const original = safeUrl(reference?.reference_url ?? "");

  return (
    <aside
      className="reference-focus-notice"
      aria-label="Video reference"
      data-reference-fallback={!matched ? "true" : undefined}
      tabIndex={-1}
    >
      <p>
        {matched
          ? `The matching video link is highlighted in this saved ${target.kind}.`
          : `The referenced ${target.kind === "comment" ? "comment or its video link" : "video link"} is not available in the current saved post.`}
      </p>
      {target.saved && (
        <p className="reference-focus-date">
          Reference saved{" "}
          <time dateTime={target.saved}>
            {new Intl.DateTimeFormat(undefined, {
              dateStyle: "medium",
              timeStyle: "short",
              timeZone: "UTC",
            }).format(new Date(target.saved))}{" "}
            UTC
          </time>
          . You are viewing the current saved post.
        </p>
      )}
      {!matched && (
        <>
          {sameCapture && reference.excerpt && (
            <blockquote>{reference.excerpt}</blockquote>
          )}
          <div className="reference-focus-actions">
            <button type="button" className="text-button" onClick={close}>
              Back to video evidence
            </button>
            {original && (
              <a href={original} target="_blank" rel="noopener noreferrer">
                Original {target.kind}
              </a>
            )}
          </div>
        </>
      )}
    </aside>
  );
}
