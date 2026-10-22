import { useQuery } from "@tanstack/react-query";
import { normalizedLinkUrl } from "../lib/linkIdentity";
import { fetchReferenceEvidence } from "../lib/referenceEvidence";
import type { ReferenceTarget } from "../lib/referenceFocus";
import { safeUrl } from "../lib/urls";
import { SourceActions } from "./SourceActions";

export function ReferenceFocusNotice({
  target,
  domain,
  postId,
  matched,
  close,
  backLabel,
}: {
  target: ReferenceTarget;
  domain: string;
  postId: string;
  matched: boolean;
  close: () => void;
  backLabel?: string;
}) {
  const evidence = useQuery({
    queryKey: ["reference-evidence", domain, postId, target],
    queryFn: ({ signal }) =>
      fetchReferenceEvidence(domain, postId, target, signal),
    enabled: !matched,
  });
  const reference = evidence.data;
  const original =
    reference && normalizedLinkUrl(reference.reference_url)
      ? safeUrl(reference.reference_url)
      : undefined;
  const label = target.videoId ? "video link" : "link";

  return (
    <aside
      className="reference-focus-notice"
      aria-label={target.videoId ? "Video reference" : "Link reference"}
      data-reference-fallback={!matched ? "true" : undefined}
      tabIndex={-1}
    >
      <p>
        {matched
          ? `The matching ${label} is highlighted in this saved ${target.kind}.`
          : `The referenced ${target.kind === "comment" ? `comment or its ${label}` : label} is not available in the current saved post.`}
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
          {reference?.excerpt && <blockquote>{reference.excerpt}</blockquote>}
          {evidence.isPending && <p role="status">Loading saved evidence…</p>}
          {evidence.isError && (
            <p>
              Saved evidence could not be loaded.{" "}
              <button
                type="button"
                className="text-button"
                onClick={() => void evidence.refetch()}
              >
                Try again
              </button>
            </p>
          )}
          {evidence.isSuccess && !reference && (
            <p>No archived evidence is available for this reference.</p>
          )}
          <div className="reference-focus-actions">
            <button type="button" className="text-button" onClick={close}>
              {backLabel ??
                (target.videoId
                  ? "Back to video evidence"
                  : "Back to connections")}
            </button>
            {original && (
              <SourceActions
                href={original}
                target="_blank"
                rel="noopener noreferrer"
              >
                Original {target.kind}
              </SourceActions>
            )}
          </div>
        </>
      )}
    </aside>
  );
}
