import { ArrowRight, ArrowUpRight } from "lucide-react";
import type { ReactNode } from "react";
import {
  connectionNeighbor,
  connectionTargetLabel,
  type PostConnection,
  type PostConnectionEvidence,
  type PostConnectionTarget,
} from "../lib/postConnections";
import { connectionSourceLabel } from "../lib/postConnectionsGraph";
import { safeUrl } from "../lib/urls";
import { ReaderLink } from "./ReaderLink";
import { SourceActions } from "./SourceActions";

export interface ConnectionNavigation {
  onOpenEvidence: (evidence: PostConnectionEvidence) => void;
  evidenceHref: (evidence: PostConnectionEvidence) => string;
  onExploreTarget: (target: PostConnectionTarget) => void;
  targetHref: (target: PostConnectionTarget) => string | undefined;
}

export function ExploreConnection({
  target,
  onExploreTarget,
  targetHref,
}: Pick<ConnectionNavigation, "onExploreTarget" | "targetHref"> & {
  target: PostConnectionTarget;
}) {
  const destination = targetHref(target);
  const href = destination ? safeUrl(destination) : undefined;

  if ((!target.post && !target.youtube_item_id) || !href) return null;

  return (
    <ReaderLink href={href} onNavigate={() => onExploreTarget(target)}>
      {target.post ? "Explore this post" : "Explore video in DANK"}{" "}
      <ArrowRight size={14} aria-hidden="true" />
    </ReaderLink>
  );
}

function ExternalLink({ url, children }: { url: string; children: ReactNode }) {
  const href = url ? safeUrl(url) : undefined;

  return href ? (
    <SourceActions href={href} target="_blank" rel="noopener noreferrer">
      {children} <ArrowUpRight size={14} aria-hidden="true" />
    </SourceActions>
  ) : null;
}

export function ConnectionEvidence({
  connection,
  ...navigation
}: ConnectionNavigation & {
  connection: PostConnection;
}) {
  const { evidence, direction, origin } = connection;
  const neighbor = connectionNeighbor(connection);
  const href = safeUrl(navigation.evidenceHref(evidence));
  const captured = evidence.observed_at ? new Date(evidence.observed_at) : null;
  const saved =
    captured && Number.isFinite(captured.valueOf()) ? captured : null;

  return (
    <article className="post-connection-evidence">
      <p className="post-connection-kind">
        {direction === "incoming"
          ? "Incoming"
          : direction === "related"
            ? "Shared article"
            : "Outgoing"}{" "}
        · {evidence.kind === "comment" ? "Comment mention" : "Post link"}
        {evidence.author && ` · ${evidence.author}`}
      </p>
      <h4>{connectionTargetLabel(neighbor)}</h4>
      <p className="post-connection-source">
        {direction !== "outgoing"
          ? connectionSourceLabel(origin)
          : `From ${connectionSourceLabel(origin)}`}
        {!neighbor.post && (
          <span className="post-connection-state">
            {neighbor.youtube_item_id
              ? "YouTube details in DANK"
              : "Destination not collected"}
          </span>
        )}
      </p>
      {evidence.excerpt ? (
        <blockquote>{evidence.excerpt}</blockquote>
      ) : (
        <p className="post-connection-source">
          Saved destination: {evidence.matched_url}
        </p>
      )}
      <div className="post-connection-actions">
        {href && (
          <ReaderLink
            className="post-connection-open"
            href={href}
            onNavigate={() => navigation.onOpenEvidence(evidence)}
          >
            Open {evidence.kind === "comment" ? "comment" : "link"} in DANK{" "}
            <ArrowRight size={14} aria-hidden="true" />
          </ReaderLink>
        )}
        <ExploreConnection target={neighbor} {...navigation} />
        <ExternalLink url={evidence.reference_url}>
          Original {evidence.kind}
        </ExternalLink>
        <ExternalLink url={evidence.matched_url}>
          Linked destination
        </ExternalLink>
      </div>
      {saved && (
        <time className="post-connection-date" dateTime={evidence.observed_at}>
          Saved{" "}
          {new Intl.DateTimeFormat(undefined, {
            dateStyle: "medium",
            timeStyle: "short",
            timeZone: "UTC",
          }).format(saved)}{" "}
          UTC
        </time>
      )}
    </article>
  );
}
