import { ArrowRight, ArrowUpRight, Network } from "lucide-react";
import { useMemo, useState } from "react";
import { readGraphState } from "../lib/relationshipGraphState";
import { safeUrl } from "../lib/urls";
import type { YouTubeItem, YouTubeReference } from "../lib/youtubeCollection";
import { youtubeReferenceGraph } from "../lib/youtubeReferenceGraph";
import { RelationshipGraph } from "./RelationshipGraph";

export default function YouTubeReferenceGraph({
  item,
  onOpenReference,
  referenceHref,
}: {
  item: YouTubeItem;
  onOpenReference: (reference: YouTubeReference) => void;
  referenceHref?: (reference: YouTubeReference) => string;
}) {
  const { graph, referencesByNode } = useMemo(
    () => youtubeReferenceGraph(item),
    [item],
  );
  const [selection, setSelection] = useState<{
    graph: string;
    node: string | null;
  }>(() => ({
    graph: item.id,
    node: readGraphState(graph)?.selectedNodeId ?? null,
  }));
  const candidate =
    selection.graph === item.id
      ? selection.node
      : readGraphState(graph)?.selectedNodeId;
  const selected = graph.nodes.find((node) => node.id === candidate);
  const selectedNodeId = selected?.id ?? null;
  const references = selected ? (referencesByNode.get(selected.id) ?? []) : [];
  const postCount = graph.nodes.filter((node) => node.kind === "post").length;
  const sourceCount = graph.nodes.filter(
    (node) => node.kind === "source",
  ).length;

  return (
    <div className="yt-reference-graph">
      <p className="yt-graph-summary">
        <Network size={16} aria-hidden="true" />
        {postCount} {postCount === 1 ? "discussion" : "discussions"} across{" "}
        {sourceCount} {sourceCount === 1 ? "source" : "sources"} link to this
        video.
      </p>
      <RelationshipGraph
        graph={graph}
        selectedNodeId={selectedNodeId}
        onSelectNode={(node) => setSelection({ graph: item.id, node })}
      >
        <section
          className="relationship-graph-details"
          aria-label="Selected connection"
          aria-live="polite"
        >
          {selected ? (
            <>
              <span className="relationship-graph-kind">
                {selected.kindLabel}
              </span>
              <h3>{selected.label}</h3>
              {selected.kind === "video" ? (
                <p>
                  {references.length} saved{" "}
                  {references.length === 1
                    ? "reference points"
                    : "references point"}{" "}
                  to this same video. Choose a post or comment to inspect the
                  matching evidence.
                </p>
              ) : (
                <div className="relationship-graph-evidence">
                  {references.map((reference) => {
                    const original = safeUrl(reference.reference_url);

                    return (
                      <article
                        key={`${reference.domain}:${reference.post_id}:${reference.kind}:${reference.reference_id}`}
                      >
                        <p className="relationship-graph-kind">
                          {reference.kind === "comment"
                            ? "Comment mention"
                            : "Post link"}
                          {reference.author && ` · ${reference.author}`}
                        </p>
                        {selected.kind !== "post" && (
                          <p className="relationship-graph-post">
                            {reference.title || reference.url}
                          </p>
                        )}
                        <blockquote>{reference.excerpt}</blockquote>
                        <div className="relationship-graph-actions">
                          {reference.reader_url && (
                            <a
                              className="relationship-graph-reader"
                              href={safeUrl(
                                referenceHref?.(reference) ||
                                  reference.reader_url,
                              )}
                              onClick={(event) => {
                                if (
                                  event.button ||
                                  event.metaKey ||
                                  event.ctrlKey ||
                                  event.shiftKey ||
                                  event.altKey
                                )
                                  return;
                                event.preventDefault();
                                onOpenReference(reference);
                              }}
                            >
                              Open {reference.kind} in reader{" "}
                              <ArrowRight size={14} />
                            </a>
                          )}
                          {original && (
                            <a
                              href={original}
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              Original {reference.kind}{" "}
                              <ArrowUpRight size={14} />
                            </a>
                          )}
                        </div>
                      </article>
                    );
                  })}
                </div>
              )}
            </>
          ) : (
            <p>
              Select a source, post or comment to follow its evidence. Arrows
              show saved links and comment relationships.
            </p>
          )}
        </section>
      </RelationshipGraph>
    </div>
  );
}
