import { useMemo, useState } from "react";
import type { PostConnectionsData } from "../lib/postConnections";
import {
  type ConnectionDirection,
  postConnectionsGraph,
} from "../lib/postConnectionsGraph";
import { readGraphState } from "../lib/relationshipGraphState";
import { dateLabel } from "../lib/timestamps";
import type { CollectionInfo } from "../lib/types";
import {
  ConnectionEvidence,
  type ConnectionNavigation,
  ExploreConnection,
} from "./ConnectionEvidence";
import { RelationshipGraph } from "./RelationshipGraph";

export default function PostConnectionsGraph({
  data,
  collectionInfo,
  direction,
  onDirectionChange,
  ...navigation
}: ConnectionNavigation & {
  data: PostConnectionsData;
  collectionInfo?: CollectionInfo | null;
  direction: ConnectionDirection;
  onDirectionChange: (direction: ConnectionDirection) => void;
}) {
  const {
    graph,
    connectionsByNode,
    targetsByNode,
    rootNodeId,
    discoveryNodeId,
  } = useMemo(
    () => postConnectionsGraph(data, direction, collectionInfo),
    [data, direction, collectionInfo],
  );
  const [selection, setSelection] = useState<{
    graph: string;
    node: string | null;
  }>(() => ({
    graph: graph.id,
    node: readGraphState(graph)?.selectedNodeId ?? null,
  }));
  const candidate =
    selection.graph === graph.id
      ? selection.node
      : readGraphState(graph)?.selectedNodeId;
  const selected = graph.nodes.find((node) => node.id === candidate);
  const connections = connectionsByNode.get(selected?.id ?? rootNodeId) ?? [];
  const target = selected ? targetsByNode.get(selected.id) : undefined;

  return (
    <div className="post-connections-graph">
      <fieldset
        className="post-connections-directions"
        aria-label="Graph direction"
      >
        {(["all", "incoming", "outgoing"] as const).map((value) => (
          <button
            type="button"
            key={value}
            aria-pressed={direction === value}
            onClick={() => onDirectionChange(value)}
          >
            {value === "all"
              ? "All connections"
              : value === "incoming"
                ? "Incoming links"
                : "Outgoing links"}
          </button>
        ))}
      </fieldset>
      {(graph.nodes.some((node) => node.kind === "article") ||
        discoveryNodeId) && (
        <p className="post-connections-scope">
          Arrows follow saved links. This layout is not a timeline.{" "}
          {discoveryNodeId && (
            <>
              First discovered records DANK’s earliest observation; its dashed
              line adds history without expanding other sources.
            </>
          )}
        </p>
      )}
      <RelationshipGraph
        graph={graph}
        initialFocusNodeId={rootNodeId}
        selectedNodeId={selected?.id ?? null}
        onSelectNode={(node) => setSelection({ graph: graph.id, node })}
      >
        <section
          className="relationship-graph-details post-connections-details"
          aria-label="Selected connection"
          aria-live="polite"
        >
          {selected ? (
            <>
              <span className="relationship-graph-kind">
                {selected.kindLabel}
              </span>
              <h3>{selected.label}</h3>
              {selected.id === discoveryNodeId && collectionInfo && (
                <div className="post-connections-discovery">
                  <p>
                    First observed by DANK:{" "}
                    {collectionInfo.first_seen_at ? (
                      <time
                        dateTime={collectionInfo.first_seen_at}
                        title={collectionInfo.first_seen_at}
                      >
                        {dateLabel(collectionInfo.first_seen_at)}
                      </time>
                    ) : (
                      "Time not recorded"
                    )}
                    .
                  </p>
                  {collectionInfo.first_seen_from.length > 1 && (
                    <p>
                      These sources share the earliest recorded observation.
                    </p>
                  )}
                  {collectionInfo.first_seen_from.some(
                    (origin) =>
                      origin.domain === data.root.domain &&
                      origin.id === data.root.id,
                  ) && (
                    <p>This post is among the first recorded discoveries.</p>
                  )}
                  {collectionInfo.post_published_at && (
                    <p>
                      This post published:{" "}
                      <time
                        dateTime={collectionInfo.post_published_at}
                        title={collectionInfo.post_published_at}
                      >
                        {dateLabel(collectionInfo.post_published_at)}
                      </time>
                      .
                    </p>
                  )}
                  <p>
                    Discovery records when DANK noticed the link or article, not
                    when the publisher page was saved or originally published.
                  </p>
                </div>
              )}
              {target && selected.id !== rootNodeId && (
                <div className="post-connection-actions post-connection-explore">
                  <ExploreConnection target={target} {...navigation} />
                </div>
              )}
            </>
          ) : (
            <p>
              Select a source, post, comment or destination to inspect its saved
              links. Arrows show where links lead. Explore a saved post to
              center its connections.
            </p>
          )}
          <div className="post-connections-selected-evidence">
            {connections.map((connection) => (
              <ConnectionEvidence
                key={connection.id}
                connection={connection}
                {...navigation}
              />
            ))}
          </div>
        </section>
      </RelationshipGraph>
    </div>
  );
}
