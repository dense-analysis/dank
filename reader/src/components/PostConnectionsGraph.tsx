import { useMemo, useState } from "react";
import type { PostConnectionsData } from "../lib/postConnections";
import {
  type ConnectionDirection,
  postConnectionsGraph,
} from "../lib/postConnectionsGraph";
import { readGraphState } from "../lib/relationshipGraphState";
import {
  ConnectionEvidence,
  type ConnectionNavigation,
  ExploreConnection,
} from "./ConnectionEvidence";
import { RelationshipGraph } from "./RelationshipGraph";

export default function PostConnectionsGraph({
  data,
  direction,
  onDirectionChange,
  ...navigation
}: ConnectionNavigation & {
  data: PostConnectionsData;
  direction: ConnectionDirection;
  onDirectionChange: (direction: ConnectionDirection) => void;
}) {
  const { graph, connectionsByNode, targetsByNode, rootNodeId } = useMemo(
    () => postConnectionsGraph(data, direction),
    [data, direction],
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
      <RelationshipGraph
        graph={graph}
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
