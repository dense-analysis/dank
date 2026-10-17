import {
  type GraphPosition,
  graphTopology,
  type RelationshipGraphData,
} from "./relationshipGraph";

export interface GraphViewport {
  zoom: number;
  pan: GraphPosition;
  positions: Record<string, GraphPosition>;
  vertical: boolean;
}

interface GraphState {
  topology: string;
  selectedNodeId: string | null;
  viewport?: GraphViewport;
}

// Retain recent exploration across reader navigation, without retaining renderers
// or growing memory indefinitely. A reload intentionally starts a fresh session.
export const MAX_REMEMBERED_GRAPHS = 12;
const states = new Map<string, GraphState>();

export function readGraphState(graph: RelationshipGraphData) {
  const state = states.get(graph.id);

  return state?.topology === graphTopology(graph) ? state : undefined;
}

function remember(graph: RelationshipGraphData, update: Partial<GraphState>) {
  const state = readGraphState(graph);
  states.delete(graph.id);
  states.set(graph.id, {
    topology: graphTopology(graph),
    selectedNodeId: null,
    ...state,
    ...update,
  });

  while (states.size > MAX_REMEMBERED_GRAPHS) {
    const oldest = states.keys().next().value;

    if (oldest !== undefined) states.delete(oldest);
  }
}

export function rememberGraphSelection(
  graph: RelationshipGraphData,
  selectedNodeId: string | null,
) {
  remember(graph, {
    selectedNodeId: graph.nodes.some((node) => node.id === selectedNodeId)
      ? selectedNodeId
      : null,
  });
}

export function rememberGraphViewport(
  graph: RelationshipGraphData,
  viewport: GraphViewport,
) {
  remember(graph, { viewport });
}

export function forgetGraphState(id: string) {
  states.delete(id);
}
