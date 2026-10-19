/** A renderer-independent graph of explicit, directed relationships. */
export interface RelationshipNode {
  id: string;
  kind: string;
  label: string;
  kindLabel: string;
  detail?: string;
  tone: "neutral" | "blue" | "amber" | "accent";
  layer: number;
}

export interface RelationshipEdge {
  id: string;
  source: string;
  target: string;
  kind: string;
  label: string;
  bend?: number;
}

export interface RelationshipGraphData {
  id: string;
  nodes: readonly RelationshipNode[];
  edges: readonly RelationshipEdge[];
}

export interface GraphPosition {
  x: number;
  y: number;
}

// Fixed spacing keeps small evidence paths legible and avoids moving force layouts.
// Callers supply layers; the renderer has no knowledge of source-specific semantics.
export function relationshipPositions(
  nodes: readonly RelationshipNode[],
  vertical = false,
): Record<string, GraphPosition> {
  const layers = new Map<number, RelationshipNode[]>();

  for (const node of nodes) {
    const layer = layers.get(node.layer) ?? [];
    layer.push(node);
    layers.set(node.layer, layer);
  }

  const positions: Record<string, GraphPosition> = {};

  for (const [layer, members] of layers) {
    for (const [index, node] of members.entries()) {
      const offset = index - (members.length - 1) / 2;
      positions[node.id] = vertical
        ? { x: offset * 196, y: layer * 158 }
        : { x: layer * 252, y: offset * 150 };
    }
  }

  return positions;
}

export function graphTopology(graph: RelationshipGraphData): string {
  return JSON.stringify([
    graph.nodes.map(({ id, layer }) => [id, layer]),
    graph.edges.map(({ id, source, target }) => [id, source, target]),
  ]);
}

export function compactGraphLabel(label: string): string {
  const plain = label.replace(/\s+/g, " ").trim();

  return plain.length > 74 ? `${plain.slice(0, 71).trimEnd()}…` : plain;
}
