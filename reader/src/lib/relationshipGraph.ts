/** A renderer-independent graph of explicit, directed relationships. */
export interface RelationshipNode {
  id: string;
  kind: string;
  label: string;
  kindLabel: string;
  detail?: string;
  height?: number;
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

// Deterministic spacing reserves room for annotated nodes without a force layout.
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

  let previousCenter = 0;
  let previousHeight = 0;

  for (const [index, [layer, members]] of [...layers]
    .sort(([a], [b]) => a - b)
    .entries()) {
    const heights = members.map((node) => node.height ?? 112);
    const layerHeight = Math.max(...heights);
    const center =
      index === 0
        ? 0
        : previousCenter + previousHeight / 2 + 46 + layerHeight / 2;
    const totalHeight =
      heights.reduce((sum, height) => sum + height, 0) +
      (members.length - 1) * 38;
    let offsetY = -totalHeight / 2;

    for (const [memberIndex, node] of members.entries()) {
      positions[node.id] = vertical
        ? { x: (memberIndex - (members.length - 1) / 2) * 196, y: center }
        : { x: layer * 252, y: offsetY + heights[memberIndex] / 2 };
      offsetY += heights[memberIndex] + 38;
    }

    previousCenter = center;
    previousHeight = layerHeight;
  }

  return positions;
}

export function graphTopology(graph: RelationshipGraphData): string {
  return JSON.stringify([
    graph.nodes.map(({ id, layer, height }) => [id, layer, height ?? 112]),
    graph.edges.map(({ id, source, target }) => [id, source, target]),
  ]);
}

export function compactGraphLabel(label: string): string {
  const plain = label.replace(/\s+/g, " ").trim();

  return plain.length > 74 ? `${plain.slice(0, 71).trimEnd()}…` : plain;
}
