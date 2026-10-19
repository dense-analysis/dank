import { expect, it } from "vitest";
import {
  graphTopology,
  type RelationshipNode,
  relationshipPositions,
} from "./relationshipGraph";

const node: RelationshipNode = {
  id: "source",
  kind: "source",
  kindLabel: "Source",
  label: "Community",
  tone: "neutral",
  layer: 0,
};

it("reserves space for publication annotations vertically and among horizontal siblings", () => {
  const nodes = [
    node,
    { ...node, id: "post", layer: 1, height: 208 },
    { ...node, id: "related", layer: 1 },
  ];
  const vertical = relationshipPositions(nodes, true);
  expect(vertical.post.y - 208 / 2 - (vertical.source.y + 112 / 2)).toBe(46);
  const horizontal = relationshipPositions(nodes);
  expect(horizontal.related.y - 112 / 2 - (horizontal.post.y + 208 / 2)).toBe(
    38,
  );
  expect(horizontal.post.x).toBe(horizontal.related.x);
  expect(
    relationshipPositions([node, { ...node, id: "next", layer: 1 }], true).next
      .y,
  ).toBe(158);
});

it("invalidates saved positions when annotation heights change, but not when their text changes", () => {
  const base = { id: "layout", nodes: [node], edges: [] };
  expect(graphTopology(base)).not.toBe(
    graphTopology({ ...base, nodes: [{ ...node, height: 208 }] }),
  );
  expect(graphTopology(base)).toBe(
    graphTopology({
      ...base,
      nodes: [{ ...node, label: "Updated community" }],
    }),
  );
});
