import { expect, it } from "vitest";
import type { RelationshipGraphData } from "./relationshipGraph";
import {
  forgetGraphState,
  MAX_REMEMBERED_GRAPHS,
  readGraphState,
  rememberGraphSelection,
  rememberGraphViewport,
} from "./relationshipGraphState";

const graph: RelationshipGraphData = {
  id: "state-test",
  nodes: [
    {
      id: "one",
      kind: "post",
      kindLabel: "Post",
      label: "One",
      tone: "neutral",
      layer: 0,
    },
  ],
  edges: [],
};

it("retains selection and viewport together, rejects stale topology and invalid selection, and clears explicitly", () => {
  rememberGraphSelection(graph, "one");
  rememberGraphViewport(graph, {
    zoom: 1.2,
    pan: { x: 30, y: 40 },
    positions: { one: { x: 50, y: 60 } },
    vertical: false,
  });
  expect(readGraphState(graph)).toMatchObject({
    selectedNodeId: "one",
    viewport: { zoom: 1.2 },
  });
  expect(readGraphState({ ...graph, nodes: [] })).toBeUndefined();
  rememberGraphSelection(graph, "missing");
  expect(readGraphState(graph)?.selectedNodeId).toBeNull();
  expect(readGraphState(graph)?.viewport?.zoom).toBe(1.2);
  forgetGraphState(graph.id);
  expect(readGraphState(graph)).toBeUndefined();
});

it("keeps only a bounded set of the most recently saved graphs", () => {
  for (let index = 0; index <= MAX_REMEMBERED_GRAPHS; index++) {
    rememberGraphSelection({ ...graph, id: `bounded-${index}` }, "one");
  }

  expect(readGraphState({ ...graph, id: "bounded-0" })).toBeUndefined();
  expect(
    readGraphState({ ...graph, id: `bounded-${MAX_REMEMBERED_GRAPHS}` })
      ?.selectedNodeId,
  ).toBe("one");
});
