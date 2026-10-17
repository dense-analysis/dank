import type { Core, ElementDefinition, StylesheetStyle } from "cytoscape";
import { Maximize, Minus, Plus, RotateCcw } from "lucide-react";
import {
  type KeyboardEvent,
  type ReactNode,
  useEffect,
  useId,
  useRef,
  useState,
} from "react";
import {
  compactGraphLabel,
  graphTopology,
  type RelationshipGraphData,
  relationshipPositions,
} from "../lib/relationshipGraph";
import {
  forgetGraphState,
  readGraphState,
  rememberGraphSelection,
  rememberGraphViewport,
} from "../lib/relationshipGraphState";
import "../relationship-graph.css";

const styles: StylesheetStyle[] = [
  {
    selector: "node",
    style: {
      label: "data(displayLabel)",
      shape: "round-rectangle",
      width: 176,
      height: 112,
      "background-color": "#202528",
      "border-color": "#697681",
      "border-width": 1.5,
      color: "#f0f1f4",
      "font-family": "DM Sans Variable, sans-serif",
      "font-size": 14,
      "font-weight": 500,
      "text-wrap": "wrap",
      "text-max-width": "156px",
      "text-valign": "center",
      "text-halign": "center",
      "line-height": 1.3,
      "overlay-opacity": 0,
    },
  },
  {
    selector: 'node[tone = "blue"]',
    style: { "background-color": "#1d2c3b", "border-color": "#82a9d0" },
  },
  {
    selector: 'node[tone = "amber"]',
    style: { "background-color": "#302c22", "border-color": "#cdb47d" },
  },
  {
    selector: 'node[tone = "accent"]',
    style: {
      "background-color": "#24382e",
      "border-color": "#b1e2bd",
      "border-width": 2,
      "font-weight": 600,
    },
  },
  {
    selector: "edge",
    style: {
      label: "data(label)",
      width: 1.5,
      "line-color": "#66747e",
      "target-arrow-color": "#66747e",
      "target-arrow-shape": "triangle",
      "curve-style": "bezier",
      "font-family": "DM Sans Variable, sans-serif",
      "font-size": 11,
      color: "#c1cad2",
      "text-background-color": "#14181b",
      "text-background-opacity": 1,
      "text-background-padding": "4px",
      "text-rotation": "autorotate",
      "text-margin-y": -9,
      "arrow-scale": 0.8,
      "overlay-opacity": 0,
    },
  },
  {
    selector: "node.selected",
    style: { "border-width": 3, "border-color": "#eff8f1" },
  },
  {
    selector: "edge.connected",
    style: {
      width: 2.5,
      "line-color": "#b1e2bd",
      "target-arrow-color": "#b1e2bd",
      color: "#d4ecdb",
    },
  },
  { selector: ".dimmed", style: { opacity: 0.3 } },
];

function elements(graph: RelationshipGraphData): ElementDefinition[] {
  return [
    ...graph.nodes.map((node) => ({
      group: "nodes" as const,
      data: {
        ...node,
        displayLabel: `${node.kindLabel.toUpperCase()}\n${compactGraphLabel(node.label).replace(/(\S{12})(?=\S)/g, "$1\u200b")}`,
      },
    })),
    ...graph.edges.map((edge) => ({
      group: "edges" as const,
      data: { ...edge },
    })),
  ];
}

function selectNode(cy: Core, id: string | null) {
  cy.elements().removeClass("selected connected dimmed");

  if (!id) return;
  const selected = cy.getElementById(id);

  if (selected.empty()) return;
  selected.addClass("selected");
  const connected = selected.closedNeighborhood();
  connected.edges().addClass("connected");
  cy.elements().difference(connected).addClass("dimmed");
}

function fitGraph(cy: Core) {
  cy.fit(undefined, 28);

  // A single node should remain at a reading size instead of filling the canvas.
  if (cy.zoom() > 1.15) {
    cy.zoom(1.15);
    cy.center();
  }
}

/** Cytoscape owns only this canvas; React owns controls, selection, and details. */
export function RelationshipGraph({
  graph,
  selectedNodeId,
  onSelectNode,
  children,
}: {
  graph: RelationshipGraphData;
  selectedNodeId: string | null;
  onSelectNode: (id: string | null) => void;
  children?: ReactNode;
}) {
  const canvas = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const renderedGraph = useRef(graph);
  const current = useRef({ graph, selectedNodeId, onSelectNode });
  current.current = { graph, selectedNodeId, onSelectNode };
  const topology = useRef("");
  const vertical = useRef(false);
  const [status, setStatus] = useState<"loading" | "ready" | "failed">(
    "loading",
  );
  const helpId = useId();
  const selectId = useId();
  const graphId = graph.id;

  useEffect(() => {
    const container = canvas.current;

    if (!container) return;
    let disposed = false;
    let observer: ResizeObserver | undefined;
    setStatus("loading");

    import("cytoscape")
      .then(({ default: cytoscape }) => {
        if (disposed || current.current.graph.id !== graphId) return;
        const activeGraph = current.current.graph;
        vertical.current = container.clientWidth < 760;
        const remembered = readGraphState(activeGraph)?.viewport;
        const viewport =
          remembered?.vertical === vertical.current ? remembered : undefined;
        const cy = cytoscape({
          container,
          elements: elements(activeGraph),
          style: styles,
          layout: {
            name: "preset",
            positions:
              viewport?.positions ??
              relationshipPositions(activeGraph.nodes, vertical.current),
            fit: false,
          },
          zoom: viewport?.zoom ?? 1,
          pan: viewport?.pan ?? { x: 0, y: 0 },
          minZoom: 0.12,
          maxZoom: 2.5,
          boxSelectionEnabled: false,
          autounselectify: true,
        });
        cyRef.current = cy;
        renderedGraph.current = activeGraph;
        topology.current = graphTopology(activeGraph);
        if (!viewport) fitGraph(cy);
        selectNode(cy, current.current.selectedNodeId);
        cy.on("tap", "node", (event) => {
          current.current.onSelectNode(event.target.id());
        });
        cy.on("tap", (event) => {
          if (event.target === cy) current.current.onSelectNode(null);
        });
        observer = new ResizeObserver(() => {
          cy.resize();
          const nextVertical = container.clientWidth < 760;

          if (nextVertical !== vertical.current) {
            vertical.current = nextVertical;
            cy.layout({
              name: "preset",
              positions: relationshipPositions(
                current.current.graph.nodes,
                nextVertical,
              ),
              fit: false,
            }).run();
            fitGraph(cy);
          }
        });
        observer.observe(container);
        setStatus("ready");
      })
      .catch(() => {
        if (disposed) return;
        observer?.disconnect();
        cyRef.current?.destroy();
        cyRef.current = null;
        setStatus("failed");
      });

    return () => {
      disposed = true;
      observer?.disconnect();
      const cy = cyRef.current;

      if (cy) {
        rememberGraphViewport(renderedGraph.current, {
          zoom: cy.zoom(),
          pan: { ...cy.pan() },
          positions: Object.fromEntries(
            cy.nodes().map((node) => [node.id(), { ...node.position() }]),
          ),
          vertical: vertical.current,
        });
        cy.destroy();
      }

      cyRef.current = null;
    };
  }, [graphId]);

  useEffect(() => {
    rememberGraphSelection(graph, selectedNodeId);
    const cy = cyRef.current;

    if (!cy) return;
    renderedGraph.current = graph;
    const nextTopology = graphTopology(graph);
    const changed = topology.current !== nextTopology;
    const incoming = elements(graph);
    cy.batch(() => {
      if (changed) {
        cy.elements().remove();
        cy.add(incoming);
      } else {
        for (const element of incoming) {
          cy.getElementById(element.data.id ?? "").data(element.data);
        }
      }
    });

    if (changed) {
      topology.current = nextTopology;
      cy.layout({
        name: "preset",
        positions: relationshipPositions(graph.nodes, vertical.current),
        fit: false,
      }).run();
      fitGraph(cy);
    }

    selectNode(cy, selectedNodeId);
  }, [graph, selectedNodeId]);

  function zoom(factor: number) {
    const cy = cyRef.current;

    if (!cy) return;
    cy.zoom({
      level: cy.zoom() * factor,
      renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 },
    });
  }

  function reset() {
    forgetGraphState(graph.id);
    const cy = cyRef.current;

    if (!cy) return;
    selectNode(cy, null);
    cy.layout({
      name: "preset",
      positions: relationshipPositions(graph.nodes, vertical.current),
      fit: false,
    }).run();
    fitGraph(cy);
    onSelectNode(null);
  }

  function keyboard(event: KeyboardEvent<HTMLDivElement>) {
    const cy = cyRef.current;

    if (!cy || event.target !== event.currentTarget) return;
    const movement: Record<string, { x: number; y: number }> = {
      ArrowLeft: { x: 45, y: 0 },
      ArrowRight: { x: -45, y: 0 },
      ArrowUp: { x: 0, y: 45 },
      ArrowDown: { x: 0, y: -45 },
    };

    if (movement[event.key]) cy.panBy(movement[event.key]);
    else if (event.key === "+" || event.key === "=") zoom(1.25);
    else if (event.key === "-") zoom(0.8);
    else if (event.key === "Home") fitGraph(cy);
    else if (event.key === "Escape") onSelectNode(null);
    else return;
    event.preventDefault();
  }

  return (
    <div className="relationship-graph" data-status={status}>
      <div className="relationship-graph-toolbar">
        <div className="relationship-graph-legend">
          {[
            ...new Map(graph.nodes.map((node) => [node.kind, node])).values(),
          ].map((node) => (
            <span key={node.kind} data-tone={node.tone}>
              <i aria-hidden="true" /> {node.kindLabel}
            </span>
          ))}
        </div>
        <div
          className="relationship-graph-controls"
          role="toolbar"
          aria-label="Graph controls"
        >
          <button
            type="button"
            onClick={() => zoom(1.25)}
            disabled={status !== "ready"}
            aria-label="Zoom in"
            title="Zoom in"
          >
            <Plus size={16} />
          </button>
          <button
            type="button"
            onClick={() => zoom(0.8)}
            disabled={status !== "ready"}
            aria-label="Zoom out"
            title="Zoom out"
          >
            <Minus size={16} />
          </button>
          <button
            type="button"
            onClick={() => cyRef.current && fitGraph(cyRef.current)}
            disabled={status !== "ready"}
            title="Fit all nodes"
          >
            <Maximize size={15} /> Fit
          </button>
          <button
            type="button"
            onClick={reset}
            disabled={status !== "ready"}
            title="Reset positions and selection"
          >
            <RotateCcw size={15} /> Reset
          </button>
        </div>
      </div>
      <div
        className="relationship-graph-viewport"
        role="application"
        aria-label="Interactive relationship graph"
        aria-describedby={helpId}
        // biome-ignore lint/a11y/noNoninteractiveTabindex: Canvas viewport implements keyboard panning and zooming.
        tabIndex={0}
        onKeyDown={keyboard}
      >
        <div
          className="relationship-graph-canvas"
          ref={canvas}
          aria-hidden="true"
        />
        {status !== "ready" && (
          <p className="relationship-graph-status" role="status">
            {status === "loading"
              ? "Loading graph…"
              : "Graph could not load. Explore the saved connections below or switch to List."}
          </p>
        )}
      </div>
      <p id={helpId} className="relationship-graph-help">
        Drag to pan, pinch or scroll to zoom. Select a node to inspect it.
        <span> Keyboard: arrow keys to pan, + / − to zoom, Home to fit.</span>
      </p>
      <div className="relationship-graph-explore">
        <label htmlFor={selectId}>Explore nodes</label>
        <select
          id={selectId}
          value={selectedNodeId ?? ""}
          onChange={(event) => onSelectNode(event.target.value || null)}
        >
          <option value="">All connections</option>
          {graph.nodes.map((node) => (
            <option key={node.id} value={node.id}>
              {node.kindLabel}: {node.label}
            </option>
          ))}
        </select>
      </div>
      {children}
    </div>
  );
}
