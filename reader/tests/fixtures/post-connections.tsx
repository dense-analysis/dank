import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";
import { createRoot } from "react-dom/client";
import {
  type ConnectionsMode,
  PostConnections,
} from "../../src/components/PostConnections";
import type {
  PostConnectionEvidence,
  PostConnectionTarget,
} from "../../src/lib/postConnections";
import type { ConnectionDirection } from "../../src/lib/postConnectionsGraph";
import "../../src/styles.css";
import { connectionRoot } from "./post-connections-data";

const client = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});

function evidenceHref(evidence: PostConnectionEvidence) {
  return `/reader/?${new URLSearchParams({ article: evidence.post_id, article_source: evidence.domain, reference_kind: evidence.kind, reference_id: evidence.reference_id, reference_url: evidence.matched_url })}`;
}

function targetHref(target: PostConnectionTarget) {
  if (target.post)
    return `/reader/?${new URLSearchParams({ article: target.post.id, article_source: target.post.domain, connections: "graph" })}`;
  if (target.youtube_item_id)
    return `/reader/?${new URLSearchParams({ view: "youtube", feed: "linked", youtube_item: target.youtube_item_id, references: "graph" })}`;

  return undefined;
}

function Fixture() {
  const [mode, setMode] = useState<ConnectionsMode>(null);
  const [direction, setDirection] = useState<ConnectionDirection>("all");
  const [shown, setShown] = useState(true);
  const [opened, setOpened] = useState("");

  return (
    <QueryClientProvider client={client}>
      <div style={{ maxWidth: 1040, margin: "32px auto", padding: "0 16px" }}>
        <h1>{connectionRoot.title}</h1>
        <button type="button" onClick={() => setShown(!shown)}>
          Toggle connections
        </button>
        <output aria-label="Opened destination">{opened}</output>
        {shown && (
          <PostConnections
            post={connectionRoot}
            mode={mode}
            onModeChange={setMode}
            direction={direction}
            onDirectionChange={setDirection}
            evidenceHref={evidenceHref}
            targetHref={targetHref}
            onOpenEvidence={(evidence) =>
              setOpened(
                `evidence:${evidence.domain}:${evidence.post_id}:${evidence.reference_id}`,
              )
            }
            onExploreTarget={(target) =>
              setOpened(
                `explore:${target.post?.domain ?? "youtube"}:${target.post?.id ?? target.youtube_item_id}`,
              )
            }
          />
        )}
      </div>
    </QueryClientProvider>
  );
}

const container = document.getElementById("root");

if (container) createRoot(container).render(<Fixture />);
