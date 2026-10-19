import { useQuery } from "@tanstack/react-query";
import { ChevronDown, List, Network } from "lucide-react";
import { lazy, Suspense, useId, useState } from "react";
import {
  type ConnectionsContext,
  connectionContextLabel,
  fetchPostConnections,
  type PostConnection,
  type PostConnectionsData,
} from "../lib/postConnections";
import type { ConnectionDirection } from "../lib/postConnectionsGraph";
import type { Post } from "../lib/types";
import {
  ConnectionEvidence,
  type ConnectionNavigation,
} from "./ConnectionEvidence";
import { Count } from "./Count";

export type { ConnectionNavigation } from "./ConnectionEvidence";

import "../post-connections.css";

const PostConnectionsGraph = lazy(() => import("./PostConnectionsGraph"));

export type ConnectionsMode = "list" | "graph" | null;

export function PostConnections({
  post,
  context = {},
  mode,
  onModeChange,
  direction,
  onDirectionChange,
  ...navigation
}: ConnectionNavigation & {
  post: Pick<Post, "domain" | "id" | "collection_info">;
  context?: ConnectionsContext;
  mode: ConnectionsMode;
  onModeChange: (mode: ConnectionsMode) => void;
  direction?: ConnectionDirection;
  onDirectionChange?: (direction: ConnectionDirection) => void;
}) {
  const [localDirection, setLocalDirection] =
    useState<ConnectionDirection>("all");
  const contentId = useId();
  const headingId = useId();
  const { data, error, isPending, refetch } = useQuery({
    queryKey: ["post-connections", post.domain, post.id, context],
    queryFn: ({ signal }) =>
      fetchPostConnections(post.domain, post.id, signal, context),
    enabled: mode !== null,
    staleTime: 60_000,
  });

  return (
    <section
      className="post-connections"
      aria-labelledby={headingId}
      tabIndex={-1}
      data-connections-pending={mode !== null && isPending ? "true" : undefined}
    >
      <div className="post-connections-heading">
        <h2 id={headingId}>
          <button
            type="button"
            className="post-connections-toggle"
            aria-expanded={mode !== null}
            aria-controls={contentId}
            onClick={() => onModeChange(mode === null ? "list" : null)}
          >
            <Network size={18} aria-hidden="true" /> Connections
            <ChevronDown size={16} aria-hidden="true" />
          </button>
        </h2>
        <fieldset
          className="post-connections-modes"
          aria-label="Connections view"
        >
          {mode !== null && (
            <button
              type="button"
              aria-label="List view"
              aria-pressed={mode === "list"}
              onClick={() => onModeChange("list")}
            >
              <List size={15} aria-hidden="true" /> List
            </button>
          )}
          <button
            type="button"
            aria-label="Graph view"
            aria-pressed={mode === "graph"}
            onClick={() => onModeChange("graph")}
          >
            <Network size={15} aria-hidden="true" /> Graph
          </button>
        </fieldset>
      </div>
      <div id={contentId} hidden={mode === null}>
        {mode !== null && (
          <>
            <p className="post-connections-scope">
              {connectionContextLabel(context)}: saved links and comment
              mentions, including other posts linking to the same saved article.
              {context.source &&
                " Other feeds are excluded; linked articles stay visible for context."}{" "}
              Only collected evidence is shown; missing captures may hide
              connections.
            </p>
            {isPending ? (
              <p role="status" className="post-connections-status">
                Loading connections…
              </p>
            ) : error ? (
              <div className="post-connections-status" role="alert">
                <p>{error.message}</p>
                <button type="button" onClick={() => void refetch()}>
                  Try again
                </button>
              </div>
            ) : data ? (
              <>
                <ConnectionCounts data={data} />
                {data.incoming.length + data.outgoing.length === 0 ? (
                  <p className="post-connections-status">
                    No verified links found in the saved evidence for this post.
                  </p>
                ) : mode === "graph" ? (
                  <Suspense
                    fallback={
                      <p role="status" data-connections-loading>
                        Loading graph…
                      </p>
                    }
                  >
                    <PostConnectionsGraph
                      key={JSON.stringify([post.domain, post.id, context])}
                      data={{ ...data, context }}
                      collectionInfo={post.collection_info}
                      direction={direction ?? localDirection}
                      onDirectionChange={onDirectionChange ?? setLocalDirection}
                      {...navigation}
                    />
                  </Suspense>
                ) : (
                  <div className="post-connections-list">
                    <ConnectionGroup
                      label="Links to this post"
                      connections={data.incoming}
                      {...navigation}
                    />
                    <ConnectionGroup
                      label="Other posts linking to the same article"
                      connections={data.related ?? []}
                      {...navigation}
                    />
                    <ConnectionGroup
                      label="Links from this post"
                      connections={data.outgoing}
                      {...navigation}
                    />
                  </div>
                )}
              </>
            ) : null}
          </>
        )}
      </div>
    </section>
  );
}

function ConnectionCounts({ data }: { data: PostConnectionsData }) {
  return (
    <div className="post-connections-counts">
      <p>
        <Count value={data.incoming_count} /> incoming ·{" "}
        <Count value={data.outgoing_count} /> outgoing references
        {(data.related_count ?? 0) > 0 && (
          <>
            {" "}
            · <Count value={data.related_count ?? 0} /> shared-article
            references
          </>
        )}
      </p>
      {(data.related?.length ?? 0) < (data.related_count ?? 0) && (
        <p className="post-connections-limit" role="status">
          Showing {data.related?.length.toLocaleString()} of{" "}
          {data.related_count?.toLocaleString()} references to shared articles,
          within this source context.
        </p>
      )}
      {(data.incoming.length < data.incoming_count ||
        data.outgoing.length < data.outgoing_count) && (
        <p className="post-connections-limit" role="status">
          Showing {data.incoming.length.toLocaleString()} of{" "}
          {data.incoming_count.toLocaleString()} incoming and{" "}
          {data.outgoing.length.toLocaleString()} of{" "}
          {data.outgoing_count.toLocaleString()} outgoing references. The list
          and graph show the same saved evidence, limited to{" "}
          {data.limit.toLocaleString()} per direction.
        </p>
      )}
    </div>
  );
}

function ConnectionGroup({
  label,
  connections,
  ...navigation
}: ConnectionNavigation & {
  label: string;
  connections: readonly PostConnection[];
}) {
  if (connections.length === 0) return null;

  return (
    <section className="post-connections-group" aria-label={label}>
      <h3>{label}</h3>
      <ul>
        {connections.map((connection) => (
          <li key={connection.id}>
            <ConnectionEvidence connection={connection} {...navigation} />
          </li>
        ))}
      </ul>
    </section>
  );
}
