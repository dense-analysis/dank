import type {
  PostConnection,
  PostConnectionsData,
} from "../../src/lib/postConnections";
import type { PostReference } from "../../src/lib/types";

export const connectionRoot: PostReference = {
  domain: "example.com",
  id: "kernel-article",
  title: "What changed in the kernel",
  url: "https://example.com/kernel-article",
  source: "rss",
};

export const connectionDiscussion: PostReference = {
  domain: "reddit.com",
  id: "discussion",
  title: "Discussing a better kernel",
  url: "https://www.reddit.com/r/kernel/comments/discussion/",
  source: "reddit",
};

export const incomingConnection: PostConnection = {
  id: "incoming-comment",
  direction: "incoming",
  origin: connectionDiscussion,
  target: {
    url: connectionRoot.url,
    post: connectionRoot,
    youtube_item_id: null,
  },
  evidence: {
    domain: connectionDiscussion.domain,
    post_id: connectionDiscussion.id,
    kind: "comment",
    reference_id: "comment-1",
    reference_url: `${connectionDiscussion.url}comment/comment-1/`,
    author: "KernelReader",
    matched_url: `${connectionRoot.url}#security`,
    excerpt: "This source explains why kernel security matters.",
    observed_at: "2026-10-17T12:00:00Z",
  },
};

export const outgoingConnection: PostConnection = {
  id: "outgoing-post",
  direction: "outgoing",
  origin: connectionRoot,
  target: {
    url: "https://source.example/research",
    post: {
      domain: "source.example",
      id: "research",
      url: "https://source.example/research",
      title: "The original security research",
      source: "rss",
    },
    youtube_item_id: null,
  },
  evidence: {
    domain: connectionRoot.domain,
    post_id: connectionRoot.id,
    kind: "post",
    reference_id: "",
    reference_url: connectionRoot.url,
    author: "",
    matched_url: "https://source.example/research#findings",
    excerpt: "Read the original research for the detailed findings.",
    observed_at: "2026-10-17T13:00:00Z",
  },
};

export const externalConnection: PostConnection = {
  ...outgoingConnection,
  id: "outgoing-external",
  target: {
    url: "https://uncollected.example/a-long-destination-with-a-query?identity=one",
    post: null,
    youtube_item_id: null,
  },
  evidence: {
    ...outgoingConnection.evidence,
    kind: "comment",
    reference_id: "publisher-comment",
    author: "The author",
    reference_url: `${connectionRoot.url}#publisher-comment`,
    matched_url:
      "https://uncollected.example/a-long-destination-with-a-query?identity=one#section",
    excerpt: "An external comment link that has not been collected.",
  },
};

export const videoConnection: PostConnection = {
  ...outgoingConnection,
  id: "outgoing-video",
  target: {
    url: "https://www.youtube.com/watch?v=NnV_cWeoo5Q",
    title: "Kernel Recipes 2026 - Security in the LLM age",
    post: null,
    youtube_item_id: "video:NnV_cWeoo5Q",
  },
  evidence: {
    ...outgoingConnection.evidence,
    matched_url: "https://youtu.be/NnV_cWeoo5Q?t=1468",
    excerpt: "Watch the conference talk for more context.",
  },
};

export const connectionsFixture: PostConnectionsData = {
  root: connectionRoot,
  incoming: [incomingConnection],
  outgoing: [outgoingConnection, externalConnection, videoConnection],
  incoming_count: 1,
  outgoing_count: 3,
  limit: 100,
  coverage: "saved_evidence",
};
