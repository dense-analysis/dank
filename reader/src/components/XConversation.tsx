import { ExternalLink, MessageCircle } from "lucide-react";
import type { CSSProperties } from "react";
import type {
  XConversation as Capture,
  Post,
  XConversationNode,
} from "../lib/types";
import { dateLabel, safeUrl } from "./PostCard";
import { XPostContent } from "./XPostContent";
import { XPostHeader } from "./XPostHeader";

const reasons: Record<string, string> = {
  limit: "The reply limit was reached.",
  cursor: "More replies were available on X.",
  unexpanded: "Some reply branches were not expanded.",
  missing_context: "Some earlier posts or reply parents were not available.",
  reported_replies: "X reported more replies than were returned.",
  timeout: "Collection timed out before the conversation finished loading.",
  missing_source: "X did not provide a reliable reply count.",
  no_matching_response: "X did not return this conversation.",
  login_required:
    "X requires sign-in before this conversation can be collected.",
  rate_limited: "X limited the collection request.",
  unavailable: "The conversation was unavailable on X.",
  invalid_capture:
    "The saved response could not be matched to this conversation.",
};

export function XThreadContext({
  capture,
  selectAccount,
}: {
  capture?: Capture;
  selectAccount: (handle: string) => void;
}) {
  if (!capture?.context.length) return null;

  return (
    <section className="x-thread-context" aria-label="Thread context">
      <h2>Thread context</h2>
      <ConversationPosts
        posts={capture.context}
        selectAccount={selectAccount}
        context
      />
    </section>
  );
}

export function XConversation({
  post,
  selectAccount,
  pending = false,
  loadError = false,
}: {
  post: Post;
  selectAccount: (handle: string) => void;
  pending?: boolean;
  loadError?: boolean;
}) {
  const capture = post.x?.conversation;
  const status = capture?.status ?? "not_collected";
  const archived = status === "captured" || status === "partial";
  const partial = status === "partial" || capture?.has_more;
  const replies = capture?.replies ?? [];
  const reported = capture?.reported_replies ?? post.x?.reply_count;
  const capturedAt = capture?.captured_at ?? post.x?.captured_at;
  const unavailable = loadError && !capture;
  const title = unavailable
    ? "Conversation could not be loaded"
    : pending
      ? "Loading conversation…"
      : archived
        ? partial
          ? "Partial conversation"
          : "Captured replies"
        : status === "failed"
          ? "Conversation unavailable"
          : status === "disabled"
            ? "Conversation capture disabled"
            : "Replies not collected";

  return (
    <section className="x-conversation" aria-label="Conversation">
      <section className="x-replies-notice" aria-label="Reply collection">
        <div className="x-replies-heading">
          <MessageCircle size={20} aria-hidden="true" />
          <h2>{title}</h2>
        </div>
        {pending ? (
          <p role="status">Loading the saved conversation.</p>
        ) : unavailable ? (
          <p>
            The saved replies could not be loaded. Use Try again above, or view
            the conversation on X.
          </p>
        ) : (
          <>
            {capture?.latest_attempt_status && (
              <p className="x-capture-refresh">
                {capture.latest_attempt_status === "failed"
                  ? "The latest refresh failed."
                  : capture.latest_attempt_status === "partial"
                    ? "The latest refresh was incomplete."
                    : "The latest collection did not refresh the conversation."}{" "}
                Showing the previously saved conversation.
                {capture.latest_attempt_at && (
                  <>
                    {" "}
                    Latest attempt:{" "}
                    <time dateTime={capture.latest_attempt_at}>
                      {dateLabel(capture.latest_attempt_at)}
                    </time>
                    .
                  </>
                )}
              </p>
            )}
            {archived && (
              <p className="x-captured-total">
                {replies.length.toLocaleString()}{" "}
                {replies.length === 1 ? "reply" : "replies"} saved.
              </p>
            )}
            {reported != null && (
              <p>
                {reported.toLocaleString()}{" "}
                {reported === 1 ? "reply" : "replies"} reported when collected.
              </p>
            )}
            {archived ? (
              partial ? (
                <p>
                  {reasons[capture?.reason ?? ""] ||
                    "Only part of the conversation was captured."}{" "}
                  More replies or context may be available on X.
                </p>
              ) : replies.length === 0 ? (
                <p>No replies were returned in this captured conversation.</p>
              ) : (
                <p>Replies are shown in their saved thread order.</p>
              )
            ) : (
              <p>
                {status === "disabled"
                  ? "Conversation capture was turned off for this collection."
                  : status === "failed"
                    ? reasons[capture?.reason ?? ""] ||
                      "The conversation could not be collected."
                    : "Replies haven’t been saved for this post. View the conversation on X to read them."}
              </p>
            )}
            {capturedAt && (
              <p className="x-capture-date">
                Captured{" "}
                <time dateTime={capturedAt}>{dateLabel(capturedAt)}</time>
              </p>
            )}
          </>
        )}
        <a
          className="text-button"
          href={safeUrl(post.url)}
          target="_blank"
          rel="noreferrer"
        >
          View conversation on X <ExternalLink size={14} />
        </a>
      </section>
      {!pending && archived && replies.length > 0 && (
        <ConversationPosts posts={replies} selectAccount={selectAccount} />
      )}
    </section>
  );
}

function ConversationPosts({
  posts,
  selectAccount,
  context = false,
}: {
  posts: XConversationNode[];
  selectAccount: (handle: string) => void;
  context?: boolean;
}) {
  return (
    <ol className={`x-conversation-list ${context ? "x-context-list" : ""}`}>
      {posts.map((post) => (
        <li
          key={`${post.domain}:${post.id}`}
          className="x-conversation-entry"
          style={
            {
              "--reply-depth": context ? 0 : Math.min(post.depth, 3),
            } as CSSProperties
          }
        >
          <article
            aria-label={`Post by ${post.x?.name || post.author || "unknown account"}`}
          >
            <XPostHeader post={post} selectAccount={selectAccount} />
            <div className="x-reply-context">
              {post.is_author && <span className="x-author-badge">Author</span>}
              {post.missing_parent && <span>Parent post not captured</span>}
              {post.depth > 0 && (
                <span className="visually-hidden">
                  Reply at level {post.depth}
                </span>
              )}
            </div>
            <XPostContent post={post} compact />
            <a
              className="x-conversation-original"
              href={safeUrl(post.url)}
              target="_blank"
              rel="noreferrer"
            >
              {context ? "View post on X" : "View reply on X"}{" "}
              <ExternalLink size={12} />
            </a>
          </article>
        </li>
      ))}
    </ol>
  );
}
