import { ArrowUp, ExternalLink, MessageCircle } from "lucide-react";
import type { CSSProperties } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import type { ReferenceTarget } from "../lib/referenceFocus";
import type { RedditPost } from "../lib/types";
import { ArticleBody } from "./ArticleBody";
import { dateLabel, safeUrl } from "./PostCard";

export function RedditComments({
  reddit,
  originalUrl,
  referenceTarget,
}: {
  reddit: RedditPost;
  originalUrl: string;
  referenceTarget?: ReferenceTarget;
}) {
  const { preferences, updatePreferences } = useReaderPreferences();
  const capture = reddit.comment_capture;
  const comments = capture?.status === "captured" ? capture.comments : [];
  const total = capture?.total_available ?? reddit.comments;
  const captured = capture?.status === "captured";
  const revealReference = referenceTarget?.kind === "comment";

  if (!capture && total === 0) return null;

  return (
    <section className="reddit-comments" aria-label="Comments">
      <header className="reddit-comments-heading">
        <h2>
          <MessageCircle size={18} aria-hidden="true" /> Comments
        </h2>
        {captured && (
          <span className="reddit-comment-sort">
            {capture.sort.replace(/^./, (first) => first.toUpperCase())} · saved
            snapshot
          </span>
        )}
      </header>
      {!preferences.showRedditComments && !revealReference ? (
        <div className="reddit-comments-notice">
          <p>Comments are hidden in your reading preferences.</p>
          <button
            type="button"
            className="text-button"
            onClick={() =>
              updatePreferences({ ...preferences, showRedditComments: true })
            }
          >
            Show comments
          </button>
        </div>
      ) : captured ? (
        <>
          {revealReference && !preferences.showRedditComments && (
            <p className="reddit-comments-summary">
              Comments are temporarily shown for this reference. Your reading
              preference is unchanged.
            </p>
          )}
          {capture.latest_attempt_status && (
            <p className="reddit-comments-summary">
              Previously saved
              {capture.captured_at ? ` ${dateLabel(capture.captured_at)}` : ""}.{" "}
              {capture.latest_attempt_status === "disabled"
                ? "The latest collection skipped comments."
                : capture.latest_attempt_status === "deferred"
                  ? "Post details and comments are awaiting a separate refresh."
                  : "The latest collection couldn’t refresh comments."}
            </p>
          )}
          <p className="reddit-comments-summary">
            {comments.length.toLocaleString()} saved
            {total !== null ? ` of ${total.toLocaleString()}` : ""} · counts may
            have changed on Reddit
          </p>
          {comments.length > 0 ? (
            <ol className="reddit-comment-list">
              {comments.map((comment) => (
                <li
                  className="reddit-comment"
                  key={comment.id}
                  data-reference-comment={comment.id}
                  style={
                    {
                      "--comment-depth": Math.min(comment.depth, 3),
                    } as CSSProperties
                  }
                >
                  <div className="reddit-comment-meta">
                    <span className="reddit-comment-avatar" aria-hidden="true">
                      {(comment.author || "?").slice(0, 1).toUpperCase()}
                    </span>
                    <strong>{comment.author || "[deleted]"}</strong>
                    {comment.is_op && (
                      <span className="reddit-comment-op">OP</span>
                    )}
                    {comment.created_utc !== null && (
                      <time
                        dateTime={new Date(
                          comment.created_utc * 1000,
                        ).toISOString()}
                      >
                        {dateLabel(
                          new Date(comment.created_utc * 1000).toISOString(),
                        )}
                      </time>
                    )}
                  </div>
                  {comment.depth > 0 && (
                    <span className="visually-hidden">
                      Reply at level {comment.depth}
                    </span>
                  )}
                  <div className="reddit-comment-body">
                    {comment.body_html ? (
                      <ArticleBody
                        html={comment.body_html}
                        referenceVideoId={
                          revealReference && referenceTarget.id === comment.id
                            ? referenceTarget.videoId
                            : undefined
                        }
                      />
                    ) : (
                      <ArticleBody
                        html={comment.body
                          .replaceAll("&", "&amp;")
                          .replaceAll("<", "&lt;")
                          .replaceAll(">", "&gt;")
                          .replaceAll("\n", "<br>")}
                        referenceVideoId={
                          revealReference && referenceTarget.id === comment.id
                            ? referenceTarget.videoId
                            : undefined
                        }
                      />
                    )}
                  </div>
                  <div className="reddit-comment-footer">
                    {comment.score !== null && (
                      <span>
                        <ArrowUp size={13} aria-hidden="true" />
                        <span className="visually-hidden">Score: </span>
                        {comment.score.toLocaleString()}
                      </span>
                    )}
                    {safeUrl(comment.permalink) && (
                      <a
                        href={safeUrl(comment.permalink)}
                        target="_blank"
                        rel="noreferrer"
                      >
                        On Reddit <ExternalLink size={11} aria-hidden="true" />
                      </a>
                    )}
                  </div>
                </li>
              ))}
            </ol>
          ) : (
            <p className="reddit-comments-notice">
              {total === 0
                ? "No comments when this post was collected."
                : "No comments were returned in this snapshot."}
            </p>
          )}
          {capture.has_more && (
            <footer className="reddit-comments-more">
              <p>
                This is a saved selection. More comments and replies are
                available on Reddit.
              </p>
              <a href={safeUrl(originalUrl)} target="_blank" rel="noreferrer">
                More comments on Reddit{" "}
                <ExternalLink size={13} aria-hidden="true" />
              </a>
            </footer>
          )}
        </>
      ) : (
        <div className="reddit-comments-notice">
          <h3>Comments not captured</h3>
          <p>
            {capture?.status === "disabled"
              ? "Comment collection was turned off when this post was saved."
              : capture?.status === "deferred"
                ? "The post is saved. Details and comments will be refreshed separately."
                : capture?.status === "failed"
                  ? "The conversation couldn’t be collected with this post."
                  : "This saved post doesn’t include its conversation."}
            {total !== null && total > 0
              ? ` Reddit reported ${total.toLocaleString()} ${total === 1 ? "comment" : "comments"} at collection.`
              : ""}
          </p>
          <a href={safeUrl(originalUrl)} target="_blank" rel="noreferrer">
            Read comments on Reddit{" "}
            <ExternalLink size={13} aria-hidden="true" />
          </a>
        </div>
      )}
    </section>
  );
}
