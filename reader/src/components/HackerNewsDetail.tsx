import { ExternalLink, MessageCircle } from "lucide-react";
import type { CSSProperties } from "react";
import { hasArticleContent } from "../lib/article";
import type { Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { ArticleBody } from "./ArticleBody";
import { HackerNewsMetrics } from "./HackerNewsFeed";
import { PlatformIcon } from "./PlatformIcon";
import { dateLabel } from "./PostCard";

export function HackerNewsDetail({
  post,
  pending,
  loadError,
}: {
  post: Post;
  pending: boolean;
  loadError: boolean;
}) {
  const story = post.hacker_news;
  if (!story) return null;
  const capture = story.comment_capture ?? {
    status: "not_captured",
    captured: 0,
    total: story.comments,
    reason: null,
  };
  const comments = story.comments_items ?? [];
  const hasComments = comments.length > 0;
  const hasText = hasArticleContent(post.html);
  const externalUrl = story.external_url && safeUrl(story.external_url);
  const discussionUrl = safeUrl(story.discussion_url);
  const complete = capture.status === "captured";
  const partial = capture.status === "partial" || (hasComments && !complete);

  return (
    <div className="article-inner hn-detail">
      <div className="post-meta">
        <PlatformIcon platform="hacker_news" /> Hacker News{" "}
        <span className="meta-dot">·</span>
        <time dateTime={post.created_at}>{dateLabel(post.created_at)}</time>
      </div>
      <h1>{post.title || "Untitled post"}</h1>
      <div className="article-byline">
        {post.author && <span>by {post.author}</span>}
        <HackerNewsMetrics story={story} />
      </div>
      <div className="hn-detail-links">
        {externalUrl && (
          <a
            className="original-link"
            href={externalUrl}
            target="_blank"
            rel="noreferrer"
          >
            Read original article <ExternalLink size={13} aria-hidden="true" />
          </a>
        )}
        {discussionUrl && (
          <a
            className="original-link"
            href={discussionUrl}
            target="_blank"
            rel="noreferrer"
          >
            View discussion on Hacker News{" "}
            <ExternalLink size={13} aria-hidden="true" />
          </a>
        )}
      </div>
      {hasText ? (
        <ArticleBody html={post.html} />
      ) : (
        <p className="hn-snapshot-note">
          Saved from the Hacker News front page.
          {externalUrl
            ? " Open the original article to read the full story."
            : " This post has no saved body text."}
        </p>
      )}
      <section className="hn-comments" aria-label="Hacker News comments">
        <header className="hn-comments-heading">
          <h2>
            <MessageCircle size={18} aria-hidden="true" /> Comments
          </h2>
        </header>
        {pending ? (
          <p role="status">Loading saved discussion…</p>
        ) : loadError ? (
          <p>
            Saved comments could not be loaded. Try opening the story again.
          </p>
        ) : (
          <>
            {complete || partial ? (
              <div className="hn-comments-notice">
                {partial && <h3>Partial discussion</h3>}
                <p>
                  {comments.length.toLocaleString()} saved
                  {capture.total !== null
                    ? ` of ${capture.total.toLocaleString()} reported`
                    : ""}{" "}
                  · counts may have changed on Hacker News.
                </p>
                {partial && (
                  <p>
                    {capture.reason ||
                      "Some comments or replies were not captured."}
                  </p>
                )}
              </div>
            ) : (
              <div className="hn-comments-notice">
                <h3>
                  {capture.status === "failed"
                    ? "Comment collection failed"
                    : "Comments not captured"}
                </h3>
                <p>
                  {capture.status === "failed"
                    ? "The story was saved, but its discussion could not be collected."
                    : "Comments are optional and were not collected with this story. A detailed scrape can capture the discussion."}
                </p>
              </div>
            )}
            {hasComments && (
              <ol className="hn-comment-list">
                {comments.map((comment) => (
                  <li
                    className="hn-comment"
                    key={comment.id}
                    style={
                      {
                        "--comment-depth": Math.min(comment.depth, 4),
                      } as CSSProperties
                    }
                  >
                    <div className="hn-comment-meta">
                      <strong>{comment.author || "[deleted]"}</strong>
                      {comment.created_at && (
                        <time dateTime={comment.created_at}>
                          {dateLabel(comment.created_at)}
                        </time>
                      )}
                    </div>
                    {comment.depth > 0 && (
                      <span className="visually-hidden">
                        Reply at level {comment.depth}
                      </span>
                    )}
                    <ArticleBody html={comment.html} />
                  </li>
                ))}
              </ol>
            )}
          </>
        )}
        {discussionUrl && (
          <a
            className="original-link"
            href={discussionUrl}
            target="_blank"
            rel="noreferrer"
          >
            Read comments on Hacker News{" "}
            <ExternalLink size={13} aria-hidden="true" />
          </a>
        )}
      </section>
    </div>
  );
}
