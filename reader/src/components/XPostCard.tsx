import { Check, ExternalLink, MessageCircle } from "lucide-react";
import type { Post } from "../lib/types";
import { safeUrl } from "./PostCard";
import { ReaderLink } from "./ReaderLink";
import { XPostContent } from "./XPostContent";
import { XPostHeader } from "./XPostHeader";

export function XPostCard({
  post,
  read,
  href,
  open,
  selectAccount,
  query,
}: {
  post: Post;
  read: boolean;
  href: string;
  open: () => void;
  selectAccount: (handle: string) => void;
  query: string;
}) {
  return (
    <article className={`post-card x-post-card ${read ? "is-read" : ""}`}>
      <XPostHeader post={post} selectAccount={selectAccount} />
      <XPostContent post={post} compact query={query} />
      <footer className="post-bottom x-post-actions">
        <ReaderLink
          href={href}
          className="text-button post-open"
          onNavigate={open}
          aria-label={`Read ${post.title || "Untitled post"}`}
        >
          <MessageCircle size={15} aria-hidden="true" /> Open post
          {post.x?.reply_count != null && (
            <span
              className="x-reply-count"
              title="Replies reported when collected"
            >
              {" "}
              · {post.x.reply_count.toLocaleString()}{" "}
              {post.x.reply_count === 1 ? "reply" : "replies"}
            </span>
          )}
        </ReaderLink>
        {(post.x?.replies_status === "captured" ||
          post.x?.replies_status === "partial") &&
          post.x.replies_captured_count != null && (
            <span className="x-conversation-summary">
              {post.x.replies_captured_count.toLocaleString()} saved
              {post.x.replies_status === "partial" ? " · partial" : ""}
            </span>
          )}
        {read && (
          <span className="read-indicator">
            <Check size={12} /> Read
          </span>
        )}
        <a
          className="x-original-link"
          href={safeUrl(post.url)}
          target="_blank"
          rel="noreferrer"
          aria-label={`Open original: ${post.title}`}
        >
          <ExternalLink size={14} /> View on X
        </a>
      </footer>
    </article>
  );
}
