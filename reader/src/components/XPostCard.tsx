import { Check, ExternalLink } from "lucide-react";
import type { Post } from "../lib/types";
import { safeUrl } from "./PostCard";
import { XEngagement } from "./XEngagement";
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
  const navigation = { href, open };

  return (
    <article className={`post-card x-post-card ${read ? "is-read" : ""}`}>
      <XPostHeader
        post={post}
        selectAccount={selectAccount}
        navigation={navigation}
      />
      <XPostContent post={post} compact query={query} navigation={navigation} />
      <footer className="post-bottom x-post-actions">
        <XEngagement post={post.x} navigation={navigation} />
        {(post.x?.replies_status === "captured" ||
          post.x?.replies_status === "partial") &&
          post.x.replies_captured_count != null && (
            <span className="x-conversation-summary">
              {post.x.replies_captured_count.toLocaleString()} saved
              {post.x.replies_status === "partial" ? " · partial" : ""}
            </span>
          )}
        {read && (
          <span className="read-indicator" title="Read">
            <Check size={13} />
            <span className="visually-hidden">Read</span>
          </span>
        )}
        <a
          className="x-original-link icon-button"
          href={safeUrl(post.url)}
          target="_blank"
          rel="noreferrer"
          aria-label={`Open original: ${post.title}`}
          title="View on X"
        >
          <ExternalLink size={16} />
        </a>
      </footer>
    </article>
  );
}
