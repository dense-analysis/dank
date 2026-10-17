import { ExternalLink, MessageCircle } from "lucide-react";
import type { Post } from "../lib/types";
import { hasXContent, xProfile } from "../lib/x";
import { dateLabel, safeUrl } from "./PostCard";
import { XPostContent } from "./XPostContent";
import { XPostHeader } from "./XPostHeader";

export function XPostDetail({
  post,
  selectAccount,
}: {
  post: Post;
  selectAccount: (handle: string) => void;
}) {
  const account = xProfile(post);
  const replyCount = post.x?.reply_count;

  return (
    <div className="article-inner x-reader">
      <h1 className="visually-hidden">
        Post by {account?.name || post.author || "unknown account"}
      </h1>
      <XPostHeader post={post} selectAccount={selectAccount} />
      <XPostContent post={post} />
      {!hasXContent(post) && (
        <p className="x-content-unavailable">
          Post content is unavailable in this capture. View the original on X.
        </p>
      )}
      <section className="x-replies-notice" aria-label="Reply collection">
        <div className="x-replies-heading">
          <MessageCircle size={20} aria-hidden="true" />
          <h2>Replies not collected</h2>
        </div>
        {replyCount != null && (
          <p>
            {replyCount.toLocaleString()}{" "}
            {replyCount === 1 ? "reply" : "replies"} reported when collected.
          </p>
        )}
        <p>
          Replies haven’t been saved for this post. View the conversation on X
          to read them.
        </p>
        {post.x?.captured_at && (
          <p className="x-capture-date">
            Captured{" "}
            <time dateTime={post.x.captured_at}>
              {dateLabel(post.x.captured_at)}
            </time>
          </p>
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
    </div>
  );
}
