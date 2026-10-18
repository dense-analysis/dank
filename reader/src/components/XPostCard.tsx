import { Check, ExternalLink } from "lucide-react";
import type { ReactNode } from "react";
import type { Post } from "../lib/types";
import { Count } from "./Count";
import { NavigableCard } from "./NavigableCard";
import { safeUrl } from "./PostCard";
import { XEngagement } from "./XEngagement";
import { XParentLink } from "./XParentLink";
import { XPostContent } from "./XPostContent";
import { XPostHeader } from "./XPostHeader";

export function XPostCard({
  post,
  links,
  read,
  href,
  open,
  selectAccount,
  query,
  params,
  openPost,
}: {
  post: Post;
  links?: ReactNode;
  read: boolean;
  href: string;
  open: () => void;
  selectAccount: (handle: string) => void;
  query: string;
  params: URLSearchParams;
  openPost: (post: Pick<Post, "id" | "domain">) => void;
}) {
  const navigation = { href, open };

  return (
    <NavigableCard className={`post-card x-post-card ${read ? "is-read" : ""}`}>
      <XPostHeader
        post={post}
        selectAccount={selectAccount}
        navigation={navigation}
      />
      <XParentLink post={post} params={params} openPost={openPost} />
      <XPostContent
        quoteNavigation={{ params, openPost, selectAccount }}
        post={post}
        compact
        query={query}
        navigation={navigation}
      />
      <footer className="post-bottom x-post-actions social-post-actions">
        <XEngagement post={post.x} navigation={navigation} />
        {(post.x?.replies_status === "captured" ||
          post.x?.replies_status === "partial") &&
          post.x.replies_captured_count != null && (
            <span className="x-conversation-summary">
              <Count value={post.x.replies_captured_count} /> saved
              {post.x.replies_status === "partial" ? " · partial" : ""}
            </span>
          )}
        <div className="x-post-utilities">
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
        </div>
      </footer>
      {links}
    </NavigableCard>
  );
}
