import { Check, ExternalLink } from "lucide-react";
import type { ReactNode } from "react";
import type { Post } from "../lib/types";
import { Count } from "./Count";
import { safeUrl } from "./PostCard";
import { XEngagement } from "./XEngagement";
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
}: {
  post: Post;
  links?: ReactNode;
  read: boolean;
  href: string;
  open: () => void;
  selectAccount: (handle: string) => void;
  query: string;
}) {
  const navigation = { href, open };

  return (
    // biome-ignore lint/a11y/useKeyWithClickEvents: Existing post links provide keyboard access; the card adds a pointer shortcut.
    <article
      className={`post-card x-post-card ${read ? "is-read" : ""}`}
      onClick={(event) => {
        if (
          event.defaultPrevented ||
          event.button !== 0 ||
          event.metaKey ||
          event.ctrlKey ||
          event.shiftKey ||
          event.altKey ||
          !(event.target instanceof Element) ||
          event.target.closest(
            "a, button, input, select, textarea, summary, video, audio, [role='button'], [role='link'], [contenteditable]",
          )
        )
          return;

        const selection = window.getSelection();

        if (
          selection &&
          !selection.isCollapsed &&
          selection.rangeCount > 0 &&
          selection.getRangeAt(0).intersectsNode(event.currentTarget)
        )
          return;

        // Back navigation should restore focus to this post's keyboard link.
        event.currentTarget
          .querySelector<HTMLAnchorElement>(".x-post-timestamp")
          ?.focus({ preventScroll: true });
        open();
      }}
    >
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
              <Count value={post.x.replies_captured_count} /> saved
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
      {links}
    </article>
  );
}
