import type { ReactNode } from "react";
import type { Post } from "../lib/types";
import { xProfile } from "../lib/x";
import { PostTime } from "./PostTime";
import { ReaderLink } from "./ReaderLink";
import { XAccountLink } from "./XAccountLink";

export function XPostHeader({
  post,
  selectAccount,
  navigation,
  badge,
}: {
  post: Post;
  badge?: ReactNode;
  selectAccount: (handle: string) => void;
  navigation?: { href: string; open: () => void };
}) {
  const account = xProfile(post);

  return (
    <header className="x-post-header">
      {account ? (
        <XAccountLink account={account} select={selectAccount} showName />
      ) : (
        <strong>{post.author || "X post"}</strong>
      )}
      {badge}
      {navigation ? (
        <ReaderLink
          className="x-post-timestamp post-open"
          data-card-link
          href={navigation.href}
          onNavigate={navigation.open}
          aria-label={`Read ${post.title || "Untitled post"}`}
        >
          <PostTime date={post.created_at} />
        </ReaderLink>
      ) : (
        <PostTime className="x-post-timestamp" date={post.created_at} />
      )}
    </header>
  );
}
