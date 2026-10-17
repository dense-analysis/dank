import type { Post } from "../lib/types";
import { xProfile } from "../lib/x";
import { dateLabel } from "./PostCard";
import { ReaderLink } from "./ReaderLink";
import { XAccountLink } from "./XAccountLink";

export function XPostHeader({
  post,
  selectAccount,
  navigation,
}: {
  post: Post;
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
      {navigation ? (
        <ReaderLink
          className="x-post-timestamp post-open"
          href={navigation.href}
          onNavigate={navigation.open}
          aria-label={`Read ${post.title || "Untitled post"}`}
        >
          <time dateTime={post.created_at}>{dateLabel(post.created_at)}</time>
        </ReaderLink>
      ) : (
        <time className="x-post-timestamp" dateTime={post.created_at}>
          {dateLabel(post.created_at)}
        </time>
      )}
    </header>
  );
}
