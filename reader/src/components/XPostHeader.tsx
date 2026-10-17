import type { Post } from "../lib/types";
import { xProfile } from "../lib/x";
import { dateLabel } from "./PostCard";
import { XAccountLink } from "./XAccountLink";

export function XPostHeader({
  post,
  selectAccount,
}: {
  post: Post;
  selectAccount: (handle: string) => void;
}) {
  const account = xProfile(post);

  return (
    <header className="x-post-header">
      {account ? (
        <XAccountLink account={account} select={selectAccount} showName />
      ) : (
        <strong>{post.author || "X post"}</strong>
      )}
      <time dateTime={post.created_at}>{dateLabel(post.created_at)}</time>
    </header>
  );
}
