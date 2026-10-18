import { articleParams } from "../lib/navigation";
import type { Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { ReaderLink } from "./ReaderLink";

export function XParentLink({
  post,
  params,
  openPost,
}: {
  post: Post;
  params: URLSearchParams;
  openPost: (post: Pick<Post, "id" | "domain">) => void;
}) {
  const parent = post.x?.reply_to;
  if (!parent) return null;
  const label = parent.is_self ? "Reply to own post" : "Reply to a post";

  return parent.collected ? (
    <ReaderLink
      className="x-parent-link"
      href={`/reader/?${articleParams(params, parent)}`}
      onNavigate={() => openPost(parent)}
    >
      {label}
    </ReaderLink>
  ) : (
    <a
      className="x-parent-link"
      href={safeUrl(parent.url)}
      target="_blank"
      rel="noreferrer"
    >
      {label} · parent not collected · view on X
    </a>
  );
}
