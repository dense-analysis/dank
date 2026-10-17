import { Link as LinkIcon } from "lucide-react";
import { sourceKind } from "../lib/collections";
import { articleParams } from "../lib/navigation";
import { postKey } from "../lib/storage";
import type { Post, PostReference } from "../lib/types";
import { Count } from "./Count";
import { ReaderLink } from "./ReaderLink";

function sourceLabel(post: PostReference) {
  switch (sourceKind(post.domain)) {
    case "hacker_news":
      return "Hacker News";
    case "x":
      return "X";
    case "reddit":
      return "Reddit";
    case "youtube":
      return "YouTube";
    default:
      return post.domain.replace(/^www\./, "");
  }
}

export function PostLinks({
  post,
  params,
  open,
}: {
  post: Post;
  params: URLSearchParams;
  open: (post: PostReference) => void;
}) {
  const links = post.links ?? [];
  const count = Math.max(post.links_count ?? 0, links.length);
  if (!count) return null;

  return (
    <details className="post-links">
      <summary>
        <LinkIcon size={13} aria-hidden="true" />
        <span>
          Links (<Count value={count} />)
        </span>
      </summary>
      <ul>
        {links.map((link) => (
          <li key={postKey(link)}>
            <ReaderLink
              href={`/reader/?${articleParams(params, link)}`}
              onNavigate={() => open(link)}
            >
              <span className="post-link-source">{sourceLabel(link)}</span>
              <span>{link.title || "Untitled post"}</span>
            </ReaderLink>
          </li>
        ))}
      </ul>
      {links.length < count && (
        <p>
          Showing <Count value={links.length} /> of <Count value={count} />{" "}
          collected links.
        </p>
      )}
    </details>
  );
}
