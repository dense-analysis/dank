import type { ReactNode } from "react";
import type { Post } from "../lib/types";
import { hasXContent, xProfile } from "../lib/x";
import { XConversation, XThreadContext } from "./XConversation";
import { XEngagement } from "./XEngagement";
import { XPostContent } from "./XPostContent";
import { XPostHeader } from "./XPostHeader";

export function XPostDetail({
  post,
  links,
  selectAccount,
  pending = false,
  loadError = false,
  referenceVideoId,
}: {
  post: Post;
  links?: ReactNode;
  selectAccount: (handle: string) => void;
  pending?: boolean;
  loadError?: boolean;
  referenceVideoId?: string;
}) {
  const account = xProfile(post);

  return (
    <div className="article-inner x-reader">
      <h1 className="visually-hidden">
        Post by {account?.name || post.author || "unknown account"}
      </h1>
      <XThreadContext
        capture={post.x?.conversation}
        selectAccount={selectAccount}
      />
      <div className="x-focused-post">
        <XPostHeader post={post} selectAccount={selectAccount} />
        <XPostContent post={post} referenceVideoId={referenceVideoId} />
      </div>
      {!hasXContent(post) && (
        <p className="x-content-unavailable">
          Post content is unavailable in this capture. View the original on X.
        </p>
      )}
      <XEngagement post={post.x} />
      {links}
      <XConversation
        post={post}
        selectAccount={selectAccount}
        pending={pending}
        loadError={loadError}
      />
    </div>
  );
}
