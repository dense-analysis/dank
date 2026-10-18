import type { ReactNode } from "react";
import type { Post } from "../lib/types";
import { hasXContent, xProfile } from "../lib/x";
import { XConversation, XThreadContext } from "./XConversation";
import { XEngagement } from "./XEngagement";
import { XParentLink } from "./XParentLink";
import { XPostContent } from "./XPostContent";
import { XPostHeader } from "./XPostHeader";

export function XPostDetail({
  post,
  links,
  selectAccount,
  pending = false,
  loadError = false,
  referenceUrl,
  params,
  openPost,
}: {
  post: Post;
  links?: ReactNode;
  selectAccount: (handle: string) => void;
  pending?: boolean;
  loadError?: boolean;
  referenceUrl?: string;
  params: URLSearchParams;
  openPost: (post: Pick<Post, "id" | "domain">) => void;
}) {
  const account = xProfile(post);

  return (
    <div className="article-inner x-reader">
      <h1 className="visually-hidden">
        Post by {account?.name || post.author || "unknown account"}
      </h1>
      <XThreadContext
        params={params}
        openPost={openPost}
        capture={post.x?.conversation}
        selectAccount={selectAccount}
      />
      <div className="x-focused-post">
        <XPostHeader post={post} selectAccount={selectAccount} />
        <XParentLink post={post} params={params} openPost={openPost} />
        <XPostContent
          quoteNavigation={{ params, openPost, selectAccount }}
          post={post}
          referenceUrl={referenceUrl}
        />
      </div>
      {!hasXContent(post) && (
        <p className="x-content-unavailable">
          Post content is unavailable in this capture. View the original on X.
        </p>
      )}
      <XEngagement post={post.x} />
      {links}
      <XConversation
        params={params}
        openPost={openPost}
        post={post}
        selectAccount={selectAccount}
        pending={pending}
        loadError={loadError}
      />
    </div>
  );
}
