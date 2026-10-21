import type { ReactNode } from "react";
import type { Post } from "../lib/types";
import { Count } from "./Count";
import { FeedSourceBadge } from "./FeedSourceBadge";
import { NavigableCard } from "./NavigableCard";
import { PostFooter } from "./PostFooter";
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
  mixedFeed = false,
}: {
  post: Post;
  mixedFeed?: boolean;
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
        badge={mixedFeed ? <FeedSourceBadge source="x" /> : undefined}
      />
      <XParentLink post={post} params={params} openPost={openPost} />
      <XPostContent
        quoteNavigation={{ params, openPost, selectAccount }}
        post={post}
        compact
        query={query}
        navigation={navigation}
      />
      <PostFooter
        post={post}
        readerHref={href}
        read={read}
        source={{
          url: post.url,
          label: `Open original: ${post.title}`,
          title: "View on X",
        }}
        summary={
          (post.x?.replies_status === "captured" ||
            post.x?.replies_status === "partial") &&
          post.x.replies_captured_count != null && (
            <>
              <Count value={post.x.replies_captured_count} /> saved
              {post.x.replies_status === "partial" ? " · partial" : ""}
            </>
          )
        }
      >
        <XEngagement post={post.x} navigation={navigation} />
      </PostFooter>
      {links}
    </NavigableCard>
  );
}
