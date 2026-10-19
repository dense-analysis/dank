import { ChevronDown, Eye } from "lucide-react";
import { hackerNewsExcerpt, hasHackerNewsBody } from "../lib/hackerNews";
import { articleParams } from "../lib/navigation";
import { savedPublisherExcerpt } from "../lib/postExcerpt";
import { dateLabel } from "../lib/timestamps";
import type { CollectionInfo, Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { ReaderLink } from "./ReaderLink";

export function collectionInfoTitle(info: CollectionInfo): string {
  const labels = [
    ...new Set(info.first_seen_from.map((origin) => origin.label)),
  ];

  return info.first_seen_at && labels.length
    ? `First seen from ${labels.join(" and ")} at ${dateLabel(info.first_seen_at)}`
    : "Collection info: first seen source or time unavailable";
}

function CollectedTime({ value }: { value: string | null }) {
  return value ? (
    <time dateTime={value} title={value}>
      {dateLabel(value)}
    </time>
  ) : (
    <>Not recorded</>
  );
}

function usesPublisherExcerpt(post: Post): boolean {
  if (!post.linked_post?.excerpt) return false;

  if (post.hacker_news) {
    if (
      post.html.trim() &&
      hasHackerNewsBody(post.html, post.hacker_news.external_url)
    )
      return false;

    return !hackerNewsExcerpt(
      post.excerpt,
      post.hacker_news.external_url,
      post.html,
    );
  }

  return !post.x && !!savedPublisherExcerpt(post, "");
}

export function CollectionInfoContent({
  post,
  info,
  readerHref,
  openPost,
}: {
  post: Post;
  info: CollectionInfo;
  readerHref?: string;
  openPost?: (post: Pick<Post, "id" | "domain">) => void;
}) {
  const labels = [
    ...new Set(info.first_seen_from.map((origin) => origin.label)),
  ];
  const article = info.article;
  const params = new URLSearchParams(readerHref?.split("?")[1]);
  const savedHref = article && `/reader/?${articleParams(params, article)}`;
  const original = safeUrl(info.article_url);
  const isArticle = article?.id === post.id && article?.domain === post.domain;
  const status = {
    collected: "Saved",
    incomplete: "Incomplete — some article content may be missing",
    unavailable: "Not collected",
  }[info.article_status];

  return (
    <div className="collection-info-content">
      <dl>
        <div>
          <dt>Publisher page</dt>
          <dd>
            {status}
            {article &&
              !isArticle &&
              savedHref &&
              info.article_status !== "unavailable" && (
                <>
                  {" · "}
                  {openPost ? (
                    <ReaderLink
                      href={savedHref}
                      onNavigate={() => openPost(article)}
                    >
                      Read saved article
                    </ReaderLink>
                  ) : (
                    <a href={savedHref}>Read saved article</a>
                  )}
                </>
              )}
            {info.article_status === "unavailable" && original && (
              <>
                {" · "}
                <a href={original} target="_blank" rel="noreferrer">
                  Open publisher
                </a>
              </>
            )}
          </dd>
        </div>
        <div>
          <dt>First seen from</dt>
          <dd>
            {labels.length ? labels.join(" and ") : "Source not recorded"}
          </dd>
        </div>
        <div>
          <dt>First observed by DANK</dt>
          <dd>
            <CollectedTime value={info.first_seen_at} />
          </dd>
        </div>
        {!isArticle && (
          <>
            <div>
              <dt>This post first collected</dt>
              <dd>
                <CollectedTime value={info.post_first_seen_at} />
              </dd>
            </div>
            <div>
              <dt>This link first observed</dt>
              <dd>
                <CollectedTime value={info.link_first_seen_at} />
              </dd>
            </div>
          </>
        )}
        <div>
          <dt>Publisher page first collected</dt>
          <dd>
            <CollectedTime value={info.article_collected_at} />
          </dd>
        </div>
        <div>
          <dt>Publisher page collected through</dt>
          <dd>
            {info.collected_via?.label ?? "Not recorded for this saved history"}
          </dd>
        </div>
      </dl>
      {!isArticle && (
        <p
          className={
            info.seen_before_post ? "collection-info-earlier" : undefined
          }
        >
          {info.seen_before_post === true
            ? "This article was already known to DANK before this link was observed."
            : info.seen_before_post === false
              ? "No earlier discovery appears in the saved history."
              : "There isn’t enough saved history to compare discovery times."}
        </p>
      )}
      {usesPublisherExcerpt(post) && (
        <p>The feed preview uses text from the saved publisher article.</p>
      )}
      <p className="collection-info-note">
        These are collection times from your saved history, not publication
        times. They don’t establish who reported the story first.
      </p>
    </div>
  );
}

export function CollectionInfoDetails({
  post,
  readerHref,
  openPost,
}: {
  post: Post;
  readerHref?: string;
  openPost?: (post: Pick<Post, "id" | "domain">) => void;
}) {
  const info = post.collection_info;
  if (!info) return null;

  return (
    <details className="collection-info-details">
      <summary>
        <Eye size={17} aria-hidden="true" />
        Collection info
        <ChevronDown
          size={16}
          className="collection-info-chevron"
          aria-hidden="true"
        />
      </summary>
      <CollectionInfoContent
        post={post}
        info={info}
        readerHref={readerHref}
        openPost={openPost}
      />
    </details>
  );
}
