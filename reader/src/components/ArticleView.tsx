import { useQuery } from "@tanstack/react-query";
import {
  ArrowLeft,
  CircleAlert,
  CircleDashed,
  ExternalLink,
  Minus,
  Plus,
} from "lucide-react";
import {
  type ReactNode,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { useRedditContent } from "../hooks/useRedditContent";
import { useVideoPlayback } from "../hooks/useVideoPlayback";
import { fetchPost } from "../lib/api";
import { hasArticleContent } from "../lib/article";
import { hasHackerNewsBody } from "../lib/hackerNews";
import { matchesLinkUrl, normalizedLinkUrl } from "../lib/linkIdentity";
import { articleParams } from "../lib/navigation";
import type { ReferenceTarget } from "../lib/referenceFocus";
import { postKey } from "../lib/storage";
import type { Filters, Post } from "../lib/types";
import { videoPositionKey } from "../lib/videoPositions";
import { hasXContent, isXPost } from "../lib/x";
import { ArticleBody } from "./ArticleBody";
import { AuthorLink } from "./AuthorLink";
import { CollectionBadge } from "./CollectionBadge";
import { CommunityAvatar } from "./CommunityAvatar";
import { HackerNewsDetail } from "./HackerNewsDetail";
import { PostTimestamp, readingTime, SourceMark, safeUrl } from "./PostCard";
import { ReaderLink } from "./ReaderLink";
import { RedditComments } from "./RedditComments";
import {
  RedditCommunityLink,
  RedditFlair,
  RedditMetadata,
} from "./RedditMetadata";
import { RedditVideo } from "./RedditVideo";
import { ReferenceFocusNotice } from "./ReferenceFocusNotice";
import { SourceLink } from "./SourceLink";
import { XPostDetail } from "./XPostDetail";

export function ArticleView({
  domain,
  id,
  initial,
  params,
  openPost,
  close,
  selectSource,
  markRead,
  filters,
  selectAuthor,
  selectCommunity,
  selectAccount,
  selectFlair,
  referenceTarget,
  renderConnections,
  backLabel,
}: {
  domain: string;
  id: string;
  initial?: Post;
  referenceTarget?: ReferenceTarget;
  renderConnections: (post: Post) => ReactNode;
  backLabel?: string;
  params: URLSearchParams;
  openPost: (post: Pick<Post, "id" | "domain">) => void;
  close: () => void;
  selectSource: (domain: string) => void;
  markRead: (post: Post) => void;
  filters: Filters;
  selectAuthor: (author: string) => void;
  selectCommunity: (community: string) => void;
  selectAccount: (handle: string) => void;
  selectFlair: (community: string, flair: string) => void;
}) {
  const { preferences, updatePreferences, preferencesError } =
    useReaderPreferences();
  const { fontSize } = preferences;
  const articleRef = useRef<HTMLElement>(null);
  const targetKey = referenceTarget ? JSON.stringify(referenceTarget) : "";
  const [referenceResult, setReferenceResult] = useState({
    key: "",
    matched: false,
  });
  function setFontSize(size: number) {
    updatePreferences({ ...preferences, fontSize: size });
  }
  const {
    data: post,
    isPending,
    isPlaceholderData,
    error,
    refetch,
  } = useQuery({
    queryKey: ["post", domain, id],
    queryFn: ({ signal }) => fetchPost(domain, id, signal),
    // Feed summaries omit discussion details and must never count as a fresh detail response.
    placeholderData: initial,
  });
  const {
    videos: redditVideos,
    html,
    onUnavailable,
    displayed: redditVideoKeys,
  } = useRedditContent(post, { preserveVideoLinks: true });
  const hasContent = useMemo(
    () =>
      post?.hacker_news
        ? hasHackerNewsBody(html, post.hacker_news.external_url)
        : hasArticleContent(html),
    [html, post?.hacker_news],
  );
  const xPost = isXPost(post ?? { domain, source: "" });
  const hasReadableContent =
    post && xPost
      ? hasXContent(post)
      : hasContent ||
        redditVideos.length > 0 ||
        (post?.hacker_news?.comments_items?.length ?? 0) > 0;
  const attachedMedia =
    post?.media.filter(
      (asset) =>
        /^(video|audio)\//.test(asset.content_type) &&
        safeUrl(asset.url) &&
        !redditVideos.some((video) => video.url === asset.url),
    ) ?? [];

  useLayoutEffect(() => {
    if (!targetKey || !post || isPlaceholderData) return;

    setReferenceResult({
      key: targetKey,
      matched: !!articleRef.current?.querySelector(
        '[data-reference-match="true"]',
      ),
    });
  }, [targetKey, post, isPlaceholderData]);
  const referenceReady =
    !!post && !isPlaceholderData && referenceResult.key === targetKey;
  const originalUrl = post && normalizedLinkUrl(post.url);
  const originalMatch = !!(
    post?.source === "rss" &&
    originalUrl &&
    referenceTarget?.kind === "post" &&
    new URL(originalUrl).hostname.replace(/^www\./, "") !==
      post.domain.toLowerCase().replace(/^www\./, "") &&
    matchesLinkUrl(post.url, referenceTarget.url)
  );
  const postLinks = post && renderConnections(post);

  useEffect(() => {
    if (post && hasReadableContent) markRead(post);
  }, [post, hasReadableContent, markRead]);
  return (
    <main
      id="main"
      ref={articleRef}
      className="article-page"
      data-reference-pending={
        referenceTarget ? String(!referenceReady && !error) : undefined
      }
      data-source={post?.source}
      aria-label={xPost ? "X post reader" : "Article reader"}
      data-reader-pending={
        xPost && isPlaceholderData && !error ? "true" : undefined
      }
    >
      <div className="reader-toolbar">
        <button
          type="button"
          className="back-button"
          onClick={close}
          data-page-focus
        >
          <ArrowLeft size={17} />
          {backLabel ?? "Back to stories"}
        </button>
        <div className="reader-type">
          <button
            type="button"
            className="icon-button"
            aria-label={xPost ? "Smaller post text" : "Smaller article text"}
            disabled={fontSize <= 16}
            onClick={() => setFontSize(fontSize - 1)}
          >
            <Minus size={14} />
          </button>
          <span aria-hidden="true">Aa</span>
          <button
            type="button"
            className="icon-button"
            aria-label={xPost ? "Larger post text" : "Larger article text"}
            disabled={fontSize >= 26}
            onClick={() => setFontSize(fontSize + 1)}
          >
            <Plus size={14} />
          </button>
        </div>
      </div>
      {preferencesError && (
        <div className="error-banner" role="alert">
          Browser storage is unavailable or full. Text size changes will last
          until this page closes.
        </div>
      )}
      {isPending && (
        <div className="empty-state" role="status">
          Opening your story…
        </div>
      )}
      {error && (
        <div className="empty-state" role="alert">
          <h2>We couldn’t open this story</h2>
          <p>{error.message}</p>
          <button
            type="button"
            className="primary-button"
            onClick={() => void refetch()}
          >
            Try again
          </button>
        </div>
      )}
      {referenceTarget && referenceReady && (
        <ReferenceFocusNotice
          target={referenceTarget}
          domain={domain}
          postId={id}
          matched={referenceResult.matched}
          close={close}
          backLabel={backLabel}
        />
      )}
      {post && xPost && (
        <XPostDetail
          params={params}
          openPost={openPost}
          post={post}
          links={postLinks}
          selectAccount={selectAccount}
          referenceUrl={
            referenceTarget?.kind === "post" ? referenceTarget.url : undefined
          }
          pending={isPlaceholderData && !error}
          loadError={!!error}
        />
      )}
      {post?.hacker_news && (
        <HackerNewsDetail
          post={post}
          referenceTarget={referenceTarget}
          links={postLinks}
          linkedArticle={
            post.linked_post && (
              <ReaderLink
                className="original-link"
                href={`/reader/?${articleParams(params, post.linked_post)}`}
                onNavigate={() =>
                  post.linked_post && openPost(post.linked_post)
                }
              >
                Read collected article
              </ReaderLink>
            )
          }
          filters={filters}
          selectAuthor={selectAuthor}
          pending={isPlaceholderData && !error}
          loadError={!!error}
        />
      )}
      {post && !xPost && !post.hacker_news && (
        <div className="article-inner">
          <div className="post-meta">
            {post.reddit ? (
              <CommunityAvatar
                community={post.reddit.community}
                iconUrl={post.reddit.icon_url}
              />
            ) : (
              <SourceMark domain={post.domain} />
            )}
            {post.reddit ? (
              <RedditCommunityLink
                community={post.reddit.community}
                select={selectCommunity}
              />
            ) : (
              <SourceLink domain={post.domain} select={selectSource} />
            )}
            <span className="meta-dot">·</span>
            <PostTimestamp post={post} />
            {hasContent && post.collection_incomplete && (
              <CollectionBadge incomplete />
            )}
          </div>
          <h1>{post.title || "Untitled post"}</h1>
          <div className="article-byline">
            <AuthorLink
              author={post.author}
              fallback={post.domain}
              filters={filters}
              select={selectAuthor}
            />
            {post.reddit ? (
              <>
                <RedditMetadata reddit={post.reddit} />
                <RedditFlair reddit={post.reddit} selectFlair={selectFlair} />
              </>
            ) : (
              hasContent && <span> {readingTime(post)} min read</span>
            )}
          </div>
          {postLinks}
          {hasReadableContent ? (
            <>
              <a
                className={`original-link${originalMatch ? " reference-match" : ""}`}
                data-reference-match={originalMatch ? "true" : undefined}
                href={safeUrl(post.url)}
                target="_blank"
                rel="noreferrer"
              >
                Read original
                <ExternalLink size={13} />
              </a>
              {post.collection_incomplete && (
                <aside
                  className="collection-notice collection-notice-partial"
                  aria-label="Collection status"
                >
                  <CircleAlert
                    className="collection-notice-icon"
                    size={20}
                    aria-hidden="true"
                  />
                  <div>
                    <h2>Incomplete scrape</h2>
                    <p>
                      The article page couldn’t be fetched. Available text is
                      shown below, but some text, formatting or media may be
                      missing. Read the original for the full article.
                    </p>
                  </div>
                </aside>
              )}
              {redditVideos.map((video) => (
                <RedditVideo
                  key={video.url}
                  video={video}
                  originalUrl={post.url}
                  positionScope={postKey(post)}
                  onUnavailable={onUnavailable}
                />
              ))}
              {hasContent && (
                <ArticleBody
                  html={html}
                  sourceUrl={post.url}
                  referenceUrl={
                    referenceTarget?.kind === "post" ||
                    (!post.reddit && referenceTarget?.kind === "comment")
                      ? referenceTarget.url
                      : undefined
                  }
                  referenceIsPost={referenceTarget?.kind === "post"}
                  referenceCommentId={
                    !post.reddit && referenceTarget?.kind === "comment"
                      ? referenceTarget.id
                      : undefined
                  }
                  redditImages={post.reddit?.images}
                  redditVideos={post.reddit ? redditVideos : undefined}
                  redditVideoKeys={redditVideoKeys}
                  redditPostId={post.reddit ? post.id : undefined}
                />
              )}
            </>
          ) : (
            <aside className="collection-notice" aria-label="Collection status">
              <CircleDashed
                className="collection-notice-icon"
                size={20}
                aria-hidden="true"
              />
              <div>
                <h2>Full text not collected</h2>
                <p>
                  This story is in your feed, but its article text isn’t
                  available here.
                </p>
                {safeUrl(post.url) && (
                  <a
                    className={`collection-source-link${originalMatch ? " reference-match" : ""}`}
                    data-reference-match={originalMatch ? "true" : undefined}
                    href={safeUrl(post.url)}
                    target="_blank"
                    rel="noreferrer"
                  >
                    Read original article{" "}
                    <ExternalLink size={13} aria-hidden="true" />
                  </a>
                )}
              </div>
            </aside>
          )}
          {attachedMedia.length > 0 && (
            <section className="article-media" aria-label="Attached media">
              {attachedMedia.map((asset) =>
                asset.content_type.startsWith("video/") ? (
                  <AttachedVideo
                    key={asset.url}
                    url={asset.url}
                    positionKey={videoPositionKey(
                      postKey(post),
                      "attachment",
                      asset.url,
                    )}
                  />
                ) : (
                  <audio
                    key={asset.url}
                    controls
                    preload="none"
                    src={safeUrl(asset.url)}
                    aria-label="Article audio"
                  >
                    <track kind="captions" />
                  </audio>
                ),
              )}
            </section>
          )}
          {post.reddit && !isPlaceholderData && (
            <RedditComments
              reddit={post.reddit}
              originalUrl={post.url}
              referenceTarget={referenceTarget}
            />
          )}
          <div className="article-end">
            {hasReadableContent && <span>YOU’VE REACHED THE END</span>}
            <button type="button" className="secondary-button" onClick={close}>
              <ArrowLeft size={15} />
              {backLabel ?? "Back to your reading"}
            </button>
          </div>
        </div>
      )}
    </main>
  );
}

function AttachedVideo({
  url,
  positionKey,
}: {
  url: string;
  positionKey: string;
}) {
  const videoRef = useVideoPlayback(positionKey);

  return (
    <video
      ref={videoRef}
      controls
      preload="none"
      src={safeUrl(url)}
      aria-label="Article video"
    >
      <track kind="captions" />
    </video>
  );
}
