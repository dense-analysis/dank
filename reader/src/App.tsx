import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import {
  ArrowDown,
  ArrowRight,
  CircleAlert,
  Inbox,
  RefreshCw,
} from "lucide-react";
import {
  type CSSProperties,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { ArticleView } from "./components/ArticleView";
import { CollectionCount } from "./components/CollectionCount";
import { CollectionHeader } from "./components/CollectionHeader";
import { CommunityAvatar } from "./components/CommunityAvatar";
import { FeedToolbar } from "./components/FeedToolbar";
import { IdentityAvatar } from "./components/IdentityAvatar";
import { Modal } from "./components/Modal";
import { PlatformIcon } from "./components/PlatformIcon";
import { PostCard } from "./components/PostCard";
import { ReaderSettings } from "./components/ReaderSettings";
import { Sidebar } from "./components/Sidebar";
import { SourcesView } from "./components/SourcesView";
import { XPostCard } from "./components/XPostCard";
import { YouTubeCollection } from "./components/YouTubeCollection";
import { useLocation } from "./hooks/useLocation";
import { useReaderPreferences } from "./hooks/useReaderPreferences";
import { useReadPosts } from "./hooks/useReadPosts";
import { fetchPosts, fetchSources } from "./lib/api";
import {
  collectedAccounts,
  feedLabels,
  sectionFromView,
  sourceKind,
  sourcesForFeed,
} from "./lib/collections";
import { articleParams, filterParams, parseFilters } from "./lib/navigation";
import { readingFonts } from "./lib/preferences";
import { defaultFilters, postKey } from "./lib/storage";
import type { Filters, Post, Section } from "./lib/types";
import { isXPost } from "./lib/x";
import { fetchYouTubeSources } from "./lib/youtubeCollection";

export function App() {
  const { params, navigate, backTo, positionError } = useLocation();
  const { read, markRead, storageError } = useReadPosts();
  const { preferences } = useReaderPreferences();
  const [mobileOpen, setMobileOpen] = useState(false);
  const section = sectionFromView(params.get("view"));
  const feedSection =
    section !== "sources" && section !== "settings" && section !== "youtube";
  const filters = parseFilters(params);
  const articleId = params.get("article");
  const articleDomain = params.get("article_source");
  const readingArticle = Boolean(articleId && articleDomain);
  const sources = useQuery({
    queryKey: ["sources"],
    queryFn: ({ signal }) => fetchSources(signal),
  });
  const youtubeSources = useQuery({
    queryKey: ["youtube-sources"],
    queryFn: ({ signal }) => fetchYouTubeSources(signal),
  });
  const posts = useInfiniteQuery({
    queryKey: ["posts", filters],
    queryFn: ({ pageParam, signal }) => fetchPosts(filters, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: feedSection,
  });
  const loadMoreRef = useRef<HTMLDivElement>(null);
  const { fetchNextPage, hasNextPage, isFetching, isError } = posts;

  useEffect(() => {
    const target = loadMoreRef.current;
    if (
      !target ||
      !feedSection ||
      readingArticle ||
      !hasNextPage ||
      isFetching ||
      isError ||
      typeof IntersectionObserver === "undefined"
    )
      return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries.some((entry) => entry.isIntersecting)) return;
        observer.disconnect();
        void fetchNextPage({ cancelRefetch: false });
      },
      { rootMargin: "0px 0px 240px 0px" },
    );
    observer.observe(target);
    return () => observer.disconnect();
  }, [
    feedSection,
    readingArticle,
    hasNextPage,
    isFetching,
    isError,
    fetchNextPage,
  ]);

  const timeline = useMemo(() => {
    const unique = new Map<string, Post>();
    for (const page of posts.data?.pages ?? [])
      for (const post of page.posts) unique.set(postKey(post), post);
    return [...unique.values()];
  }, [posts.data]);
  const selected = timeline.find(
    (post) => post.id === articleId && post.domain === articleDomain,
  );
  const readKeys = new Set(read);
  const allSources = sources.data?.sources ?? [];
  const redditSources = sourcesForFeed(allSources, "reddit");
  const xSources = sourcesForFeed(allSources, "x");
  const accounts = collectedAccounts(xSources);
  const activeAccount = accounts.find(
    (account) => account.handle.toLowerCase() === filters.account.toLowerCase(),
  );
  const accountHandle = activeAccount?.handle ?? filters.account;
  const accountName = activeAccount?.name || accountHandle;
  const accountHasName =
    accountName.toLowerCase() !== accountHandle.toLowerCase();
  const accountContext = section === "x" && !!filters.account;
  const communities = redditSources.flatMap(
    (source) => source.communities ?? [],
  );
  const communityContext = section === "reddit" && !!filters.community;
  const activeCommunity = communities.find(
    (community) =>
      community.name.toLowerCase() === filters.community.toLowerCase(),
  );
  const communityName = activeCommunity?.name ?? filters.community;
  const youtubeChannelId = params.get("channel");
  const youtubeFeedId = params.get("feed");
  const activeYouTubeChannel = youtubeSources.data?.channels.find(
    (channel) => channel.id === youtubeChannelId,
  );
  const activeYouTubeFeed = youtubeSources.data?.feeds.find(
    (feed) => feed.id === youtubeFeedId,
  );
  const youtubeContext =
    section === "youtube" && !!(youtubeChannelId || youtubeFeedId);
  const youtubeTitle = youtubeChannelId
    ? activeYouTubeChannel?.name || "Channel feed"
    : youtubeFeedId === "linked"
      ? "From your feeds"
      : activeYouTubeFeed?.kind === "news"
        ? "News"
        : activeYouTubeFeed?.kind === "hype"
          ? "Hyped"
          : activeYouTubeFeed?.name || "YouTube";
  const collectionContext =
    accountContext || communityContext || youtubeContext;
  const visibleSources = sourcesForFeed(allSources, filters.kind);
  const title =
    section === "youtube"
      ? youtubeTitle
      : section === "settings"
        ? "Settings"
        : section === "sources"
          ? "Sources"
          : communityContext
            ? `r/${communityName}`
            : accountContext
              ? accountHasName
                ? accountName
                : `@${accountHandle}`
              : feedLabels[filters.kind];
  const overviewPlatform =
    !accountContext &&
    !communityContext &&
    (section === "x" || section === "reddit")
      ? section
      : null;
  const headerTitle =
    section === "youtube" && !youtubeContext ? (
      <>
        <PlatformIcon platform="youtube" size={23} />
        <span>{youtubeTitle}</span>
      </>
    ) : overviewPlatform ? (
      <>
        <PlatformIcon platform={overviewPlatform} size={24} />
        <span className="visually-hidden">{title}</span>
      </>
    ) : (
      title
    );
  const date = new Intl.DateTimeFormat(undefined, {
    weekday: "long",
    month: "long",
    day: "numeric",
  }).format(new Date());

  function select(nextView: Section, nextFilters = defaultFilters) {
    const next = filterParams({
      ...nextFilters,
      kind:
        nextView === "sources" ||
        nextView === "settings" ||
        nextView === "youtube"
          ? "all"
          : nextView,
    });
    if (nextView !== "all") next.set("view", nextView);
    navigate(next);
    setMobileOpen(false);
  }
  function changeFilters(nextFilters: Filters) {
    navigate(filterParams(nextFilters), true);
  }
  function openPost(post: Pick<Post, "id" | "domain">) {
    const next = articleParams(params, post);
    navigate(next, false, params.toString());
  }
  function closeArticle() {
    const next = new URLSearchParams(params);
    next.delete("article");
    next.delete("article_source");
    backTo(next);
  }
  function selectSource(domain: string) {
    select(sourceKind(domain), {
      ...defaultFilters,
      domains: [domain],
    });
  }
  function selectCommunity(community: string) {
    select("reddit", { ...defaultFilters, community });
  }
  function selectAccount(account: string) {
    select("x", { ...defaultFilters, account });
  }
  function selectFlair(community: string, flair: string) {
    select("reddit", { ...defaultFilters, community, flairs: [flair] });
  }
  function selectAuthor(author: string) {
    navigate(filterParams({ ...filters, author, authorExact: true }));
  }

  function selectYouTube(selection: string) {
    const next = new URLSearchParams({ view: "youtube" });
    if (selection === "linked") next.set("feed", "linked");
    else if (selection.startsWith("channel:"))
      next.set("channel", selection.slice(8));
    else if (selection.startsWith("feed:"))
      next.set("feed", selection.slice(5));
    navigate(next);
    setMobileOpen(false);
  }

  const sidebar = (
    <Sidebar
      section={section}
      select={select}
      redditEnabled={redditSources.length > 0}
      communities={communities}
      community={filters.community}
      selectCommunity={selectCommunity}
      xEnabled={xSources.length > 0}
      accounts={accounts}
      account={filters.account}
      selectAccount={selectAccount}
      youtubeSources={youtubeSources.data}
      youtubeSelection={
        params.get("channel")
          ? `channel:${params.get("channel")}`
          : params.get("feed") === "linked"
            ? "linked"
            : params.get("feed")
              ? `feed:${params.get("feed")}`
              : ""
      }
      selectYouTube={selectYouTube}
      mobileOpen={mobileOpen}
      close={() => setMobileOpen(false)}
    />
  );

  return (
    <div
      className="app-shell"
      style={
        {
          "--reading-font": readingFonts[preferences.font].family,
          "--reading-size": `${preferences.fontSize}px`,
        } as CSSProperties
      }
    >
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      {mobileOpen ? (
        <Modal
          label="Navigation"
          onClose={() => setMobileOpen(false)}
          className="navigation-dialog"
        >
          {sidebar}
        </Modal>
      ) : (
        sidebar
      )}
      <div
        className={`main-shell ${section === "youtube" ? "youtube-shell" : ""} ${collectionContext ? "collection-context" : ""}`}
      >
        <CollectionHeader
          title={title}
          titleContent={headerTitle}
          pageHeading={
            (feedSection || section === "youtube") && !readingArticle
          }
          openNavigation={() => setMobileOpen(true)}
          back={
            collectionContext
              ? {
                  label: `Back to ${accountContext ? "X posts" : communityContext ? "Reddit" : "YouTube"}`,
                  onClick: () =>
                    select(
                      accountContext
                        ? "x"
                        : communityContext
                          ? "reddit"
                          : "youtube",
                    ),
                }
              : undefined
          }
          avatar={
            accountContext ? (
              <IdentityAvatar
                name={accountName}
                iconUrl={activeAccount?.icon_url}
                kind="account"
              />
            ) : communityContext ? (
              <CommunityAvatar
                community={communityName}
                iconUrl={activeCommunity?.icon_url}
              />
            ) : youtubeContext && youtubeChannelId ? (
              <IdentityAvatar
                name={youtubeTitle}
                iconUrl={activeYouTubeChannel?.avatar_url}
                kind="channel"
              />
            ) : youtubeContext ? (
              <PlatformIcon platform="youtube" size={23} />
            ) : undefined
          }
          handle={
            accountContext && accountHasName ? `@${accountHandle}` : undefined
          }
          count={
            accountContext && activeAccount ? (
              <CollectionCount
                count={activeAccount.count}
                total={activeAccount.total_posts}
              />
            ) : communityContext && activeCommunity ? (
              <CollectionCount
                count={activeCommunity.count}
                estimate={activeCommunity.post_total}
              />
            ) : youtubeContext && activeYouTubeChannel ? (
              <CollectionCount
                count={activeYouTubeChannel.count}
                total={activeYouTubeChannel.reported_video_count}
                reportedText={activeYouTubeChannel.reported_video_count_text}
                unit="videos"
                source="YouTube"
              />
            ) : undefined
          }
        />
        {(storageError || positionError) && (
          <div className="error-banner reading-storage-error" role="alert">
            <CircleAlert size={16} /> Reading progress could not be saved in
            this browser.
          </div>
        )}
        {articleId && articleDomain && (
          <ArticleView
            key={`${articleDomain}:${articleId}`}
            domain={articleDomain}
            id={articleId}
            initial={selected}
            close={closeArticle}
            selectSource={selectSource}
            markRead={markRead}
            filters={filters}
            selectAuthor={selectAuthor}
            selectCommunity={selectCommunity}
            selectAccount={selectAccount}
            selectFlair={selectFlair}
          />
        )}
        <div hidden={readingArticle}>
          <div
            className={`page-grid ${section === "youtube" ? "youtube-page" : ""} ${section === "settings" ? "settings-page" : ""}`}
          >
            <main
              id={readingArticle ? undefined : "main"}
              className="timeline"
              aria-label={feedSection ? title : undefined}
              tabIndex={-1}
              data-page-focus
            >
              {!accountContext &&
                !communityContext &&
                section !== "youtube" && (
                  <div className="page-heading">
                    {feedSection && <p className="eyebrow">{date}</p>}
                    {section === "reddit" && (
                      <p className="collection-description">
                        Collected posts from your communities.
                      </p>
                    )}
                    {section === "x" && (
                      <p className="collection-description">
                        Collected posts from your X accounts.
                      </p>
                    )}
                    {!feedSection && (
                      <div className="title-row">
                        <h1>{title}</h1>
                      </div>
                    )}
                  </div>
                )}
              {section === "youtube" ? (
                !readingArticle && (
                  <YouTubeCollection
                    params={params}
                    navigate={navigate}
                    backTo={backTo}
                    sources={youtubeSources.data}
                    sourcesError={
                      youtubeSources.isError
                        ? youtubeSources.error.message
                        : undefined
                    }
                    retrySources={() => void youtubeSources.refetch()}
                    openReference={(reference) =>
                      openPost({
                        id: reference.post_id,
                        domain: reference.domain,
                      })
                    }
                  />
                )
              ) : section === "settings" ? (
                <ReaderSettings />
              ) : section === "sources" ? (
                sources.isError ? (
                  <ErrorState
                    message={sources.error.message}
                    retry={() => void sources.refetch()}
                  />
                ) : sources.isPending ? (
                  <Loading />
                ) : (
                  <SourcesView
                    sources={sources.data.sources}
                    select={(source) => selectSource(source.domain)}
                  />
                )
              ) : (
                <>
                  <FeedToolbar
                    filters={filters}
                    sources={visibleSources}
                    change={changeFilters}
                  />
                  <div className="timeline-label">
                    <span>
                      {filters.q
                        ? "SEARCH RESULTS"
                        : section === "reddit" || section === "x"
                          ? "COLLECTED POSTS"
                          : "LATEST"}
                      {posts.isFetching && !posts.isPending && (
                        <span className="updating">Updating…</span>
                      )}
                    </span>
                    <button
                      type="button"
                      className="text-button"
                      onClick={() => void posts.refetch()}
                      disabled={posts.isFetching}
                    >
                      <RefreshCw size={13} /> Refresh
                    </button>
                  </div>
                  {posts.isPending ? (
                    <Loading />
                  ) : posts.isError && !posts.isFetchNextPageError ? (
                    <ErrorState
                      message={posts.error.message}
                      retry={() => void posts.refetch()}
                    />
                  ) : timeline.length ? (
                    <div className="post-list">
                      {timeline.map((post) =>
                        isXPost(post) ? (
                          <XPostCard
                            key={postKey(post)}
                            post={post}
                            read={readKeys.has(postKey(post))}
                            open={() => openPost(post)}
                            href={`/reader/?${articleParams(params, post)}`}
                            selectAccount={selectAccount}
                            query={filters.q}
                          />
                        ) : (
                          <PostCard
                            key={postKey(post)}
                            post={post}
                            read={readKeys.has(postKey(post))}
                            open={() => openPost(post)}
                            href={`/reader/?${articleParams(params, post)}`}
                            selectSource={selectSource}
                            query={filters.q}
                            filters={filters}
                            selectAuthor={selectAuthor}
                            selectCommunity={selectCommunity}
                            selectFlair={selectFlair}
                          />
                        ),
                      )}
                    </div>
                  ) : (
                    <div className="empty-state">
                      <Inbox size={30} strokeWidth={1.3} />
                      <h2>
                        {section === "reddit" || section === "x"
                          ? "No collected posts found"
                          : "No stories found"}
                      </h2>
                      <p>
                        Try a wider date range, fewer filters, or a different
                        search.
                      </p>
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => select(section)}
                      >
                        {section === "reddit" || section === "x"
                          ? `Show all ${feedLabels[section]} posts`
                          : section === "stories"
                            ? "Show all blog articles"
                            : "Show everything"}{" "}
                        <ArrowRight size={14} />
                      </button>
                    </div>
                  )}
                  {posts.hasNextPage && (
                    <div className="load-more" ref={loadMoreRef}>
                      {posts.isFetchNextPageError ? (
                        <ErrorState
                          message={posts.error.message}
                          retry={() =>
                            void posts.fetchNextPage({ cancelRefetch: false })
                          }
                        />
                      ) : (
                        <button
                          type="button"
                          className="secondary-button"
                          disabled={posts.isFetching}
                          onClick={() =>
                            void posts.fetchNextPage({ cancelRefetch: false })
                          }
                        >
                          {posts.isFetchingNextPage
                            ? "Loading stories…"
                            : "More stories"}
                          <ArrowDown size={15} />
                        </button>
                      )}
                    </div>
                  )}
                  {posts.data?.pages.some((page) => page.limited) && (
                    <p className="result-note">
                      Showing the top {timeline.length} matches. Narrow your
                      search, or choose a chronological order to browse further.
                    </p>
                  )}
                </>
              )}
            </main>
          </div>
        </div>
      </div>
    </div>
  );
}

function Loading() {
  return (
    <div className="loading" role="status" aria-label="Loading stories">
      {[0, 1, 2].map((index) => (
        <div key={index} className="skeleton">
          <span />
          <span />
          <span />
        </div>
      ))}
    </div>
  );
}
function ErrorState({
  message,
  retry,
}: {
  message: string;
  retry: () => void;
}) {
  return (
    <div className="empty-state" role="alert">
      <CircleAlert size={30} strokeWidth={1.3} />
      <h2>Unable to load stories</h2>
      <p>{message}</p>
      <button type="button" className="secondary-button" onClick={retry}>
        <RefreshCw size={15} /> Try again
      </button>
    </div>
  );
}
