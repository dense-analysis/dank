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
  Fragment,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { ArticleView } from "./components/ArticleView";
import { CollectionCount } from "./components/CollectionCount";
import { CollectionHeader } from "./components/CollectionHeader";
import { CommunityAvatar } from "./components/CommunityAvatar";
import { Count } from "./components/Count";
import { FeedToolbar } from "./components/FeedToolbar";
import {
  HackerNewsCard,
  HackerNewsDay,
  HackerNewsFilters,
} from "./components/HackerNewsFeed";
import { IdentityAvatar } from "./components/IdentityAvatar";
import { Modal } from "./components/Modal";
import { PlatformFilters } from "./components/PlatformFilters";
import { PlatformIcon } from "./components/PlatformIcon";
import { PostCard } from "./components/PostCard";
import {
  type ConnectionsMode,
  PostConnections,
} from "./components/PostConnections";
import { PostLinks } from "./components/PostLinks";
import { ReaderSettings } from "./components/ReaderSettings";
import { RefreshStatus } from "./components/RefreshStatus";
import { Sidebar } from "./components/Sidebar";
import { SourcesView } from "./components/SourcesView";
import { XPostCard } from "./components/XPostCard";
import { YouTubeCollection } from "./components/YouTubeCollection";
import { YouTubePostCard } from "./components/YouTubePostCard";
import { useLocation } from "./hooks/useLocation";
import { useReaderPreferences } from "./hooks/useReaderPreferences";
import { useReadPosts } from "./hooks/useReadPosts";
import { useRefresh } from "./hooks/useRefresh";
import { fetchPosts, fetchSources } from "./lib/api";
import {
  collectedAccounts,
  feedLabels,
  sectionFromView,
  sourceFeedKind,
  sourcesForFeed,
} from "./lib/collections";
import {
  connectionEvidenceParams,
  connectionReturn,
  connectionsClosedParams,
  connectionTargetParams,
} from "./lib/connectionNavigation";
import { articleParams, filterParams, parseFilters } from "./lib/navigation";
import {
  connectionContext,
  type PostConnectionTarget,
} from "./lib/postConnections";
import type { ConnectionDirection } from "./lib/postConnectionsGraph";
import { readingFonts } from "./lib/preferences";
import {
  parseReferenceTarget,
  referenceParams,
  type SourceReference,
} from "./lib/referenceFocus";
import { refreshActive, refreshContext, refreshScopeKey } from "./lib/refresh";
import { defaultFilters, postKey } from "./lib/storage";
import type { Filters, Post, Section } from "./lib/types";
import { isXPost } from "./lib/x";
import { fetchYouTubeSources } from "./lib/youtubeCollection";

export function App() {
  const { params, navigate, backTo, backToConnection, positionError } =
    useLocation();
  const refresh = useRefresh(navigate);
  const refreshTarget = refreshContext(params);
  const refreshingHere =
    refreshTarget &&
    (refresh.jobs.some(
      (job) =>
        refreshActive(job) &&
        refreshScopeKey(job.scope) === refreshScopeKey(refreshTarget.scope),
    ) ||
      (refresh.starting &&
        refresh.startingScope &&
        refreshScopeKey(refresh.startingScope) ===
          refreshScopeKey(refreshTarget.scope)));
  const returnToConnections = connectionReturn(params);
  const connectionsMode = params.get("connections");
  const connectionsDirection = params.get("connections_direction");
  const { read, markRead, storageError } = useReadPosts();
  const { preferences } = useReaderPreferences();
  const [mobileOpen, setMobileOpen] = useState(false);

  useEffect(() => {
    if (!mobileOpen) return;

    const desktop = window.matchMedia("(min-width: 761px)");
    const closeOnDesktop = () => {
      if (desktop.matches) setMobileOpen(false);
    };
    closeOnDesktop();
    desktop.addEventListener("change", closeOnDesktop);

    return () => desktop.removeEventListener("change", closeOnDesktop);
  }, [mobileOpen]);
  const section = sectionFromView(params.get("view"));
  const feedSection =
    section !== "sources" && section !== "settings" && section !== "youtube";
  const filters = parseFilters(params);
  const mixedVideoDetail = section === "all" && !!params.get("youtube_item");
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
  const blogSources = sourcesForFeed(allSources, "stories").filter(
    (source) => source.configured !== false,
  );
  const activeBlogSource =
    section === "stories" && filters.domains.length === 1
      ? allSources.find((source) => source.domain === filters.domains[0])
      : undefined;
  const blogContext =
    section === "hacker_news" ||
    (section === "stories" && (!!filters.blogFeed || !!activeBlogSource));
  const blogTitle =
    filters.blogFeed === "external"
      ? "From external"
      : activeBlogSource?.name ||
        (filters.blogFeed === "sources" ? "Source Feeds" : "Blogs");
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
    accountContext || communityContext || youtubeContext || blogContext;
  const visibleSources = sourcesForFeed(allSources, filters.kind).filter(
    (source) => filters.blogFeed !== "sources" || source.configured !== false,
  );
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
              : section === "stories"
                ? blogTitle
                : feedLabels[filters.kind];
  const overviewPlatform =
    !accountContext &&
    !communityContext &&
    (section === "x" || section === "reddit")
      ? section
      : null;
  const headerTitle =
    section === "hacker_news" ? (
      <span className="hn-title">
        <PlatformIcon platform="hacker_news" size={23} />
        <span>{title}</span>
      </span>
    ) : section === "youtube" && !youtubeContext ? (
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
  function videoParams(post: Post) {
    const next = new URLSearchParams(params);
    next.set("youtube_item", post.id);

    return next;
  }
  function closeArticle() {
    if (returnToConnections) backToConnection(returnToConnections);
    else backTo(connectionsClosedParams(params));
  }
  function setConnectionsMode(mode: ConnectionsMode) {
    const next = new URLSearchParams(params);
    if (mode) next.set("connections", mode);
    else next.delete("connections");
    navigate(next, true, undefined, { preservePosition: true });
  }
  function setConnectionsDirection(direction: ConnectionDirection) {
    const next = new URLSearchParams(params);
    if (direction === "all") next.delete("connections_direction");
    else next.set("connections_direction", direction);
    navigate(next, true, undefined, { preservePosition: true });
  }
  function evidenceHref(evidence: SourceReference) {
    return `/reader/?${connectionEvidenceParams(params, evidence)}`;
  }
  function openEvidence(evidence: SourceReference) {
    navigate(
      connectionEvidenceParams(params, evidence),
      false,
      params.toString(),
    );
  }
  function targetHref(target: PostConnectionTarget) {
    const next = connectionTargetParams(params, target);
    return next ? `/reader/?${next}` : undefined;
  }
  function exploreTarget(target: PostConnectionTarget) {
    const next = connectionTargetParams(params, target);
    if (next) navigate(next, false, params.toString());
  }
  function selectSource(domain: string) {
    select(sourceFeedKind(domain), {
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
  function selectBlog(selection: string) {
    if (selection.startsWith("source:")) {
      selectSource(selection.slice(7));
    } else if (selection === "external" || selection === "sources") {
      select("stories", { ...defaultFilters, blogFeed: selection });
    }
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
      blogSources={blogSources}
      blogExternalCount={sources.data?.blog_external_count ?? 0}
      blogSelection={
        section === "hacker_news"
          ? "source:news.ycombinator.com"
          : section === "stories"
            ? filters.domains.length === 1
              ? `source:${filters.domains[0]}`
              : filters.blogFeed
            : ""
      }
      selectBlog={selectBlog}
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
      data-fit-image-previews={preferences.fitImagePreviews}
      data-fit-video-previews={preferences.fitVideoPreviews}
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
          dismissOnBackdrop
        >
          {sidebar}
        </Modal>
      ) : (
        sidebar
      )}
      <RefreshStatus refresh={refresh} />
      <div
        className={`main-shell ${section === "youtube" ? "youtube-shell" : ""} ${collectionContext ? "collection-context" : ""}`}
      >
        <CollectionHeader
          title={title}
          actions={
            refreshTarget && (
              <button
                type="button"
                className="refresh-button"
                aria-label={`Refresh ${refreshTarget.label}`}
                title={
                  refreshTarget.disabled ??
                  `Collect fresh content for ${refreshTarget.label}. Search, dates and reading filters do not limit collection.`
                }
                disabled={
                  !!refreshTarget.disabled ||
                  !!refreshingHere ||
                  refresh.starting
                }
                onClick={() => refresh.start(refreshTarget.scope)}
              >
                <RefreshCw
                  size={17}
                  className={refreshingHere ? "refresh-spinning" : undefined}
                />
                <span>{refreshingHere ? "Refreshing…" : "Refresh"}</span>
              </button>
            )
          }
          titleContent={headerTitle}
          pageHeading={
            (feedSection || section === "youtube") && !readingArticle
          }
          openNavigation={() => setMobileOpen(true)}
          navigationOpen={mobileOpen}
          back={
            collectionContext
              ? {
                  label: `Back to ${blogContext ? "Blogs" : accountContext ? "X posts" : communityContext ? "Reddit" : "YouTube"}`,
                  onClick: () =>
                    select(
                      blogContext
                        ? "stories"
                        : accountContext
                          ? "x"
                          : communityContext
                            ? "reddit"
                            : "youtube",
                    ),
                }
              : undefined
          }
          avatar={
            activeBlogSource ? (
              <IdentityAvatar
                name={activeBlogSource.name}
                iconUrl={activeBlogSource.icon_url}
                kind="source"
              />
            ) : accountContext ? (
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
            activeBlogSource ? (
              <CollectionCount count={activeBlogSource.count} />
            ) : accountContext && activeAccount ? (
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
            referenceTarget={parseReferenceTarget(params)}
            params={params}
            openPost={openPost}
            close={closeArticle}
            backLabel={returnToConnections ? "Back to connections" : undefined}
            renderConnections={(post) => (
              <PostConnections
                key={postKey(post)}
                post={post}
                context={connectionContext(filters)}
                mode={
                  connectionsMode === "graph" || connectionsMode === "list"
                    ? connectionsMode
                    : null
                }
                onModeChange={setConnectionsMode}
                direction={
                  connectionsDirection === "incoming" ||
                  connectionsDirection === "outgoing"
                    ? connectionsDirection
                    : "all"
                }
                onDirectionChange={setConnectionsDirection}
                evidenceHref={evidenceHref}
                onOpenEvidence={openEvidence}
                targetHref={targetHref}
                onExploreTarget={exploreTarget}
              />
            )}
            sourceFeed={sourceFeedKind(articleDomain)}
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
            className={`page-grid ${section === "youtube" || mixedVideoDetail ? "youtube-page" : ""} ${section === "settings" ? "settings-page" : ""}`}
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
                section !== "youtube" &&
                !mixedVideoDetail && (
                  <div className="page-heading">
                    {feedSection && <p className="eyebrow">{date}</p>}
                    {section === "hacker_news" && (
                      <p className="collection-description">
                        Stories from hckr news, grouped by their front-page day.
                      </p>
                    )}
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
              {section === "youtube" || mixedVideoDetail ? (
                !readingArticle && (
                  <YouTubeCollection
                    backLabel={mixedVideoDetail ? "Back to all" : undefined}
                    params={params}
                    navigate={navigate}
                    backTo={backTo}
                    returnToConnections={
                      returnToConnections
                        ? () => backToConnection(returnToConnections)
                        : undefined
                    }
                    sources={youtubeSources.data}
                    sourcesError={
                      youtubeSources.isError
                        ? youtubeSources.error.message
                        : undefined
                    }
                    retrySources={() => void youtubeSources.refetch()}
                    referenceHref={
                      returnToConnections ? evidenceHref : undefined
                    }
                    openReference={(reference) => {
                      if (returnToConnections) openEvidence(reference);
                      else
                        navigate(
                          referenceParams(params, reference),
                          false,
                          params.toString(),
                        );
                    }}
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
                  {(section === "all" || section === "stories") && (
                    <p className="feed-description">
                      {section === "stories"
                        ? filters.blogFeed === "external"
                          ? "Articles linked from your feeds, including X and Reddit."
                          : filters.blogFeed === "sources" || activeBlogSource
                            ? "Posts from the source feeds you follow."
                            : "Your source feeds and discovered articles, together."
                        : "Your followed sources and discoveries, together."}
                    </p>
                  )}
                  <FeedToolbar
                    filters={filters}
                    sources={visibleSources}
                    change={changeFilters}
                  />
                  {section === "all" && (
                    <PlatformFilters filters={filters} change={changeFilters} />
                  )}
                  {section === "hacker_news" && (
                    <HackerNewsFilters
                      value={filters.hnFilter}
                      change={(hnFilter) =>
                        changeFilters({ ...filters, hnFilter })
                      }
                    />
                  )}
                  <div className="timeline-label">
                    <span>
                      {filters.q
                        ? "SEARCH RESULTS"
                        : section === "reddit" ||
                            section === "x" ||
                            section === "hacker_news"
                          ? "COLLECTED POSTS"
                          : "LATEST"}
                      {posts.isFetching && !posts.isPending && (
                        <span className="updating">Updating…</span>
                      )}
                    </span>
                  </div>
                  {posts.isError &&
                    posts.data &&
                    !posts.isFetchNextPageError && (
                      <div className="error-banner" role="alert">
                        Saved stories are still available. Updates could not be
                        loaded.
                        <button
                          type="button"
                          className="text-button"
                          onClick={() => void posts.refetch()}
                        >
                          Try again
                        </button>
                      </div>
                    )}
                  {posts.isPending ? (
                    <Loading />
                  ) : posts.isError && !posts.data ? (
                    <ErrorState
                      message={posts.error.message}
                      retry={() => void posts.refetch()}
                    />
                  ) : timeline.length ? (
                    <div
                      className={`post-list ${section === "all" ? "universal-feed" : ""}`}
                    >
                      {timeline.map((post, index) =>
                        post.hacker_news ? (
                          <Fragment key={postKey(post)}>
                            {section === "hacker_news" &&
                              filters.sort !== "relevance" &&
                              post.hacker_news.day !==
                                timeline[index - 1]?.hacker_news?.day && (
                                <HackerNewsDay day={post.hacker_news.day} />
                              )}
                            <HackerNewsCard
                              post={post}
                              read={
                                readKeys.has(
                                  postKey(post.linked_post ?? post),
                                ) || readKeys.has(postKey(post))
                              }
                              open={() => openPost(post.linked_post ?? post)}
                              href={`/reader/?${articleParams(params, post.linked_post ?? post)}`}
                              discussionHref={`/reader/?${articleParams(params, post)}`}
                              openDiscussion={() => openPost(post)}
                              links={
                                <PostLinks
                                  post={post}
                                  params={params}
                                  open={openPost}
                                />
                              }
                              query={filters.q}
                              filters={filters}
                              selectAuthor={selectAuthor}
                            />
                          </Fragment>
                        ) : isXPost(post) ? (
                          <XPostCard
                            mixedFeed={section === "all"}
                            params={params}
                            openPost={openPost}
                            key={postKey(post)}
                            post={post}
                            links={
                              <PostLinks
                                post={post}
                                params={params}
                                open={openPost}
                              />
                            }
                            read={readKeys.has(postKey(post))}
                            open={() => openPost(post)}
                            href={`/reader/?${articleParams(params, post)}`}
                            selectAccount={selectAccount}
                            query={filters.q}
                          />
                        ) : post.source === "youtube" || post.youtube ? (
                          <YouTubePostCard
                            key={postKey(post)}
                            post={post}
                            read={readKeys.has(postKey(post))}
                            href={`/reader/?${videoParams(post)}`}
                            open={() =>
                              navigate(
                                videoParams(post),
                                false,
                                params.toString(),
                              )
                            }
                            query={filters.q}
                            filters={filters}
                            selectAuthor={selectAuthor}
                            links={
                              <PostLinks
                                post={post}
                                params={params}
                                open={openPost}
                              />
                            }
                          />
                        ) : (
                          <PostCard
                            sourceFeed={sourceFeedKind(post.domain)}
                            key={postKey(post)}
                            post={post}
                            links={
                              <PostLinks
                                post={post}
                                params={params}
                                open={openPost}
                              />
                            }
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
                        {section === "reddit" ||
                        section === "x" ||
                        section === "hacker_news"
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
                        {section === "hacker_news"
                          ? "Reset Hacker News filters"
                          : section === "reddit" || section === "x"
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
                      Showing the top <Count value={timeline.length} /> matches.
                      Narrow your search, or choose a chronological order to
                      browse further.
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
