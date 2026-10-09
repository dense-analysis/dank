import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import {
  ArrowDown,
  ArrowRight,
  CircleAlert,
  Inbox,
  Menu,
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
import { FeedToolbar } from "./components/FeedToolbar";
import { Modal } from "./components/Modal";
import { PostCard } from "./components/PostCard";
import { ReaderSettings } from "./components/ReaderSettings";
import { Sidebar } from "./components/Sidebar";
import { SourcesView } from "./components/SourcesView";
import { useLocation } from "./hooks/useLocation";
import { useReaderPreferences } from "./hooks/useReaderPreferences";
import { useReadPosts } from "./hooks/useReadPosts";
import { fetchPosts, fetchSources } from "./lib/api";
import { articleParams, filterParams, parseFilters } from "./lib/navigation";
import { readingFonts } from "./lib/preferences";
import { defaultFilters, postKey } from "./lib/storage";
import type { Filters, Post } from "./lib/types";

export function App() {
  const { params, navigate, backTo, positionError } = useLocation();
  const { read, markRead, storageError } = useReadPosts();
  const { preferences } = useReaderPreferences();
  const [mobileOpen, setMobileOpen] = useState(false);
  const view = params.get("view");
  const section = view === "sources" || view === "settings" ? view : "all";
  const filters = parseFilters(params);
  const articleId = params.get("article");
  const articleDomain = params.get("article_source");
  const readingArticle = Boolean(articleId && articleDomain);
  const sources = useQuery({
    queryKey: ["sources"],
    queryFn: ({ signal }) => fetchSources(signal),
  });
  const posts = useInfiniteQuery({
    queryKey: ["posts", filters],
    queryFn: ({ pageParam, signal }) => fetchPosts(filters, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: section === "all",
  });
  const loadMoreRef = useRef<HTMLDivElement>(null);
  const { fetchNextPage, hasNextPage, isFetching, isError } = posts;

  useEffect(() => {
    const target = loadMoreRef.current;
    if (
      !target ||
      section !== "all" ||
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
    section,
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
  const title =
    section === "settings"
      ? "Settings"
      : section === "sources"
        ? "Sources"
        : "All stories";
  const date = new Intl.DateTimeFormat(undefined, {
    weekday: "long",
    month: "long",
    day: "numeric",
  }).format(new Date());

  function select(nextView: string, nextFilters = defaultFilters) {
    const next = filterParams(nextFilters);
    if (nextView !== "all") next.set("view", nextView);
    navigate(next);
    setMobileOpen(false);
  }
  function changeFilters(nextFilters: Filters) {
    navigate(filterParams(nextFilters), true);
  }
  function openPost(post: Post) {
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
    select("all", { ...defaultFilters, domains: [domain] });
  }
  function selectAuthor(author: string) {
    navigate(filterParams({ ...filters, author, authorExact: true }));
  }

  const sidebar = (
    <Sidebar
      section={section}
      select={select}
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
      <div className="main-shell">
        <header className="topbar">
          <button
            type="button"
            className="icon-button menu-button"
            onClick={() => setMobileOpen(true)}
            aria-label="Open navigation"
          >
            <Menu size={21} />
          </button>
          <div className="breadcrumb">
            {section === "all" && !readingArticle ? (
              <h1>{title}</h1>
            ) : (
              <span>{title}</span>
            )}
          </div>
        </header>
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
          />
        )}
        <div hidden={readingArticle}>
          <div
            className={`page-grid ${section === "settings" ? "settings-page" : ""}`}
          >
            <main
              id={readingArticle ? undefined : "main"}
              className="timeline"
              aria-label={section === "all" ? "All stories" : undefined}
              tabIndex={-1}
              data-page-focus
            >
              <div className="page-heading">
                {section === "all" && <p className="eyebrow">{date}</p>}
                {section !== "all" && (
                  <div className="title-row">
                    <h1>{title}</h1>
                  </div>
                )}
              </div>
              {section === "settings" ? (
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
                    sources={sources.data?.sources ?? []}
                    change={changeFilters}
                  />
                  <div className="timeline-label">
                    <span>
                      {filters.q ? "SEARCH RESULTS" : "LATEST"}
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
                      {timeline.map((post) => (
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
                        />
                      ))}
                    </div>
                  ) : (
                    <div className="empty-state">
                      <Inbox size={30} strokeWidth={1.3} />
                      <h2>No stories found</h2>
                      <p>
                        Try a wider date range, fewer filters, or a different
                        search.
                      </p>
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => select("all")}
                      >
                        Show all stories <ArrowRight size={14} />
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
