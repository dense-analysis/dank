import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import {
  ArrowDown,
  ArrowRight,
  Bookmark,
  Check,
  ChevronRight,
  CircleAlert,
  Inbox,
  Menu,
  Plus,
  RefreshCw,
  Settings2,
  Sparkles,
  X,
} from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { ArticleDialog } from "./components/ArticleDialog";
import { FeedDialog } from "./components/FeedDialog";
import { FeedToolbar } from "./components/FeedToolbar";
import { Modal } from "./components/Modal";
import { PostCard, SourceMark } from "./components/PostCard";
import { Sidebar } from "./components/Sidebar";
import { SourcesView } from "./components/SourcesView";
import { useLibrary } from "./hooks/useLibrary";
import { useLocation } from "./hooks/useLocation";
import { fetchPosts, fetchSources } from "./lib/api";
import { filterParams, parseFilters } from "./lib/navigation";
import { defaultFilters, postKey } from "./lib/storage";
import type { Filters, Post, SavedFeed, Source } from "./lib/types";

export function App() {
  const { params, navigate } = useLocation();
  const { library, update, storageError } = useLibrary();
  const [feedEditor, setFeedEditor] = useState<SavedFeed | "new" | null>(null);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [notice, setNotice] = useState("");
  const articleOpenedHere = useRef(false);
  const section = params.get("view") || "all";
  const activeFeed = library.feeds.find((feed) => feed.id === section);
  const filters = parseFilters(params);
  const feedChanged =
    activeFeed &&
    filterParams(filters).toString() !==
      filterParams(activeFeed.filters).toString();
  const articleId = params.get("article");
  const articleDomain = params.get("article_source");
  const sources = useQuery({
    queryKey: ["sources"],
    queryFn: ({ signal }) => fetchSources(signal),
  });
  const posts = useInfiniteQuery({
    queryKey: ["posts", filters],
    queryFn: ({ pageParam, signal }) => fetchPosts(filters, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: section !== "saved" && section !== "sources",
  });
  const timeline = useMemo(() => {
    const unique = new Map<string, Post>();
    for (const page of posts.data?.pages ?? [])
      for (const post of page.posts) unique.set(postKey(post), post);
    return [...unique.values()];
  }, [posts.data]);
  const displayed = section === "saved" ? library.bookmarks : timeline;
  const selected = [...displayed, ...library.bookmarks].find(
    (post) => post.id === articleId && post.domain === articleDomain,
  );
  const savedKeys = new Set(library.bookmarks.map(postKey));
  const readKeys = new Set(library.read);
  const title =
    section === "saved"
      ? "Saved for a quieter moment."
      : section === "sources"
        ? "Good reading starts here."
        : (activeFeed?.name ?? "All stories");
  const subtitle =
    section === "saved"
      ? "The stories you want to come back to."
      : section === "sources"
        ? "Different voices. A wider perspective."
        : activeFeed
          ? "Your sources, your interests, your perspective."
          : "Your world, at your own pace.";
  const date = new Intl.DateTimeFormat(undefined, {
    weekday: "long",
    month: "long",
    day: "numeric",
  }).format(new Date());

  function select(view: string, nextFilters?: Filters) {
    const feed = library.feeds.find((item) => item.id === view);
    const next = filterParams(nextFilters ?? feed?.filters ?? defaultFilters);
    if (view !== "all") next.set("view", view);
    navigate(next);
    setMobileOpen(false);
    window.scrollTo(0, 0);
  }
  function changeFilters(nextFilters: Filters) {
    const next = filterParams(nextFilters);
    if (activeFeed) next.set("view", activeFeed.id);
    navigate(next, true);
  }
  function openPost(post: Post) {
    const next = new URLSearchParams(params);
    next.set("article", post.id);
    next.set("article_source", post.domain);
    articleOpenedHere.current = true;
    navigate(next);
    if (!readKeys.has(postKey(post)))
      update((current) => ({
        ...current,
        read: [...current.read, postKey(post)].slice(-5000),
      }));
  }
  function closeArticle() {
    if (articleOpenedHere.current) {
      articleOpenedHere.current = false;
      window.history.back();
    } else {
      const next = new URLSearchParams(params);
      next.delete("article");
      next.delete("article_source");
      navigate(next, true);
    }
  }
  function toggleSave(post: Post) {
    const saved = savedKeys.has(postKey(post));
    update((current) => ({
      ...current,
      bookmarks: saved
        ? current.bookmarks.filter((item) => postKey(item) !== postKey(post))
        : [post, ...current.bookmarks],
    }));
    setNotice(saved ? "Removed from saved stories" : "Saved for later");
  }
  function createFeed() {
    setMobileOpen(false);
    setFeedEditor("new");
  }
  function saveFeed(feed: SavedFeed) {
    update((current) => ({
      ...current,
      feeds: current.feeds.some((item) => item.id === feed.id)
        ? current.feeds.map((item) => (item.id === feed.id ? feed : item))
        : [...current.feeds, feed],
    }));
    setFeedEditor(null);
    const next = filterParams(feed.filters);
    next.set("view", feed.id);
    navigate(next);
    setNotice("Feed saved");
  }
  function removeFeed(id: string) {
    update((current) => ({
      ...current,
      feeds: current.feeds.filter((feed) => feed.id !== id),
    }));
    setFeedEditor(null);
    select("all");
    setNotice("Feed deleted");
  }
  function selectSource(source: Source) {
    select("all", { ...defaultFilters, domains: [source.domain] });
  }

  const sidebar = (
    <Sidebar
      section={section}
      feeds={library.feeds}
      savedCount={library.bookmarks.length}
      select={select}
      newFeed={createFeed}
      mobileOpen={mobileOpen}
      close={() => setMobileOpen(false)}
    />
  );

  return (
    <div className="app-shell">
      <a className="skip-link" href="#main">
        Skip to stories
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
            Your workspace
            <ChevronRight size={13} />
            <span>
              {activeFeed?.name ??
                (section === "saved"
                  ? "Saved"
                  : section === "sources"
                    ? "Sources"
                    : "All stories")}
            </span>
          </div>
          <span className="topbar-right">
            <span
              className={`connection-dot ${sources.isError ? "offline" : ""}`}
            />
            {sources.isError
              ? "Connection unavailable"
              : sources.isPending
                ? "Connecting"
                : "Your personal reader"}
          </span>
        </header>
        <div className="page-grid">
          <main id="main" className="timeline">
            <div className="page-heading">
              <p className="eyebrow">{date}</p>
              <div className="title-row">
                <h1>{title}</h1>
                {activeFeed ? (
                  <button
                    type="button"
                    className="icon-button"
                    aria-label="Edit feed"
                    onClick={() => setFeedEditor({ ...activeFeed, filters })}
                  >
                    <Settings2 size={20} />
                  </button>
                ) : (
                  <span className="heading-icon">
                    <Sparkles size={23} strokeWidth={1.4} />
                  </span>
                )}
              </div>
              <p className="subtitle">{subtitle}</p>
            </div>
            {storageError && (
              <div className="error-banner" role="alert">
                <CircleAlert size={16} />
                Browser storage is unavailable or full. Changes will last only
                until this page closes.
              </div>
            )}
            {section !== "sources" && section !== "saved" && (
              <FeedToolbar
                filters={filters}
                sources={sources.data?.sources ?? []}
                change={changeFilters}
                saveFeed={createFeed}
              />
            )}
            {feedChanged && (
              <div className="feed-changed">
                <span>You’ve adjusted this feed’s filters.</span>
                <button
                  type="button"
                  className="text-button"
                  onClick={() => setFeedEditor({ ...activeFeed, filters })}
                >
                  Save changes
                </button>
                <button
                  type="button"
                  className="text-button"
                  onClick={() => changeFilters(activeFeed.filters)}
                >
                  Reset
                </button>
              </div>
            )}
            {section === "sources" ? (
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
                  select={selectSource}
                />
              )
            ) : (
              <>
                <div className="timeline-label">
                  <span>
                    {section === "saved"
                      ? `${displayed.length} SAVED ${displayed.length === 1 ? "STORY" : "STORIES"}`
                      : filters.q
                        ? "SEARCH RESULTS"
                        : "THE LATEST"}
                    {posts.isFetching &&
                      !posts.isPending &&
                      section !== "saved" && (
                        <span className="updating">Updating…</span>
                      )}
                  </span>
                  {section !== "saved" && (
                    <button
                      type="button"
                      className="text-button"
                      onClick={() => void posts.refetch()}
                      disabled={posts.isFetching}
                    >
                      <RefreshCw size={13} />
                      Refresh
                    </button>
                  )}
                </div>
                {section !== "saved" && posts.isPending ? (
                  <Loading />
                ) : section !== "saved" && posts.isError ? (
                  <ErrorState
                    message={posts.error.message}
                    retry={() => void posts.refetch()}
                  />
                ) : displayed.length ? (
                  <div className="post-list">
                    {displayed.map((post) => (
                      <PostCard
                        key={postKey(post)}
                        post={post}
                        saved={savedKeys.has(postKey(post))}
                        read={readKeys.has(postKey(post))}
                        open={() => openPost(post)}
                        toggleSave={() => toggleSave(post)}
                      />
                    ))}
                  </div>
                ) : (
                  <div className="empty-state">
                    {section === "saved" ? (
                      <Bookmark size={30} strokeWidth={1.3} />
                    ) : (
                      <Inbox size={30} strokeWidth={1.3} />
                    )}
                    <h2>
                      {section === "saved"
                        ? "Keep a good story for later."
                        : "A little too quiet here."}
                    </h2>
                    <p>
                      {section === "saved"
                        ? "Tap the bookmark on any story. It’ll be waiting here when you have a moment."
                        : "No stories match this view. Try a wider date range, fewer filters, or a different search."}
                    </p>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => select("all")}
                    >
                      {section === "saved"
                        ? "Explore stories"
                        : "Show all stories"}
                      <ArrowRight size={14} />
                    </button>
                  </div>
                )}
                {section !== "saved" && posts.hasNextPage && (
                  <div className="load-more">
                    <button
                      type="button"
                      className="secondary-button"
                      disabled={posts.isFetchingNextPage}
                      onClick={() => void posts.fetchNextPage()}
                    >
                      {posts.isFetchingNextPage
                        ? "Loading stories…"
                        : "More stories"}
                      <ArrowDown size={15} />
                    </button>
                  </div>
                )}
                {section !== "saved" &&
                  posts.data?.pages.some((page) => page.limited) && (
                    <p className="result-note">
                      Showing the top {displayed.length} matches. Narrow your
                      search, or choose a chronological order to browse further.
                    </p>
                  )}
                {displayed.length > 0 &&
                  (section === "saved" || !posts.hasNextPage) && (
                    <p className="end-note">
                      <span />A little more perspective.
                      <span />
                    </p>
                  )}
              </>
            )}
          </main>
          <aside className="context-rail" aria-label="Your library">
            <div className="rail-heading">
              YOUR LIBRARY<span className="tiny-icon">↗</span>
            </div>
            <div className="library-stats">
              <div>
                <strong>
                  {sources.data?.total_posts.toLocaleString() ?? "—"}
                </strong>
                <span>stories collected</span>
              </div>
              <div>
                <strong>{sources.data?.sources.length ?? "—"}</strong>
                <span>sources</span>
              </div>
            </div>
            <div className="rail-divider" />
            <div className="rail-heading">
              VOICES IN YOUR FEED
              <button
                type="button"
                className="text-button"
                onClick={() => select("sources")}
              >
                View all
              </button>
            </div>
            <div className="rail-sources">
              {(sources.data?.sources ?? [])
                .filter((source) => source.count > 0)
                .slice(0, 5)
                .map((source) => (
                  <button
                    type="button"
                    className="rail-source"
                    key={source.domain}
                    onClick={() => selectSource(source)}
                  >
                    <SourceMark domain={source.domain} />
                    <span>
                      <strong>{source.name}</strong>
                      <small>{source.count} stories</small>
                    </span>
                    <ChevronRight size={14} />
                  </button>
                ))}
            </div>
            <div className="feed-invitation">
              <span className="invitation-icon">
                <LayersIcon />
              </span>
              <h2>Follow your curiosity.</h2>
              <p>
                A few trusted sources. A topic you care about. Make a feed that
                feels like you.
              </p>
              <button
                type="button"
                className="secondary-button"
                onClick={createFeed}
              >
                <Plus size={14} />
                Create a feed
              </button>
            </div>
            <p className="rail-footnote">
              Made for reading, at your pace.
              <br />
              No noise. Just your sources.
            </p>
          </aside>
        </div>
      </div>
      {notice && (
        <div className="toast" role="status">
          <Check size={16} />
          {notice}
          <button
            type="button"
            className="icon-button"
            aria-label="Dismiss notification"
            onClick={() => setNotice("")}
          >
            <X size={14} />
          </button>
        </div>
      )}
      {articleId && articleDomain && (
        <ArticleDialog
          key={`${articleDomain}:${articleId}`}
          domain={articleDomain}
          id={articleId}
          initial={selected}
          saved={savedKeys.has(
            postKey({ id: articleId, domain: articleDomain }),
          )}
          close={closeArticle}
          toggleSave={toggleSave}
        />
      )}
      {feedEditor && (
        <FeedDialog
          initial={feedEditor === "new" ? undefined : feedEditor}
          filters={filters}
          sources={sources.data?.sources ?? []}
          close={() => setFeedEditor(null)}
          save={saveFeed}
          remove={removeFeed}
        />
      )}
    </div>
  );
}
function LayersIcon() {
  return (
    <svg
      width="27"
      height="27"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      aria-hidden="true"
    >
      <path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5" />
    </svg>
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
      <h2>Let’s reconnect your reader.</h2>
      <p>{message}</p>
      <button type="button" className="secondary-button" onClick={retry}>
        <RefreshCw size={15} />
        Try again
      </button>
    </div>
  );
}
