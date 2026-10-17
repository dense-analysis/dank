import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import {
  ArrowDown,
  ArrowLeft,
  ArrowUpRight,
  CircleAlert,
  Clock3,
  Film,
  Link2,
  MessageSquare,
  RefreshCw,
  Search,
  Sparkles,
  X,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { articleParams } from "../lib/navigation";
import { safeUrl } from "../lib/urls";
import {
  fetchYouTubeItems,
  type YouTubeItem,
  type YouTubeReference,
  type YouTubeSources,
  youtubeFilters,
  youtubeShelves,
} from "../lib/youtubeCollection";
import { youtubeFeedLabel, youtubeGroupFeeds } from "../lib/youtubeNavigation";
import { CountryFlag } from "./CountryFlag";
import { YouTubeCard } from "./YouTubeCard";

export function YouTubeCollection({
  params,
  navigate,
  backTo,
  sources,
  sourcesError,
  retrySources,
  openReference,
}: {
  params: URLSearchParams;
  navigate: (
    params: URLSearchParams,
    replace?: boolean,
    backTo?: string,
  ) => void;
  backTo: (params: URLSearchParams) => void;
  sources?: YouTubeSources;
  sourcesError?: string;
  retrySources: () => void;
  openReference: (reference: YouTubeReference) => void;
}) {
  const filters = youtubeFilters(params);
  const [search, setSearch] = useState(filters.q);
  const selectedId = params.get("youtube_item");
  const activeFeed = sources?.feeds.find((feed) => feed.id === filters.feed);
  const activeChannel = sources?.channels.find(
    (channel) => channel.id === filters.channel,
  );
  const linked = filters.feed === "linked";
  const groupKind =
    activeFeed?.kind === "news" || activeFeed?.kind === "hype"
      ? activeFeed.kind
      : undefined;
  const groupFeeds = groupKind ? youtubeGroupFeeds(sources, groupKind) : [];
  const scoped = !!filters.feed || !!filters.channel;
  const title = linked
    ? "From your feeds"
    : activeChannel?.name ||
      activeFeed?.name ||
      (scoped ? "Collected feed" : "Collected recommendations");
  const query = useInfiniteQuery({
    queryKey: ["youtube", filters],
    queryFn: ({ pageParam, signal }) =>
      fetchYouTubeItems(filters, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
  const items = useMemo(
    () => [
      ...new Map(
        (query.data?.pages.flatMap((page) => page.items) ?? []).map((item) => [
          item.id,
          item,
        ]),
      ).values(),
    ],
    [query.data],
  );
  const cachedItem = items.find((item) => item.id === selectedId);
  const detail = useQuery({
    queryKey: ["youtube-item", selectedId],
    queryFn: ({ signal }) =>
      fetchYouTubeItems(
        { feed: "", channel: "", kind: "", q: "" },
        undefined,
        signal,
        selectedId ?? undefined,
      ),
    enabled: !!selectedId && !cachedItem,
  });
  const selected =
    cachedItem ?? detail.data?.items.find((item) => item.id === selectedId);
  const more = useRef<HTMLDivElement>(null);
  const { fetchNextPage, hasNextPage, isFetching, isError } = query;

  useEffect(() => setSearch(filters.q), [filters.q]);
  useEffect(() => {
    const target = more.current;
    if (
      !target ||
      selectedId ||
      !hasNextPage ||
      isFetching ||
      isError ||
      typeof IntersectionObserver === "undefined"
    )
      return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          observer.disconnect();
          void fetchNextPage({ cancelRefetch: false });
        }
      },
      { rootMargin: "0px 0px 240px 0px" },
    );
    observer.observe(target);

    return () => observer.disconnect();
  }, [selectedId, hasNextPage, isFetching, isError, fetchNextPage]);

  function change(name: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(name, value);
    else next.delete(name);
    next.delete("youtube_item");
    navigate(next);
  }
  function open(item: YouTubeItem) {
    const next = new URLSearchParams(params);
    next.set("youtube_item", item.id);
    navigate(next, false, params.toString());
  }
  function close() {
    const next = new URLSearchParams(params);
    next.delete("youtube_item");
    backTo(next);
  }
  function href(item: YouTubeItem) {
    const next = new URLSearchParams(params);
    next.set("youtube_item", item.id);

    return `/reader/?${next}`;
  }

  if (selectedId) {
    return (
      <div className="yt-detail">
        <button type="button" className="text-button yt-back" onClick={close}>
          <ArrowLeft size={16} /> Back to {title.toLowerCase()}
        </button>
        {selected ? (
          <>
            <div className="yt-detail-layout">
              <YouTubeCard
                key={selected.id}
                item={selected}
                feed={filters.feed}
                open={() => {}}
                href={href(selected)}
                detailed
              />
              <aside
                className="yt-detail-context"
                aria-label="Discovery context"
              >
                <p className="eyebrow">Discovery context</p>
                <h2>How it found you</h2>
                <p className="yt-detail-note">
                  {selected.discoveries.length
                    ? "Saved from the feed as it appeared at collection time."
                    : "Connected through links in your collected posts."}
                </p>
                {selected.discoveries.length ? (
                  selected.discoveries.map((discovery) => (
                    <div
                      className="yt-discovery"
                      key={`${discovery.run_id}:${discovery.feed_url}:${discovery.shelf}:${discovery.rank}:${discovery.observed_at}`}
                    >
                      <strong>{discovery.shelf || discovery.feed_name}</strong>
                      <dl>
                        <dt>Feed</dt>
                        <dd>{discovery.feed_name || discovery.feed_kind}</dd>
                        {discovery.tab && (
                          <>
                            <dt>News tab</dt>
                            <dd>{discovery.tab.replaceAll("_", " ")}</dd>
                          </>
                        )}
                        {discovery.rank != null && (
                          <>
                            <dt>Position</dt>
                            <dd>#{discovery.rank}</dd>
                          </>
                        )}
                        {discovery.region && (
                          <>
                            <dt>Region</dt>
                            <dd>{discovery.region}</dd>
                          </>
                        )}
                        <dt>Captured</dt>
                        <dd>
                          <CaptureTime value={discovery.observed_at} />
                        </dd>
                        {discovery.playlist_id && (
                          <>
                            <dt>Playlist</dt>
                            <dd>
                              <a
                                href={`https://www.youtube.com/playlist?list=${encodeURIComponent(discovery.playlist_id)}`}
                                target="_blank"
                                rel="noreferrer"
                              >
                                View playlist <ArrowUpRight size={12} />
                              </a>
                            </dd>
                          </>
                        )}
                      </dl>
                      {safeUrl(discovery.feed_url) && (
                        <a
                          className="text-button"
                          href={safeUrl(discovery.feed_url)}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Open original feed <ArrowUpRight size={12} />
                        </a>
                      )}
                    </div>
                  ))
                ) : (
                  <p className="yt-detail-note">
                    Found in links shared by your collected sources.
                  </p>
                )}
                <p className="yt-capture-note">
                  <Clock3 size={15} />
                  {selected.video_url
                    ? "Video downloaded for local playback."
                    : selected.detail_capture.status === "captured"
                      ? "Additional video details have been collected. Playback connects to YouTube."
                      : "Feed information is saved. Transcripts and comments have not been collected."}
                </p>
              </aside>
            </div>
            {!!selected.references.length && (
              <section
                className="yt-references"
                aria-label="Linked from collected posts"
              >
                <div className="yt-shelf-heading">
                  <h2>
                    <Link2 size={18} /> Shared in your feeds
                  </h2>
                  <span>
                    {selected.reference_count} source{" "}
                    {selected.reference_count === 1 ? "post" : "posts"}
                  </span>
                </div>
                <div className="yt-reference-list">
                  {selected.references.map((reference) => (
                    <YouTubeSourceReference
                      key={`${reference.domain}:${reference.post_id}`}
                      reference={reference}
                      params={params}
                      open={() => openReference(reference)}
                    />
                  ))}
                </div>
                {selected.reference_count > selected.references.length && (
                  <p className="result-note">
                    Showing {selected.references.length} of{" "}
                    {selected.reference_count} source posts.
                  </p>
                )}
              </section>
            )}
          </>
        ) : detail.isError ? (
          <YouTubeError
            message={detail.error.message}
            retry={() => void detail.refetch()}
          />
        ) : detail.isPending ? (
          <YouTubeLoading />
        ) : (
          <div className="empty-state">
            <Film size={30} />
            <h2>This discovery is unavailable</h2>
            <p>Return to the collection to browse the saved feed.</p>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="yt-collection">
      {!scoped && (
        <div className="yt-intro">
          <div>
            <p className="eyebrow">
              {linked
                ? "Connected discoveries"
                : activeFeed?.region
                  ? `${activeFeed.region} · Your collection`
                  : "Your collection"}
            </p>
            <h2>{title}</h2>
            <p>
              {linked
                ? "YouTube videos shared across your collected X posts, Reddit discussions and articles. One video, every connection."
                : "The recommendations, topics and moments worth keeping. Saved from your feeds, ready when you are."}
            </p>
          </div>
          <span className="yt-total">
            <strong>
              {(
                query.data?.pages[0]?.total ??
                sources?.total ??
                0
              ).toLocaleString()}
            </strong>
            <span>{linked ? "linked videos" : "discoveries"}</span>
          </span>
        </div>
      )}
      {!!groupFeeds.length && (
        <fieldset
          className="yt-feed-tabs"
          aria-label={groupKind === "news" ? "News topics" : "Hyped countries"}
        >
          {groupFeeds.map((feed) => (
            <button
              key={feed.id}
              type="button"
              aria-pressed={feed.id === filters.feed}
              onClick={() => change("feed", feed.id)}
            >
              {groupKind === "hype" && <CountryFlag region={feed.region} />}
              {youtubeFeedLabel(feed)}
            </button>
          ))}
        </fieldset>
      )}
      <div className="yt-tools">
        <form
          className="yt-search"
          aria-label="Search YouTube discoveries"
          onSubmit={(event) => {
            event.preventDefault();
            change("q", search.trim());
          }}
        >
          <Search size={18} />
          <input
            aria-label={`Search ${scoped ? title : "YouTube"}`}
            type="search"
            placeholder={`Search ${scoped ? title : "YouTube discoveries"}`}
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
          <button
            type="submit"
            className="icon-button"
            aria-label="Search YouTube collection"
          >
            <ArrowUpRight size={17} />
          </button>
        </form>
        <button
          type="button"
          className="icon-button yt-refresh"
          aria-label="Refresh YouTube collection"
          disabled={query.isFetching}
          onClick={() => {
            void query.refetch();
            retrySources();
          }}
        >
          <RefreshCw size={17} />
        </button>
      </div>
      <div className="yt-filter-row">
        <div className="yt-tabs">
          {[
            ["", "All"],
            ["video", "Videos"],
            ["short", "Shorts"],
            ["post", "Posts"],
          ].map(([kind, label]) => (
            <button
              type="button"
              key={kind}
              className={filters.kind === kind ? "active" : ""}
              aria-pressed={filters.kind === kind}
              onClick={() => change("format", kind)}
            >
              {label}
            </button>
          ))}
        </div>
        {filters.q && (
          <button
            type="button"
            className="text-button"
            onClick={() => change("q", "")}
          >
            <X size={13} /> Clear search
          </button>
        )}
        <span className="yt-snapshot-label">
          {query.isFetching && !query.isPending
            ? "Refreshing…"
            : "Collected feed snapshots"}
        </span>
      </div>
      {sourcesError && (
        <div className="yt-source-error" role="alert">
          <CircleAlert size={15} />
          <span>Feed navigation could not be loaded.</span>
          <button className="text-button" type="button" onClick={retrySources}>
            Retry
          </button>
        </div>
      )}
      {query.isPending ? (
        <YouTubeLoading />
      ) : query.isError && !query.isFetchNextPageError ? (
        <YouTubeError
          message={query.error.message}
          retry={() => void query.refetch()}
        />
      ) : items.length ? (
        youtubeShelves(items, filters.feed, !!filters.channel).map(
          ([key, shelf]) => (
            <section
              className={`yt-shelf yt-shelf-${shelf.kind}`}
              key={key}
              aria-label={shelf.title}
            >
              <div className="yt-shelf-heading">
                <div>
                  <p className="eyebrow">
                    {shelf.context ||
                      (shelf.kind === "short"
                        ? "Short-form videos"
                        : shelf.kind === "post"
                          ? "From the community"
                          : "Saved discoveries")}
                  </p>
                  <h2>
                    {shelf.kind === "short" ? (
                      <Sparkles size={19} />
                    ) : shelf.kind === "post" ? (
                      <MessageSquare size={19} />
                    ) : null}
                    {shelf.title}
                  </h2>
                </div>
                <span>
                  {shelf.items.length}{" "}
                  {shelf.kind === "post"
                    ? shelf.items.length === 1
                      ? "post"
                      : "posts"
                    : shelf.items.length === 1
                      ? "video"
                      : "videos"}
                </span>
              </div>
              <div className={`yt-grid yt-grid-${shelf.kind}`}>
                {shelf.items.map((item) => (
                  <YouTubeCard
                    key={item.id}
                    item={item}
                    feed={filters.feed}
                    open={() => open(item)}
                    href={href(item)}
                  />
                ))}
              </div>
            </section>
          ),
        )
      ) : (
        <div className="empty-state yt-empty">
          <Film size={36} strokeWidth={1.2} />
          <h2>
            {filters.q || filters.kind
              ? "No matching discoveries"
              : linked
                ? "Your feeds haven't linked a video yet"
                : "Your next discovery starts here"}
          </h2>
          <p>
            {filters.q || filters.kind
              ? "Try another search or choose All to see every format."
              : linked
                ? "YouTube links in collected posts will appear here, with their original sources attached."
                : "Run YouTube collection to save recommendations from Hype, News and your chosen channels."}
          </p>
          {(filters.q || filters.kind) && (
            <button
              type="button"
              className="secondary-button"
              onClick={() => {
                const next = new URLSearchParams(params);
                next.delete("q");
                next.delete("format");
                navigate(next);
              }}
            >
              Clear search and format
            </button>
          )}
        </div>
      )}
      {query.hasNextPage && (
        <div className="load-more" ref={more}>
          {query.isFetchNextPageError ? (
            <YouTubeError
              message={query.error.message}
              retry={() => void query.fetchNextPage({ cancelRefetch: false })}
            />
          ) : (
            <button
              type="button"
              className="secondary-button"
              disabled={query.isFetching}
              onClick={() => void query.fetchNextPage({ cancelRefetch: false })}
            >
              {query.isFetchingNextPage
                ? "Loading discoveries…"
                : "More discoveries"}
              <ArrowDown size={15} />
            </button>
          )}
        </div>
      )}
    </div>
  );
}

function CaptureTime({ value }: { value: string }) {
  const date = new Date(value);
  if (!Number.isFinite(date.valueOf())) return <>Unknown capture time</>;

  return (
    <time dateTime={value}>
      {new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
        timeZone: "UTC",
      }).format(date)}{" "}
      UTC
    </time>
  );
}
function YouTubeLoading() {
  return (
    <div
      className="yt-loading"
      role="status"
      aria-label="Loading YouTube discoveries"
    >
      {[0, 1, 2].map((index) => (
        <div className="skeleton" key={index}>
          <span />
          <span />
          <span />
        </div>
      ))}
    </div>
  );
}
function YouTubeError({
  message,
  retry,
}: {
  message: string;
  retry: () => void;
}) {
  return (
    <div className="empty-state" role="alert">
      <CircleAlert size={30} strokeWidth={1.3} />
      <h2>Unable to load YouTube discoveries</h2>
      <p>{message}</p>
      <button type="button" className="secondary-button" onClick={retry}>
        <RefreshCw size={15} /> Try again
      </button>
    </div>
  );
}

function YouTubeSourceReference({
  reference,
  params,
  open,
}: {
  reference: YouTubeReference;
  params: URLSearchParams;
  open: () => void;
}) {
  const original = safeUrl(reference.url);
  const reader = reference.reader_url
    ? `/reader/?${articleParams(params, { domain: reference.domain, id: reference.post_id })}`
    : undefined;
  const source =
    reference.source === "x"
      ? "X"
      : reference.source === "reddit"
        ? "Reddit"
        : reference.domain;

  return (
    <div className="yt-reference-row">
      <a
        className="yt-reference-main"
        href={reader || original}
        target={reader ? undefined : "_blank"}
        rel={reader ? undefined : "noopener noreferrer"}
        onClick={
          reader
            ? (event) => {
                if (
                  event.button ||
                  event.metaKey ||
                  event.ctrlKey ||
                  event.shiftKey ||
                  event.altKey
                )
                  return;
                event.preventDefault();
                open();
              }
            : undefined
        }
      >
        <span className="yt-reference-source">{source}</span>
        <strong>{reference.title || reference.url}</strong>
      </a>
      {original && (
        <a
          className="yt-reference-original"
          href={original}
          target="_blank"
          rel="noopener noreferrer"
          aria-label={`Open original source: ${reference.title || reference.domain}`}
        >
          Open original <ArrowUpRight size={14} />
        </a>
      )}
    </div>
  );
}
