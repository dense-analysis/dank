import { useInfiniteQuery } from "@tanstack/react-query";
import { ArrowDown } from "lucide-react";
import {
  fetchYouTubeItems,
  type YouTubeFilters,
  type YouTubeItem,
  type YouTubeKind,
  youtubeShelves,
} from "../lib/youtubeCollection";
import { Count } from "./Count";
import { YouTubeCard } from "./YouTubeCard";

export const YOUTUBE_SECTION_PAGE_SIZE = 500;
function useSection(filters: YouTubeFilters, kind: YouTubeKind) {
  return useInfiniteQuery({
    queryKey: ["youtube-section", filters, kind],
    queryFn: ({ pageParam, signal }) =>
      fetchYouTubeItems(
        { ...filters, kind },
        pageParam,
        signal,
        undefined,
        YOUTUBE_SECTION_PAGE_SIZE,
      ),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
}
export function YouTubeAllSections({
  filters,
  open,
  href,
  emptyTitle,
  emptyMessage,
  clearFilters,
}: {
  filters: YouTubeFilters;
  open: (item: YouTubeItem) => void;
  href: (item: YouTubeItem) => string;
  emptyTitle: string;
  emptyMessage: string;
  clearFilters?: () => void;
}) {
  const video = useSection(filters, "video");
  const short = useSection(filters, "short");
  const post = useSection(filters, "post");
  const sections = [
    { kind: "video" as const, label: "videos", query: video },
    { kind: "short" as const, label: "Shorts", query: short },
    { kind: "post" as const, label: "posts", query: post },
  ];
  if (sections.every(({ query }) => query.isError && !query.data)) {
    return (
      <div className="yt-source-error" role="alert">
        <span>{video.error?.message}</span>
        <button
          type="button"
          className="text-button"
          onClick={() =>
            void Promise.all(sections.map(({ query }) => query.refetch()))
          }
        >
          Try again
        </button>
      </div>
    );
  }
  const empty = sections.every(
    ({ query }) => query.isSuccess && query.data.pages[0].total === 0,
  );
  if (empty) {
    return (
      <div className="empty-state yt-empty">
        <h2>{emptyTitle}</h2>
        <p>{emptyMessage}</p>
        {clearFilters && (
          <button
            type="button"
            className="secondary-button"
            onClick={clearFilters}
          >
            Clear search and format
          </button>
        )}
      </div>
    );
  }
  return (
    <div className="yt-all-sections">
      {sections.map(({ kind, label, query }) => {
        const items = [
          ...new Map(
            (query.data?.pages.flatMap((page) => page.items) ?? [])
              .filter((item) => item.kind === kind)
              .map((item) => [item.id, item]),
          ).values(),
        ];
        const total = query.data?.pages[0]?.total ?? 0;
        return (
          <div className="yt-type-section" data-kind={kind} key={kind}>
            {query.isPending && (
              <p role="status" className="yt-section-status">
                Loading {label}…
              </p>
            )}
            {youtubeShelves(items, filters.feed, !!filters.channel).map(
              ([key, shelf]) => (
                <section
                  className={`yt-shelf yt-shelf-${shelf.kind}`}
                  key={key}
                  aria-label={shelf.title}
                >
                  <div className="yt-shelf-heading">
                    <div>
                      <p className="eyebrow">
                        {shelf.context || "Saved recommendations"}
                      </p>
                      <h2>{shelf.title}</h2>
                    </div>
                    <span>
                      <Count value={shelf.items.length} /> {label}
                    </span>
                  </div>
                  <div className={`yt-grid yt-grid-${kind}`}>
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
            )}
            {query.isError && (
              <div className="yt-source-error" role="alert">
                <span>{query.error.message}</span>
                <button
                  type="button"
                  className="text-button"
                  onClick={() =>
                    void (query.isFetchNextPageError
                      ? query.fetchNextPage({ cancelRefetch: false })
                      : query.refetch())
                  }
                >
                  Retry {label}
                </button>
              </div>
            )}
            {query.hasNextPage && !query.isFetchNextPageError && (
              <div className="yt-section-more">
                <span>
                  Showing <Count value={items.length} /> of{" "}
                  <Count value={total} /> {label}
                </span>
                <button
                  type="button"
                  className="secondary-button"
                  disabled={query.isFetching}
                  onClick={() =>
                    void query.fetchNextPage({ cancelRefetch: false })
                  }
                >
                  {query.isFetchingNextPage
                    ? `Loading ${label}…`
                    : `Show more ${label}`}
                  <ArrowDown size={15} />
                </button>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
