import { useInfiniteQuery } from "@tanstack/react-query";
import { ArrowDown, ArrowUpRight } from "lucide-react";
import { useEffect, useRef } from "react";
import { fetchYouTubeChannels } from "../lib/youtubeChannels";
import {
  type YouTubeFilters,
  type YouTubeItem,
  youtubeImageUrl,
} from "../lib/youtubeCollection";
import { Count } from "./Count";
import { ReaderLink } from "./ReaderLink";

export function YouTubeChannels({
  filters,
  params,
  navigate,
  open,
  href,
}: {
  filters: YouTubeFilters;
  params: URLSearchParams;
  navigate: (params: URLSearchParams) => void;
  open: (item: YouTubeItem) => void;
  href: (item: YouTubeItem) => string;
}) {
  const query = useInfiniteQuery({
    queryKey: ["youtube-channels", filters],
    queryFn: ({ pageParam, signal }) =>
      fetchYouTubeChannels(filters, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
  const more = useRef<HTMLDivElement>(null);
  const { hasNextPage, isFetching, isError, fetchNextPage } = query;
  useEffect(() => {
    if (
      !more.current ||
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
    observer.observe(more.current);
    return () => observer.disconnect();
  }, [hasNextPage, isFetching, isError, fetchNextPage]);
  const channels = [
    ...new Map(
      (query.data?.pages.flatMap((page) => page.channels) ?? []).map(
        (channel) => [channel.id, channel],
      ),
    ).values(),
  ];
  const total = query.data?.pages[0]?.total ?? 0;
  const unattributed = query.data?.pages[0]?.unattributed_items ?? 0;
  return (
    <div className="yt-channel-explorer">
      <div className="yt-channel-summary">
        <h2>
          <Count value={total} /> channels in these recommendations
        </h2>
        <p>
          Channels with the most items in this saved feed first, with up to
          three recent recommendations each.
        </p>
        {unattributed > 0 && (
          <p>
            <Count value={unattributed} /> items have no saved channel identity.
          </p>
        )}
      </div>
      {query.isPending && <p role="status">Loading recommended channels…</p>}
      {query.isSuccess && !channels.length && (
        <div className="empty-state yt-empty">
          <h2>No channels match</h2>
          <p>
            {filters.channel_scope
              ? "None of these recommendations matches your configured channel feeds."
              : "No identified channels match this feed and search."}
          </p>
        </div>
      )}
      <div className="yt-channel-groups">
        {channels.map((channel) => {
          const next = new URLSearchParams(params);
          next.set("channel", channel.id);
          next.set("channel_name", channel.name);
          next.set("format", "video");
          return (
            <section
              className="yt-channel-group"
              key={channel.id}
              aria-label={channel.name}
            >
              <div className="yt-channel-group-header">
                <span className="yt-channel-initial" aria-hidden="true">
                  {channel.name.slice(0, 1).toUpperCase()}
                </span>
                <div>
                  <h3>
                    <ReaderLink
                      href={`/reader/?${next}`}
                      onNavigate={() => navigate(next)}
                    >
                      {channel.name}
                    </ReaderLink>
                  </h3>
                  <span>
                    <Count value={channel.count} /> recommendations
                    {channel.configured ? " · Configured channel" : ""}
                  </span>
                </div>
                <ReaderLink
                  className="icon-button"
                  href={`/reader/?${next}`}
                  onNavigate={() => navigate(next)}
                  aria-label={`View ${channel.name} recommendations`}
                >
                  <ArrowUpRight size={17} />
                </ReaderLink>
              </div>
              <div className="yt-channel-examples">
                {channel.items.map((item) => {
                  const thumbnail = youtubeImageUrl(item.thumbnail_url);
                  return (
                    <ReaderLink
                      className="yt-channel-example"
                      key={item.id}
                      href={href(item)}
                      onNavigate={() => open(item)}
                      preserveTextSelection
                    >
                      {thumbnail ? (
                        <img src={thumbnail} alt="" loading="lazy" />
                      ) : (
                        <span className="yt-channel-image-placeholder" />
                      )}
                      <span>
                        <strong>{item.title}</strong>
                        <small>
                          {[
                            item.kind === "short"
                              ? "Short"
                              : item.kind === "post"
                                ? "Post"
                                : "Video",
                            item.published_text,
                          ]
                            .filter(Boolean)
                            .join(" · ")}
                        </small>
                      </span>
                    </ReaderLink>
                  );
                })}
              </div>
            </section>
          );
        })}
      </div>
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
            Retry channels
          </button>
        </div>
      )}
      {query.hasNextPage && (
        <div className="load-more" ref={more}>
          <button
            type="button"
            className="secondary-button"
            disabled={query.isFetching}
            onClick={() => void query.fetchNextPage({ cancelRefetch: false })}
          >
            {query.isFetchingNextPage ? "Loading channels…" : "More channels"}
            <ArrowDown size={15} />
          </button>
        </div>
      )}
    </div>
  );
}
