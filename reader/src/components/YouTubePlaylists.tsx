import "../youtube-playlists.css";
import { useInfiniteQuery } from "@tanstack/react-query";
import {
  ArrowLeft,
  ArrowUpRight,
  ListVideo,
  RefreshCw,
  VideoOff,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { safeUrl } from "../lib/urls";
import { type YouTubeItem, youtubeImageUrl } from "../lib/youtubeCollection";
import {
  fetchYouTubePlaylist,
  fetchYouTubePlaylists,
  type YouTubePlaylist,
  type YouTubePlaylistSort,
} from "../lib/youtubePlaylists";
import { Count } from "./Count";
import { NavigableCard } from "./NavigableCard";
import { ReaderLink } from "./ReaderLink";
import { SearchField } from "./SearchField";
import { SourceActions } from "./SourceActions";
import { YouTubeCard } from "./YouTubeCard";

function PlaylistImage({ playlist }: { playlist: YouTubePlaylist }) {
  const [failed, setFailed] = useState(false);
  const image = failed ? undefined : youtubeImageUrl(playlist.thumbnail_url);

  return image ? (
    <img
      src={image}
      alt=""
      loading="lazy"
      referrerPolicy="no-referrer"
      onError={() => setFailed(true)}
    />
  ) : (
    <span className="yt-playlist-placeholder">
      <ListVideo size={38} strokeWidth={1.3} />
    </span>
  );
}
function PlaylistStatus({ playlist }: { playlist: YouTubePlaylist }) {
  const discovered = ["not_collected", "discovered"].includes(playlist.status);
  const message =
    playlist.status === "failed"
      ? "The latest check failed. Previously saved videos remain available."
      : playlist.status === "partial" || (!playlist.complete && !discovered)
        ? "Partial playlist. Some videos or their order could not be collected; this view may be incomplete."
        : discovered
          ? "Videos have not been collected yet. Refresh this playlist to collect them."
          : "Videos follow the playlist order saved from YouTube.";

  return (
    <p
      className={`yt-playlist-status ${playlist.status === "complete" ? "" : "yt-playlist-incomplete"}`}
    >
      {message}
    </p>
  );
}
function PlaylistCount({ playlist }: { playlist: YouTubePlaylist }) {
  return (
    <>
      <Count value={playlist.collected_count} /> saved
      {playlist.video_count !== null && (
        <>
          {" "}
          / <Count value={playlist.video_count} /> videos
        </>
      )}
    </>
  );
}

export function YouTubePlaylists({
  collectPlaylist,
  collectingPlaylist,
  params,
  navigate,
  backTo,
  open,
  href,
}: {
  collectPlaylist?: (id: string) => void;
  collectingPlaylist?: boolean;
  params: URLSearchParams;
  navigate: (
    params: URLSearchParams,
    replace?: boolean,
    backTo?: string,
  ) => void;
  backTo: (params: URLSearchParams) => void;
  open: (item: YouTubeItem) => void;
  href: (item: YouTubeItem) => string;
}) {
  const id = params.get("youtube_playlist") ?? "";
  const q = (params.get("q") ?? "").trim();
  const sort: YouTubePlaylistSort =
    params.get("playlist_sort") === "az" ? "az" : "recent";
  const [search, setSearch] = useState(q);
  useEffect(() => setSearch(q), [q]);
  const library = useInfiniteQuery({
    queryKey: ["youtube-playlists", q, sort],
    queryFn: ({ pageParam, signal }) =>
      fetchYouTubePlaylists(q, pageParam, signal, sort),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: !id,
  });
  const detail = useInfiniteQuery({
    queryKey: ["youtube-playlist", id],
    queryFn: ({ pageParam, signal }) =>
      fetchYouTubePlaylist(id, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: !!id,
  });
  const playlists = [
    ...new Map(
      (library.data?.pages.flatMap((page) => page.playlists) ?? []).map(
        (playlist) => [playlist.id, playlist],
      ),
    ).values(),
  ];
  const playlist = detail.data?.pages[0]?.playlist;
  // Membership entries, including repeated video IDs, belong to YouTube's order.
  const entries = detail.data?.pages.flatMap((page) => page.items) ?? [];
  const query = id ? detail : library;
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
  const libraryParams = new URLSearchParams(params);
  libraryParams.delete("youtube_playlist");
  libraryParams.delete("youtube_item");

  return (
    <div className="yt-collection yt-playlists">
      {id ? (
        <>
          <ReaderLink
            className="text-button yt-back"
            href={`/reader/?${libraryParams}`}
            onNavigate={() => backTo(libraryParams)}
          >
            <ArrowLeft size={16} /> Back to playlists
          </ReaderLink>
          {playlist && (
            <>
              <div className="yt-playlist-heading">
                <div className="yt-playlist-cover">
                  <PlaylistImage
                    key={playlist.thumbnail_url}
                    playlist={playlist}
                  />
                </div>
                <div className="yt-playlist-heading-text">
                  <p className="eyebrow">YouTube playlist</p>
                  <h2>{playlist.title || "Untitled playlist"}</h2>
                  {playlist.channel.name && (
                    <p className="yt-playlist-owner">{playlist.channel.name}</p>
                  )}
                  <p>
                    <PlaylistCount playlist={playlist} /> ·{" "}
                    {playlist.order || "YouTube order"}
                  </p>
                  <PlaylistStatus playlist={playlist} />
                  {!playlist.snapshot_id &&
                    !playlist.complete &&
                    collectPlaylist && (
                      <button
                        type="button"
                        className="secondary-button yt-playlist-collect"
                        disabled={collectingPlaylist}
                        onClick={() => collectPlaylist(playlist.id)}
                      >
                        <RefreshCw size={15} />
                        {collectingPlaylist
                          ? "Collecting playlist…"
                          : "Collect playlist"}
                      </button>
                    )}
                  {playlist.checked_at && (
                    <p className="yt-playlist-checked">
                      Last checked{" "}
                      <time dateTime={playlist.checked_at}>
                        {new Date(playlist.checked_at).toLocaleString()}
                      </time>
                    </p>
                  )}
                  {safeUrl(playlist.url) && (
                    <SourceActions
                      className="text-button"
                      href={safeUrl(playlist.url)}
                      target="_blank"
                      rel="noreferrer"
                    >
                      View playlist on YouTube <ArrowUpRight size={14} />
                    </SourceActions>
                  )}
                </div>
              </div>
              {!entries.length && !detail.isPending && (
                <div className="empty-state yt-empty">
                  <ListVideo size={30} />
                  <h2>
                    {playlist.complete
                      ? "This playlist is empty"
                      : "No saved videos yet"}
                  </h2>
                  <p>
                    {playlist.complete
                      ? "No videos were present in the captured playlist."
                      : "Refresh this playlist to check its videos and order."}
                  </p>
                </div>
              )}
              <ol className="yt-playlist-entries" aria-label="Playlist videos">
                {entries.map((entry) => (
                  <li
                    className="yt-playlist-entry"
                    key={`${entry.playlist_position}:${entry.playlist_entry_id}`}
                    value={entry.playlist_position}
                  >
                    <span
                      className="yt-playlist-position"
                      title={`Position ${entry.playlist_position}`}
                    >
                      {entry.playlist_position}
                    </span>
                    {entry.available ? (
                      <YouTubeCard
                        item={entry}
                        feed={playlist.url}
                        open={() => open(entry)}
                        href={href(entry)}
                      />
                    ) : (
                      <div className="yt-playlist-unavailable">
                        <VideoOff size={25} />
                        <div>
                          <h3>{entry.title || "Unavailable video"}</h3>
                          <p>This playlist entry is unavailable on YouTube.</p>
                        </div>
                      </div>
                    )}
                  </li>
                ))}
              </ol>
            </>
          )}
        </>
      ) : (
        <>
          <div className="yt-intro">
            <div>
              <p className="eyebrow">Your collection</p>
              <h2>Playlists</h2>
              <p>
                Browse your saved playlists and the videos inside, in their
                YouTube order.
              </p>
            </div>
            <div className="yt-total">
              <strong>
                <Count value={library.data?.pages[0]?.total ?? 0} />
              </strong>
              <span>saved playlists</span>
            </div>
          </div>
          <div className="yt-tools yt-playlist-search">
            <select
              className="sort-select"
              aria-label="Sort playlists"
              value={sort}
              onChange={(event) => {
                const next = new URLSearchParams(params);
                if (event.target.value === "az")
                  next.set("playlist_sort", "az");
                else next.delete("playlist_sort");
                navigate(next);
              }}
            >
              <option value="recent">Recently added</option>
              <option value="az">A–Z</option>
            </select>
            <SearchField
              className="yt-search"
              value={search}
              onChange={setSearch}
              onSubmit={() => {
                const next = new URLSearchParams(params);
                if (search.trim()) next.set("q", search.trim());
                else next.delete("q");
                navigate(next);
              }}
              onClear={() => {
                setSearch("");
                const next = new URLSearchParams(params);
                next.delete("q");
                navigate(next);
              }}
              placeholder="Search playlists"
              label="Search playlists"
            />
          </div>
          {library.isSuccess && !playlists.length && (
            <div className="empty-state yt-empty">
              <ListVideo size={30} />
              <h2>{q ? "No playlists match" : "No playlists collected yet"}</h2>
              <p>
                {q
                  ? "Try a different playlist title or channel."
                  : "Refresh to discover playlists from your YouTube library and collect their videos."}
              </p>
            </div>
          )}
          <div className="yt-playlist-grid">
            {playlists.map((item) => {
              const next = new URLSearchParams(params);
              next.set("youtube_playlist", item.id);
              const target = `/reader/?${next}`;
              const visit = () => navigate(next, false, params.toString());

              return (
                <NavigableCard key={item.id} className="yt-playlist-card">
                  <ReaderLink
                    className="yt-playlist-artwork"
                    href={target}
                    onNavigate={visit}
                    aria-label={`Open playlist ${item.title || "Untitled playlist"}`}
                  >
                    <PlaylistImage key={item.thumbnail_url} playlist={item} />
                    <span className="yt-playlist-badge">
                      <ListVideo size={15} />
                      <Count value={item.video_count ?? item.collected_count} />{" "}
                      videos
                    </span>
                  </ReaderLink>
                  <h3>
                    <ReaderLink
                      href={target}
                      onNavigate={visit}
                      data-card-link
                      preserveTextSelection
                    >
                      {item.title || "Untitled playlist"}
                    </ReaderLink>
                  </h3>
                  {item.channel.name && (
                    <p className="yt-playlist-owner">{item.channel.name}</p>
                  )}
                  <p className="yt-playlist-card-count">
                    <PlaylistCount playlist={item} />
                    {item.status !== "complete" && (
                      <span>
                        {" "}
                        ·{" "}
                        {["not_collected", "discovered"].includes(item.status)
                          ? "Not collected"
                          : item.status === "failed"
                            ? "Check failed"
                            : "Partial playlist"}
                      </span>
                    )}
                  </p>
                </NavigableCard>
              );
            })}
          </div>
        </>
      )}
      {query.isPending && (
        <p className="yt-playlist-loading" role="status">
          {id ? "Loading playlist videos…" : "Loading playlists…"}
        </p>
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
            Retry {id ? "playlist" : "playlists"}
          </button>
        </div>
      )}
      {query.hasNextPage && !query.isError && (
        <div className="load-more" ref={more}>
          <span role="status">
            {query.isFetchingNextPage
              ? id
                ? "Loading more videos…"
                : "Loading more playlists…"
              : "Scroll for more"}
          </span>
        </div>
      )}
    </div>
  );
}
