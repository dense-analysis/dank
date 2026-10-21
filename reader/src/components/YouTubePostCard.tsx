import { Film, Play, X } from "lucide-react";
import { type ReactNode, useState } from "react";
import type { Filters, Post } from "../lib/types";
import { youtubeEmbedUrl } from "../lib/youtube";
import {
  formatYouTubeViews,
  youtubeImageUrl,
  youtubeLocalVideoUrl,
} from "../lib/youtubeCollection";
import { AuthorLink } from "./AuthorLink";
import { FeedSourceBadge } from "./FeedSourceBadge";
import { HighlightedText } from "./HighlightedText";
import { NavigableCard } from "./NavigableCard";
import { PostTimestamp } from "./PostCard";
import { PostFooter } from "./PostFooter";
import { ReaderLink } from "./ReaderLink";
import { VideoSource } from "./VideoSource";
import { YouTubeLocalVideo } from "./YouTubeLocalVideo";
import { YouTubePlayer } from "./YouTubePlayer";
import { YouTubeThumbnail } from "./YouTubeThumbnail";
import "../universal-feed.css";

export function YouTubePostCard({
  post,
  links,
  read,
  href,
  open,
  query,
  filters,
  selectAuthor,
}: {
  post: Post;
  links?: ReactNode;
  read: boolean;
  href: string;
  open: () => void;
  query: string;
  filters: Filters;
  selectAuthor: (author: string) => void;
}) {
  const [playing, setPlaying] = useState(false);
  const [localFailed, setLocalFailed] = useState(false);
  const [imageFailed, setImageFailed] = useState(false);
  const metadata = post.youtube;
  const embed = youtubeEmbedUrl(post.url, window.location.origin);
  const videoId = embed ? new URL(embed).pathname.split("/").at(-1) : undefined;
  const kind =
    metadata?.kind ?? (post.id.startsWith("post:") ? "post" : "video");
  const video = kind !== "post";
  const localVideo = localFailed
    ? undefined
    : youtubeLocalVideoUrl(
        metadata?.video_url ??
          post.media.find((item) => item.content_type.startsWith("video/"))
            ?.url,
      );
  const image = imageFailed
    ? undefined
    : youtubeImageUrl(metadata?.thumbnail_url ?? post.thumbnail);
  const fallback = videoId
    ? `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`
    : undefined;
  const title = post.title || (video ? "YouTube video" : "Community post");
  const summary = post.excerpt.trim() === title.trim() ? "" : post.excerpt;
  const format =
    kind === "short" ? "Short" : video ? "Video" : "Community post";
  const views = metadata?.views_text
    ? formatYouTubeViews(metadata.views_text)
    : "";
  const positionKey = `youtube:${metadata?.video_id ?? videoId ?? post.id}`;

  return (
    <NavigableCard
      className={`post-card youtube-post-card youtube-feed-${kind} ${playing ? "youtube-feed-playing" : ""} ${read ? "is-read" : ""}`}
    >
      <div className="post-meta">
        <FeedSourceBadge source="youtube" />
        <AuthorLink
          author={metadata?.channel.name || post.author}
          fallback="YouTube"
          filters={filters}
          select={selectAuthor}
        />
        <span className="meta-dot">·</span>
        <PostTimestamp post={post} />
      </div>
      <div
        className={`youtube-feed-layout ${video || image ? "has-media" : ""}`}
      >
        {video ? (
          <div className="youtube-feed-media">
            <div className={`yt-thumbnail ${playing ? "yt-playing" : ""}`}>
              {playing && localVideo ? (
                <YouTubeLocalVideo
                  src={localVideo}
                  title={title}
                  positionKey={positionKey}
                  remoteAvailable={!!embed}
                  useRemote={() => setLocalFailed(true)}
                />
              ) : playing && embed ? (
                <YouTubePlayer
                  src={embed}
                  title={title}
                  positionKey={positionKey}
                />
              ) : (
                <YouTubeThumbnail
                  sources={image ? [image] : []}
                  fallback={fallback}
                  title={title}
                  disabled={!embed && !localVideo}
                  play={() => setPlaying(true)}
                >
                  <span className="yt-play-circle">
                    <Play size={21} fill="currentColor" aria-hidden="true" />
                  </span>
                  {metadata?.duration_text && (
                    <span className="yt-duration">
                      {metadata.duration_text}
                    </span>
                  )}
                </YouTubeThumbnail>
              )}
            </div>
            <div className="youtube-feed-playback">
              <VideoSource
                url={post.url}
                postUrl={post.url}
                remote={!localVideo}
              />
              {playing && (
                <button
                  type="button"
                  className="text-button"
                  onClick={() => setPlaying(false)}
                >
                  <X size={13} aria-hidden="true" /> Close video
                </button>
              )}
            </div>
          </div>
        ) : image ? (
          <ReaderLink
            href={href}
            className="youtube-feed-post-image"
            onNavigate={open}
            aria-label={`Open community post: ${title}`}
            tabIndex={-1}
          >
            <img
              src={image}
              alt="Community post attachment"
              loading="lazy"
              referrerPolicy="no-referrer"
              onError={() => setImageFailed(true)}
            />
          </ReaderLink>
        ) : null}
        <ReaderLink
          href={href}
          className="post-open youtube-feed-text"
          data-card-link
          preserveTextSelection
          draggable={false}
          onNavigate={open}
          aria-label={`${video ? "Video details" : "Read community post"}: ${title}`}
        >
          <h2>
            <HighlightedText text={title} query={query} />
          </h2>
          {summary && (
            <p>
              <HighlightedText text={summary} query={query} />
            </p>
          )}
          {video && <span className="youtube-feed-details">Video details</span>}
        </ReaderLink>
      </div>
      <PostFooter
        post={post}
        readerHref={href}
        read={read}
        source={{
          url: post.url,
          label: video ? `Watch ${title} on YouTube` : "View post on YouTube",
          title: "Open on YouTube",
        }}
      >
        <div className="post-footer-details">
          {video && <Film size={14} aria-hidden="true" />}
          <span>{format}</span>
          {views && (
            <>
              <span className="meta-dot">·</span>
              <span title={metadata?.views_text}>{views}</span>
            </>
          )}
        </div>
      </PostFooter>
      {links}
    </NavigableCard>
  );
}
