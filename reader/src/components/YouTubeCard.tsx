import { ArrowUpRight, Check, Info, Link2, Play, X } from "lucide-react";
import { useState } from "react";
import { safeUrl } from "../lib/urls";
import { youtubeEmbedUrl } from "../lib/youtube";
import {
  formatYouTubeViews,
  itemDiscovery,
  type YouTubeItem,
  youtubeImageUrl,
  youtubeLocalVideoUrl,
  youtubePostLinks,
  youtubeThumbnailSources,
} from "../lib/youtubeCollection";
import { Count } from "./Count";
import { NavigableCard } from "./NavigableCard";
import { ReaderLink } from "./ReaderLink";
import { YouTubeLocalVideo } from "./YouTubeLocalVideo";
import { YouTubePlayer } from "./YouTubePlayer";
import { YouTubeThumbnail } from "./YouTubeThumbnail";

export function YouTubeCard({
  item,
  feed,
  open,
  href,
  detailed = false,
}: {
  item: YouTubeItem;
  feed: string;
  open: () => void;
  href: string;
  detailed?: boolean;
}) {
  const [localFailed, setLocalFailed] = useState(false);
  const localVideo = localFailed
    ? undefined
    : youtubeLocalVideoUrl(item.video_url);
  const [playing, setPlaying] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [imageFailed, setImageFailed] = useState(false);
  const image = imageFailed ? undefined : youtubeImageUrl(item.thumbnail_url);
  const thumbnails = youtubeThumbnailSources(item);
  const thumbnailFallback = item.video_id
    ? `https://i.ytimg.com/vi/${item.video_id}/hqdefault.jpg`
    : undefined;
  const embed = youtubeEmbedUrl(item.url, window.location.origin);
  const original = safeUrl(item.url);
  const discovery = itemDiscovery(item, feed);
  const metadataSaved = item.metadata_capture?.available === true;
  const ranked =
    !!feed && discovery?.feed_url === feed && discovery.feed_kind === "hype";
  const views = formatYouTubeViews(item.views_text);
  const text = item.text || item.title;
  const longPost = item.kind === "post" && text.length > 350;
  const title =
    item.title || (item.kind === "post" ? "Community post" : "YouTube video");

  return (
    <NavigableCard
      enabled={!detailed}
      className={`yt-card yt-${item.kind} ${detailed ? "yt-card-detailed" : ""}`}
    >
      {item.kind === "post" ? (
        <>
          <div className="yt-post-identity">
            <span className="yt-channel-initial" aria-hidden="true">
              {(item.channel.name || "Y").slice(0, 1)}
            </span>
            <div>
              <strong>{item.channel.name || "YouTube community"}</strong>
              <span>{item.published_text || "Captured community post"}</span>
            </div>
          </div>
          <p className="yt-post-text">
            {youtubePostLinks(text)
              .filter(
                (segment) =>
                  !longPost || expanded || detailed || segment.start < 350,
              )
              .map((segment) => {
                const label =
                  longPost && !expanded && !detailed
                    ? segment.text.slice(0, Math.max(0, 350 - segment.start))
                    : segment.text;

                return segment.href ? (
                  <a
                    key={segment.start}
                    href={segment.href}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {label}
                  </a>
                ) : (
                  <span key={segment.start}>{label}</span>
                );
              })}
            {longPost && !expanded && !detailed && "…"}
          </p>
          {longPost && !detailed && (
            <button
              type="button"
              className="text-button"
              aria-expanded={expanded}
              onClick={() => setExpanded(!expanded)}
            >
              {expanded ? "Show less" : "Show more"}
            </button>
          )}
          {image && (
            <img
              className="yt-post-image"
              src={image}
              alt="Community post attachment"
              loading="lazy"
              referrerPolicy="no-referrer"
              onError={() => setImageFailed(true)}
            />
          )}
        </>
      ) : (
        <>
          <div className={`yt-thumbnail ${playing ? "yt-playing" : ""}`}>
            {playing && localVideo ? (
              <YouTubeLocalVideo
                src={localVideo}
                title={title}
                positionKey={`youtube:${item.video_id}`}
                remoteAvailable={!!embed}
                useRemote={() => setLocalFailed(true)}
              />
            ) : playing && embed ? (
              <YouTubePlayer
                src={embed}
                title={title}
                positionKey={`youtube:${item.video_id}`}
              />
            ) : (
              <YouTubeThumbnail
                key={`${item.id}:${thumbnails.join("|")}`}
                sources={thumbnails}
                fallback={thumbnailFallback}
                title={title}
                disabled={!embed && !localVideo}
                play={() => setPlaying(true)}
              >
                <span className="yt-play-circle">
                  <Play size={23} fill="currentColor" />
                </span>
                {ranked && discovery.rank != null && (
                  <span className="yt-rank">#{discovery.rank}</span>
                )}
                {item.duration_text && (
                  <span className="yt-duration">{item.duration_text}</span>
                )}
              </YouTubeThumbnail>
            )}
          </div>
          {playing && (
            <div className="yt-player-actions">
              <span>
                {localVideo
                  ? "Playing downloaded video"
                  : "Playing from YouTube"}
              </span>
              <button
                type="button"
                className="text-button"
                onClick={() => setPlaying(false)}
              >
                <X size={13} /> Close video
              </button>
            </div>
          )}
          <h3>
            <ReaderLink
              href={href}
              onNavigate={open}
              data-card-link
              preserveTextSelection
              draggable={false}
            >
              {title}
            </ReaderLink>
          </h3>
          {item.channel.name && (
            <p className="yt-channel">{item.channel.name}</p>
          )}
          <p className="yt-video-meta" title={item.views_text || undefined}>
            {[views, item.published_text].filter(Boolean).join(" · ") ||
              "Discovered in your collected feeds"}
          </p>
        </>
      )}
      <div className="yt-card-footer">
        <span
          className={`yt-capture-status ${localVideo || metadataSaved || item.detail_capture.status === "captured" ? "captured" : ""}`}
          title={
            localVideo
              ? "The video is downloaded and plays locally."
              : item.detail_capture.status === "captured"
                ? "Additional video details were collected."
                : metadataSaved
                  ? "Video preview details were collected from YouTube."
                  : item.discoveries.length
                    ? "Feed information is saved. Open details to check for a transcript. Comments are not part of this snapshot."
                    : "Found in collected posts or comments. Video title and channel details have not been collected."
          }
        >
          {localVideo ? (
            <>
              <Check size={11} /> Downloaded
            </>
          ) : item.detail_capture.status === "captured" ? (
            <>
              <Check size={11} /> Details collected
            </>
          ) : metadataSaved ? (
            <>
              <Check size={11} /> Metadata saved
            </>
          ) : item.detail_capture.status === "failed" ? (
            "Details unavailable"
          ) : item.discoveries.length ? (
            "Feed snapshot"
          ) : (
            "Linked video"
          )}
        </span>
        {item.reference_count > 0 && (
          <span
            className="yt-reference-count"
            title={`${item.reference_count.toLocaleString()} verified post links or comment mentions`}
          >
            <Link2 size={12} />
            <Count value={item.reference_count} />
          </span>
        )}
        {!detailed && (
          <ReaderLink
            href={href}
            onNavigate={open}
            data-card-link={item.kind === "post" ? true : undefined}
            className="icon-button yt-context-button"
            aria-label={`Discovery details for ${title}`}
          >
            <Info size={15} />
          </ReaderLink>
        )}
        {original && (
          <a
            className="yt-original"
            href={original}
            target="_blank"
            rel="noreferrer"
            aria-label={
              item.kind === "post"
                ? "View post on YouTube"
                : `Watch ${title} on YouTube`
            }
            title="Open on YouTube"
          >
            <ArrowUpRight size={15} />
          </a>
        )}
      </div>
    </NavigableCard>
  );
}
