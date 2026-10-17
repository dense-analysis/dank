import { ArrowUpRight, Check, Info, Link2, Play, X } from "lucide-react";
import { useState } from "react";
import { safeUrl } from "../lib/urls";
import { youtubeEmbedUrl } from "../lib/youtube";
import {
  itemDiscovery,
  type YouTubeItem,
  youtubeImageUrl,
  youtubeLocalVideoUrl,
  youtubePostLinks,
} from "../lib/youtubeCollection";
import { YouTubeLocalVideo } from "./YouTubeLocalVideo";
import { YouTubePlayer } from "./YouTubePlayer";

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
  const embed = youtubeEmbedUrl(item.url, window.location.origin);
  const original = safeUrl(item.url);
  const discovery = itemDiscovery(item, feed);
  const ranked =
    !!feed && discovery?.feed_url === feed && discovery.feed_kind === "hype";
  const text = item.text || item.title;
  const longPost = item.kind === "post" && text.length > 350;
  const title =
    item.title || (item.kind === "post" ? "Community post" : "YouTube video");

  return (
    <article
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
              <button
                type="button"
                className="yt-play"
                aria-label={`Play ${title}`}
                disabled={!embed && !localVideo}
                onClick={() => setPlaying(true)}
              >
                {image ? (
                  <img
                    src={image}
                    alt=""
                    loading="lazy"
                    referrerPolicy="no-referrer"
                    onError={() => setImageFailed(true)}
                  />
                ) : (
                  <span className="yt-image-placeholder">
                    <Play size={30} strokeWidth={1} />
                  </span>
                )}
                <span className="yt-play-circle">
                  <Play size={23} fill="currentColor" />
                </span>
                {ranked && discovery.rank != null && (
                  <span className="yt-rank">#{discovery.rank}</span>
                )}
                {item.duration_text && (
                  <span className="yt-duration">{item.duration_text}</span>
                )}
              </button>
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
            <a
              href={href}
              onClick={(event) => {
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
              }}
            >
              {title}
            </a>
          </h3>
          {item.channel.name && (
            <p className="yt-channel">{item.channel.name}</p>
          )}
          <p className="yt-video-meta">
            {[item.views_text, item.published_text]
              .filter(Boolean)
              .join(" · ") || "Discovered in your collected feeds"}
          </p>
        </>
      )}
      <div className="yt-card-footer">
        <span
          className={`yt-capture-status ${localVideo || item.detail_capture.status === "captured" ? "captured" : ""}`}
          title={
            localVideo
              ? "The video is downloaded and plays locally."
              : item.detail_capture.status === "captured"
                ? "Additional video details were collected."
                : item.discoveries.length
                  ? "Feed information is saved. Video details, transcripts and comments are not part of this snapshot."
                  : "Found in collected source posts. A YouTube feed snapshot has not been captured."
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
            title={`${item.reference_count} collected posts link to this video`}
          >
            <Link2 size={12} />
            {item.reference_count}
          </span>
        )}
        {!detailed && (
          <button
            type="button"
            className="icon-button yt-context-button"
            aria-label={`Discovery details for ${title}`}
            onClick={open}
          >
            <Info size={15} />
          </button>
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
    </article>
  );
}
