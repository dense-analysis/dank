import { ExternalLink, ImageOff } from "lucide-react";
import { type CSSProperties, useMemo, useState } from "react";
import { useVideoPlayback } from "../hooks/useVideoPlayback";
import type { Post, XMedia } from "../lib/types";
import { xMedia, xMediaUrl } from "../lib/x";
import {
  previewReferenceKey,
  videoReferenceKey,
  withoutDisplayedLinks,
} from "../lib/xText";
import { ArticleBody } from "./ArticleBody";
import { safeUrl } from "./PostCard";
import { XLinkPreview } from "./XLinkPreview";
import { XPostPreview } from "./XPostPreview";

export function XPostContent({
  post,
  compact = false,
  query = "",
  navigation,
}: {
  post: Post;
  compact?: boolean;
  query?: string;
  navigation?: { href: string; open: () => void };
}) {
  const [failedMedia, setFailedMedia] = useState<Set<string>>(() => new Set());
  const media = useMemo(() => xMedia(post), [post]);
  const displayedVideos = useMemo(
    () =>
      new Set(
        media.flatMap((asset) => {
          if (
            asset.type === "image" ||
            failedMedia.has(asset.url) ||
            !xMediaUrl(asset.url)
          )
            return [];

          return [asset.source_url, asset.url].flatMap((value) => {
            const key = value ? videoReferenceKey(value) : undefined;
            return key ? [key] : [];
          });
        }),
      ),
    [media, failedMedia],
  );
  const previews = useMemo(
    () =>
      (post.x?.links ?? []).filter(
        (link) =>
          previewReferenceKey(link.url) &&
          !displayedVideos.has(videoReferenceKey(link.url) ?? ""),
      ),
    [post.x?.links, displayedVideos],
  );
  const displayedPreviews = useMemo(
    () => new Set(previews.map((link) => previewReferenceKey(link.url) ?? "")),
    [previews],
  );
  const html = useMemo(
    () => withoutDisplayedLinks(post.html, displayedVideos, displayedPreviews),
    [post.html, displayedVideos, displayedPreviews],
  );

  return (
    <div className={`x-post-content ${compact ? "x-content-compact" : ""}`}>
      {compact ? (
        <XPostPreview html={html} navigation={navigation} query={query} />
      ) : (
        <ArticleBody html={html} query={query} />
      )}
      {media.length > 0 && (
        <section className="x-attachments" aria-label="Post attachments">
          {media.map((asset, index) => (
            <XAttachment
              key={`${asset.type}:${asset.url}`}
              asset={asset}
              original={post.url}
              index={index}
              onVideoFailure={() =>
                setFailedMedia((previous) => new Set(previous).add(asset.url))
              }
            />
          ))}
        </section>
      )}
      {previews.map((link) => (
        <XLinkPreview key={link.url} link={link} />
      ))}
    </div>
  );
}

function XAttachment({
  asset,
  original,
  index,
  onVideoFailure,
}: {
  asset: XMedia;
  original: string;
  index: number;
  onVideoFailure: () => void;
}) {
  const [failed, setFailed] = useState(false);
  const videoRef = useVideoPlayback();
  const [loadedRatio, setLoadedRatio] = useState<number>();
  const ratio = loadedRatio ?? asset.aspect_ratio;
  const url = xMediaUrl(asset.url);
  const poster = xMediaUrl(asset.poster_url);

  if (!url || failed)
    return (
      <div className="x-media-unavailable">
        <ImageOff size={20} aria-hidden="true" />
        <span>{asset.type === "image" ? "Image" : "Video"} unavailable</span>
        <a href={safeUrl(original)} target="_blank" rel="noreferrer">
          View on X <ExternalLink size={13} />
        </a>
      </div>
    );

  return (
    <figure className={`x-attachment x-attachment-${asset.type}`}>
      {asset.type === "image" ? (
        <a
          href={url}
          target="_blank"
          rel="noreferrer"
          aria-label={`Open full-size image ${index + 1}`}
        >
          <img
            src={url}
            alt={asset.alt || `Image ${index + 1} attached to this post`}
            loading="lazy"
            referrerPolicy="no-referrer"
            onError={() => setFailed(true)}
          />
        </a>
      ) : (
        <video
          ref={videoRef}
          className={ratio ? "x-video-sized" : undefined}
          style={
            ratio ? ({ "--x-video-ratio": ratio } as CSSProperties) : undefined
          }
          src={url}
          poster={poster}
          controls
          preload="none"
          playsInline
          loop={asset.type === "gif"}
          muted={asset.type === "gif"}
          aria-label="X post video"
          onLoadedMetadata={(event) => {
            const video = event.currentTarget;
            if (video.videoWidth > 0 && video.videoHeight > 0)
              setLoadedRatio(video.videoWidth / video.videoHeight);
          }}
          onError={() => {
            setFailed(true);
            onVideoFailure();
          }}
        >
          <track kind="captions" />
        </video>
      )}
      {asset.remote && (
        <figcaption>
          {asset.type === "image" ? "Image" : "Video"} from X
        </figcaption>
      )}
    </figure>
  );
}
