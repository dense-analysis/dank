import { ExternalLink, ImageOff } from "lucide-react";
import { type CSSProperties, useMemo, useState } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { useVideoPlayback } from "../hooks/useVideoPlayback";
import { matchesLinkUrl } from "../lib/linkIdentity";
import { postKey } from "../lib/storage";
import type { Post, XMedia } from "../lib/types";
import { videoPositionKey } from "../lib/videoPositions";
import { xMedia, xMediaUrl } from "../lib/x";
import {
  imageReferenceKey,
  previewReferenceKey,
  videoReferenceKey,
  withoutDisplayedLinks,
} from "../lib/xText";
import { youtubeEmbedUrl } from "../lib/youtube";
import { ArticleBody } from "./ArticleBody";
import { safeUrl } from "./PostCard";
import { VideoSource } from "./VideoSource";
import { XLinkPreview } from "./XLinkPreview";
import { XPostPreview } from "./XPostPreview";

export function XPostContent({
  post,
  compact = false,
  query = "",
  navigation,
  referenceUrl,
}: {
  post: Post;
  compact?: boolean;
  query?: string;
  referenceUrl?: string;
  navigation?: { href: string; open: () => void };
}) {
  const { preferences } = useReaderPreferences();
  const { hideRawImageLinks, hideRawVideoLinks } = preferences;
  const [failedMedia, setFailedMedia] = useState<Set<string>>(() => new Set());
  const media = useMemo(() => xMedia(post), [post]);
  const representedMedia = useMemo(() => {
    const displayed = new Set<string>();
    const hidden = new Set<string>();

    for (const asset of media) {
      if (failedMedia.has(asset.url) || !xMediaUrl(asset.url)) continue;

      const image = asset.type === "image";
      const referenceKey = image ? imageReferenceKey : videoReferenceKey;

      for (const value of [asset.source_url, asset.url]) {
        const key = value ? referenceKey(value) : undefined;

        if (!key) continue;

        displayed.add(key);

        if (
          (image ? hideRawImageLinks : hideRawVideoLinks) &&
          !(referenceUrl && matchesLinkUrl(value ?? "", referenceUrl))
        )
          hidden.add(key);
      }
    }

    return { displayed, hidden };
  }, [media, failedMedia, hideRawImageLinks, hideRawVideoLinks, referenceUrl]);
  const previews = useMemo(
    () =>
      (post.x?.links ?? []).filter(
        (link) =>
          previewReferenceKey(link.url) &&
          !representedMedia.displayed.has(videoReferenceKey(link.url) ?? ""),
      ),
    [post.x?.links, representedMedia.displayed],
  );
  const displayedPreviews = useMemo(
    () =>
      new Set(
        previews.flatMap((link) => {
          if (referenceUrl && matchesLinkUrl(link.url, referenceUrl)) return [];
          if (
            (!hideRawImageLinks && imageReferenceKey(link.url)) ||
            (!hideRawVideoLinks &&
              (videoReferenceKey(link.url) ||
                youtubeEmbedUrl(link.url, window.location.origin)))
          )
            return [];

          const key = previewReferenceKey(link.url);

          return key ? [key] : [];
        }),
      ),
    [previews, hideRawImageLinks, hideRawVideoLinks, referenceUrl],
  );
  const html = useMemo(
    () =>
      withoutDisplayedLinks(
        post.html,
        representedMedia.hidden,
        displayedPreviews,
      ),
    [post.html, representedMedia.hidden, displayedPreviews],
  );

  return (
    <div className={`x-post-content ${compact ? "x-content-compact" : ""}`}>
      {compact ? (
        <XPostPreview html={html} navigation={navigation} query={query} />
      ) : (
        <ArticleBody
          html={html}
          sourceUrl={post.url}
          query={query}
          referenceUrl={referenceUrl}
          referenceIsPost
        />
      )}
      {media.length > 0 && (
        <section className="x-attachments" aria-label="Post attachments">
          {media.map((asset, index) => (
            <XAttachment
              key={`${asset.type}:${asset.url}`}
              asset={asset}
              original={post.url}
              index={index}
              positionKey={videoPositionKey(
                postKey(post),
                "x",
                asset.source_url
                  ? (videoReferenceKey(asset.source_url) ?? index)
                  : index,
              )}
              onFailure={() =>
                setFailedMedia((previous) => new Set(previous).add(asset.url))
              }
            />
          ))}
        </section>
      )}
      {previews.map((link) => (
        <XLinkPreview
          key={link.url}
          link={link}
          originalUrl={post.url}
          highlighted={!!referenceUrl && matchesLinkUrl(link.url, referenceUrl)}
          positionScope={postKey(post)}
        />
      ))}
    </div>
  );
}

function XAttachment({
  asset,
  original,
  index,
  onFailure,
  positionKey,
}: {
  asset: XMedia;
  original: string;
  index: number;
  onFailure: () => void;
  positionKey: string;
}) {
  const [failed, setFailed] = useState(false);
  const videoRef = useVideoPlayback(positionKey);
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
            onError={() => {
              setFailed(true);
              onFailure();
            }}
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
            onFailure();
          }}
        >
          <track kind="captions" />
        </video>
      )}
      {asset.type === "image" ? (
        asset.remote && <figcaption>Image from X</figcaption>
      ) : (
        <figcaption className="video-caption">
          <VideoSource
            url={asset.source_url || asset.url}
            postUrl={original}
            remote={asset.remote}
          />
        </figcaption>
      )}
    </figure>
  );
}
