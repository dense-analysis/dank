import { ExternalLink, Play, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { XLink } from "../lib/types";
import { xMediaUrl } from "../lib/x";
import { previewReferenceKey } from "../lib/xText";
import { youtubeEmbedUrl } from "../lib/youtube";

export function XLinkPreview({ link }: { link: XLink }) {
  const [imageFailed, setImageFailed] = useState(false);
  const [playing, setPlaying] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const player = useRef<HTMLIFrameElement>(null);
  const restoreFocus = useRef(false);
  const url = previewReferenceKey(link.url);
  const embed = url ? youtubeEmbedUrl(url) : undefined;

  useEffect(() => {
    if (!playing) {
      if (restoreFocus.current) {
        trigger.current?.focus({ preventScroll: true });
        restoreFocus.current = false;
      }

      return;
    }

    player.current?.focus({ preventScroll: true });

    // Navigation keeps the feed mounted but hidden; unload its player to stop audio.
    const stop = () => setPlaying(false);
    window.addEventListener("popstate", stop);

    return () => window.removeEventListener("popstate", stop);
  }, [playing]);

  if (!url) return null;

  const domain = new URL(url).hostname.replace(/^www\./, "");
  const image = xMediaUrl(link.image_url);
  const title = link.title || domain;
  const content = (
    <>
      {image && !imageFailed && (
        <img
          src={image}
          alt=""
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => setImageFailed(true)}
        />
      )}
      <span className="x-link-copy">
        <span className="x-link-domain">
          {embed ? (
            <>
              <Play size={16} fill="currentColor" aria-hidden="true" /> YouTube
              · Play video
            </>
          ) : (
            <>
              {domain} <ExternalLink size={12} aria-hidden="true" />
            </>
          )}
        </span>
        <strong>{link.title || link.url}</strong>
        {link.description && (
          <span className="x-link-description">{link.description}</span>
        )}
      </span>
    </>
  );

  if (!embed)
    return (
      <a
        className="x-link-preview"
        href={url}
        target="_blank"
        rel="noreferrer"
        aria-label={`Open link: ${title}`}
      >
        {content}
      </a>
    );

  return (
    <div className="x-youtube-preview">
      {playing ? (
        <div className="x-youtube-player">
          <iframe
            src={embed}
            title={`YouTube video: ${title}`}
            allow="autoplay; encrypted-media; picture-in-picture; fullscreen"
            allowFullScreen
            // YouTube requires a Referer for embedded playback (error 153).
            // https://developers.google.com/youtube/iframe_api_reference#onError
            referrerPolicy="strict-origin-when-cross-origin"
            ref={player}
          />
        </div>
      ) : (
        <button
          type="button"
          className="x-link-preview x-youtube-trigger"
          ref={trigger}
          aria-label={`Play video: ${title}`}
          onClick={() => setPlaying(true)}
        >
          {content}
        </button>
      )}
      <div className="x-youtube-actions">
        <a href={url} target="_blank" rel="noreferrer">
          Watch on YouTube <ExternalLink size={13} aria-hidden="true" />
        </a>
        {playing && (
          <button
            type="button"
            className="text-button"
            onClick={() => {
              restoreFocus.current = true;
              setPlaying(false);
            }}
          >
            <X size={14} aria-hidden="true" /> Close video
          </button>
        )}
      </div>
    </div>
  );
}
