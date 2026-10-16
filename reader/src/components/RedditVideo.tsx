import type Hls from "hls.js";
import { ExternalLink, Globe2, Play } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import type { RedditVideo as Video } from "../lib/types";
import { safeUrl } from "./PostCard";

export function RedditVideo({
  video,
  originalUrl,
}: {
  video: Video;
  originalUrl: string;
}) {
  const { preferences } = useReaderPreferences();
  const element = useRef<HTMLVideoElement>(null);
  const [started, setStarted] = useState(false);
  const [failed, setFailed] = useState(false);
  const url = safeUrl(video.remote ? video.hls_url || video.url : video.url);
  const fallback =
    video.remote && video.fallback_url
      ? safeUrl(video.fallback_url)
      : undefined;
  const isHls =
    Boolean(video.remote && video.hls_url) || /\.m3u8(?:\?|$)/i.test(url ?? "");

  useEffect(() => {
    const media = element.current;

    if (!started || !url || !media) return;

    let active = true;
    let player: Hls | undefined;
    let usingFallback = false;
    const play = () => {
      if (active)
        void media.play().catch(() => {
          // Browser playback policies may require another press of the native play control.
        });
    };

    const fail = () => {
      if (!active) return;

      // The API exposes only MP4 fallbacks known to include audio.
      if (fallback && fallback !== url && !usingFallback) {
        usingFallback = true;
        player?.destroy();
        player = undefined;
        media.src = fallback;
        play();
      } else {
        setFailed(true);
      }
    };
    media.addEventListener("error", fail);

    // Prefer the browser's native HLS support; load MSE playback only on demand.
    // https://github.com/video-dev/hls.js#embedding-hlsjs
    if (!isHls || media.canPlayType("application/vnd.apple.mpegurl")) {
      media.src = url;
      play();
    } else {
      void import("hls.js")
        .then(({ default: Hls }) => {
          if (!active) return;

          if (!Hls.isSupported()) {
            fail();
            return;
          }

          player = new Hls();
          player.on(Hls.Events.MANIFEST_PARSED, play);
          player.on(Hls.Events.ERROR, (_event, data) => {
            if (active && data.fatal) {
              fail();
            }
          });
          player.loadSource(url);
          player.attachMedia(media);
        })
        .catch(() => {
          fail();
        });
    }

    return () => {
      active = false;
      media.removeEventListener("error", fail);
      player?.destroy();
      media.pause();
      media.removeAttribute("src");
      media.load();
    };
  }, [started, url, isHls, fallback]);

  return (
    <figure className="reddit-video">
      <div className="reddit-video-frame">
        <video
          ref={element}
          controls={started}
          playsInline
          preload="none"
          poster={video.poster_url ? safeUrl(video.poster_url) : undefined}
          aria-label="Reddit video"
        >
          <track kind="captions" />
        </video>
        {!started && url && (
          <button
            type="button"
            className="reddit-video-play"
            onClick={() => setStarted(true)}
            aria-label="Play video"
          >
            <Play size={28} fill="currentColor" aria-hidden="true" />
          </button>
        )}
      </div>
      <figcaption>
        {video.remote && preferences.showRemoteContentLabels && (
          <span className="remote-content-label">
            <Globe2 size={13} aria-hidden="true" /> Remote video · streams from
            Reddit
          </span>
        )}
        <a href={safeUrl(originalUrl)} target="_blank" rel="noreferrer">
          Watch on Reddit <ExternalLink size={12} aria-hidden="true" />
        </a>
      </figcaption>
      {(failed || !url) && (
        <p className="reddit-video-error" role="status">
          This video couldn’t be played here. Watch on Reddit to try the
          original.
        </p>
      )}
    </figure>
  );
}
