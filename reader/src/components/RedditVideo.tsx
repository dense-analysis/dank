import type Hls from "hls.js";
import { ExternalLink, Globe2, Play } from "lucide-react";
import { type CSSProperties, useEffect, useRef, useState } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { observeVideoPlayback } from "../lib/playback";
import { redditVideoKey } from "../lib/redditText";
import type { RedditVideo as Video } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { videoPositionKey } from "../lib/videoPositions";

export function RedditVideo({
  video,
  originalUrl,
  onUnavailable,
  positionScope,
}: {
  video: Video;
  originalUrl: string;
  onUnavailable?: (url: string) => void;
  positionScope: string;
}) {
  const { preferences } = useReaderPreferences();
  const positionKey = videoPositionKey(
    positionScope,
    "reddit",
    redditVideoKey(video.source_url || video.hls_url || video.url) ?? video.url,
  );
  const element = useRef<HTMLVideoElement>(null);
  const [started, setStarted] = useState(false);
  const [failed, setFailed] = useState(false);
  const [loadedRatio, setLoadedRatio] = useState<number>();
  const [posterSize, setPosterSize] = useState<{
    url: string;
    ratio: number;
  }>();
  const poster = video.poster_url ? safeUrl(video.poster_url) : undefined;
  const ratio =
    loadedRatio ?? (posterSize?.url === poster ? posterSize?.ratio : undefined);

  useEffect(() => {
    if (!poster) return;

    const image = new Image();
    image.onload = () => {
      if (image.naturalWidth > 0 && image.naturalHeight > 0)
        setPosterSize({
          url: poster,
          ratio: image.naturalWidth / image.naturalHeight,
        });
    };
    image.src = poster;

    return () => {
      image.onload = null;
    };
  }, [poster]);
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

    const playback = observeVideoPlayback(media, positionKey);
    // Reserve the user's click while HLS loads; a newer player or scroll can cancel it.
    playback.playing();
    let active = true;
    let player: Hls | undefined;
    let usingFallback = false;
    const play = () => {
      if (active && playback.isActive())
        void media.play().catch(() => {
          // Browser playback policies may require another press of the native play control.
        });
    };

    const fail = () => {
      if (!active) return;

      // The API exposes only MP4 fallbacks known to include audio.
      if (fallback && fallback !== url && !usingFallback) {
        const resume = playback.isActive();
        playback.pause();
        usingFallback = true;
        player?.destroy();
        player = undefined;
        media.src = fallback;

        if (resume) playback.playing();
        play();
      } else {
        playback.pause();
        setFailed(true);
        onUnavailable?.(video.url);
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
      playback.dispose();
      player?.destroy();
      media.removeAttribute("src");
      media.load();
    };
  }, [started, url, isHls, fallback, onUnavailable, video.url, positionKey]);

  return (
    <figure className="reddit-video">
      <div
        className={`reddit-video-frame${ratio ? " reddit-video-sized" : ""}`}
        style={
          ratio
            ? ({ "--reddit-video-ratio": ratio } as CSSProperties)
            : undefined
        }
      >
        <video
          ref={element}
          controls={started}
          playsInline
          preload="none"
          poster={poster}
          aria-label="Reddit video"
          onLoadedMetadata={(event) => {
            const media = event.currentTarget;
            if (media.videoWidth > 0 && media.videoHeight > 0)
              setLoadedRatio(media.videoWidth / media.videoHeight);
          }}
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
