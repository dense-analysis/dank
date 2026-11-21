import { useState } from "react";
import { useVideoPlayback } from "../hooks/useVideoPlayback";
import { safeUrl } from "../lib/urls";
import { youtubeEmbedUrl } from "../lib/youtube";
import { YouTubePlayer } from "./YouTubePlayer";

export function AttachedVideo({
  url,
  positionKey,
}: {
  url: string;
  positionKey: string;
}) {
  const videoRef = useVideoPlayback(positionKey);
  const [playing, setPlaying] = useState(false);
  const embed = youtubeEmbedUrl(url, window.location.origin);

  if (embed)
    return (
      <div className="yt-thumbnail">
        {playing ? (
          <YouTubePlayer
            src={embed}
            title="Article video"
            positionKey={positionKey}
          />
        ) : (
          <button
            type="button"
            className="secondary-button"
            onClick={() => setPlaying(true)}
          >
            Play from YouTube
          </button>
        )}
      </div>
    );

  return (
    <video
      ref={videoRef}
      controls
      preload="none"
      src={safeUrl(url)}
      aria-label="Article video"
    >
      <track kind="captions" />
    </video>
  );
}
