import { useEffect, useRef, useState } from "react";
import { observeVideoPlayback } from "../lib/playback";

export function YouTubeLocalVideo({
  src,
  title,
  positionKey,
  remoteAvailable,
  useRemote,
}: {
  src: string;
  title: string;
  positionKey: string;
  remoteAvailable: boolean;
  useRemote: () => void;
}) {
  const element = useRef<HTMLVideoElement>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    const video = element.current;
    if (!video || failed) return;
    const playback = observeVideoPlayback(video, positionKey);
    playback.playing();
    void video.play().catch(() => {
      // Native controls remain available if the browser requires another gesture.
    });

    return () => playback.dispose();
  }, [positionKey, failed]);

  return failed ? (
    <div className="yt-local-error" role="status">
      <p>The downloaded video couldn't be played.</p>
      {remoteAvailable && (
        <button type="button" className="secondary-button" onClick={useRemote}>
          Play from YouTube
        </button>
      )}
    </div>
  ) : (
    <video
      ref={element}
      src={src}
      controls
      playsInline
      aria-label={`Downloaded video: ${title}`}
      onError={() => setFailed(true)}
    >
      <track kind="captions" />
    </video>
  );
}
