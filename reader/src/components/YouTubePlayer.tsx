import { useEffect, useId, useRef, useState } from "react";
import { videoPosition } from "../lib/videoPositions";
import { connectYouTubePlayback } from "../lib/youtubePlayback";

export function YouTubePlayer({
  src,
  title,
  positionKey,
}: {
  src: string;
  title: string;
  positionKey: string;
}) {
  const player = useRef<HTMLIFrameElement>(null);
  const id = useId();
  const [playerUrl] = useState(() => {
    const url = new URL(src);
    const time = videoPosition(positionKey);
    if (time !== undefined)
      url.searchParams.set("start", String(Math.floor(time)));

    return url.href;
  });

  useEffect(() => {
    const frame = player.current;
    if (!frame) return;

    frame.focus({ preventScroll: true });

    return connectYouTubePlayback(frame, positionKey);
  }, [positionKey]);

  return (
    <iframe
      id={id}
      src={playerUrl}
      title={`YouTube video: ${title}`}
      allow="autoplay; encrypted-media; picture-in-picture; fullscreen"
      allowFullScreen
      // YouTube requires a Referer for embedded playback (error 153).
      // https://developers.google.com/youtube/iframe_api_reference#onError
      referrerPolicy="strict-origin-when-cross-origin"
      ref={player}
    />
  );
}
