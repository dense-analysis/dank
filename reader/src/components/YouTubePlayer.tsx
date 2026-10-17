import { useEffect, useId, useRef } from "react";
import { connectYouTubePlayback } from "../lib/youtubePlayback";

export function YouTubePlayer({ src, title }: { src: string; title: string }) {
  const player = useRef<HTMLIFrameElement>(null);
  const id = useId();

  useEffect(() => {
    const frame = player.current;
    if (!frame) return;

    frame.focus({ preventScroll: true });

    return connectYouTubePlayback(frame);
  }, []);

  return (
    <iframe
      id={id}
      src={src}
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
