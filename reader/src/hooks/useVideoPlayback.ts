import { useCallback } from "react";
import { observePlayback } from "../lib/playback";

export function useVideoPlayback() {
  return useCallback((video: HTMLVideoElement | null) => {
    if (!video) return;

    const playback = observePlayback(video, () => video.pause());
    const onPlay = () => {
      if (!video.paused) playback.playing();
    };
    const onPause = () => {
      if (video.paused) playback.paused();
    };
    video.addEventListener("play", onPlay);
    video.addEventListener("pause", onPause);
    video.addEventListener("ended", onPause);

    return () => {
      video.removeEventListener("play", onPlay);
      video.removeEventListener("pause", onPause);
      video.removeEventListener("ended", onPause);
      playback.dispose();
    };
  }, []);
}
