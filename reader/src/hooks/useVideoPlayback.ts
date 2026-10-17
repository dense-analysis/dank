import { useCallback } from "react";
import { observeVideoPlayback } from "../lib/playback";

export function useVideoPlayback() {
  return useCallback((video: HTMLVideoElement | null) => {
    if (!video) return;

    return observeVideoPlayback(video).dispose;
  }, []);
}
