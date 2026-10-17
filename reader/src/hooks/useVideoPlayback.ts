import { useCallback } from "react";
import { observeVideoPlayback } from "../lib/playback";

export function useVideoPlayback(positionKey?: string) {
  return useCallback(
    (video: HTMLVideoElement | null) => {
      if (!video) return;

      return observeVideoPlayback(video, positionKey).dispose;
    },
    [positionKey],
  );
}
