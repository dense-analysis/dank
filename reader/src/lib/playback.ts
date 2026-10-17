import { rememberVideoPosition, videoPosition } from "./videoPositions";

interface Playback {
  isActive: () => boolean;
  playing: () => void;
  paused: () => void;
  pause: () => void;
  dispose: () => void;
}

let active: Playback | undefined;

/** One active player across the feed, reader, and replies; pausing preserves position. */
export function observePlayback(
  element: HTMLElement,
  pause: () => void,
  onVisible?: () => void,
): Playback {
  let disposed = false;
  let playStartedAt = 0;
  const playback: Playback = {
    isActive: () => active === playback,
    playing() {
      if (disposed) return;

      // Also check synchronously: a buffered player can start before the observer fires.
      const rect = element.getBoundingClientRect();
      if (
        !element.isConnected ||
        rect.width <= 0 ||
        rect.height <= 0 ||
        rect.bottom <= 0 ||
        rect.top >= window.innerHeight ||
        rect.right <= 0 ||
        rect.left >= window.innerWidth
      ) {
        playback.pause();
        return;
      }

      const previous = active;
      playStartedAt = performance.now();
      active = playback;

      if (previous !== playback) previous?.pause();
    },
    paused() {
      if (active === playback) active = undefined;
    },
    pause() {
      playback.paused();
      pause();
    },
    dispose() {
      disposed = true;
      observer?.disconnect();
      window.removeEventListener("popstate", pauseActive);
      playback.pause();
    },
  };
  const pauseActive = () => {
    if (active === playback) playback.pause();
  };
  const observer =
    typeof IntersectionObserver === "undefined"
      ? undefined
      : new IntersectionObserver((entries) => {
          if (
            entries.some(
              (entry) =>
                (!entry.isIntersecting || entry.intersectionRatio === 0) &&
                // A queued sample from before this play must not cancel a resume.
                (entry.time === undefined || entry.time >= playStartedAt),
            )
          )
            pauseActive();
          else onVisible?.();
        });
  observer?.observe(element);
  window.addEventListener("popstate", pauseActive);

  return playback;
}

/** Bind native controls to the same owner used by embedded players. */
export function observeVideoPlayback(
  video: HTMLVideoElement,
  positionKey?: string,
): Playback {
  let disconnected = false;
  let lastStored = videoPosition(positionKey);
  let restoringTo: number | undefined;
  const capture = () => {
    if (!positionKey || video.readyState === 0 || restoringTo !== undefined)
      return;

    // A paused, hidden copy must not overwrite progress made in the other view.
    if (!playback.isActive() && videoPosition(positionKey) !== lastStored)
      return;

    rememberVideoPosition(positionKey, video.currentTime);
    lastStored = videoPosition(positionKey);
  };
  const restore = (force = false) => {
    if (!force && playback.isActive()) return;

    const saved = videoPosition(positionKey);
    if (saved === undefined) return;

    lastStored = saved;
    restoringTo = saved;
    // With no metadata, currentTime sets the default playback start without loading
    // the source. Reapply after metadata for streams that replace their MediaSource.
    // https://html.spec.whatwg.org/multipage/media.html#dom-media-currenttime
    if (video.readyState === 0) {
      video.currentTime = saved;
      return;
    }

    const target =
      Number.isFinite(video.duration) &&
      video.duration > 0 &&
      saved >= video.duration
        ? 0
        : saved;
    restoringTo = target;
    if (Math.abs(video.currentTime - target) < 0.05) {
      restoringTo = undefined;
      return;
    }

    try {
      video.currentTime = target;
    } catch {
      // Some streams cannot seek until their first buffered range is ready.
    }
  };
  const playback = observePlayback(
    video,
    () => {
      capture();
      video.pause();
    },
    () => restore(),
  );
  const onPlay = () => {
    if (!video.paused) {
      if (
        restoringTo !== undefined ||
        videoPosition(positionKey) !== lastStored
      )
        restore(true);
      playback.playing();
    }
  };
  const onPause = () => {
    if (video.paused) {
      capture();
      playback.paused();
    }
  };
  const onMetadata = () => {
    if (restoringTo !== undefined || videoPosition(positionKey) !== lastStored)
      restore(true);
  };
  const onEmptied = () => {
    restoringTo = videoPosition(positionKey);
  };
  const onProgress = () => {
    if (
      restoringTo !== undefined &&
      Math.abs(video.currentTime - restoringTo) < 0.25
    )
      restoringTo = undefined;

    if (playback.isActive()) capture();
  };
  const onSeeked = () => {
    // The latest native seek wins, including a user seek during restoration.
    restoringTo = undefined;
    capture();
  };
  const onNavigation = () =>
    queueMicrotask(() => {
      // Run after outgoing players have checkpointed and the reader has unmounted.
      if (!disconnected) restore();
    });
  if (positionKey) window.addEventListener("popstate", onNavigation);
  video.addEventListener("play", onPlay);
  video.addEventListener("pause", onPause);
  video.addEventListener("ended", onPause);
  video.addEventListener("loadedmetadata", onMetadata);
  video.addEventListener("emptied", onEmptied);
  video.addEventListener("timeupdate", onProgress);
  video.addEventListener("seeked", onSeeked);
  restore();

  return {
    ...playback,
    dispose() {
      disconnected = true;
      capture();
      window.removeEventListener("popstate", onNavigation);
      video.removeEventListener("play", onPlay);
      video.removeEventListener("pause", onPause);
      video.removeEventListener("ended", onPause);
      video.removeEventListener("loadedmetadata", onMetadata);
      video.removeEventListener("emptied", onEmptied);
      video.removeEventListener("timeupdate", onProgress);
      video.removeEventListener("seeked", onSeeked);
      playback.dispose();
    },
  };
}
