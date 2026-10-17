interface Playback {
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
): Playback {
  let disposed = false;
  const playback: Playback = {
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
              (entry) => !entry.isIntersecting || entry.intersectionRatio === 0,
            )
          )
            pauseActive();
        });
  observer?.observe(element);
  window.addEventListener("popstate", pauseActive);

  return playback;
}
