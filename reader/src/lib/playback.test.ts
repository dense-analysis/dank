import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { observePlayback, observeVideoPlayback } from "./playback";
import { rememberVideoPosition, videoPosition } from "./videoPositions";
import { connectYouTubePlayback } from "./youtubePlayback";

const cleanups: (() => void)[] = [];
const observers: {
  change: (entries: Partial<IntersectionObserverEntry>[]) => void;
  disconnect: ReturnType<typeof vi.fn>;
}[] = [];

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal(
    "IntersectionObserver",
    class {
      disconnect = vi.fn();
      observe = vi.fn();
      constructor(
        change: (entries: Partial<IntersectionObserverEntry>[]) => void,
      ) {
        observers.push({ change, disconnect: this.disconnect });
      }
    },
  );
});
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  observers.length = 0;
  document.body.replaceChildren();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

function visible<T extends HTMLElement>(element: T) {
  document.body.append(element);
  vi.spyOn(element as HTMLElement, "getBoundingClientRect").mockReturnValue(
    new DOMRect(10, 10, 320, 200),
  );

  return element;
}
function player() {
  const element = visible(document.createElement("video"));
  const pause = vi.fn();
  const playback = observePlayback(element, pause);
  cleanups.push(playback.dispose);

  return { element, pauseSpy: pause, ...playback };
}

describe("shared playback", () => {
  it("pauses only the previous player and ignores late pause notifications from it", () => {
    const first = player();
    const second = player();
    first.playing();
    second.playing();
    expect(first.pauseSpy).toHaveBeenCalledOnce();
    first.paused();
    window.dispatchEvent(new PopStateEvent("popstate"));
    expect(second.pauseSpy).toHaveBeenCalledOnce();
  });

  it("pauses when fully outside the viewport and never resumes on visibility alone", () => {
    const video = player();
    video.playing();
    observers[0].change([{ isIntersecting: true, intersectionRatio: 0.01 }]);
    expect(video.pauseSpy).not.toHaveBeenCalled();
    observers[0].change([{ isIntersecting: false, intersectionRatio: 0 }]);
    expect(video.pauseSpy).toHaveBeenCalledOnce();
    observers[0].change([{ isIntersecting: true, intersectionRatio: 1 }]);
    window.dispatchEvent(new PopStateEvent("popstate"));
    expect(video.pauseSpy).toHaveBeenCalledOnce();
  });

  it("ignores visibility samples older than a new play request", () => {
    const video = player();
    vi.advanceTimersByTime(10);
    video.playing();
    observers[0].change([
      { isIntersecting: false, intersectionRatio: 0, time: 0 },
    ]);
    expect(video.pauseSpy).not.toHaveBeenCalled();
    observers[0].change([
      { isIntersecting: false, intersectionRatio: 0, time: 20 },
    ]);
    expect(video.pauseSpy).toHaveBeenCalledOnce();
  });

  it("rejects delayed playback from hidden or detached players before observer delivery", () => {
    const video = player();
    vi.mocked(video.element.getBoundingClientRect).mockReturnValue(
      new DOMRect(),
    );
    video.playing();
    expect(video.pauseSpy).toHaveBeenCalledOnce();
    video.element.remove();
    video.playing();
    expect(video.pauseSpy).toHaveBeenCalledTimes(2);
  });

  it("cleans up observers and navigation listeners when the player is removed", () => {
    const video = player();
    video.playing();
    video.dispose();
    video.pauseSpy.mockClear();
    video.playing();
    window.dispatchEvent(new PopStateEvent("popstate"));
    expect(video.pauseSpy).not.toHaveBeenCalled();
    expect(observers[0].disconnect).toHaveBeenCalledOnce();
    cleanups.length = 0;
  });
});

function youtube(positionKey?: string) {
  const frame = visible(document.createElement("iframe"));
  frame.id = "youtube-test";
  frame.src =
    "https://www.youtube-nocookie.com/embed/LKrwhjOI8Nc?enablejsapi=1";
  const target = frame.contentWindow;
  if (!target) throw new Error("Expected iframe window");

  const send = vi.spyOn(target, "postMessage");
  const disconnect = connectYouTubePlayback(frame, positionKey);
  cleanups.push(disconnect);
  const message = (
    event: string,
    info?: unknown,
    origin = "https://www.youtube-nocookie.com",
    source = frame.contentWindow,
  ) => {
    window.dispatchEvent(
      new MessageEvent("message", {
        origin,
        source,
        data: JSON.stringify({ id: frame.id, channel: "widget", event, info }),
      }),
    );
  };
  const commands = () =>
    send.mock.calls
      .map(([data]) => JSON.parse(data as string))
      .filter((data) => data.event === "command")
      .map((data) => data.func);

  return { frame, send, message, commands };
}

describe("YouTube playback messages", () => {
  it("requires the matching frame and origin and waits for readiness before playing", () => {
    const yt = youtube();
    yt.message("onReady", undefined, "https://evil.test");
    yt.message("onReady", undefined, undefined, window);
    expect(yt.commands()).toEqual([]);
    yt.message("onReady");
    expect(yt.commands()).toContain("playVideo");
    const count = yt.send.mock.calls.length;
    vi.advanceTimersByTime(1000);
    expect(yt.send.mock.calls).toHaveLength(count);
  });

  it("a cancelled initial click does not autoplay when the frame is finally ready", () => {
    const yt = youtube();
    const native = player();
    native.playing();
    yt.message("onReady");
    expect(yt.commands()).not.toContain("playVideo");
    yt.message("onStateChange", 1);
    expect(native.pauseSpy).toHaveBeenCalledOnce();
  });

  it("late buffering events cannot steal playback, but manual resume after pause can", () => {
    const yt = youtube();
    yt.message("onReady");
    yt.message("onStateChange", 1);
    const native = player();
    native.playing();
    expect(yt.commands().at(-1)).toBe("pauseVideo");
    yt.message("onStateChange", 3);
    yt.message("onStateChange", 1);
    expect(native.pauseSpy).not.toHaveBeenCalled();
    yt.message("onStateChange", 2);
    yt.message("onStateChange", 1);
    expect(native.pauseSpy).toHaveBeenCalledOnce();
  });

  it("disconnecting removes the handshake timer and ignores later frame messages", () => {
    const yt = youtube();
    cleanups.shift()?.();
    yt.send.mockClear();
    yt.message("onReady");
    vi.advanceTimersByTime(1000);
    expect(yt.send).not.toHaveBeenCalled();
  });
});

function native(key: string, initialReady = 1) {
  const video = visible(document.createElement("video"));
  let ready = initialReady;
  let paused = true;
  Object.defineProperties(video, {
    readyState: { get: () => ready },
    duration: { get: () => 60 },
    paused: { get: () => paused },
  });
  vi.spyOn(video, "pause").mockImplementation(() => {
    paused = true;
    video.dispatchEvent(new Event("pause"));
  });
  const binding = observeVideoPlayback(video, key);
  cleanups.push(binding.dispose);

  return {
    video,
    binding,
    ready() {
      ready = 1;
      video.dispatchEvent(new Event("loadedmetadata"));
    },
    play() {
      paused = false;
      video.dispatchEvent(new Event("play"));
    },
    seek(time: number) {
      video.currentTime = time;
      video.dispatchEvent(new Event("seeked"));
    },
  };
}

describe("native position handoff", () => {
  it("restores the reader's newer progress when the old feed copy becomes visible", () => {
    const key = "native-handoff";
    const feed = native(key);
    feed.play();
    feed.seek(12.5);
    feed.binding.pause();
    const reader = native(key);
    expect(reader.video.currentTime).toBe(12.5);
    reader.video.dispatchEvent(new Event("seeked"));
    reader.play();
    reader.seek(26);
    reader.binding.pause();
    feed.binding.dispose();
    expect(videoPosition(key)).toBe(26);
    cleanups.shift();
    const restored = native(key);
    expect(restored.video.currentTime).toBe(26);
  });
  it("does not replace a pending restore with zero before metadata arrives", () => {
    const key = "metadata-handoff";
    rememberVideoPosition(key, 14.5);
    const incoming = native(key, 0);
    incoming.play();
    incoming.video.dispatchEvent(new Event("timeupdate"));
    expect(videoPosition(key)).toBe(14.5);
    incoming.ready();
    expect(incoming.video.currentTime).toBe(14.5);
    incoming.seek(14.5);
    incoming.binding.pause();
    expect(videoPosition(key)).toBe(14.5);
  });
  it("keeps deliberate backward seeks and restarts completed videos sensibly", () => {
    const key = "backward-handoff";
    rememberVideoPosition(key, 40);
    const incoming = native(key);
    incoming.seek(40);
    incoming.seek(3);
    expect(videoPosition(key)).toBe(3);
    rememberVideoPosition(key, 60);
    const completed = native(key);
    expect(completed.video.currentTime).toBe(0);
  });
});

describe("YouTube position handoff", () => {
  it("seeks to saved progress before play and ignores untrusted or initial zero samples", () => {
    const key = "youtube-handoff";
    rememberVideoPosition(key, 123.5);
    const yt = youtube(key);
    yt.message("onReady");
    expect(yt.commands()).toEqual([
      "addEventListener",
      "addEventListener",
      "seekTo",
      "playVideo",
    ]);
    yt.message("onStateChange", 1);
    yt.message("infoDelivery", { currentTime: 0 });
    expect(videoPosition(key)).toBe(123.5);
    yt.message("infoDelivery", { currentTime: 123.5 });
    yt.message("infoDelivery", { currentTime: 130 }, "https://evil.test");
    expect(videoPosition(key)).toBe(123.5);
    yt.message("infoDelivery", { currentTime: 126 });
    expect(videoPosition(key)).toBe(126);
    yt.message("onStateChange", 2);
    yt.message("infoDelivery", { currentTime: 50 });
    window.dispatchEvent(new PopStateEvent("popstate"));
    cleanups.shift()?.();
    expect(videoPosition(key)).toBe(50);
  });
});
