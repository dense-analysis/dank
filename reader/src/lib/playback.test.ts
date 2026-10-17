import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { observePlayback } from "./playback";
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

function youtube() {
  const frame = visible(document.createElement("iframe"));
  frame.id = "youtube-test";
  frame.src =
    "https://www.youtube-nocookie.com/embed/LKrwhjOI8Nc?enablejsapi=1";
  const target = frame.contentWindow;
  if (!target) throw new Error("Expected iframe window");

  const send = vi.spyOn(target, "postMessage");
  const disconnect = connectYouTubePlayback(frame);
  cleanups.push(disconnect);
  const message = (
    event: string,
    info?: number,
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
