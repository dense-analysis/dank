import { observePlayback } from "./playback";
import { rememberVideoPosition, videoPosition } from "./videoPositions";

// YouTube's iframe client uses this handshake and command channel. Keep it scoped
// to our own frame, without injecting the provider's remote script into the reader.
// https://www.youtube.com/iframe_api
// State values and commands: https://developers.google.com/youtube/iframe_api_reference
const HANDSHAKE_INTERVAL_MS = 250;

export function connectYouTubePlayback(
  frame: HTMLIFrameElement,
  positionKey?: string,
) {
  const origin = new URL(frame.src).origin;
  let ready = false;
  let startWhenReady = true;
  let pausePending = false;
  let lastStored = videoPosition(positionKey);
  let restoreTarget = lastStored;
  let restoreIssued = false;
  let currentTime: number | undefined;
  let hasPlayed = false;
  let duration: number | undefined;
  const send = (message: object) =>
    frame.contentWindow?.postMessage(
      JSON.stringify({ ...message, id: frame.id, channel: "widget" }),
      origin,
    );
  const command = (func: string, args: (string | number | boolean)[] = []) =>
    send({ event: "command", func, args });
  const capture = () => {
    if (currentTime === undefined || restoreTarget !== undefined || !hasPlayed)
      return;
    if (!playback.isActive() && videoPosition(positionKey) !== lastStored)
      return;

    rememberVideoPosition(positionKey, currentTime);
    lastStored = videoPosition(positionKey);
  };
  const restore = () => {
    if (restoreIssued || restoreTarget === undefined) return;

    if (duration !== undefined && restoreTarget >= duration) restoreTarget = 0;
    command("seekTo", [restoreTarget, true]);
    restoreIssued = true;
  };
  const playback = observePlayback(frame, () => {
    capture();
    startWhenReady = false;

    if (ready) {
      pausePending = true;
      command("pauseVideo");
    }
  });
  const listen = () => send({ event: "listening" });
  const onMessage = (event: MessageEvent) => {
    if (
      event.source !== frame.contentWindow ||
      event.origin !== origin ||
      typeof event.data !== "string"
    )
      return;

    let message: unknown;
    try {
      message = JSON.parse(event.data);
    } catch {
      return;
    }

    if (
      !message ||
      typeof message !== "object" ||
      !("id" in message) ||
      message.id !== frame.id ||
      !("channel" in message) ||
      message.channel !== "widget" ||
      !("event" in message)
    )
      return;

    if (message.event === "onReady" || message.event === "alreadyInitialized") {
      if (ready) return;

      ready = true;
      window.clearInterval(handshake);
      command("addEventListener", ["onStateChange"]);
      command("addEventListener", ["onError"]);

      // A different video or a scroll may have cancelled the click while loading.
      if (startWhenReady) {
        restore();
        command("playVideo");
      }
    } else if (message.event === "onStateChange" && "info" in message) {
      if (message.info === 1 || message.info === 3) {
        // Ignore late buffering/playing events from a player we just paused.
        if (pausePending) command("pauseVideo");
        else {
          restore();
          hasPlayed = message.info === 1 || hasPlayed;
          playback.playing();
        }
      } else if (
        message.info === 2 ||
        message.info === 0 ||
        message.info === 5
      ) {
        pausePending = false;
        capture();
        playback.paused();
      }
    } else if (
      ["initialDelivery", "infoDelivery"].includes(String(message.event)) &&
      "info" in message &&
      message.info &&
      typeof message.info === "object"
    ) {
      const info = message.info;
      if (
        "duration" in info &&
        typeof info.duration === "number" &&
        Number.isFinite(info.duration) &&
        info.duration > 0
      )
        duration = info.duration;

      if (
        "currentTime" in info &&
        typeof info.currentTime === "number" &&
        Number.isFinite(info.currentTime) &&
        info.currentTime >= 0
      ) {
        currentTime = info.currentTime;
        // Ignore initial zero-time messages until the restored seek has landed.
        if (
          restoreTarget !== undefined &&
          restoreIssued &&
          Math.abs(currentTime - restoreTarget) < 0.25
        )
          restoreTarget = undefined;

        if (playback.isActive()) capture();
      }
    } else if (message.event === "onError") {
      playback.paused();
    }
  };
  window.addEventListener("message", onMessage);
  frame.addEventListener("load", listen);
  const handshake = window.setInterval(listen, HANDSHAKE_INTERVAL_MS);
  listen();
  // Reserve playback for this explicit click, including while the iframe loads.
  playback.playing();

  return () => {
    window.clearInterval(handshake);
    window.removeEventListener("message", onMessage);
    frame.removeEventListener("load", listen);
    playback.dispose();
  };
}
