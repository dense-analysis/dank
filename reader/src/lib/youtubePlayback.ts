import { observePlayback } from "./playback";

// YouTube's iframe client uses this handshake and command channel. Keep it scoped
// to our own frame, without injecting the provider's remote script into the reader.
// https://www.youtube.com/iframe_api
// State values and commands: https://developers.google.com/youtube/iframe_api_reference
const HANDSHAKE_INTERVAL_MS = 250;

export function connectYouTubePlayback(frame: HTMLIFrameElement) {
  const origin = new URL(frame.src).origin;
  let ready = false;
  let startWhenReady = true;
  let pausePending = false;
  const send = (message: object) =>
    frame.contentWindow?.postMessage(
      JSON.stringify({ ...message, id: frame.id, channel: "widget" }),
      origin,
    );
  const command = (func: string, args: string[] = []) =>
    send({ event: "command", func, args });
  const playback = observePlayback(frame, () => {
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
      if (startWhenReady) command("playVideo");
    } else if (message.event === "onStateChange" && "info" in message) {
      if (message.info === 1 || message.info === 3) {
        // Ignore late buffering/playing events from a player we just paused.
        if (pausePending) command("pauseVideo");
        else playback.playing();
      } else if (
        message.info === 2 ||
        message.info === 0 ||
        message.info === 5
      ) {
        pausePending = false;
        playback.paused();
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
