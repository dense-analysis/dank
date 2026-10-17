import { Images, Play } from "lucide-react";
import { type ReactNode, useEffect, useRef, useState } from "react";

// Leave time to recognise each saved image; only an engaged card animates.
const THUMBNAIL_HOLD_MS = 2400;

export function YouTubeThumbnail({
  sources,
  fallback,
  title,
  disabled,
  play,
  children,
}: {
  sources: string[];
  fallback?: string;
  title: string;
  disabled: boolean;
  play: () => void;
  children: ReactNode;
}) {
  const host = useRef<HTMLButtonElement>(null);
  const [hovered, setHovered] = useState(false);
  const [focused, setFocused] = useState(false);
  const [visible, setVisible] = useState(true);
  const [motion, setMotion] = useState(false);
  const [failed, setFailed] = useState<string[]>([]);
  const [loaded, setLoaded] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const available = sources.filter((url) => !failed.includes(url));
  const images = available.length
    ? available
    : fallback && !failed.includes(fallback)
      ? [fallback]
      : [];
  const current = images.includes(selected) ? selected : images[0];
  const cycling = (hovered || focused) && visible && motion;
  const ready = images.filter((url) => loaded.includes(url));
  const readyKey = JSON.stringify(ready);

  useEffect(() => {
    const target = host.current;
    if (!target) return;
    const preference = window.matchMedia("(prefers-reduced-motion: reduce)");
    const syncMotion = () =>
      setMotion(!preference.matches && document.visibilityState !== "hidden");
    syncMotion();
    preference.addEventListener("change", syncMotion);
    document.addEventListener("visibilitychange", syncMotion);
    const observer =
      typeof IntersectionObserver !== "undefined"
        ? new IntersectionObserver(([entry]) =>
            setVisible(entry.isIntersecting),
          )
        : undefined;
    observer?.observe(target);

    return () => {
      preference.removeEventListener("change", syncMotion);
      document.removeEventListener("visibilitychange", syncMotion);
      observer?.disconnect();
    };
  }, []);

  useEffect(() => {
    if (!cycling) {
      setSelected("");
      return;
    }
    const candidates: string[] = JSON.parse(readyKey);
    if (candidates.length < 2) return;
    const timer = window.setInterval(() => {
      setSelected((previous) => {
        const position = Math.max(0, candidates.indexOf(previous));

        return candidates[(position + 1) % candidates.length];
      });
    }, THUMBNAIL_HOLD_MS);

    return () => window.clearInterval(timer);
  }, [cycling, readyKey]);

  return (
    <button
      ref={host}
      type="button"
      className="yt-play"
      aria-label={`Play ${title}`}
      disabled={disabled}
      onClick={play}
      onPointerEnter={(event) => {
        if (event.pointerType === "mouse") setHovered(true);
      }}
      onPointerLeave={() => {
        setHovered(false);
        setSelected("");
      }}
      onFocus={() => setFocused(true)}
      onBlur={() => {
        setFocused(false);
        setSelected("");
      }}
    >
      <span className="yt-thumbnail-versions">
        {images.length ? (
          images.map((url) => (
            <img
              key={url}
              src={url}
              alt=""
              loading="lazy"
              referrerPolicy="no-referrer"
              className={url === current ? "is-current" : ""}
              aria-hidden={url !== current}
              onLoad={() =>
                setLoaded((values) =>
                  values.includes(url) ? values : [...values, url],
                )
              }
              onError={() =>
                setFailed((values) =>
                  values.includes(url) ? values : [...values, url],
                )
              }
            />
          ))
        ) : (
          <span className="yt-image-placeholder">
            <Play size={30} strokeWidth={1} />
          </span>
        )}
        {available.length > 1 && (
          <span
            className="yt-thumbnail-count"
            title={`${available.length} saved thumbnails. Hover or focus to preview; open discovery details to inspect them.`}
          >
            <Images size={12} aria-hidden="true" />
            <span>{available.length} thumbnails</span>
          </span>
        )}
      </span>
      {children}
    </button>
  );
}
