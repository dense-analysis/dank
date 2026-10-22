import { useState } from "react";
import { sourceIconTransform } from "../lib/sourceIcon";

function imageUrl(iconUrl: string | null | undefined, allowXImage: boolean) {
  if (!iconUrl) return undefined;

  try {
    const url = new URL(iconUrl, window.location.origin);
    const local =
      url.origin === window.location.origin &&
      (url.pathname.startsWith("/assets/") ||
        url.pathname === "/api/reader/source-icon");
    const profile =
      allowXImage &&
      url.protocol === "https:" &&
      url.hostname === "pbs.twimg.com" &&
      !url.port &&
      (url.pathname.startsWith("/profile_images/") ||
        url.pathname.startsWith("/sticky/default_profile_images/"));

    if (!url.username && !url.password && (local || profile)) return url.href;
  } catch {
    return undefined;
  }

  return undefined;
}

export function IdentityAvatar({
  name,
  iconUrl,
  kind,
  fitArtwork = kind === "source",
}: {
  name: string;
  iconUrl?: string | null;
  kind: "community" | "account" | "channel" | "source";
  fitArtwork?: boolean;
}) {
  const [failedUrl, setFailedUrl] = useState<string>();
  const [loadedUrl, setLoadedUrl] = useState<string>();
  const [artwork, setArtwork] = useState<{ url: string; transform?: string }>();
  const src = imageUrl(iconUrl, kind === "account");
  const initial =
    name
      .replace(/^(r\/|@)/i, "")
      .slice(0, 1)
      .toUpperCase() || "?";
  const pending =
    kind === "source" && src && src !== failedUrl && src !== loadedUrl;

  return (
    <span
      className={`source-mark identity-avatar ${kind}-avatar tone-${name.length % 5}`}
      aria-hidden="true"
    >
      {pending && initial}
      {src && src !== failedUrl ? (
        <img
          src={src}
          alt=""
          className={pending ? "avatar-pending" : undefined}
          style={
            fitArtwork && artwork?.url === src
              ? { transform: artwork.transform }
              : undefined
          }
          loading={kind === "source" ? "lazy" : undefined}
          referrerPolicy="no-referrer"
          onLoad={(event) => {
            setLoadedUrl(src);

            if (kind === "source" && fitArtwork) {
              setArtwork({
                url: src,
                transform: sourceIconTransform(event.currentTarget),
              });
            }
          }}
          onError={() => setFailedUrl(src)}
        />
      ) : (
        initial
      )}
    </span>
  );
}
