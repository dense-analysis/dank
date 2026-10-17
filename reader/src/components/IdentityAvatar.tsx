import { useState } from "react";

function imageUrl(iconUrl: string | null | undefined, allowXImage: boolean) {
  if (!iconUrl) return undefined;

  try {
    const url = new URL(iconUrl, window.location.origin);
    const local =
      url.origin === window.location.origin &&
      url.pathname.startsWith("/assets/");
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
}: {
  name: string;
  iconUrl?: string | null;
  kind: "community" | "account";
}) {
  const [failedUrl, setFailedUrl] = useState<string>();
  const src = imageUrl(iconUrl, kind === "account");

  return (
    <span
      className={`source-mark identity-avatar ${kind}-avatar tone-${name.length % 5}`}
      aria-hidden="true"
    >
      {src && src !== failedUrl ? (
        <img
          src={src}
          alt=""
          referrerPolicy="no-referrer"
          onError={() => setFailedUrl(src)}
        />
      ) : (
        name
          .replace(/^(r\/|@)/i, "")
          .slice(0, 1)
          .toUpperCase() || "?"
      )}
    </span>
  );
}
