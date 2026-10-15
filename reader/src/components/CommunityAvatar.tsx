import { useState } from "react";

// Community images are downloaded assets, never a request to Reddit while reading.
function localIcon(iconUrl: string | null | undefined): string | undefined {
  if (!iconUrl) return undefined;

  try {
    const url = new URL(iconUrl, window.location.origin);
    if (
      url.origin === window.location.origin &&
      url.pathname.startsWith("/assets/")
    ) {
      return url.href;
    }
  } catch {
    return undefined;
  }

  return undefined;
}

export function CommunityAvatar({
  community,
  iconUrl,
}: {
  community: string;
  iconUrl?: string | null;
}) {
  const [failedUrl, setFailedUrl] = useState<string>();
  const src = localIcon(iconUrl);

  return (
    <span
      className={`source-mark community-avatar tone-${community.length % 5}`}
      aria-hidden="true"
    >
      {src && src !== failedUrl ? (
        <img src={src} alt="" onError={() => setFailedUrl(src)} />
      ) : (
        community.replace(/^r\//i, "").slice(0, 1).toUpperCase() || "?"
      )}
    </span>
  );
}
