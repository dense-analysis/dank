import { useState } from "react";
import { type YouTubeItem, youtubeImageUrl } from "../lib/youtubeCollection";

function ObservedTime({ value }: { value: string }) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime()))
    return <span>Unknown capture date</span>;

  return (
    <time dateTime={value}>
      {new Intl.DateTimeFormat("en-GB", {
        dateStyle: "medium",
        timeStyle: "short",
        timeZone: "UTC",
      }).format(date)}{" "}
      UTC
    </time>
  );
}

export function YouTubeVersionHistory({ item }: { item: YouTubeItem }) {
  const titles = item.title_history ?? [];
  const thumbnails = (item.thumbnail_variants ?? []).filter((version) =>
    youtubeImageUrl(version.url),
  );

  return (
    <>
      {thumbnails.length > 1 && (
        <section className="yt-version-history" aria-label="Saved thumbnails">
          <h2>Saved thumbnails</h2>
          <p>
            Different images observed for this video. Hover or focus its preview
            to cycle through them.
          </p>
          <div className="yt-version-thumbnails">
            {thumbnails.map((version) => (
              <figure key={version.url}>
                <SavedThumbnail
                  url={version.url}
                  captured={version.last_seen}
                />
                <figcaption>
                  First seen <ObservedTime value={version.first_seen} />
                  <br />
                  Last seen <ObservedTime value={version.last_seen} />
                </figcaption>
              </figure>
            ))}
          </div>
        </section>
      )}
      {titles.length > 1 && (
        <section className="yt-version-history" aria-label="Observed titles">
          <h2>Observed titles</h2>
          <p>
            Titles seen in our saved captures, all belonging to this same video.
            Changes alone do not tell us whether the creator edited the title or
            ran a test.
          </p>
          <ol>
            {titles.map((version) => (
              <li key={version.title}>
                <strong>{version.title}</strong>
                {version.title === item.title && (
                  <span>Title in this snapshot</span>
                )}
                <span>
                  First seen <ObservedTime value={version.first_seen} />
                  <br />
                  Last seen <ObservedTime value={version.last_seen} />
                </span>
              </li>
            ))}
          </ol>
        </section>
      )}
    </>
  );
}

function SavedThumbnail({ url, captured }: { url: string; captured: string }) {
  const [failed, setFailed] = useState(false);
  const image = youtubeImageUrl(url);
  if (failed || !image)
    return <p className="yt-version-unavailable">Thumbnail unavailable</p>;

  return (
    <a
      href={image}
      target="_blank"
      rel="noreferrer"
      aria-label={`Open thumbnail last seen ${captured}`}
    >
      <img
        src={image}
        alt="Saved video thumbnail"
        loading="lazy"
        referrerPolicy="no-referrer"
        onError={() => setFailed(true)}
      />
    </a>
  );
}
