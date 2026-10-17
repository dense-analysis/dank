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
      {(thumbnails.length > 1 ||
        (thumbnails.length > 0 && titles.length > 1)) && (
        <section className="yt-version-history" aria-label="Saved thumbnails">
          <h2>Title &amp; thumbnail combinations</h2>
          <p>
            Titles shown with each saved image. Dates belong to each
            combination; older pairings are matched only when the saved captures
            agree.
          </p>
          <div className="yt-version-thumbnails">
            {thumbnails.map((version) => (
              <figure key={version.url}>
                <SavedThumbnail
                  url={version.url}
                  captured={version.last_seen}
                />
                <figcaption>
                  {version.title_pairs?.length ? (
                    <ul
                      className="yt-title-pairs"
                      aria-label="Titles captured with this thumbnail"
                    >
                      {version.title_pairs.map((pair) => (
                        <li key={pair.title}>
                          <strong>{pair.title}</strong>
                          <span>
                            First seen <ObservedTime value={pair.first_seen} />
                          </span>
                          <span>
                            Last seen <ObservedTime value={pair.last_seen} />
                          </span>
                          {pair.association === "matched" && (
                            <span className="yt-pair-provenance">
                              Matched from saved captures
                            </span>
                          )}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <>
                      <strong className="yt-pair-unknown">
                        Title pairing unavailable
                      </strong>
                      <span>
                        This image could not be linked unambiguously to a
                        captured title.
                      </span>
                      <span>
                        Image first saved{" "}
                        <ObservedTime value={version.first_seen} />
                      </span>
                      <span>
                        Image last saved{" "}
                        <ObservedTime value={version.last_seen} />
                      </span>
                    </>
                  )}
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
