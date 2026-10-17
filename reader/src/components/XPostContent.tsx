import { ExternalLink, ImageOff } from "lucide-react";
import { useMemo, useState } from "react";
import { articleText } from "../lib/article";
import type { Post, XLink, XMedia } from "../lib/types";
import { xMedia, xMediaUrl } from "../lib/x";
import { ArticleBody } from "./ArticleBody";
import { HighlightedText } from "./HighlightedText";
import { safeUrl } from "./PostCard";

// Short posts stay fully readable; long notes expand without duplicating their text.
const PREVIEW_CHARACTERS = 600;

export function XPostContent({
  post,
  compact = false,
  query = "",
}: {
  post: Post;
  compact?: boolean;
  query?: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const text = useMemo(() => articleText(post.html), [post.html]);
  const canExpand = compact && text.length > PREVIEW_CHARACTERS;
  const media = xMedia(post);

  return (
    <div className={`x-post-content ${compact ? "x-content-compact" : ""}`}>
      {canExpand && !expanded ? (
        <div className="article-body x-text-preview">
          <p>
            <HighlightedText
              text={`${text.slice(0, PREVIEW_CHARACTERS).trimEnd()}…`}
              query={query}
            />
          </p>
        </div>
      ) : (
        <ArticleBody html={post.html} query={query} />
      )}
      {canExpand && (
        <button
          type="button"
          className="text-button x-expand"
          aria-expanded={expanded}
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? "Show less" : "Show more"}
        </button>
      )}
      {media.length > 0 && (
        <section className="x-attachments" aria-label="Post attachments">
          {media.map((asset, index) => (
            <XAttachment
              key={`${asset.type}:${asset.url}`}
              asset={asset}
              original={post.url}
              index={index}
            />
          ))}
        </section>
      )}
      {post.x?.links?.map((link) => (
        <XLinkPreview key={link.url} link={link} />
      ))}
    </div>
  );
}

function XAttachment({
  asset,
  original,
  index,
}: {
  asset: XMedia;
  original: string;
  index: number;
}) {
  const [failed, setFailed] = useState(false);
  const url = xMediaUrl(asset.url);
  const poster = xMediaUrl(asset.poster_url);

  if (!url || failed)
    return (
      <div className="x-media-unavailable">
        <ImageOff size={20} aria-hidden="true" />
        <span>{asset.type === "image" ? "Image" : "Video"} unavailable</span>
        <a href={safeUrl(original)} target="_blank" rel="noreferrer">
          View on X <ExternalLink size={13} />
        </a>
      </div>
    );

  return (
    <figure className={`x-attachment x-attachment-${asset.type}`}>
      {asset.type === "image" ? (
        <a
          href={url}
          target="_blank"
          rel="noreferrer"
          aria-label={`Open full-size image ${index + 1}`}
        >
          <img
            src={url}
            alt={asset.alt || `Image ${index + 1} attached to this post`}
            loading="lazy"
            referrerPolicy="no-referrer"
            onError={() => setFailed(true)}
          />
        </a>
      ) : (
        <video
          src={url}
          poster={poster}
          controls
          preload="none"
          playsInline
          loop={asset.type === "gif"}
          muted={asset.type === "gif"}
          aria-label="X post video"
          onError={() => setFailed(true)}
        >
          <track kind="captions" />
        </video>
      )}
      {asset.remote && (
        <figcaption>
          {asset.type === "image" ? "Image" : "Video"} from X
        </figcaption>
      )}
    </figure>
  );
}

function XLinkPreview({ link }: { link: XLink }) {
  const [imageFailed, setImageFailed] = useState(false);
  const url = safeUrl(link.url);
  if (!url) return null;

  const domain = new URL(url).hostname.replace(/^www\./, "");
  const image = xMediaUrl(link.image_url);

  return (
    <a
      className="x-link-preview"
      href={url}
      target="_blank"
      rel="noreferrer"
      aria-label={`Open link: ${link.title || domain}`}
    >
      {image && !imageFailed && (
        <img
          src={image}
          alt=""
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => setImageFailed(true)}
        />
      )}
      <span className="x-link-copy">
        <span className="x-link-domain">
          {domain} <ExternalLink size={12} aria-hidden="true" />
        </span>
        <strong>{link.title || link.url}</strong>
        {link.description && (
          <span className="x-link-description">{link.description}</span>
        )}
      </span>
    </a>
  );
}
