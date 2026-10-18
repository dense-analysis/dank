import { ExternalLink, Globe2 } from "lucide-react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { videoSource } from "../lib/videoSource";

export function VideoSource({
  url,
  postUrl,
  remote,
}: {
  url: string;
  postUrl: string;
  remote: boolean;
}) {
  const { preferences } = useReaderPreferences();
  const post = videoSource(postUrl);
  const source = videoSource(url) ?? post;
  const external = source && source.name !== post?.name;
  const labelled = remote && preferences.showRemoteContentLabels;

  if (!source || (!labelled && !external)) return null;

  return (
    <span className="video-source">
      {labelled && (
        <span className="remote-content-label">
          <Globe2 size={13} aria-hidden="true" /> Remote video - {source.name}
        </span>
      )}
      {external && (
        <a
          className="icon-button video-source-link"
          href={source.url}
          target="_blank"
          rel="noreferrer"
          title={`Watch on ${source.name}`}
          aria-label={`Watch on ${source.name}`}
        >
          <ExternalLink size={13} aria-hidden="true" />
        </a>
      )}
    </span>
  );
}
