import type { XTextSegment } from "../lib/xText";
import { HighlightedText } from "./HighlightedText";
import { ReaderLink } from "./ReaderLink";

export function XPostText({
  paragraphs,
  navigation,
  query,
}: {
  paragraphs: XTextSegment[][];
  navigation?: { href: string; open: () => void };
  query: string;
}) {
  return (
    <div className="article-body">
      {paragraphs.map((segments) => (
        <p key={segments[0].start}>
          {segments.map((segment) =>
            segment.href ? (
              <a
                key={segment.start}
                href={segment.href}
                target="_blank"
                rel="noopener noreferrer"
              >
                <HighlightedText text={segment.text} query={query} />
              </a>
            ) : navigation ? (
              <ReaderLink
                key={segment.start}
                className="x-post-text-link"
                href={navigation.href}
                onNavigate={navigation.open}
                tabIndex={-1}
                preserveTextSelection
                draggable={false}
              >
                <HighlightedText text={segment.text} query={query} />
              </ReaderLink>
            ) : (
              <HighlightedText
                key={segment.start}
                text={segment.text}
                query={query}
              />
            ),
          )}
        </p>
      ))}
    </div>
  );
}
