import { useMemo } from "react";
import { xTextParagraphs } from "../lib/xText";
import { HighlightedText } from "./HighlightedText";
import { ReaderLink } from "./ReaderLink";

export function XPostText({
  html,
  href,
  open,
  query,
}: {
  html: string;
  href: string;
  open: () => void;
  query: string;
}) {
  const paragraphs = useMemo(() => xTextParagraphs(html), [html]);

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
            ) : (
              <ReaderLink
                key={segment.start}
                className="x-post-text-link"
                href={href}
                onNavigate={open}
                tabIndex={-1}
                preserveTextSelection
                draggable={false}
              >
                <HighlightedText text={segment.text} query={query} />
              </ReaderLink>
            ),
          )}
        </p>
      ))}
    </div>
  );
}
