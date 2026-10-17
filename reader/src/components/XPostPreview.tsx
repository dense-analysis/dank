import { useId, useMemo, useState } from "react";
import {
  truncateXText,
  X_COLLAPSED_CHARACTERS,
  X_EXPANDED_CHARACTERS,
  xPostLength,
} from "../lib/xPostPreview";
import { xTextParagraphs } from "../lib/xText";
import { ArticleBody } from "./ArticleBody";
import { ReaderLink } from "./ReaderLink";
import { XPostText } from "./XPostText";

export function XPostPreview({
  html,
  query,
  navigation,
}: {
  html: string;
  query: string;
  navigation?: { href: string; open: () => void };
}) {
  const [expanded, setExpanded] = useState(false);
  const textId = useId();
  const paragraphs = useMemo(() => xTextParagraphs(html), [html]);
  const length = useMemo(() => xPostLength(paragraphs), [paragraphs]);
  const canExpand = length.characters > X_COLLAPSED_CHARACTERS;
  // Replies without an internal reader destination must remain fully expandable.
  const needsReader = length.articleLength && navigation;
  const limit = !expanded
    ? X_COLLAPSED_CHARACTERS
    : needsReader
      ? X_EXPANDED_CHARACTERS
      : undefined;
  const visible = useMemo(
    () => (limit === undefined ? paragraphs : truncateXText(paragraphs, limit)),
    [paragraphs, limit],
  );

  return (
    <>
      <div id={textId}>
        {expanded && !navigation ? (
          <ArticleBody html={html} query={query} />
        ) : (
          <XPostText
            paragraphs={visible}
            navigation={navigation}
            query={query}
          />
        )}
      </div>
      {canExpand && (
        <div className="x-text-actions">
          <button
            type="button"
            className="text-button"
            aria-expanded={expanded}
            aria-controls={textId}
            onClick={() => setExpanded(!expanded)}
          >
            {expanded ? "Show less" : "Show more"}
          </button>
          {needsReader && (
            <ReaderLink
              className="text-button x-read-full"
              href={navigation.href}
              onNavigate={navigation.open}
            >
              Read full post{" "}
              <span className="x-reading-time">
                · About {length.minutes} min read
              </span>
            </ReaderLink>
          )}
        </div>
      )}
    </>
  );
}
