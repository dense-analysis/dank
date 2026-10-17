import { useId, useLayoutEffect, useMemo, useRef, useState } from "react";
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
  const text = useRef<HTMLDivElement>(null);
  const returnToPost = useRef(false);

  useLayoutEffect(() => {
    if (expanded || !returnToPost.current) return;

    returnToPost.current = false;
    const post = text.current?.closest("article");
    if (!post) return;

    // Measure after collapsing, before paint, so browser anchoring cannot leave
    // the reader among the following cards. Respect both feed and reader headers.
    const header =
      Number.parseFloat(
        getComputedStyle(post).getPropertyValue("--sticky-header-height"),
      ) || 0;
    const toolbar = post
      .closest(".article-page")
      ?.querySelector(".reader-toolbar");
    const offset = header + (toolbar?.getBoundingClientRect().height ?? 0);
    window.scrollTo({
      top: Math.max(
        0,
        window.scrollY + post.getBoundingClientRect().top - offset,
      ),
      behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches
        ? "instant"
        : "smooth",
    });
  }, [expanded]);
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
      <div id={textId} ref={text}>
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
            onClick={() => {
              returnToPost.current = expanded;
              setExpanded(!expanded);
            }}
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
