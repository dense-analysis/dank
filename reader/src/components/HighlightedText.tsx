import { Fragment } from "react";
import { highlightMatches } from "../lib/highlightMatches";

export function HighlightedText({
  text,
  query,
}: {
  text: string;
  query: string;
}) {
  return highlightMatches(text, query).map((part) =>
    part.highlighted ? (
      <mark key={part.start}>{part.text}</mark>
    ) : (
      <Fragment key={part.start}>{part.text}</Fragment>
    ),
  );
}
