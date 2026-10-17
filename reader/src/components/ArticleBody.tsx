import { useCallback, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { type ArticleContent, renderArticle } from "../lib/article";
import { enhanceRedditImageLinks } from "../lib/redditImages";
import { prepareRedditLinkVisibility } from "../lib/redditLinkVisibility";
import {
  displayedRedditVideos,
  redditVideoKey,
  withoutRedditVideoUrls,
} from "../lib/redditText";
import type { RedditImage, RedditVideo } from "../lib/types";
import { CodeCopyButton } from "./CodeCopyButton";
import { ImageBackgroundToggle } from "./ImageBackgroundToggle";

export function ArticleBody({
  html,
  query = "",
  redditImages,
  redditPostId,
  redditVideos,
  redditVideoKeys,
}: {
  html: string;
  query?: string;
  redditImages?: RedditImage[];
  redditPostId?: string;
  redditVideos?: RedditVideo[];
  redditVideoKeys?: Set<string>;
}) {
  const { preferences } = useReaderPreferences();
  const imageLinks = useRef<
    ReturnType<typeof enhanceRedditImageLinks> | undefined
  >(undefined);
  const videoLinks = useRef<
    ReturnType<typeof prepareRedditLinkVisibility> | undefined
  >(undefined);
  const [article, setArticle] = useState<ArticleContent | null>(null);
  const attach = useCallback(
    (element: HTMLDivElement | null) => {
      if (!element) return;

      const content = renderArticle(element, html, query);
      setArticle(content);
      const imageVisibility = redditImages
        ? enhanceRedditImageLinks(element, redditImages, redditPostId)
        : undefined;
      const videoVisibility = redditVideos
        ? prepareRedditLinkVisibility(
            element,
            displayedRedditVideos(redditVideos, new Set()),
            redditVideoKey,
            withoutRedditVideoUrls,
          )
        : undefined;
      imageLinks.current = imageVisibility;
      videoLinks.current = videoVisibility;
      let active = true;

      if (
        content.codeBlocks.some(
          (block) => block.language && block.language !== "plaintext",
        )
      ) {
        void import("../lib/highlightCode")
          .then(({ highlightCode }) => {
            if (active) highlightCode(content.codeBlocks);
          })
          .catch(() => {
            // Code remains selectable and copyable if the optional highlighter cannot load.
          });
      }

      return () => {
        active = false;
        imageVisibility?.dispose();
        videoVisibility?.dispose();

        if (imageLinks.current === imageVisibility)
          imageLinks.current = undefined;
        if (videoLinks.current === videoVisibility)
          videoLinks.current = undefined;
      };
    },
    [html, query, redditImages, redditPostId, redditVideos],
  );

  useLayoutEffect(() => {
    if (!article) return;

    imageLinks.current?.setHidden(preferences.hideRawImageLinks);
    videoLinks.current?.update(
      redditVideoKeys ?? new Set(),
      preferences.hideRawVideoLinks,
    );
  }, [
    article,
    preferences.hideRawImageLinks,
    preferences.hideRawVideoLinks,
    redditVideoKeys,
  ]);

  // The adapter owns source markup; React owns only the controls in its portal hosts.
  return (
    <>
      <div className="article-body" ref={attach} />
      {article?.codeBlocks.map((block, index) =>
        createPortal(
          <CodeCopyButton text={block.text} />,
          block.controls,
          `code-${index}`,
        ),
      )}
      {article?.images.map((image, index) =>
        createPortal(
          <ImageBackgroundToggle {...image} />,
          image.frame,
          `image-${index}`,
        ),
      )}
    </>
  );
}
