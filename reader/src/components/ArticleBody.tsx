import { useCallback, useState } from "react";
import { createPortal } from "react-dom";
import { type ArticleContent, renderArticle } from "../lib/article";
import { CodeCopyButton } from "./CodeCopyButton";
import { ImageBackgroundToggle } from "./ImageBackgroundToggle";

export function ArticleBody({
  html,
  query = "",
}: {
  html: string;
  query?: string;
}) {
  const [article, setArticle] = useState<ArticleContent | null>(null);
  const attach = useCallback(
    (element: HTMLDivElement | null) => {
      if (!element) return;

      const content = renderArticle(element, html, query);
      setArticle(content);
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
      };
    },
    [html, query],
  );

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
