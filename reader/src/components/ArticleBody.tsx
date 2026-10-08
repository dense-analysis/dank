import { useCallback } from "react";
import { renderArticle } from "../lib/article";

export function ArticleBody({ html }: { html: string }) {
  const attach = useCallback(
    (element: HTMLDivElement | null) => {
      if (!element) return;
      const blocks = renderArticle(element, html);
      const controller = new AbortController();
      const timers = new Set<ReturnType<typeof setTimeout>>();
      for (const block of blocks) {
        let resetTimer: ReturnType<typeof setTimeout> | undefined;
        block.copy.addEventListener(
          "click",
          async () => {
            if (resetTimer) {
              clearTimeout(resetTimer);
              timers.delete(resetTimer);
            }
            block.copy.disabled = true;
            block.copy.textContent = "Copying…";
            block.status.textContent = "";
            try {
              await navigator.clipboard.writeText(block.text);
              if (controller.signal.aborted) return;
              block.copy.textContent = "Copied";
              block.status.textContent = "Code copied.";
            } catch {
              if (controller.signal.aborted) return;
              block.copy.textContent = "Copy failed";
              block.status.textContent =
                "Select the code and copy it manually.";
            } finally {
              if (!controller.signal.aborted) {
                block.copy.disabled = false;
                const timer = setTimeout(() => {
                  block.copy.textContent = "Copy";
                  block.status.textContent = "";
                  timers.delete(timer);
                }, 3000);
                resetTimer = timer;
                timers.add(timer);
              }
            }
          },
          { signal: controller.signal },
        );
      }
      if (
        blocks.some((block) => block.language && block.language !== "plaintext")
      ) {
        void import("../lib/highlightCode")
          .then(({ highlightCode }) => {
            if (!controller.signal.aborted) highlightCode(blocks);
          })
          .catch(() => {
            // Code remains selectable and copyable if the optional highlighter cannot load.
          });
      }
      return () => {
        controller.abort();
        for (const timer of timers) clearTimeout(timer);
      };
    },
    [html],
  );

  return <div className="article-body" ref={attach} />;
}
