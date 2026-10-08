import hljs from "highlight.js/lib/common";
import type { ArticleCodeBlock } from "./article";

export function highlightCode(blocks: ArticleCodeBlock[]) {
  let remaining = 250_000;
  for (const block of blocks) {
    if (!block.language || block.language === "plaintext") continue;
    const language = hljs.getLanguage(block.language);
    if (!language) continue;
    block.label.textContent = language.name ?? block.language.toUpperCase();
    // Very large samples stay readable without blocking article navigation.
    if (block.text.length > 50_000 || block.text.length > remaining) continue;
    remaining -= block.text.length;
    try {
      const result = hljs.highlight(block.text, {
        language: block.language,
        ignoreIllegals: true,
      });
      // Only the highlighter's escaped plain-text output enters the code element.
      block.code.innerHTML = result.value;
      block.code.className = "hljs";
    } catch {
      block.code.textContent = block.text;
    }
  }
}
