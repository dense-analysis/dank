// Imported HTML owns these nodes; controllers only synchronize link visibility.
export function prepareRedditLinkVisibility(
  element: HTMLElement,
  possible: Set<string>,
  keyOf: (value: string) => string | undefined,
  stripUrls: (text: string, displayed: Set<string>) => string,
) {
  const rawMediaUrls =
    /(?:https?:\/\/|(?:(?:i|preview|external-preview|v|packaged-media)\.redd\.it|(?:(?:www|old)\.)?reddit\.com)\/|\/assets\/)[^\s<>"']+/gi;
  const targets: { node: HTMLElement; key: string }[] = [];

  for (const anchor of element.querySelectorAll<HTMLAnchorElement>("a[href]")) {
    const key = keyOf(anchor.href);
    const label = anchor.textContent?.trim() ?? "";

    if (
      key &&
      possible.has(key) &&
      label &&
      !anchor.querySelector("img") &&
      stripUrls(label, new Set([key])) === ""
    )
      targets.push({ node: anchor, key });
  }

  const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
  const textNodes: Text[] = [];

  while (walker.nextNode()) {
    if (!walker.currentNode.parentElement?.closest("a, code, pre"))
      textNodes.push(walker.currentNode as Text);
  }

  for (const textNode of textNodes) {
    const text = textNode.textContent ?? "";
    const fragment = document.createDocumentFragment();
    let offset = 0;

    for (const match of text.matchAll(rawMediaUrls)) {
      const url = match[0].replace(/[.,!?;:)\]]+$/, "");
      const key = keyOf(url);

      if (!key || !possible.has(key)) continue;

      fragment.append(text.slice(offset, match.index));
      const span = document.createElement("span");
      span.textContent = url;
      fragment.append(span);
      targets.push({ node: span, key });
      offset = match.index + url.length;
    }

    if (offset) {
      fragment.append(text.slice(offset));
      textNode.replaceWith(fragment);
    }
  }

  if (!targets.length)
    return { update: () => undefined, dispose: () => undefined };

  const blocks = new Set(
    targets.flatMap(({ node }) => {
      const parent = node.closest<HTMLElement>("p, div");

      return parent && parent !== element ? [parent] : [];
    }),
  );
  const originals = new Map(
    [...targets.map(({ node }) => node), ...blocks].map((node) => [
      node,
      node.hidden,
    ]),
  );

  return {
    update(displayed: Set<string>, hide = true) {
      for (const { node, key } of targets)
        node.hidden =
          hide && displayed.has(key) && !node.closest(".reference-match");
      for (const block of blocks) block.hidden = false;

      for (const block of blocks) {
        const visible = block.cloneNode(true) as HTMLElement;

        for (const hidden of visible.querySelectorAll("[hidden]"))
          hidden.remove();

        block.hidden =
          !visible.textContent?.trim() &&
          !visible.querySelector("img, video, audio");
      }
    },
    dispose() {
      for (const [node, hidden] of originals) node.hidden = hidden;
    },
  };
}
