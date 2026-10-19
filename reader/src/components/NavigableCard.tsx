import type { MouseEvent as ReactMouseEvent, ReactNode } from "react";

function activateCard(event: ReactMouseEvent<HTMLElement>) {
  if (
    event.defaultPrevented ||
    (event.button !== 0 && event.button !== 1) ||
    !(event.target instanceof Element) ||
    event.target.closest(
      "a, button, input, select, textarea, summary, video, audio, iframe, [role='button'], [role='link'], [contenteditable], [data-card-control]",
    )
  )
    return;

  const selection = window.getSelection();

  if (
    selection &&
    !selection.isCollapsed &&
    selection.rangeCount > 0 &&
    selection.getRangeAt(0).intersectsNode(event.currentTarget)
  )
    return;

  const link =
    event.currentTarget.querySelector<HTMLAnchorElement>("a[data-card-link]");
  if (!link) return;

  event.preventDefault();

  if (
    event.button === 0 &&
    !event.metaKey &&
    !event.ctrlKey &&
    !event.shiftKey &&
    !event.altKey
  )
    link.focus({ preventScroll: true });

  // Keep native link activation and modifier keys, including background tabs.
  // https://dom.spec.whatwg.org/#concept-event-dispatch
  link.dispatchEvent(new MouseEvent("click", event.nativeEvent));
}

export function NavigableCard({
  children,
  className,
  enabled = true,
}: {
  children: ReactNode;
  className: string;
  enabled?: boolean;
}) {
  return (
    // biome-ignore lint/a11y/useKeyWithClickEvents: The existing primary link provides keyboard navigation; this expands its pointer target.
    <article
      className={`${className}${enabled ? " navigable-card" : ""}`}
      onClick={enabled ? activateCard : undefined}
      onAuxClick={enabled ? activateCard : undefined}
    >
      {children}
    </article>
  );
}
