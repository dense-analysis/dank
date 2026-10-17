import { useLayoutEffect, useRef, useState } from "react";

// A short transition keeps disclosure responsive, including for article-length text.
const DISCLOSURE_DURATION_MS = 240;

export function useDisclosureHeight(expanded: boolean) {
  const viewport = useRef<HTMLDivElement>(null);
  const before = useRef<number | undefined>(undefined);
  const active = useRef<Animation | null>(null);
  const [retainExpanded, setRetainExpanded] = useState(false);
  const [expanding, setExpanding] = useState(false);

  function prepare() {
    // Capture the interpolated height too, so a quick reversal starts where it is.
    before.current = viewport.current?.getBoundingClientRect().height;
    setRetainExpanded(false);
    setExpanding(!expanded);
  }

  useLayoutEffect(() => {
    const element = viewport.current;
    const from = before.current;
    before.current = undefined;

    if (!element || from === undefined) return;

    // Flush the new layout while scroll anchoring is disabled, even without motion.
    const to = element.getBoundingClientRect().height;
    if (
      window.matchMedia("(prefers-reduced-motion: reduce)").matches ||
      typeof element.animate !== "function" ||
      Math.abs(from - to) < 1
    ) {
      const frame = requestAnimationFrame(() => setExpanding(false));

      return () => cancelAnimationFrame(frame);
    }

    // Measure the new content first, then retain the expanded text during shrink.
    // React flushes this layout update before paint; media lives outside this box.
    if (!expanded) setRetainExpanded(true);

    const animation = element.animate(
      [
        { height: `${from}px`, overflow: "clip" },
        { height: `${to}px`, overflow: "clip" },
      ],
      { duration: DISCLOSURE_DURATION_MS, easing: "ease-in-out", fill: "both" },
    );
    active.current = animation;
    animation.onfinish = () => {
      if (expanded) {
        animation.cancel();
        active.current = null;
        setExpanding(false);
      } else {
        setRetainExpanded(false);
      }
    };

    return () => {
      animation.onfinish = null;
      animation.cancel();
      if (active.current === animation) active.current = null;
    };
  }, [expanded]);

  useLayoutEffect(() => {
    // Release the fixed height only after React restores the collapsed content.
    if (!retainExpanded && active.current?.playState === "finished") {
      active.current.cancel();
      active.current = null;
    }
  }, [retainExpanded]);

  return {
    viewport,
    displayExpanded: expanded || retainExpanded,
    expanding,
    prepare,
  };
}
