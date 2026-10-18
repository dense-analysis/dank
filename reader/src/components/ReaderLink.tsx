import type { ComponentPropsWithoutRef } from "react";

export function ReaderLink({
  onNavigate,
  href,
  preserveTextSelection = false,
  ...props
}: Omit<ComponentPropsWithoutRef<"a">, "onClick"> & {
  href: string;
  onNavigate: () => void;
  preserveTextSelection?: boolean;
}) {
  return (
    <a
      rel="noopener"
      {...props}
      href={href}
      onClick={(event) => {
        if (
          event.defaultPrevented ||
          event.button !== 0 ||
          event.metaKey ||
          event.ctrlKey ||
          event.shiftKey ||
          event.altKey ||
          (props.target && props.target !== "_self")
        )
          return;
        event.preventDefault();
        const selection = preserveTextSelection ? window.getSelection() : null;

        if (
          selection &&
          !selection.isCollapsed &&
          selection.rangeCount > 0 &&
          selection.getRangeAt(0).intersectsNode(event.currentTarget)
        )
          return;

        onNavigate();
      }}
    />
  );
}
