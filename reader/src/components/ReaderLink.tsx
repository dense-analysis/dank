import type { ComponentPropsWithoutRef } from "react";

export function ReaderLink({
  onNavigate,
  href,
  ...props
}: Omit<ComponentPropsWithoutRef<"a">, "onClick"> & {
  href: string;
  onNavigate: () => void;
}) {
  return (
    <a
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
        onNavigate();
      }}
    />
  );
}
