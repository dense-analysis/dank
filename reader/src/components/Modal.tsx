import { type ReactNode, useEffect, useRef } from "react";

export function Modal({
  children,
  onClose,
  className = "",
  label,
  dismissOnBackdrop = false,
}: {
  children: ReactNode;
  onClose: () => void;
  className?: string;
  label: string;
  dismissOnBackdrop?: boolean;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const backdropPressed = useRef(false);
  useEffect(() => {
    const dialog = ref.current;
    const opener = document.activeElement;
    dialog?.showModal();
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      dialog?.close();
      document.body.style.overflow = previous;
      if (opener instanceof HTMLElement && opener.isConnected) {
        opener.focus({ preventScroll: true });
      }
    };
  }, []);
  return (
    <dialog
      ref={ref}
      className={`modal ${className}`}
      aria-label={label}
      onPointerDown={(event) => {
        const { left, right, top, bottom } =
          event.currentTarget.getBoundingClientRect();
        backdropPressed.current =
          event.clientX < left ||
          event.clientX > right ||
          event.clientY < top ||
          event.clientY > bottom;
      }}
      onPointerCancel={() => {
        backdropPressed.current = false;
      }}
      onPointerUp={(event) => {
        if (
          dismissOnBackdrop &&
          backdropPressed.current &&
          event.target === event.currentTarget
        )
          onClose();

        backdropPressed.current = false;
      }}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
    >
      {children}
    </dialog>
  );
}
