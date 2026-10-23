import { Search, X } from "lucide-react";
import { useEffect, useRef } from "react";

export function SearchField({
  value,
  onChange,
  onClear,
  onSubmit,
  label,
  placeholder = label,
  submitLabel = "Search",
  className = "",
}: {
  value: string;
  onChange: (value: string) => void;
  onClear: () => void;
  onSubmit?: () => void;
  label: string;
  placeholder?: string;
  submitLabel?: string;
  className?: string;
}) {
  const input = useRef<HTMLInputElement>(null);
  const restoreFocus = useRef(false);

  useEffect(() => {
    if (!value && restoreFocus.current) {
      // URL updates restore page focus before this effect runs.
      input.current?.focus();
      restoreFocus.current = false;
    }
  }, [value]);

  useEffect(() => {
    function focusSearch(event: KeyboardEvent) {
      if (
        event.key !== "/" ||
        event.ctrlKey ||
        event.metaKey ||
        event.altKey ||
        !input.current?.getClientRects().length ||
        (event.target instanceof HTMLElement &&
          (event.target.closest("input, textarea, select, dialog") ||
            event.target.isContentEditable))
      ) {
        return;
      }

      event.preventDefault();
      input.current.focus();
    }

    window.addEventListener("keydown", focusSearch);

    return () => window.removeEventListener("keydown", focusSearch);
  }, []);

  return (
    <form
      className={`search-bar ${className}`}
      aria-label={label}
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit?.();
      }}
    >
      <Search size={18} aria-hidden="true" />
      <input
        ref={input}
        type="search"
        enterKeyHint="search"
        autoCapitalize="none"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        aria-label={label}
      />
      {value && (
        <button
          className="icon-button"
          type="button"
          aria-label="Clear search"
          onClick={() => {
            restoreFocus.current = true;
            onClear();
          }}
        >
          <X size={15} aria-hidden="true" />
        </button>
      )}
      <kbd>/</kbd>
      {onSubmit && (
        <button
          type="submit"
          className="search-submit"
          aria-label={submitLabel}
        >
          Search
        </button>
      )}
    </form>
  );
}
