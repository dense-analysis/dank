import { ChevronDown } from "lucide-react";
import { type ReactNode, useState } from "react";

interface CollectionItem {
  id: string;
  label: string;
  title?: string;
  accessibleLabel?: string;
  count: number;
  avatar: ReactNode;
}

export function CollectionNavGroup({
  id,
  label,
  itemsLabel,
  icon,
  active,
  selected,
  items = [],
  children,
  select,
  selectItem,
  countLabel = "collected posts",
  caseSensitive = false,
}: {
  id: string;
  label: string;
  itemsLabel: string;
  icon: ReactNode;
  active: boolean;
  selected: string;
  items?: CollectionItem[];
  children?: ReactNode;
  select?: () => void;
  selectItem: (id: string) => void;
  countLabel?: string;
  caseSensitive?: boolean;
}) {
  const storageKey = `dank-reader:collection-expanded:v1:${id}`;
  const [expanded, setExpanded] = useState(() => {
    try {
      return globalThis.localStorage.getItem(storageKey) !== "false";
    } catch {
      return true;
    }
  });

  function toggleExpanded() {
    const next = !expanded;
    setExpanded(next);

    try {
      globalThis.localStorage.setItem(storageKey, String(next));
    } catch {
      // Keep the disclosure usable when browser storage is unavailable.
    }
  }

  return (
    <div className="collection-navigation">
      <div className="collection-nav-heading">
        <button
          type="button"
          className={`nav-item ${active && !selected ? "active" : ""}`}
          aria-current={active && !selected ? "page" : undefined}
          onClick={select ?? toggleExpanded}
          aria-expanded={select ? undefined : expanded}
          aria-controls={select ? undefined : id}
        >
          {icon} {label}
        </button>
        {(items.length > 0 || children) && (
          <button
            type="button"
            className="icon-button collection-toggle"
            aria-label={`${expanded ? "Collapse" : "Expand"} ${itemsLabel}`}
            aria-expanded={expanded}
            aria-controls={id}
            onClick={toggleExpanded}
          >
            <ChevronDown size={16} />
          </button>
        )}
      </div>
      <ul
        id={id}
        className="collection-items"
        hidden={!expanded || (!items.length && !children)}
      >
        {items.map((item) => {
          const current =
            active &&
            (caseSensitive
              ? selected === item.id
              : selected.toLowerCase() === item.id.toLowerCase());

          return (
            <li key={item.id}>
              <button
                type="button"
                className={`nav-item collection-item ${current ? "active" : ""}`}
                aria-current={current ? "page" : undefined}
                onClick={() => selectItem(item.id)}
                title={item.title ?? item.label}
                aria-label={`${item.accessibleLabel ?? item.label}, ${item.count} ${countLabel}`}
              >
                {item.avatar}
                <span className="collection-name">{item.label}</span>
                <span className="collection-count" aria-hidden="true">
                  {item.count.toLocaleString()}
                </span>
              </button>
            </li>
          );
        })}
        {children}
      </ul>
    </div>
  );
}
