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
  items,
  select,
  selectItem,
}: {
  id: string;
  label: string;
  itemsLabel: string;
  icon: ReactNode;
  active: boolean;
  selected: string;
  items: CollectionItem[];
  select: () => void;
  selectItem: (id: string) => void;
}) {
  const [expanded, setExpanded] = useState(true);

  return (
    <div className="collection-navigation">
      <div className="collection-nav-heading">
        <button
          type="button"
          className={`nav-item ${active && !selected ? "active" : ""}`}
          aria-current={active && !selected ? "page" : undefined}
          onClick={select}
        >
          {icon} {label}
        </button>
        {items.length > 0 && (
          <button
            type="button"
            className="icon-button collection-toggle"
            aria-label={`${expanded ? "Collapse" : "Expand"} ${itemsLabel}`}
            aria-expanded={expanded}
            aria-controls={id}
            onClick={() => setExpanded(!expanded)}
          >
            <ChevronDown size={16} />
          </button>
        )}
      </div>
      <ul
        id={id}
        className="collection-items"
        hidden={!expanded || !items.length}
      >
        {items.map((item) => {
          const current =
            active && selected.toLowerCase() === item.id.toLowerCase();

          return (
            <li key={item.id}>
              <button
                type="button"
                className={`nav-item collection-item ${current ? "active" : ""}`}
                aria-current={current ? "page" : undefined}
                onClick={() => selectItem(item.id)}
                title={item.title ?? item.label}
                aria-label={`${item.accessibleLabel ?? item.label}, ${item.count} collected posts`}
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
      </ul>
    </div>
  );
}
