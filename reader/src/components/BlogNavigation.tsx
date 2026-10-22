import { Link2, Newspaper, Rss } from "lucide-react";
import type { Source } from "../lib/types";
import { CollectionNavGroup } from "./CollectionNavGroup";
import { IdentityAvatar } from "./IdentityAvatar";

export function BlogNavigation({
  sources,
  externalCount,
  active,
  selected,
  select,
  selectItem,
}: {
  sources: Source[];
  externalCount: number;
  active: boolean;
  selected: string;
  select: () => void;
  selectItem: (selection: string) => void;
}) {
  return (
    <CollectionNavGroup
      id="blog-feeds"
      label="Blogs"
      itemsLabel="Blog feeds"
      icon={<Newspaper size={19} />}
      active={active}
      selected={selected}
      select={select}
      selectItem={selectItem}
      items={[
        {
          id: "external",
          label: "From external",
          count: externalCount,
          avatar: <Link2 size={15} />,
        },
      ]}
    >
      <li>
        <CollectionNavGroup
          id="blog-sources"
          label="Source Feeds"
          itemsLabel="Blog Source Feeds"
          icon={<Rss size={16} />}
          active={active && selected !== "external"}
          selected={selected === "sources" ? "" : selected || "all"}
          select={() => selectItem("sources")}
          selectItem={selectItem}
          items={sources.map((source) => ({
            id: `source:${source.domain}`,
            label: source.name,
            title: source.domain,
            count: source.count,
            avatar: (
              <IdentityAvatar
                name={source.name}
                iconUrl={source.icon_url}
                kind="source"
              />
            ),
          }))}
        />
      </li>
    </CollectionNavGroup>
  );
}
