import { filterParams } from "../lib/navigation";
import type { Filters } from "../lib/types";
import { ReaderLink } from "./ReaderLink";

export function AuthorLink({
  author,
  fallback,
  filters,
  select,
}: {
  author: string;
  fallback: string;
  filters: Filters;
  select: (author: string) => void;
}) {
  const name = author.trim();
  if (!name) return <span className="author">{fallback}</span>;
  return (
    <ReaderLink
      className="author author-link"
      href={`/reader/?${filterParams({ ...filters, author: name, authorExact: true })}`}
      onNavigate={() => select(name)}
      aria-label={`Stories by ${name}`}
      title={name}
    >
      {name}
    </ReaderLink>
  );
}
