import { filterParams } from "../lib/navigation";
import { defaultFilters } from "../lib/storage";
import { ReaderLink } from "./ReaderLink";

export function sourceName(domain: string) {
  return domain.replace(/^www\./, "").replace(/\.(com|org|net|co\.uk|io)$/, "");
}

export function SourceLink({
  domain,
  select,
}: {
  domain: string;
  select: (domain: string) => void;
}) {
  return (
    <ReaderLink
      href={`/reader/?${filterParams({ ...defaultFilters, domains: [domain] })}`}
      onNavigate={() => select(domain)}
      className="source-link"
      title={domain}
      aria-label={`Stories from ${sourceName(domain)}`}
    >
      {sourceName(domain)}
    </ReaderLink>
  );
}
