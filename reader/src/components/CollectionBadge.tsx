import { CircleAlert, CircleDashed } from "lucide-react";

export function CollectionBadge({ incomplete }: { incomplete: boolean }) {
  const Icon = incomplete ? CircleAlert : CircleDashed;

  return (
    <span
      className="collection-badge"
      title={
        incomplete
          ? "The article page wasn’t collected. Text, formatting or media may be missing."
          : "Full text wasn’t collected. Read it at the original source."
      }
    >
      <Icon size={12} aria-hidden="true" />
      {incomplete ? "Incomplete scrape" : "Not collected"}
    </span>
  );
}
