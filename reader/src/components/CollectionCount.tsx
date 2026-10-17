import type { CommunityPostTotal } from "../lib/types";

const compactCount = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
  roundingMode: "trunc",
});

const provenanceDate = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "short",
  timeZone: "UTC",
});

export function CollectionCount({
  count,
  total,
  estimate,
  unit = "posts",
  source = "X",
  reportedText,
}: {
  count: number;
  total?: number | null;
  estimate?: CommunityPostTotal | null;
  unit?: "posts" | "videos";
  source?: string;
  reportedText?: string | null;
}) {
  const reportedTotal = estimate?.count ?? total;
  const description = estimate
    ? `${count.toLocaleString()} posts collected / approximately ${estimate.count.toLocaleString()} total posts, based on Arctic Shift's archive. Archive count updated ${provenanceDate.format(new Date(estimate.updated_at))} UTC; retrieved ${provenanceDate.format(new Date(estimate.captured_at))} UTC. Archive coverage may be incomplete.`
    : reportedTotal == null
      ? `${count.toLocaleString()} ${unit} collected`
      : `${count.toLocaleString()} ${unit} collected / ${reportedText || `${reportedTotal.toLocaleString()} total ${unit}`} reported by ${source} at last capture`;

  return (
    <span className="collection-count-pill" title={description}>
      <span className="visually-hidden">{description}</span>
      <span className="collection-count-values" aria-hidden="true">
        <strong>{compactCount.format(count)}</strong>
        {reportedTotal != null && (
          <>
            <span className="collection-count-divider">/</span>
            <span>
              {estimate && "≈"}
              {compactCount.format(reportedTotal)}
            </span>
          </>
        )}
        <span className="collection-count-label">
          {reportedTotal == null ? "collected" : unit}
        </span>
      </span>
    </span>
  );
}
