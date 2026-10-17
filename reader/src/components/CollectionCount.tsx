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
}: {
  count: number;
  total?: number | null;
  estimate?: CommunityPostTotal | null;
}) {
  const reportedTotal = estimate?.count ?? total;
  const description = estimate
    ? `${count.toLocaleString()} posts collected / approximately ${estimate.count.toLocaleString()} total posts, based on Arctic Shift's archive. Archive count updated ${provenanceDate.format(new Date(estimate.updated_at))} UTC; retrieved ${provenanceDate.format(new Date(estimate.captured_at))} UTC. Archive coverage may be incomplete.`
    : reportedTotal == null
      ? `${count.toLocaleString()} posts collected`
      : `${count.toLocaleString()} posts collected / ${reportedTotal.toLocaleString()} total posts reported by X at last capture`;

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
          {reportedTotal == null ? "collected" : "posts"}
        </span>
      </span>
    </span>
  );
}
