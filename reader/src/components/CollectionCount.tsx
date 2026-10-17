const compactCount = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
  roundingMode: "trunc",
});

export function CollectionCount({
  count,
  total,
}: {
  count: number;
  total?: number | null;
}) {
  const description =
    total == null
      ? `${count.toLocaleString()} posts collected`
      : `${count.toLocaleString()} posts collected / ${total.toLocaleString()} total posts reported by X at last capture`;

  return (
    <span className="collection-count-pill" title={description}>
      <span className="visually-hidden">{description}</span>
      <span className="collection-count-values" aria-hidden="true">
        <strong>{compactCount.format(count)}</strong>
        {total != null && (
          <>
            <span className="collection-count-divider">/</span>
            <span>{compactCount.format(total)}</span>
          </>
        )}
        <span className="collection-count-label">
          {total == null ? "collected" : "posts"}
        </span>
      </span>
    </span>
  );
}
