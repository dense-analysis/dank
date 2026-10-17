import { formatCount } from "../lib/counts";

export function Count({ value }: { value: number | null | undefined }) {
  return (
    <span title={value == null ? undefined : value.toLocaleString("en-US")}>
      {formatCount(value)}
    </span>
  );
}
