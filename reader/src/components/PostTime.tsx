import { dateLabel, postDateLabel } from "../lib/timestamps";

export function PostTime({
  date,
  className,
  title,
}: {
  date: string;
  className?: string;
  title?: string;
}) {
  const exact = dateLabel(date);

  return (
    <time
      className={className}
      dateTime={date}
      title={title ? `${title}: ${exact}` : exact}
    >
      {postDateLabel(date)}
    </time>
  );
}
