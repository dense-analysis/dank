import {
  ChartNoAxesColumnIncreasing,
  Heart,
  MessageCircle,
  Repeat2,
} from "lucide-react";
import { formatCount } from "../lib/counts";
import type { XPost } from "../lib/types";
import { ReaderLink } from "./ReaderLink";

export function XEngagement({
  post,
  navigation,
}: {
  post?: XPost | null;
  navigation?: { href: string; open: () => void };
}) {
  const metrics = [
    { label: "Comments", count: post?.reply_count, Icon: MessageCircle },
    { label: "Reposts", count: post?.repost_count, Icon: Repeat2 },
    { label: "Likes", count: post?.like_count, Icon: Heart },
    {
      label: "Views",
      count: post?.view_count,
      Icon: ChartNoAxesColumnIncreasing,
    },
  ];

  return (
    <fieldset className="x-engagement" aria-label="Engagement when collected">
      {metrics.map(({ label, count, Icon }, index) => {
        const title =
          count == null
            ? `${label} not captured`
            : `${count.toLocaleString()} ${label.toLowerCase()} reported when collected`;
        const value = count == null ? "—" : formatCount(count);
        const contents = (
          <>
            <Icon size={16} aria-hidden="true" />
            <span className="visually-hidden">{label}: </span>
            <span>{value}</span>
          </>
        );

        return index === 0 && navigation ? (
          <ReaderLink
            key={label}
            className="x-metric"
            title={title}
            href={navigation.href}
            onNavigate={navigation.open}
            aria-label={
              count == null
                ? "Comments not captured"
                : `${count.toLocaleString()} comments reported when collected`
            }
          >
            {contents}
          </ReaderLink>
        ) : (
          <span key={label} className="x-metric" title={title}>
            {contents}
          </span>
        );
      })}
    </fieldset>
  );
}
