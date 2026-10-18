import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, expect, it, vi } from "vitest";
import { dateLabel } from "../lib/timestamps";
import type { Post } from "../lib/types";
import { PostTimestamp } from "./PostCard";
import { PostTime } from "./PostTime";

afterEach(() => vi.useRealTimers());

it("keeps the exact time available alongside a relative label", () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(2026, 9, 18, 12));
  const date = new Date(2026, 9, 18, 11, 55).toISOString();
  const markup = renderToStaticMarkup(<PostTime date={date} />);
  expect(markup).toContain(`dateTime="${date}"`);
  expect(markup).toContain(`title="${dateLabel(date)}"`);
  expect(markup).toContain("5 minutes ago");
});

it("uses publication dates without substituting collection time for unknown dates", () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(2026, 9, 18, 12));
  const published = new Date(2026, 9, 17, 20).toISOString();
  const post = {
    created_at: new Date(2026, 9, 18, 11, 55).toISOString(),
    published_at: published,
  } as Post;
  expect(renderToStaticMarkup(<PostTimestamp post={post} />)).toContain(
    dateLabel(published),
  );
  expect(
    renderToStaticMarkup(
      <PostTimestamp post={{ ...post, published_at: null }} />,
    ),
  ).toContain("Publication date unavailable");
  expect(
    renderToStaticMarkup(
      <PostTimestamp post={{ ...post, published_at: undefined }} />,
    ),
  ).toContain("5 minutes ago");
});
