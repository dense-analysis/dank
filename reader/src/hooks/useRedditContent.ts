import { useCallback, useMemo, useState } from "react";
import { redditVideos } from "../lib/reddit";
import {
  displayedRedditVideos,
  withoutRedditVideoLinks,
  withoutRedditVideoUrls,
} from "../lib/redditText";
import type { Post } from "../lib/types";

export function useRedditContent(post?: Post) {
  const [failed, setFailed] = useState<Set<string>>(() => new Set());
  const videos = useMemo(() => redditVideos(post?.reddit), [post?.reddit]);
  const displayed = useMemo(
    () => displayedRedditVideos(videos, failed),
    [videos, failed],
  );
  const onUnavailable = useCallback((url: string) => {
    setFailed((previous) => new Set(previous).add(url));
  }, []);
  const html = useMemo(
    () => withoutRedditVideoLinks(post?.html ?? "", displayed),
    [post?.html, displayed],
  );
  const excerpt = useMemo(
    () => withoutRedditVideoUrls(post?.excerpt ?? "", displayed),
    [post?.excerpt, displayed],
  );

  return { videos, html, excerpt, onUnavailable };
}
