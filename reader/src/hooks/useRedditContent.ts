import { useCallback, useMemo, useState } from "react";
import { redditVideos } from "../lib/reddit";
import {
  displayedRedditVideos,
  withoutRedditVideoLinks,
  withoutRedditVideoUrls,
} from "../lib/redditText";
import type { Post } from "../lib/types";
import { useReaderPreferences } from "./useReaderPreferences";

export function useRedditContent(
  post?: Post,
  { preserveVideoLinks = false }: { preserveVideoLinks?: boolean } = {},
) {
  const { preferences } = useReaderPreferences();
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
    () =>
      preserveVideoLinks
        ? (post?.html ?? "")
        : withoutRedditVideoLinks(post?.html ?? "", displayed),
    [post?.html, displayed, preserveVideoLinks],
  );
  const excerpt = useMemo(
    () =>
      preferences.hideRawVideoLinks
        ? withoutRedditVideoUrls(post?.excerpt ?? "", displayed)
        : (post?.excerpt ?? ""),
    [post?.excerpt, displayed, preferences.hideRawVideoLinks],
  );

  return { videos, html, excerpt, onUnavailable, displayed };
}
