import { useCallback, useEffect, useState } from "react";
import { loadReadPosts, postKey, saveReadPosts } from "../lib/storage";
import type { Post } from "../lib/types";

export function useReadPosts() {
  const [read, setRead] = useState(loadReadPosts);
  const [storageError, setStorageError] = useState(false);
  useEffect(() => {
    const sync = (event: StorageEvent) => {
      if (event.key === "dank-reader:read:v1" || event.key === null)
        setRead(loadReadPosts());
    };
    window.addEventListener("storage", sync);
    return () => window.removeEventListener("storage", sync);
  }, []);
  const markRead = useCallback(
    (post: Post) => {
      const key = postKey(post);
      if (read.includes(key)) return;
      const next = [...read, key].slice(-5000);
      setStorageError(!saveReadPosts(next));
      setRead(next);
    },
    [read],
  );
  return { read, markRead, storageError };
}
