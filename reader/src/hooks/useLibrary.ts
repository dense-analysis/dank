import { useState } from "react";
import { type Library, loadLibrary, saveLibrary } from "../lib/storage";

export function useLibrary() {
  const [library, setLibrary] = useState(loadLibrary);
  const [storageError, setStorageError] = useState(false);
  function update(change: (current: Library) => Library) {
    const next = change(library);
    setStorageError(!saveLibrary(next));
    setLibrary(next);
  }
  return { library, update, storageError };
}
