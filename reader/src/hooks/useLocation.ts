import { useSyncExternalStore } from "react";

function subscribe(callback: () => void) {
  window.addEventListener("popstate", callback);
  return () => window.removeEventListener("popstate", callback);
}

export function useLocation() {
  const search = useSyncExternalStore(subscribe, () => window.location.search);
  function navigate(params: URLSearchParams, replace = false) {
    const url = `${window.location.pathname}${params.size ? `?${params}` : ""}`;
    if (replace) window.history.replaceState(null, "", url);
    else window.history.pushState(null, "", url);
    window.dispatchEvent(new PopStateEvent("popstate"));
  }
  return { params: new URLSearchParams(search), navigate };
}
