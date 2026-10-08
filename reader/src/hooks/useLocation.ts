import {
  useCallback,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  captureReadingPosition,
  loadReadingPosition,
  readingPositionTop,
  saveReadingPosition,
} from "../lib/readingPosition";
import { postKey } from "../lib/storage";

interface HistoryEntry {
  key: string;
  scroll: number | null;
  backTo?: string;
}

function navigationKey() {
  // getRandomValues also works on local-network HTTP hosts.
  return crypto.getRandomValues(new Uint32Array(4)).join("-");
}

function currentEntry(): HistoryEntry {
  const entry = window.history.state?.readerNavigation;
  if (entry && typeof entry.key === "string") return entry;
  const initial = { key: navigationKey(), scroll: null };
  window.history.replaceState(
    { ...window.history.state, readerNavigation: initial },
    "",
  );
  return initial;
}

export function useLocation() {
  const positions = useRef(new Map<string, number>());
  const focused = useRef(new Map<string, HTMLElement>());
  const activeKey = useRef("");
  const activeArticle = useRef<string | null>(null);
  const ready = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [positionError, setPositionError] = useState(false);

  const remember = useCallback(() => {
    clearTimeout(timer.current);
    if (!ready.current || !activeKey.current) return;
    positions.current.set(activeKey.current, window.scrollY);
    if (document.activeElement instanceof HTMLElement)
      focused.current.set(activeKey.current, document.activeElement);
    const body = document.querySelector(".article-page .article-body");
    if (activeArticle.current && body)
      setPositionError(
        !saveReadingPosition(
          activeArticle.current,
          captureReadingPosition(body),
        ),
      );
  }, []);

  const subscribe = useCallback(
    (callback: () => void) => {
      const change = () => {
        // Capture the outgoing page before Back changes the rendered content.
        remember();
        callback();
      };
      window.addEventListener("popstate", change);
      return () => window.removeEventListener("popstate", change);
    },
    [remember],
  );
  const search = useSyncExternalStore(subscribe, () => window.location.search);

  function savePosition() {
    remember();
    const entry = currentEntry();
    if (!ready.current || entry.key !== activeKey.current) return;
    window.history.replaceState(
      {
        ...window.history.state,
        readerNavigation: { ...entry, scroll: window.scrollY },
      },
      "",
    );
  }

  useLayoutEffect(() => {
    const previousRestoration = window.history.scrollRestoration;
    window.history.scrollRestoration = "manual";
    const rememberScroll = () => {
      if (!ready.current || currentEntry().key !== activeKey.current) return;
      positions.current.set(activeKey.current, window.scrollY);
      clearTimeout(timer.current);
      if (activeArticle.current) timer.current = setTimeout(remember, 400);
    };
    const rememberFocus = (event: FocusEvent) => {
      if (event.target instanceof HTMLElement)
        focused.current.set(activeKey.current, event.target);
    };
    const onHide = () => {
      if (document.visibilityState === "hidden") remember();
    };
    window.addEventListener("scroll", rememberScroll, { passive: true });
    window.addEventListener("pagehide", remember);
    document.addEventListener("visibilitychange", onHide);
    document.addEventListener("focusin", rememberFocus);
    return () => {
      clearTimeout(timer.current);
      window.history.scrollRestoration = previousRestoration;
      window.removeEventListener("scroll", rememberScroll);
      window.removeEventListener("pagehide", remember);
      document.removeEventListener("visibilitychange", onHide);
      document.removeEventListener("focusin", rememberFocus);
    };
  }, [remember]);

  useLayoutEffect(() => {
    if (window.location.search !== search) return;
    clearTimeout(timer.current);
    const entry = currentEntry();
    const params = new URLSearchParams(search);
    const id = params.get("article");
    const domain = params.get("article_source");
    const articleKey = id && domain ? postKey({ id, domain }) : null;
    activeKey.current = entry.key;
    activeArticle.current = articleKey;
    ready.current = false;
    const historyScroll =
      positions.current.get(entry.key) ?? (articleKey ? null : entry.scroll);
    const saved = articleKey ? loadReadingPosition(articleKey) : undefined;
    let observer: MutationObserver | undefined;

    const restore = () => {
      const body = document.querySelector(".article-page .article-body");
      if (articleKey && !body) {
        if (document.querySelector('.article-page [role="alert"]'))
          document
            .querySelector<HTMLElement>(".article-page .back-button")
            ?.focus({ preventScroll: true });
        return;
      }
      const top =
        historyScroll ?? (body && saved ? readingPositionTop(body, saved) : 0);
      window.scrollTo({ top, behavior: "instant" });
      ready.current = true;
      positions.current.set(entry.key, window.scrollY);
      observer?.disconnect();
      const previousFocus = focused.current.get(entry.key);
      const target =
        previousFocus?.isConnected && previousFocus.getClientRects().length > 0
          ? previousFocus
          : [
              ...document.querySelectorAll<HTMLElement>("[data-page-focus]"),
            ].find((element) => element.getClientRects().length > 0);
      target?.focus({ preventScroll: true });
    };
    restore();
    if (!ready.current) {
      window.scrollTo({ top: 0, behavior: "instant" });
      observer = new MutationObserver(restore);
      observer.observe(document.querySelector(".main-shell") ?? document.body, {
        childList: true,
        subtree: true,
      });
    }
    return () => observer?.disconnect();
  }, [search]);

  function navigate(params: URLSearchParams, replace = false, backTo?: string) {
    if (params.toString() === window.location.search.slice(1)) return;
    savePosition();
    const url = `${window.location.pathname}${params.size ? `?${params}` : ""}`;
    const entry: HistoryEntry = { key: navigationKey(), scroll: null, backTo };
    if (replace && document.activeElement instanceof HTMLElement)
      focused.current.set(entry.key, document.activeElement);
    const state = { ...window.history.state, readerNavigation: entry };
    if (replace) window.history.replaceState(state, "", url);
    else window.history.pushState(state, "", url);
    window.dispatchEvent(new PopStateEvent("popstate"));
  }

  function backTo(params: URLSearchParams) {
    if (currentEntry().backTo === params.toString()) {
      savePosition();
      window.history.back();
    } else navigate(params, true);
  }

  return {
    params: new URLSearchParams(search),
    navigate,
    backTo,
    positionError,
  };
}
