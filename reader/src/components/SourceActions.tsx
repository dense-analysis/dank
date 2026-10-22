import { Check, Link, TriangleAlert } from "lucide-react";
import { type ComponentProps, useEffect, useRef, useState } from "react";
import { safeUrl } from "../lib/urls";

function CopySourceLink({ url }: { url: string }) {
  const [status, setStatus] = useState<
    "idle" | "copying" | "copied" | "failed"
  >("idle");
  const mounted = useRef(false);

  useEffect(() => {
    mounted.current = true;

    return () => {
      mounted.current = false;
    };
  }, []);

  useEffect(() => {
    if (status !== "copied" && status !== "failed") return;

    const timer = setTimeout(() => setStatus("idle"), 3000);

    return () => clearTimeout(timer);
  }, [status]);

  async function copy() {
    setStatus("copying");

    try {
      await navigator.clipboard.writeText(url);

      if (mounted.current) setStatus("copied");
    } catch {
      if (mounted.current) setStatus("failed");
    }
  }

  const Icon =
    status === "copied" ? Check : status === "failed" ? TriangleAlert : Link;

  return (
    <span className="source-copy-control">
      <button
        type="button"
        className="icon-button source-copy-button"
        aria-label="Copy source link"
        title={`Copy source link: ${url}`}
        disabled={status === "copying"}
        onClick={(event) => {
          event.stopPropagation();
          void copy();
        }}
      >
        <Icon size={16} aria-hidden="true" />
      </button>
      <span className="source-copy-status" role="status">
        {status === "copied"
          ? "Link copied"
          : status === "failed"
            ? "Copy failed. Try again or copy the source link manually."
            : ""}
      </span>
    </span>
  );
}

/** Keep sharing tied to the exact destination of the neighboring source action. */
export function SourceActions({
  href,
  children,
  ...props
}: ComponentProps<"a">) {
  const url = href ? safeUrl(href) : undefined;

  return (
    <span className="source-link-actions">
      {url && <CopySourceLink key={url} url={url} />}
      <a {...props} href={url}>
        {children}
      </a>
    </span>
  );
}
