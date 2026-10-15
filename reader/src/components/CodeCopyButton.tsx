import { useEffect, useRef, useState } from "react";

const buttonLabels = {
  idle: "Copy",
  copying: "Copying…",
  copied: "Copied",
  failed: "Copy failed",
};

export function CodeCopyButton({ text }: { text: string }) {
  const [status, setStatus] = useState<keyof typeof buttonLabels>("idle");
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
      await navigator.clipboard.writeText(text);

      // Clipboard completion can arrive after the reader has left the article.
      if (mounted.current) setStatus("copied");
    } catch {
      if (mounted.current) setStatus("failed");
    }
  }

  return (
    <>
      <span className="code-copy-status" role="status">
        {status === "copied"
          ? "Code copied."
          : status === "failed"
            ? "Select the code and copy it manually."
            : ""}
      </span>
      <button
        type="button"
        className="code-copy"
        aria-label="Copy code"
        disabled={status === "copying"}
        onClick={() => void copy()}
      >
        {buttonLabels[status]}
      </button>
    </>
  );
}
