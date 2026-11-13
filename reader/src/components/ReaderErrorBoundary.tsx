import { CircleAlert, RefreshCw } from "lucide-react";
import { Component, type ReactNode } from "react";

/**
 * Shows a way out when rendering fails, rather than a blank page.
 *
 * API responses are typed from the server's schema and not checked again, so
 * a page left open across a deploy can meet data its code doesn't expect.
 * Reloading fetches the reader that matches the server.
 */
export class ReaderErrorBoundary extends Component<
  { children: ReactNode },
  { failed: boolean }
> {
  state = { failed: false };

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true };
  }

  componentDidCatch(error: unknown): void {
    console.error("The reader could not render.", error);
  }

  render(): ReactNode {
    if (!this.state.failed) return this.props.children;

    return (
      <div className="empty-state" role="alert">
        <CircleAlert size={30} strokeWidth={1.3} />
        <h2>The reader hit a problem</h2>
        <p>Reload to get the current version of the reader and its data.</p>
        <button
          type="button"
          className="secondary-button"
          onClick={() => window.location.reload()}
        >
          <RefreshCw size={15} /> Reload
        </button>
      </div>
    );
  }
}
