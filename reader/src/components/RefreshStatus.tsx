import {
  Check,
  ChevronDown,
  ChevronUp,
  CircleAlert,
  RefreshCw,
  X,
} from "lucide-react";
import { useState } from "react";
import type { useRefresh } from "../hooks/useRefresh";
import { refreshActive } from "../lib/refresh";

const statusLabels = {
  queued: "Waiting to refresh",
  running: "Refreshing",
  processing: "Preparing updates",
  completed: "Refresh complete",
  partial: "Refresh partly complete",
  failed: "Refresh failed",
  interrupted: "Refresh interrupted",
};

export function RefreshStatus({
  refresh,
}: {
  refresh: ReturnType<typeof useRefresh>;
}) {
  const [collapsed, setCollapsed] = useState(
    () => window.matchMedia("(max-width: 760px)").matches,
  );
  const active = refresh.visible.filter(refreshActive);
  const waiting = active.filter((job) => job.status === "queued").length;
  const running = active.length - waiting;
  const activeSummary = [
    running > 0 ? `${running} refreshing` : null,
    waiting > 0 ? `${waiting} waiting` : null,
  ]
    .filter(Boolean)
    .join(" · ");
  const ready = refresh.visible.filter(
    (job) =>
      !refreshActive(job) &&
      (job.status === "completed" ||
        job.status === "partial" ||
        job.posts_saved > 0),
  );
  const shown = [...refresh.visible].sort(
    (a, b) => Number(refreshActive(b)) - Number(refreshActive(a)),
  );
  const hasStatus = refresh.visible.length > 0 || !!refresh.startError;

  return (
    <aside
      className="refresh-status"
      aria-label="Background refresh"
      hidden={!hasStatus}
    >
      <div className="refresh-status-heading">
        <span aria-live="polite" aria-atomic="true">
          {active.length > 0
            ? activeSummary
            : refresh.startError
              ? "Refresh could not start"
              : collapsed && ready.length > 1
                ? `${ready.length} refreshes ready`
                : collapsed && refresh.visible[0]
                  ? statusLabels[refresh.visible[0].status]
                  : "Background refresh"}
        </span>
        {collapsed && ready.length === 1 && (
          <button
            type="button"
            className="text-button refresh-result-link"
            disabled={refresh.showingUpdates}
            aria-label={`Show updates for ${ready[0].label}`}
            title={`Open ${ready[0].label}`}
            onClick={() => void refresh.showUpdates(ready[0])}
          >
            {refresh.showingUpdates ? "Loading…" : `Show ${ready[0].label}`}
          </button>
        )}
        <button
          type="button"
          className="icon-button"
          aria-label={
            collapsed ? "Show refresh details" : "Hide refresh details"
          }
          aria-expanded={!collapsed}
          onClick={() => setCollapsed(!collapsed)}
        >
          {collapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
        </button>
      </div>
      {!collapsed && (
        <>
          {refresh.startError && (
            <div className="refresh-notice" role="alert">
              <CircleAlert size={16} />
              <p>{refresh.startError}</p>
              <button
                type="button"
                className="icon-button"
                aria-label="Dismiss refresh error"
                onClick={refresh.clearError}
              >
                <X size={15} />
              </button>
            </div>
          )}
          <div aria-live="polite" aria-atomic="false">
            {shown.map((job) => (
              <div
                key={job.id}
                className="refresh-notice"
                data-refresh-status={job.status}
              >
                {refreshActive(job) ? (
                  <RefreshCw
                    size={16}
                    className={
                      job.status === "queued" ? undefined : "refresh-spinning"
                    }
                  />
                ) : job.status === "completed" ? (
                  <Check size={16} />
                ) : (
                  <CircleAlert size={16} />
                )}
                <div className="refresh-notice-copy">
                  <strong>{statusLabels[job.status]}</strong>
                  <span className="refresh-job-label">{job.label}</span>
                  {job.message && <p>{job.message}</p>}
                  {ready.includes(job) && (
                    <button
                      type="button"
                      className="text-button refresh-job-updates"
                      aria-label={`Show updates for ${job.label}`}
                      disabled={refresh.showingUpdates}
                      onClick={() => void refresh.showUpdates(job)}
                    >
                      {refresh.showingJob === job.id
                        ? "Loading updates…"
                        : "Show updates"}
                    </button>
                  )}
                </div>
                {!refreshActive(job) && (
                  <button
                    type="button"
                    className="icon-button"
                    aria-label={`Dismiss refresh: ${job.label}`}
                    onClick={() => refresh.dismiss([job.id])}
                  >
                    <X size={15} />
                  </button>
                )}
              </div>
            ))}
          </div>
          {refresh.statusError && refresh.visible.length > 0 && (
            <p className="refresh-status-error" role="status">
              Progress is temporarily unavailable. Collection may still be
              running.{" "}
              <button
                type="button"
                className="text-button"
                onClick={refresh.retryStatus}
              >
                Check again
              </button>
            </p>
          )}
          {refresh.updatesError && (
            <p className="refresh-status-error" role="alert">
              {refresh.updatesError}
            </p>
          )}
        </>
      )}
    </aside>
  );
}
