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
  const ready = refresh.visible.some(
    (job) =>
      !refreshActive(job) &&
      (job.status === "completed" ||
        job.status === "partial" ||
        job.posts_saved > 0),
  );
  const shown = [...refresh.visible]
    .sort((a, b) => Number(refreshActive(b)) - Number(refreshActive(a)))
    .slice(0, 3);
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
            ? `${active.length} refresh${active.length > 1 ? "es" : ""} in progress`
            : refresh.startError
              ? "Refresh could not start"
              : collapsed && refresh.visible[0]
                ? statusLabels[refresh.visible[0].status]
                : "Background refresh"}
        </span>
        {collapsed && ready && (
          <button
            type="button"
            className="text-button"
            disabled={refresh.showingUpdates}
            onClick={() => void refresh.showUpdates()}
          >
            {refresh.showingUpdates ? "Loading…" : "Show updates"}
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
          {refresh.visible.length > shown.length && (
            <p className="refresh-more">
              {refresh.visible.length - shown.length} more refresh results
            </p>
          )}
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
          {ready && (
            <div className="refresh-status-actions">
              <button
                type="button"
                className="primary-button"
                disabled={refresh.showingUpdates}
                onClick={() => void refresh.showUpdates()}
              >
                {refresh.showingUpdates ? "Loading updates…" : "Show updates"}
              </button>
            </div>
          )}
        </>
      )}
    </aside>
  );
}
