import { fmtUtc, isOffline } from "@/lib/format";

/**
 * health.json is rewritten every 5 minutes while a session runs, so its generated_at is
 * the pipeline heartbeat. Older than 15 minutes means no session is running.
 */
export function OfflineBanner({ heartbeat, now }: { heartbeat: string | null; now: number }) {
  if (heartbeat && !isOffline(heartbeat, now)) return null;
  return (
    <div
      role="status"
      style={{ background: "var(--banner-bg)", color: "var(--banner-ink)", fontSize: "0.9rem" }}
    >
      <div className="main" style={{ padding: "10px 16px" }}>
        {heartbeat ? (
          <>
            <strong>Pipeline offline since {fmtUtc(heartbeat)}</strong>, showing last session.
          </>
        ) : (
          <strong>Pipeline status unknown, showing the last data available.</strong>
        )}
      </div>
    </div>
  );
}
