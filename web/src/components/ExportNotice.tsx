import { fmtUtc } from "@/lib/format";
import type { ExportedSnapshot, SnapshotResult } from "@/lib/types";

/**
 * Says so when the latest export of this snapshot failed. Exports are independent, so
 * the page then shows the last good copy (or nothing, if there never was one).
 */
export function ExportNotice({
  meta,
  which,
  shownAt,
}: {
  meta: SnapshotResult<"meta">;
  which: ExportedSnapshot;
  shownAt: string | null;
}) {
  if (!meta.ok || meta.data.exports?.[which].status !== "failed") return null;
  return (
    <div
      role="status"
      className="card"
      style={{ borderColor: "var(--serious)", marginBottom: 16, fontSize: "0.9rem" }}
    >
      <strong>The latest export failed</strong> ({fmtUtc(meta.data.generated_at)}).{" "}
      {shownAt
        ? `Showing the last good data, from ${fmtUtc(shownAt)}.`
        : "There is no earlier data to show yet."}
    </div>
  );
}
