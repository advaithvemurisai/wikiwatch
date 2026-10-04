import { AlertsView } from "@/components/AlertsView";
import { SnapshotError } from "@/components/SnapshotError";
import { fmtUtc } from "@/lib/format";
import { loadSnapshot, renderTime } from "@/lib/snapshots";

export const revalidate = 300;

export default async function AlertsPage() {
  const snapshot = await loadSnapshot("alerts");
  return (
    <>
      <div className="page-header">
        <h1>Alerts</h1>
        <p>
          Risky edits to watched brand, product and competitor pages on English Wikipedia. Editors
          are shown by type only.
        </p>
      </div>
      {snapshot.ok ? (
        <>
          <AlertsView snapshot={snapshot.data} now={renderTime()} />
          <p className="small muted" style={{ marginTop: 24 }}>
            Exported {fmtUtc(snapshot.data.generated_at)}.
          </p>
        </>
      ) : (
        <SnapshotError what="Alerts" problem={snapshot.problem} />
      )}
    </>
  );
}
