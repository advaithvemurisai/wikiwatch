import type { SnapshotProblem } from "@/lib/types";

const MESSAGES: Record<SnapshotProblem, string> = {
  unavailable: "This data is not available right now. Please try again in a few minutes.",
  unreadable: "This data could not be read. It will be replaced by the next export.",
  version:
    "This data was exported in a format this version of the site does not understand yet. " +
    "It will show again once the site and the export agree.",
  invalid: "This data did not pass its checks, so it is not shown. The next export replaces it.",
};

/** Friendly, detail-free message for a snapshot that cannot be shown. */
export function SnapshotError({ what, problem }: { what: string; problem: SnapshotProblem }) {
  return (
    <div className="card" role="status" data-problem={problem}>
      <h2>{what} unavailable</h2>
      <p className="secondary" style={{ margin: "6px 0 0" }}>
        {MESSAGES[problem]}
      </p>
    </div>
  );
}
