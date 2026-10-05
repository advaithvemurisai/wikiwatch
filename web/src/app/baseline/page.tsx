import type { Metadata } from "next";

import { BotShareChart } from "@/components/BotShareChart";
import { EditsPerMinuteChart } from "@/components/EditsPerMinuteChart";
import { PageActivityChart } from "@/components/PageActivityChart";
import { SnapshotError } from "@/components/SnapshotError";
import { fmtUtc } from "@/lib/format";
import { minuteSeries } from "@/lib/series";
import { loadSnapshot } from "@/lib/snapshots";

export const metadata: Metadata = { title: "Baseline" };
export const revalidate = 300;

export default async function BaselinePage() {
  const snapshot = await loadSnapshot("baseline");
  return (
    <>
      <div className="page-header">
        <h1>Baseline</h1>
        <p>
          Platform-wide activity, so a spike on a watched page can be told apart from a busy day on
          Wikipedia.
        </p>
      </div>
      {!snapshot.ok ? (
        <SnapshotError what="Baseline" problem={snapshot.problem} />
      ) : (
        <>
          <section className="section" style={{ marginTop: 0 }} aria-labelledby="epm">
            <h2 id="epm">Platform edits per minute</h2>
            <p className="section-note">
              All wikis, last 6 hours of the latest session. Real-time windows close at the
              2-minute watermark; the final count includes events that arrived later. Where the counts
              match, the lines overlap; breaks are minutes when no session was running.
            </p>
            <div className="card">
              {snapshot.data.edits_per_min.length === 0 ? (
                <p className="empty">No minutes recorded in the last 6 hours.</p>
              ) : (
                <EditsPerMinuteChart points={minuteSeries(snapshot.data.edits_per_min)} />
              )}
            </div>
          </section>

          <section className="section" aria-labelledby="bots">
            <h2 id="bots">Bot share by hour</h2>
            <p className="section-note">Share of all edits made by bots, recorded hours of the last 7 days.</p>
            <BotShareChart rows={snapshot.data.bot_share_hourly} />
          </section>

          <section className="section" aria-labelledby="pages">
            <h2 id="pages">Watched page against its own average</h2>
            <p className="section-note">
              Sessions are not continuous, so the average uses only hours when the pipeline was
              running (last 7 days).
            </p>
            <PageActivityChart snapshot={snapshot.data} />
          </section>

          <p className="small muted" style={{ marginTop: 24 }}>
            Exported {fmtUtc(snapshot.data.generated_at)}.
          </p>
        </>
      )}
    </>
  );
}
