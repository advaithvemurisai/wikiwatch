import type { Metadata } from "next";

import { Funnel } from "@/components/Funnel";
import { SnapshotError } from "@/components/SnapshotError";
import { Status } from "@/components/Status";
import { fmtDecimal, fmtInt, fmtMs, fmtSeconds, fmtUtc, timeAgo } from "@/lib/format";
import { loadSnapshot, renderTime } from "@/lib/snapshots";
import type { HealthSnapshot, MetaSnapshot } from "@/lib/types";

export const metadata: Metadata = { title: "Pipeline health" };
export const revalidate = 300;

const FRESH_MS = 10 * 60 * 1000;
const DETECTION_TARGET_S = 120; // docs/v1.md: alert within 2 minutes

const LAYERS = [
  { key: "bronze", label: "Bronze", what: "last event ingested" },
  { key: "silver", label: "Silver", what: "newest event, deduplicated" },
  { key: "gold", label: "Gold", what: "newest real-time window" },
] as const;

function Freshness({ health, now }: { health: HealthSnapshot; now: number }) {
  return (
    <div className="tiles">
      {LAYERS.map(({ key, label, what }) => {
        const at = health.freshness[key];
        const fresh = at !== null && now - new Date(at).getTime() <= FRESH_MS;
        return (
          <div className="tile" key={key}>
            <div className="tile-label">
              {label} <span className="muted">· {what}</span>
            </div>
            <div className="tile-value">{at ? timeAgo(at, now) : "No data"}</div>
            <div className="tile-detail">
              {at ? fmtUtc(at) : "nothing in the last 12 hours"}
            </div>
            <div className="small" style={{ marginTop: 6 }}>
              {fresh ? <Status level="good">Fresh</Status> : <Status level="serious">Stale</Status>}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function Detection({ detection }: { detection: HealthSnapshot["detection_seconds"] }) {
  if (detection.samples === 0 || detection.p50 === null || detection.p95 === null) {
    return (
      <p className="card secondary" style={{ margin: 0 }}>
        No on-time alerts in the last 7 days to measure. Alerts on events that arrived late (for
        example after a resume) are left out, because they measure the outage, not the pipeline.
      </p>
    );
  }
  const within = detection.p95 <= DETECTION_TARGET_S;
  return (
    <div className="tiles">
      <div className="tile">
        <div className="tile-label">Median (p50)</div>
        <div className="tile-value">{fmtSeconds(detection.p50)}</div>
        <div className="tile-detail">from the edit to the alert row</div>
      </div>
      <div className="tile">
        <div className="tile-label">p95</div>
        <div className="tile-value">{fmtSeconds(detection.p95)}</div>
        <div className="small" style={{ marginTop: 6 }}>
          {within ? (
            <Status level="good">Within the 2-minute target</Status>
          ) : (
            <Status level="serious">Above the 2-minute target</Status>
          )}
        </div>
      </div>
      <div className="tile">
        <div className="tile-label">Alerts measured</div>
        <div className="tile-value">{fmtInt(detection.samples)}</div>
        <div className="tile-detail">on-time alerts, last 7 days</div>
      </div>
    </div>
  );
}

function Streaming({ rows }: { rows: HealthSnapshot["streaming"] }) {
  if (rows.length === 0) {
    return <p className="card secondary" style={{ margin: 0 }}>No micro-batches in the last hour of the session.</p>;
  }
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th scope="col">Streaming query</th>
            <th scope="col" className="num">Batches</th>
            <th scope="col" className="num">Input rate</th>
            <th scope="col" className="num">Batch duration p50</th>
            <th scope="col" className="num">Batch duration max</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.query}>
              <td>
                <code>{r.query}</code>
              </td>
              <td className="num">{fmtInt(r.batches)}</td>
              <td className="num">
                {r.input_rows_per_second === null ? "—" : `${fmtDecimal(r.input_rows_per_second)} rows/s`}
              </td>
              <td className="num">{r.batch_duration_ms_p50 === null ? "—" : fmtMs(r.batch_duration_ms_p50)}</td>
              <td className="num">{r.batch_duration_ms_max === null ? "—" : fmtMs(r.batch_duration_ms_max)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Lag({ rows }: { rows: HealthSnapshot["lag"] }) {
  if (rows.length === 0) {
    return <p className="card secondary" style={{ margin: 0 }}>No partitions reported (no session running).</p>;
  }
  const known = rows.filter((r) => r.lag !== null);
  const total = known.reduce((sum, r) => sum + (r.lag ?? 0), 0);
  return (
    <>
      <p className="section-note">
        Total lag{" "}
        <strong>{known.length ? `${fmtInt(total)} events` : "unknown"}</strong> across {rows.length}{" "}
        partitions of <code>wiki_edits</code>.
      </p>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Partition</th>
              <th scope="col" className="num">Latest offset</th>
              <th scope="col" className="num">Processed by Silver</th>
              <th scope="col" className="num">Lag</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.partition}>
                <td>{r.partition}</td>
                <td className="num">{fmtInt(r.latest_offset)}</td>
                <td className="num">{r.processed_offset === null ? "—" : fmtInt(r.processed_offset)}</td>
                <td className="num">{r.lag === null ? "unknown" : fmtInt(r.lag)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function DbtRun({ meta, now }: { meta: MetaSnapshot; now: number }) {
  const { dbt } = meta;
  const status =
    dbt.status === "success" ? (
      <Status level="good">Succeeded</Status>
    ) : dbt.status === "failed" ? (
      <Status level="critical">Failed</Status>
    ) : (
      <Status level="neutral">Unknown</Status>
    );
  return (
    <div className="tiles">
      <div className="tile">
        <div className="tile-label">Status</div>
        <div className="tile-value" style={{ fontSize: "1.25rem" }}>
          {status}
        </div>
        <div className="tile-detail">
          {dbt.finished_at ? `finished ${timeAgo(dbt.finished_at, now)}, ${fmtUtc(dbt.finished_at)}` : "no run recorded"}
        </div>
      </div>
      <div className="tile">
        <div className="tile-label">Models built</div>
        <div className="tile-value">{fmtInt(dbt.models_ok)}</div>
      </div>
      <div className="tile">
        <div className="tile-label">Tests passed</div>
        <div className="tile-value">{fmtInt(dbt.tests_ok)}</div>
      </div>
      <div className="tile">
        <div className="tile-label">Failures</div>
        <div className="tile-value">{fmtInt(dbt.failures)}</div>
      </div>
    </div>
  );
}

export default async function HealthPage() {
  const [health, meta] = await Promise.all([loadSnapshot("health"), loadSnapshot("meta")]);
  const now = renderTime();
  return (
    <>
      <div className="page-header">
        <h1>Pipeline health</h1>
        <p>
          The alerts are only as good as the pipeline behind them. Updated every 5 minutes while a
          session runs.
        </p>
      </div>

      {!health.ok ? (
        <SnapshotError what="Pipeline health" problem={health.problem} />
      ) : (
        <>
          <section className="section" style={{ marginTop: 0 }} aria-labelledby="freshness">
            <h2 id="freshness">Freshness per layer</h2>
            <p className="section-note">How recent the newest data in each layer is.</p>
            <Freshness health={health.data} now={now} />
          </section>

          <section className="section" aria-labelledby="funnel">
            <h2 id="funnel">Record funnel, last session</h2>
            <p className="section-note">
              Every event Redpanda accepted lands in Bronze or the dead-letter queue; Silver keeps
              one row per event ID.
            </p>
            <Funnel funnel={health.data.funnel} />
          </section>

          <section className="section" aria-labelledby="batches">
            <h2 id="batches">Micro-batches, last hour of the session</h2>
            <p className="section-note">Duration and input rate per Spark streaming query.</p>
            <Streaming rows={health.data.streaming} />
          </section>

          <section className="section" aria-labelledby="detect">
            <h2 id="detect">Time to detect</h2>
            <p className="section-note">From the edit on Wikipedia to the alert row in Gold.</p>
            <Detection detection={health.data.detection_seconds} />
          </section>

          <section className="section" aria-labelledby="lag">
            <h2 id="lag">Consumer lag per partition</h2>
            <Lag rows={health.data.lag} />
          </section>
        </>
      )}

      <section className="section" aria-labelledby="dbt">
        <h2 id="dbt">Last dbt run</h2>
        <p className="section-note">Gold models and every dbt test, built every 30 minutes.</p>
        {meta.ok ? (
          <DbtRun meta={meta.data} now={now} />
        ) : (
          <SnapshotError what="dbt run status" problem={meta.problem} />
        )}
      </section>

      {health.ok && (
        <p className="small muted" style={{ marginTop: 24 }}>
          Health checked {fmtUtc(health.data.generated_at)}.
        </p>
      )}
    </>
  );
}
