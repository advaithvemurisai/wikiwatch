"use client";

import { useMemo, useState } from "react";

import { LocalTime } from "@/components/LocalTime";
import { SeverityBadge } from "@/components/Status";
import {
  CATEGORY_LABEL,
  EDITOR_LABEL,
  fmtBytes,
  fmtDay,
  fmtInt,
  fmtSeconds,
  RULE_NAME,
  teamLabel,
  timeAgo,
  wikipediaUrl,
} from "@/lib/format";
import type { Alert, AlertsSnapshot, Category, DigestRow, RuleId } from "@/lib/types";

const RULES: RuleId[] = ["R1", "R2", "R3", "R4", "R5"];
const RULE_COUNT = {
  R1: "r1_alerts",
  R2: "r2_alerts",
  R3: "r3_alerts",
  R4: "r4_alerts",
  R5: "r5_alerts",
} as const satisfies Record<RuleId, keyof DigestRow>;

type Filter = { rule: RuleId | ""; category: Category | ""; page: string };

function PageLink({ wiki, title }: { wiki: string; title: string }) {
  const url = wikipediaUrl(wiki, title);
  if (!url) return <>{title}</>;
  return (
    <a href={url} target="_blank" rel="noopener noreferrer">
      {title}
      <span className="sr-only"> (opens Wikipedia in a new tab)</span>
    </a>
  );
}

const SEVERITIES = ["high", "medium", "low"] as const;

/** One line that answers "do I need to act?" before the table does. */
function Summary({ alerts, now, days }: { alerts: Alert[]; now: number; days: number }) {
  if (alerts.length === 0) {
    return <p className="summary">No alerts in the last {days} days.</p>;
  }
  const newest = alerts.reduce((a, b) => (a.event_ts > b.event_ts ? a : b));
  return (
    <p className="summary">
      <strong>
        {fmtInt(alerts.length)} {alerts.length === 1 ? "alert" : "alerts"}
      </strong>{" "}
      in the last {days} days
      {SEVERITIES.map((sev) => {
        const n = alerts.filter((a) => a.severity === sev).length;
        return n ? (
          <span key={sev}>
            {" · "}
            <SeverityBadge severity={sev} /> {fmtInt(n)}
          </span>
        ) : null;
      })}
      {" · "}newest {timeAgo(newest.event_ts, now)}
    </p>
  );
}

function ruleDetail(alert: Alert): string | null {
  if (alert.edits_in_window !== null) return `${fmtInt(alert.edits_in_window)} edits in the window`;
  if (alert.log_action) return `Action: ${alert.log_action.replace(/_/g, " ")}`;
  return null;
}

function detectionDelay(alert: Alert): string {
  const seconds = (new Date(alert.detected_at).getTime() - new Date(alert.event_ts).getTime()) / 1000;
  return fmtSeconds(Math.max(0, seconds));
}

export function AlertsView({ snapshot, now }: { snapshot: AlertsSnapshot; now: number }) {
  const [filter, setFilter] = useState<Filter>({ rule: "", category: "", page: "" });

  const ruleNames = useMemo(() => {
    const names = new Map<RuleId, string>();
    for (const a of snapshot.alerts) names.set(a.rule_id, a.rule_name);
    return names;
  }, [snapshot.alerts]);

  const pages = useMemo(
    () =>
      [...new Set([...snapshot.alerts, ...snapshot.digest].map((r) => r.title))].sort((a, b) =>
        a.localeCompare(b),
      ),
    [snapshot.alerts, snapshot.digest],
  );

  const alerts = snapshot.alerts.filter(
    (a) =>
      (!filter.rule || a.rule_id === filter.rule) &&
      (!filter.category || a.category === filter.category) &&
      (!filter.page || a.title === filter.page),
  );
  const digest = snapshot.digest.filter(
    (d) =>
      (!filter.rule || d[RULE_COUNT[filter.rule]] > 0) &&
      (!filter.category || d.category === filter.category) &&
      (!filter.page || d.title === filter.page),
  );
  const filtered = filter.rule !== "" || filter.category !== "" || filter.page !== "";

  return (
    <>
      <Summary alerts={snapshot.alerts} now={now} days={snapshot.window_days} />
      <div className="filters" role="group" aria-label="Filters">
        <label>
          Rule
          <select
            value={filter.rule}
            onChange={(e) => setFilter({ ...filter, rule: e.target.value as RuleId | "" })}
          >
            <option value="">All rules</option>
            {RULES.map((id) => (
              <option key={id} value={id}>
                {ruleNames.has(id) ? `${id} · ${ruleNames.get(id)}` : id}
              </option>
            ))}
          </select>
        </label>
        <label>
          Category
          <select
            value={filter.category}
            onChange={(e) => setFilter({ ...filter, category: e.target.value as Category | "" })}
          >
            <option value="">All categories</option>
            {(Object.keys(CATEGORY_LABEL) as Category[]).map((c) => (
              <option key={c} value={c}>
                {CATEGORY_LABEL[c]}
              </option>
            ))}
          </select>
        </label>
        <label>
          Page
          <select
            value={filter.page}
            onChange={(e) => setFilter({ ...filter, page: e.target.value })}
          >
            <option value="">All pages</option>
            {pages.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </label>
        {filtered && (
          <button type="button" onClick={() => setFilter({ rule: "", category: "", page: "" })}>
            Clear filters
          </button>
        )}
      </div>

      <section className="section" style={{ marginTop: 0 }} aria-labelledby="open-alerts">
        <h2 id="open-alerts">
          Alerts, last {snapshot.window_days} days{" "}
          <span className="muted">({fmtInt(alerts.length)})</span>
        </h2>
        <p className="section-note">
          Highest severity first, then newest. Page names link to Wikipedia.
        </p>
        <div className="table-wrap">
          {alerts.length === 0 ? (
            <p className="empty">
              {filtered ? "No alerts match these filters." : "No alerts in the last 7 days."}
            </p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th scope="col">Severity</th>
                  <th scope="col">Page</th>
                  <th scope="col">Rule</th>
                  <th scope="col" className="num">
                    Byte change
                  </th>
                  <th scope="col">Editor type</th>
                  <th scope="col">Edited</th>
                </tr>
              </thead>
              <tbody>
                {alerts.map((a) => {
                  const detail = ruleDetail(a);
                  return (
                    <tr key={a.alert_id}>
                      <td>
                        <SeverityBadge severity={a.severity} />
                      </td>
                      <td>
                        <div style={{ fontWeight: 560 }}>
                          <PageLink wiki={a.wiki} title={a.title} />
                        </div>
                        <div className="small muted">
                          {CATEGORY_LABEL[a.category]} · {teamLabel(a.owner_team)}
                        </div>
                      </td>
                      <td>
                        <div>
                          {a.rule_id} · {a.rule_name}
                        </div>
                        {detail && <div className="small muted">{detail}</div>}
                      </td>
                      <td className="num">{a.byte_delta === null ? "—" : fmtBytes(a.byte_delta)}</td>
                      <td>{a.editor_type ? EDITOR_LABEL[a.editor_type] : "—"}</td>
                      <td>
                        <div className="nowrap">{timeAgo(a.event_ts, now)}</div>
                        <LocalTime iso={a.event_ts} className="small muted nowrap" />
                        <div className="small muted nowrap">detected in {detectionDelay(a)}</div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </section>

      <section className="section" aria-labelledby="digest">
        <h2 id="digest">Daily digest per page</h2>
        <p className="section-note">
          Last {snapshot.window_days} days, watched pages with activity. Days are UTC.
        </p>
        <div className="table-wrap">
          {digest.length === 0 ? (
            <p className="empty">No activity on watched pages for these filters.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th scope="col">Day</th>
                  <th scope="col">Page</th>
                  <th scope="col" className="num">
                    Edits
                  </th>
                  <th scope="col" className="num">
                    Net bytes
                  </th>
                  <th scope="col" className="num">
                    Registered
                  </th>
                  <th scope="col" className="num">
                    Unregistered
                  </th>
                  <th scope="col" className="num">
                    Bot
                  </th>
                  <th scope="col" className="num">
                    Log events
                  </th>
                  <th scope="col">Alerts</th>
                </tr>
              </thead>
              <tbody>
                {digest.map((d) => {
                  const counts = RULES.filter((id) => d[RULE_COUNT[id]] > 0).map(
                    (id) => `${RULE_NAME[id]} \u00d7${d[RULE_COUNT[id]]}`,
                  );
                  return (
                    <tr key={`${d.digest_date}|${d.wiki}|${d.title}`}>
                      <td className="nowrap">{fmtDay(d.digest_date)}</td>
                      <td>
                        <div style={{ fontWeight: 560 }}>
                          <PageLink wiki={d.wiki} title={d.title} />
                        </div>
                        <div className="small muted">{CATEGORY_LABEL[d.category]}</div>
                      </td>
                      <td className="num">{fmtInt(d.edits)}</td>
                      <td className="num">{fmtBytes(d.net_bytes)}</td>
                      <td className="num">{fmtInt(d.registered_edits)}</td>
                      <td className="num">{fmtInt(d.unregistered_edits)}</td>
                      <td className="num">{fmtInt(d.bot_edits)}</td>
                      <td className="num">{fmtInt(d.log_events)}</td>
                      <td className="nowrap">{counts.length ? counts.join(", ") : <span className="muted">None</span>}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </section>
    </>
  );
}
