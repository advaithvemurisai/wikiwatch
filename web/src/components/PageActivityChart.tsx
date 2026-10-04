"use client";

import { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { CATEGORY_LABEL, fmtClock, fmtDay, fmtDecimal, fmtInt } from "@/lib/format";
import { pageHours, pagesByActivity, type HourBar } from "@/lib/series";
import type { BaselineSnapshot } from "@/lib/types";

import { AXIS_TICK, TableView, TooltipBox } from "./ChartParts";

const COLOR = "var(--series-1)";

function hourLabel(iso: string) {
  return `${fmtDay(iso)} ${fmtClock(iso)}`;
}

function PageTooltip({ active, payload, average }: { active?: boolean; payload?: { payload: HourBar }[]; average: number }) {
  const row = payload?.[0]?.payload;
  if (!active || !row) return null;
  return (
    <TooltipBox
      title={`${hourLabel(row.hour)} UTC`}
      rows={[
        { label: "edits this hour", value: fmtInt(row.edits), color: COLOR },
        { label: "page average", value: fmtDecimal(average), color: "var(--ink-secondary)" },
      ]}
    />
  );
}

export function PageActivityChart({ snapshot }: { snapshot: BaselineSnapshot }) {
  const pages = pagesByActivity(snapshot);
  const [title, setTitle] = useState(pages[0]?.title ?? "");
  const page = pages.find((p) => p.title === title);
  if (!page) return <p className="empty card">No watched pages in this snapshot.</p>;
  const bars = pageHours(snapshot, page.title);
  const average = page.avg_edits_per_recorded_hour;

  return (
    <>
      <div className="filters">
        <label>
          Watched page
          <select value={title} onChange={(e) => setTitle(e.target.value)}>
            {pages.map((p) => (
              <option key={p.title} value={p.title}>
                {p.title} ({fmtInt(p.total_edits)} edits)
              </option>
            ))}
          </select>
        </label>
      </div>
      <p className="section-note">
        {CATEGORY_LABEL[page.category]} · {fmtInt(page.total_edits)} edits in{" "}
        {fmtInt(snapshot.recorded_hours.length)} recorded hours · average{" "}
        <strong>{fmtDecimal(average)}</strong> per recorded hour
      </p>
      {bars.length === 0 ? (
        <p className="empty card">No recorded hours yet.</p>
      ) : (
        <div className="card">
          <div className="chart">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={bars} margin={{ top: 20, right: 16, bottom: 0, left: 0 }} barCategoryGap={2}>
                <CartesianGrid vertical={false} stroke="var(--gridline)" />
                <XAxis
                  dataKey="hour"
                  tickFormatter={hourLabel}
                  tick={AXIS_TICK}
                  tickLine={false}
                  axisLine={{ stroke: "var(--gridline)" }}
                  minTickGap={24}
                />
                <YAxis
                  allowDecimals={false}
                  tick={AXIS_TICK}
                  tickLine={false}
                  axisLine={false}
                  width={40}
                  domain={[0, (max: number) => Math.max(max, Math.ceil(average), 1)]}
                />
                <Tooltip
                  content={<PageTooltip average={average} />}
                  cursor={{ fill: "var(--wash)" }}
                  isAnimationActive={false}
                />
                <Bar dataKey="edits" fill={COLOR} maxBarSize={24} radius={[4, 4, 0, 0]} isAnimationActive={false} />
                <ReferenceLine
                  y={average}
                  stroke="var(--ink-secondary)"
                  strokeWidth={1}
                  label={{
                    value: `average ${fmtDecimal(average)}`,
                    position: "insideTopRight",
                    fill: "var(--ink-secondary)",
                    fontSize: 12,
                  }}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <TableView
            caption={`Edits per recorded hour on ${page.title}`}
            columns={["Hour (UTC)", "Edits", "Average"]}
            rows={bars.map((b) => [hourLabel(b.hour), fmtInt(b.edits), fmtDecimal(average)])}
          />
        </div>
      )}
    </>
  );
}
