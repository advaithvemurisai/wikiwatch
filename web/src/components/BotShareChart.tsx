"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { fmtClock, fmtDay, fmtInt, fmtPercent } from "@/lib/format";
import type { BaselineSnapshot } from "@/lib/types";

import { AXIS_TICK, TableView, TooltipBox } from "./ChartParts";

type Row = BaselineSnapshot["bot_share_hourly"][number];
const COLOR = "var(--series-1)";

function hourLabel(iso: string) {
  return `${fmtDay(iso)} ${fmtClock(iso)}`;
}

function HourTooltip({ active, payload }: { active?: boolean; payload?: { payload: Row }[] }) {
  const row = payload?.[0]?.payload;
  if (!active || !row) return null;
  return (
    <TooltipBox
      title={`${hourLabel(row.hour)} UTC`}
      rows={[
        { label: "bot share", value: fmtPercent(row.bot_share), color: COLOR },
        { label: "bot edits", value: `${fmtInt(row.bot_edits)} of ${fmtInt(row.edits)}`, color: COLOR },
      ]}
    />
  );
}

export function BotShareChart({ rows }: { rows: Row[] }) {
  if (rows.length === 0) return <p className="empty card">No recorded hours yet.</p>;
  if (rows.length < 3) {
    // Too few hours for a chart to say anything: show the numbers.
    return (
      <div className="tiles">
        {rows.map((r) => (
          <div className="tile" key={r.hour}>
            <div className="tile-label">{hourLabel(r.hour)} UTC</div>
            <div className="tile-value">{fmtPercent(r.bot_share)}</div>
            <div className="tile-detail">
              {fmtInt(r.bot_edits)} bot edits of {fmtInt(r.edits)}
            </div>
          </div>
        ))}
      </div>
    );
  }
  return (
    <>
      <div className="chart">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={rows} margin={{ top: 8, right: 16, bottom: 0, left: 0 }} barCategoryGap={2}>
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
              domain={[0, 1]}
              ticks={[0, 0.25, 0.5, 0.75, 1]}
              tickFormatter={(v: number) => `${Math.round(v * 100)}%`}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={false}
              width={48}
            />
            <Tooltip content={<HourTooltip />} cursor={{ fill: "var(--wash)" }} isAnimationActive={false} />
            <Bar dataKey="bot_share" fill={COLOR} maxBarSize={24} radius={[4, 4, 0, 0]} isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <TableView
        caption="Bot share of edits by hour"
        columns={["Hour (UTC)", "Bot share", "Bot edits", "All edits"]}
        rows={rows.map((r) => [hourLabel(r.hour), fmtPercent(r.bot_share), fmtInt(r.bot_edits), fmtInt(r.edits)])}
      />
    </>
  );
}
