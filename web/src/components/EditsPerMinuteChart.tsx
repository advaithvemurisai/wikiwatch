"use client";

import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { fmtClock, fmtCompact, fmtInt } from "@/lib/format";
import type { MinutePoint } from "@/lib/series";

import { AXIS_TICK, Legend, TableView, TooltipBox } from "./ChartParts";

const FINAL = "var(--series-1)";
const REALTIME = "var(--series-2)";
const GAP = "var(--gap-fill)";

/** Draw a dot only where a point has no neighbor, so single minutes stay visible. */
function isolatedDot(points: MinutePoint[], key: "final" | "realtime", color: string) {
  function IsolatedDot(props: { cx?: number; cy?: number; index?: number }) {
    const { cx, cy, index = 0 } = props;
    const alone =
      points[index]?.[key] != null &&
      points[index - 1]?.[key] == null &&
      points[index + 1]?.[key] == null;
    if (!alone || cx == null || cy == null) return <g key={`${key}-${index}`} />;
    return (
      <circle
        key={`${key}-${index}`}
        cx={cx}
        cy={cy}
        r={4}
        fill={color}
        stroke="var(--surface-raised)"
        strokeWidth={2}
      />
    );
  }
  return IsolatedDot;
}

function MinuteTooltip({ active, payload }: { active?: boolean; payload?: { payload: MinutePoint }[] }) {
  const point = payload?.[0]?.payload;
  if (!active || !point) return null;
  const missed = point.gap ? point.gap[1] - point.gap[0] : 0;
  return (
    <TooltipBox
      title={`${fmtClock(point.t)} UTC`}
      rows={[
        { label: "final", value: point.final == null ? "—" : fmtInt(point.final), color: FINAL },
        {
          label: "real-time",
          value: point.realtime == null ? "—" : fmtInt(point.realtime),
          color: REALTIME,
        },
        ...(missed ? [{ label: "late, missed by real-time", value: fmtInt(missed), color: GAP }] : []),
      ]}
    />
  );
}

export function EditsPerMinuteChart({ points }: { points: MinutePoint[] }) {
  return (
    <>
      <Legend
        items={[
          { label: "Final (all events)", color: FINAL, shape: "line" },
          { label: "Real-time (closed at the watermark)", color: REALTIME, shape: "line" },
          { label: "Gap: late events", color: GAP, shape: "rect" },
        ]}
      />
      <div className="chart">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={points} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
            <CartesianGrid vertical={false} stroke="var(--gridline)" />
            <XAxis
              dataKey="t"
              type="number"
              scale="time"
              domain={["dataMin", "dataMax"]}
              tickFormatter={fmtClock}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={{ stroke: "var(--gridline)" }}
              minTickGap={32}
            />
            <YAxis
              tickFormatter={fmtCompact}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={false}
              width={48}
              allowDecimals={false}
            />
            <Tooltip
              content={<MinuteTooltip />}
              cursor={{ stroke: "var(--ink-muted)", strokeWidth: 1 }}
              isAnimationActive={false}
            />
            <Area
              dataKey="gap"
              stroke="none"
              fill={GAP}
              fillOpacity={0.25}
              connectNulls={false}
              isAnimationActive={false}
              activeDot={false}
            />
            {/* Final last, so it sits on top where the two counts are equal. */}
            <Line
              dataKey="realtime"
              stroke={REALTIME}
              strokeWidth={2}
              strokeLinejoin="round"
              strokeLinecap="round"
              dot={isolatedDot(points, "realtime", REALTIME)}
              activeDot={{ r: 4, stroke: "var(--surface-raised)", strokeWidth: 2 }}
              connectNulls={false}
              isAnimationActive={false}
            />
            <Line
              dataKey="final"
              stroke={FINAL}
              strokeWidth={2}
              strokeLinejoin="round"
              strokeLinecap="round"
              dot={isolatedDot(points, "final", FINAL)}
              activeDot={{ r: 4, stroke: "var(--surface-raised)", strokeWidth: 2 }}
              connectNulls={false}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      <TableView
        caption="Platform edits per minute, final and real-time"
        columns={["Minute (UTC)", "Final", "Real-time", "Late"]}
        rows={points
          .filter((p) => p.final != null || p.realtime != null)
          .map((p) => [
            fmtClock(p.t),
            p.final == null ? "—" : fmtInt(p.final),
            p.realtime == null ? "—" : fmtInt(p.realtime),
            p.gap ? fmtInt(p.gap[1] - p.gap[0]) : "0",
          ])}
      />
    </>
  );
}
