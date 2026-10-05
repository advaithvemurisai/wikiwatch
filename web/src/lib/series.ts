// Pure data shaping for the Baseline charts (tested in src/test/series.test.ts).
import type { BaselineSnapshot } from "./types";

const MINUTE = 60_000;
const MAX_GRID = 400; // the export holds at most 6 h of minutes

export interface MinutePoint {
  t: number;
  final: number | null;
  realtime: number | null;
  /** [real-time, final] where final is higher: edits the real-time view missed. */
  gap: [number, number] | null;
}

/**
 * One point per minute from the first to the last exported minute. Minutes with no
 * row stay null so the lines break where the pipeline was not running.
 */
export function minuteSeries(rows: BaselineSnapshot["edits_per_min"]): MinutePoint[] {
  const byMinute = new Map(rows.map((r) => [new Date(r.minute).getTime(), r]));
  const times = [...byMinute.keys()].sort((a, b) => a - b);
  if (times.length === 0) return [];
  const first = times[0]!;
  const last = times[times.length - 1]!;
  const grid =
    (last - first) / MINUTE + 1 <= MAX_GRID
      ? Array.from({ length: (last - first) / MINUTE + 1 }, (_, i) => first + i * MINUTE)
      : times;
  return grid.map((t) => {
    const row = byMinute.get(t);
    const final = row?.final_edits ?? null;
    const realtime = row?.realtime_edits ?? null;
    const gap =
      final !== null && final > (realtime ?? 0) ? ([realtime ?? 0, final] as [number, number]) : null;
    return { t, final, realtime, gap };
  });
}

export interface HourBar {
  hour: string;
  edits: number;
}

/** A page's edits in every recorded hour (0 when the pipeline ran but nobody edited). */
export function pageHours(snapshot: BaselineSnapshot, title: string): HourBar[] {
  const edits = new Map(
    snapshot.page_activity.filter((r) => r.title === title).map((r) => [r.hour, r.edits]),
  );
  return snapshot.recorded_hours.map((hour) => ({ hour, edits: edits.get(hour) ?? 0 }));
}

/** Pages ordered by total edits (busiest first), then by name. */
export function pagesByActivity(snapshot: BaselineSnapshot) {
  return [...snapshot.page_averages].sort(
    (a, b) => b.total_edits - a.total_edits || a.title.localeCompare(b.title),
  );
}
