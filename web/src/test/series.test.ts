import { describe, expect, it } from "vitest";

import { minuteSeries, pageHours, pagesByActivity } from "@/lib/series";
import type { BaselineSnapshot } from "@/lib/types";

describe("minuteSeries", () => {
  it("fills missing minutes with nulls so the lines break", () => {
    const points = minuteSeries([
      { minute: "2026-10-04T02:00:00Z", final_edits: 10, realtime_edits: 10 },
      { minute: "2026-10-04T02:03:00Z", final_edits: 12, realtime_edits: 9 },
    ]);
    expect(points.map((p) => p.final)).toEqual([10, null, null, 12]);
  });

  it("shades the gap where real-time missed late events", () => {
    const [onTime, late, allLate] = minuteSeries([
      { minute: "2026-10-04T02:00:00Z", final_edits: 10, realtime_edits: 10 },
      { minute: "2026-10-04T02:01:00Z", final_edits: 12, realtime_edits: 9 },
      { minute: "2026-10-04T02:02:00Z", final_edits: 4, realtime_edits: null },
    ]);
    expect(onTime!.gap).toBeNull();
    expect(late!.gap).toEqual([9, 12]);
    expect(allLate!.gap).toEqual([0, 4]);
  });

  it("is empty without rows", () => {
    expect(minuteSeries([])).toEqual([]);
  });
});

describe("page activity", () => {
  const snapshot = {
    recorded_hours: ["2026-10-04T01:00:00Z", "2026-10-04T02:00:00Z"],
    page_activity: [{ title: "Marmite", category: "product", hour: "2026-10-04T02:00:00Z", edits: 9 }],
    page_averages: [
      { title: "Axe (brand)", category: "product", total_edits: 0, avg_edits_per_recorded_hour: 0 },
      { title: "Marmite", category: "product", total_edits: 9, avg_edits_per_recorded_hour: 4.5 },
    ],
  } as unknown as BaselineSnapshot;

  it("counts recorded hours without edits as zero", () => {
    expect(pageHours(snapshot, "Marmite").map((h) => h.edits)).toEqual([0, 9]);
  });

  it("orders pages busiest first", () => {
    expect(pagesByActivity(snapshot).map((p) => p.title)).toEqual(["Marmite", "Axe (brand)"]);
  });
});
