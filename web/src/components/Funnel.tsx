import { fmtInt } from "@/lib/format";
import type { HealthSnapshot } from "@/lib/types";

/** Record funnel as labeled bars, each scaled to what Redpanda received. */
export function Funnel({ funnel }: { funnel: HealthSnapshot["funnel"] }) {
  const steps = [
    { label: "Received by Redpanda", value: funnel.received, note: "valid and invalid events" },
    { label: "Bronze", value: funnel.bronze, note: "raw, duplicates kept" },
    {
      label: "Silver after dedup",
      value: funnel.silver_unique,
      note: `${fmtInt(Math.max(funnel.bronze - funnel.silver_unique, 0))} duplicates removed`,
    },
    { label: "Dead-letter queue", value: funnel.dlq, note: "failed schema validation" },
  ];
  const scale = Math.max(funnel.received, ...steps.map((s) => s.value), 1);
  return (
    <div className="card">
      <ul style={{ listStyle: "none", margin: 0, padding: 0, display: "grid", gap: 14 }}>
        {steps.map((step) => (
          <li key={step.label}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
              <span>
                {step.label} <span className="small muted">{step.note}</span>
              </span>
              <strong>{fmtInt(step.value)}</strong>
            </div>
            <div
              aria-hidden="true"
              style={{ height: 10, marginTop: 6, background: "var(--wash)", borderRadius: 4 }}
            >
              <div
                style={{
                  height: "100%",
                  width: `${(step.value / scale) * 100}%`,
                  minWidth: step.value > 0 ? 4 : 0,
                  background: "var(--series-1)",
                  borderRadius: 4,
                }}
              />
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}
