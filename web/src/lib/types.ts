// TypeScript view of the snapshot contract in schemas/dashboard/ (schema_version 1).
// The JSON Schemas are the source of truth; ajv checks every snapshot against them
// before any of these types is trusted.

export type Category = "own_brand" | "product" | "competitor";
export type Severity = "high" | "medium" | "low";
export type RuleId = "R1" | "R2" | "R3" | "R4" | "R5";
export type EditorType = "registered" | "unregistered" | "bot" | null;
export type Timestamp = string; // ISO 8601, UTC, ends in Z

export interface Alert {
  alert_id: string;
  rule_id: RuleId;
  rule_name: string;
  severity: Severity;
  wiki: string;
  title: string;
  category: Category;
  owner_team: string;
  edit_type: string | null;
  log_action: string | null;
  editor_type: EditorType;
  byte_delta: number | null;
  edits_in_window: number | null;
  event_ts: Timestamp;
  detected_at: Timestamp;
}

export interface DigestRow {
  digest_date: string;
  wiki: string;
  title: string;
  category: Category;
  edits: number;
  log_events: number;
  net_bytes: number;
  registered_edits: number;
  unregistered_edits: number;
  bot_edits: number;
  r1_alerts: number;
  r2_alerts: number;
  r3_alerts: number;
  r4_alerts: number;
  r5_alerts: number;
}

export interface AlertsSnapshot {
  schema_version: 1;
  generated_at: Timestamp;
  window_days: 7;
  alerts: Alert[];
  digest: DigestRow[];
}

export interface BaselineSnapshot {
  schema_version: 1;
  generated_at: Timestamp;
  edits_per_min: { minute: Timestamp; realtime_edits: number | null; final_edits: number | null }[];
  bot_share_hourly: { hour: Timestamp; edits: number; bot_edits: number; bot_share: number }[];
  recorded_hours: Timestamp[];
  page_activity: { title: string; category: Category; hour: Timestamp; edits: number }[];
  page_averages: {
    title: string;
    category: Category;
    total_edits: number;
    avg_edits_per_recorded_hour: number;
  }[];
}

export interface HealthSnapshot {
  schema_version: 1;
  generated_at: Timestamp;
  freshness: { bronze: Timestamp | null; silver: Timestamp | null; gold: Timestamp | null };
  funnel: { received: number; bronze: number; silver_unique: number; dlq: number };
  streaming: {
    query: string;
    batches: number;
    input_rows_per_second: number | null;
    batch_duration_ms_p50: number | null;
    batch_duration_ms_max: number | null;
  }[];
  detection_seconds: { samples: number; p50: number | null; p95: number | null };
  lag: {
    partition: number;
    latest_offset: number;
    processed_offset: number | null;
    lag: number | null;
  }[];
}

export interface MetaSnapshot {
  schema_version: 1;
  generated_at: Timestamp;
  dbt: {
    status: "success" | "failed" | "unknown";
    finished_at: Timestamp | null;
    models_ok: number;
    tests_ok: number;
    failures: number;
  };
  snapshots: Record<
    "alerts" | "digest" | "edits_per_min" | "bot_share_hourly" | "page_activity",
    number
  >;
}

export interface Snapshots {
  alerts: AlertsSnapshot;
  baseline: BaselineSnapshot;
  health: HealthSnapshot;
  meta: MetaSnapshot;
}

export type SnapshotName = keyof Snapshots;

/** Why a snapshot could not be shown. Never carries error details to the page. */
export type SnapshotProblem = "unavailable" | "unreadable" | "version" | "invalid";

export type SnapshotResult<N extends SnapshotName> =
  | { ok: true; data: Snapshots[N] }
  | { ok: false; problem: SnapshotProblem };
