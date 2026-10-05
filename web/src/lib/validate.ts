// Validation of snapshot documents against the contract in schemas/dashboard/.
// Pure (no I/O), so tests can feed it any document.
import Ajv2020 from "ajv/dist/2020";
import addFormats from "ajv-formats";

import alertsSchema from "@/schemas/alerts.schema.json";
import baselineSchema from "@/schemas/baseline.schema.json";
import healthSchema from "@/schemas/health.schema.json";
import metaSchema from "@/schemas/meta.schema.json";

import type { SnapshotName, SnapshotResult, Snapshots } from "./types";

export const SCHEMA_VERSION = 1;

const ajv = new Ajv2020({ allErrors: false, strict: true });
addFormats(ajv);

const validators = {
  alerts: ajv.compile(alertsSchema),
  baseline: ajv.compile(baselineSchema),
  health: ajv.compile(healthSchema),
  meta: ajv.compile(metaSchema),
};

/** Parse raw snapshot text and check it against its schema. */
export function parseSnapshot<N extends SnapshotName>(name: N, text: string): SnapshotResult<N> {
  let document: unknown;
  try {
    document = JSON.parse(text);
  } catch {
    return { ok: false, problem: "unreadable" };
  }
  if (typeof document !== "object" || document === null || Array.isArray(document)) {
    return { ok: false, problem: "invalid" };
  }
  // A different schema_version is a contract change, not corrupt data: say so.
  if ((document as { schema_version?: unknown }).schema_version !== SCHEMA_VERSION) {
    return { ok: false, problem: "version" };
  }
  if (!validators[name](document)) {
    return { ok: false, problem: "invalid" };
  }
  return { ok: true, data: document as unknown as Snapshots[N] };
}
