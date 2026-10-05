// The snapshot contract on the app side: fixtures validate, and anything that does not
// match schemas/dashboard/ is rejected with a problem code instead of being trusted.
import { describe, expect, it } from "vitest";

import type { SnapshotName } from "@/lib/types";
import { parseSnapshot } from "@/lib/validate";

import { fixtureText } from "./fixtures";

const NAMES: SnapshotName[] = ["alerts", "baseline", "health", "meta"];

function withChange(name: SnapshotName, change: (doc: Record<string, unknown>) => void): string {
  const doc = JSON.parse(fixtureText(name));
  change(doc);
  return JSON.stringify(doc);
}

describe("snapshot validation", () => {
  it.each(NAMES)("the %s fixture matches its schema", (name) => {
    expect(parseSnapshot(name, fixtureText(name))).toMatchObject({ ok: true });
  });

  it.each(NAMES)("a %s snapshot with another schema_version is a version problem", (name) => {
    const text = withChange(name, (doc) => {
      doc.schema_version = 2;
    });
    expect(parseSnapshot(name, text)).toEqual({ ok: false, problem: "version" });
  });

  it("a snapshot without schema_version is a version problem", () => {
    const text = withChange("meta", (doc) => {
      delete doc.schema_version;
    });
    expect(parseSnapshot("meta", text)).toEqual({ ok: false, problem: "version" });
  });

  it("broken JSON is unreadable", () => {
    expect(parseSnapshot("alerts", '{"schema_version": 1,')).toEqual({ ok: false, problem: "unreadable" });
  });

  it.each([
    ["an unknown field", (d: Record<string, unknown>) => void (d.ip_address = "x")],
    ["a missing field", (d: Record<string, unknown>) => void delete d.digest],
    ["a wrong enum", (d: Record<string, unknown>) => void ((d.alerts as { severity: string }[])[0]!.severity = "urgent")],
    ["a non-UTC timestamp", (d: Record<string, unknown>) => void (d.generated_at = "2026-10-04T02:46:00+02:00")],
  ])("an alerts snapshot with %s is invalid", (_, change) => {
    expect(parseSnapshot("alerts", withChange("alerts", change))).toEqual({ ok: false, problem: "invalid" });
  });

  it("an array or a bare value is invalid", () => {
    expect(parseSnapshot("health", "[]")).toEqual({ ok: false, problem: "invalid" });
    expect(parseSnapshot("health", "1")).toEqual({ ok: false, problem: "invalid" });
  });

  it("the fixtures never contain an IP address", () => {
    const ipv4 = /\b(?:\d{1,3}\.){3}\d{1,3}\b/;
    const ipv6 = /\b[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){7}\b|\b[0-9a-f]{1,4}(?::[0-9a-f]{0,4}){2,6}::/i;
    for (const name of NAMES) {
      const text = fixtureText(name);
      expect(text, name).not.toMatch(ipv4);
      expect(text, name).not.toMatch(ipv6);
    }
  });
});
