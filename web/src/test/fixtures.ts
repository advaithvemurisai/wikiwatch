import { readFileSync } from "node:fs";
import path from "node:path";

import type { SnapshotName } from "@/lib/types";

/** Raw text of a fixture snapshot in web/fixtures/dashboard/v1/. */
export function fixtureText(name: SnapshotName): string {
  return readFileSync(path.join(__dirname, "../../fixtures/dashboard/v1", `${name}.json`), "utf8");
}
