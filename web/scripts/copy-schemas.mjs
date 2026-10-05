// Copy the snapshot contract (../schemas/dashboard/) into the app before dev, build,
// type check and tests, so the app validates against the exact schemas the DAG uses.
// Fails loudly if the schemas are missing: the app must never build without them.
import { copyFileSync, mkdirSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const web = join(dirname(fileURLToPath(import.meta.url)), "..");
const source = join(web, "..", "schemas", "dashboard");
const target = join(web, "src", "schemas");

const names = readdirSync(source).filter((n) => n.endsWith(".schema.json"));
const expected = ["alerts", "baseline", "health", "meta"].map((n) => `${n}.schema.json`);
const missing = expected.filter((n) => !names.includes(n));
if (missing.length) {
  console.error(`schemas/dashboard/ is missing ${missing.join(", ")}`);
  process.exit(1);
}
mkdirSync(target, { recursive: true });
for (const name of expected) copyFileSync(join(source, name), join(target, name));
console.log(`copied ${expected.length} dashboard schemas`);
