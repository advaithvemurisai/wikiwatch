// Pages render real fixture data, and a snapshot with a wrong schema_version shows a
// friendly message instead of crashing the page.
import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { SnapshotName, SnapshotResult } from "@/lib/types";
import { parseSnapshot } from "@/lib/validate";

import { fixtureText } from "./fixtures";

const overrides: Partial<Record<SnapshotName, string>> = {};

vi.mock("@/lib/snapshots", () => ({
  loadSnapshot: async <N extends SnapshotName>(name: N): Promise<SnapshotResult<N>> =>
    parseSnapshot(name, overrides[name] ?? fixtureText(name)),
  renderTime: () => Date.parse("2026-10-04T21:00:00Z"),
}));

vi.mock("next/navigation", () => ({ usePathname: () => "/" }));

const { default: AlertsPage } = await import("@/app/page");
const { default: BaselinePage } = await import("@/app/baseline/page");
const { default: HealthPage } = await import("@/app/health/page");
const { OfflineBanner } = await import("@/components/OfflineBanner");

function wrongVersion(name: SnapshotName): string {
  return JSON.stringify({ ...JSON.parse(fixtureText(name)), schema_version: 2 });
}

beforeEach(() => {
  for (const key of Object.keys(overrides)) delete overrides[key as SnapshotName];
});
afterEach(() => vi.restoreAllMocks());

describe("pages on fixtures", () => {
  it("Alerts lists the fixture alerts, high severity first", async () => {
    render(await AlertsPage());
    const rows = screen.getAllByRole("row");
    expect(screen.getByRole("heading", { name: /Alerts, last 7 days/ })).toHaveTextContent("(5)");
    expect(rows[1]).toHaveTextContent("High");
    expect(screen.getAllByText("Ben & Jerry's").length).toBeGreaterThan(0);
  });

  it("Alerts leads with a summary and speaks the analyst's language", async () => {
    render(await AlertsPage());
    expect(document.querySelector(".summary")).toHaveTextContent(
      /5 alerts in the last 7 days · High 2 · Medium 1 · Low 2 · newest/,
    );
    const [link] = screen.getAllByRole("link", { name: /Ben & Jerry's/ });
    expect(link).toHaveAttribute("href", "https://en.wikipedia.org/wiki/Ben_%26_Jerry%27s");
    expect(link).toHaveAttribute("rel", "noopener noreferrer");
    expect(screen.getAllByText(/Product comms/).length).toBeGreaterThan(0); // not product-comms
    expect(screen.getByText(/Large removal ×1, Unregistered editor ×1/)).toBeInTheDocument();
    expect(screen.queryByText(/R2 ×1/)).not.toBeInTheDocument();
  });

  it("Baseline shows its three sections", async () => {
    render(await BaselinePage());
    expect(screen.getByRole("heading", { name: "Platform edits per minute" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Bot share by hour" })).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Watched page/ })).toHaveValue("Marmite");
  });

  it("Pipeline health shows the funnel and the dbt run", async () => {
    render(await HealthPage());
    expect(screen.getByText("Received by Redpanda").parentElement).toHaveTextContent("5,085");
    expect(screen.getByText("Succeeded")).toBeInTheDocument();
  });
});

describe("a snapshot with a wrong schema_version", () => {
  it.each([
    ["alerts", AlertsPage, "Alerts unavailable"],
    ["baseline", BaselinePage, "Baseline unavailable"],
    ["health", HealthPage, "Pipeline health unavailable"],
    ["meta", HealthPage, "dbt run status unavailable"],
  ] as const)("%s shows a friendly error", async (name, Page, heading) => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    overrides[name] = wrongVersion(name);
    render(await Page());
    expect(screen.getByRole("heading", { name: heading })).toBeInTheDocument();
    expect(screen.getByText(/format this version of the site does not understand/)).toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(/schema_version|stack|Error:/);
  });
});

describe("offline banner", () => {
  const now = Date.parse("2026-10-04T21:00:00Z");

  it("is hidden while health.json is under 15 minutes old", () => {
    const { container } = render(<OfflineBanner heartbeat="2026-10-04T20:50:00Z" now={now} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("says since when the pipeline is offline after 15 minutes", () => {
    render(<OfflineBanner heartbeat="2026-10-04T20:44:00Z" now={now} />);
    // Shown in the reader's time zone (UTC in tests), full UTC time on hover.
    expect(screen.getByRole("status")).toHaveTextContent(
      /Pipeline offline since Oct 4, 8:44\s?PM UTC, showing last session\./,
    );
    expect(screen.getByRole("status").querySelector("time")).toHaveAttribute(
      "title",
      "4 Oct 2026, 20:44 UTC",
    );
  });

  it("says the status is unknown without a heartbeat", () => {
    render(<OfflineBanner heartbeat={null} now={now} />);
    expect(screen.getByRole("status")).toHaveTextContent("Pipeline status unknown");
  });
});

describe("a failed export", () => {
  function metaWithFailedExports(...failed: ("alerts" | "baseline")[]): string {
    const meta = JSON.parse(fixtureText("meta"));
    meta.exports = {
      alerts: { status: failed.includes("alerts") ? "failed" : "ok", failed_query: failed.includes("alerts") ? "alerts" : null },
      baseline: { status: failed.includes("baseline") ? "failed" : "ok", failed_query: null },
    };
    return JSON.stringify(meta);
  }

  it("keeps the last good alerts and says the latest export failed", async () => {
    overrides.meta = metaWithFailedExports("alerts");
    render(await AlertsPage());
    expect(screen.getByText("The latest export failed")).toBeInTheDocument();
    expect(screen.getByText(/Showing the last good data, from/)).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Alerts, last 7 days/ })).toBeInTheDocument();
  });

  it("says nothing on Baseline when only the alerts export failed", async () => {
    overrides.meta = metaWithFailedExports("alerts");
    render(await BaselinePage());
    expect(screen.queryByText("The latest export failed")).not.toBeInTheDocument();
  });

  it("explains a failed export with no earlier copy", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    overrides.meta = metaWithFailedExports("baseline");
    overrides.baseline = "not json";
    render(await BaselinePage());
    expect(screen.getByText(/no earlier data to show yet/)).toBeInTheDocument();
  });

  it("shows the export status on Pipeline health", async () => {
    overrides.meta = metaWithFailedExports("alerts", "baseline");
    render(await HealthPage());
    expect(screen.getByText("2 failed")).toBeInTheDocument();
  });

  it("shows all exported when both succeeded", async () => {
    overrides.meta = metaWithFailedExports();
    render(await HealthPage());
    expect(screen.getByText("All exported")).toBeInTheDocument();
  });
});
