import { describe, expect, it } from "vitest";

import { changeLink, teamLabel, wikipediaUrl } from "@/lib/format";

describe("wikipediaUrl", () => {
  it("builds the page URL from the wiki code and title", () => {
    expect(wikipediaUrl("enwiki", "Axe (brand)")).toBe("https://en.wikipedia.org/wiki/Axe_(brand)");
    expect(wikipediaUrl("dewiki", "Ben & Jerry's")).toBe(
      "https://de.wikipedia.org/wiki/Ben_%26_Jerry%27s",
    );
  });

  it("gives no link for wikis that are not Wikipedias", () => {
    expect(wikipediaUrl("commonswiki", "File")).toBeNull(); // Commons is not a Wikipedia
    expect(wikipediaUrl("https://evil.example/", "x")).toBeNull();
  });
});

describe("teamLabel", () => {
  it("turns slugs into words", () => {
    expect(teamLabel("product-comms")).toBe("Product comms");
    expect(teamLabel("corporate_comms")).toBe("Corporate comms");
  });
});

describe("changeLink", () => {
  const edit = {
    wiki: "enwiki",
    title: "Lay's",
    rule_id: "R2",
    event_ts: "2026-10-11T20:35:12Z",
    revision_old: 1001,
    revision_new: 1002,
  };

  it("links an edit to its Wikipedia diff", () => {
    expect(changeLink(edit)).toEqual({
      href: "https://en.wikipedia.org/w/index.php?diff=1002&oldid=1001",
      label: "View change",
    });
  });

  it("diffs a new page without an old revision", () => {
    expect(changeLink({ ...edit, revision_old: null })?.href).toBe(
      "https://en.wikipedia.org/w/index.php?diff=1002",
    );
  });

  it("falls back to the page history just after the edit when revisions are unknown", () => {
    // Log events (R1, R4) and alerts exported before revisions existed.
    for (const missing of [{ revision_new: null }, { revision_old: undefined, revision_new: undefined }]) {
      expect(changeLink({ ...edit, ...missing })).toEqual({
        href: "https://en.wikipedia.org/w/index.php?title=Lay%27s&action=history&offset=20261011203612",
        label: "Page history",
      });
    }
  });

  it("covers a whole R5 burst window in the history link", () => {
    const burst = { ...edit, rule_id: "R5", event_ts: "2026-10-03T12:00:00Z", revision_old: null, revision_new: null };
    expect(changeLink(burst)?.href).toContain("offset=20261003130000");
  });

  it("never builds a link from untrusted values", () => {
    expect(changeLink({ ...edit, wiki: "https://evil.example/" })).toBeNull();
    expect(changeLink({ ...edit, revision_new: -5 })?.label).toBe("Page history");
    expect(changeLink({ ...edit, revision_new: 1.5 })?.label).toBe("Page history");
  });
});
