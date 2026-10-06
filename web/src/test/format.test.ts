import { describe, expect, it } from "vitest";

import { teamLabel, wikipediaUrl } from "@/lib/format";

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
