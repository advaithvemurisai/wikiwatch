// Formatting shared by server and client components. Everything is UTC.

const integer = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });
const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

export const OFFLINE_AFTER_MS = 15 * 60 * 1000;

export function fmtInt(value: number): string {
  return integer.format(value);
}

export function fmtDecimal(value: number): string {
  return decimal.format(value);
}

export function fmtCompact(value: number): string {
  return compact.format(value);
}

export function fmtPercent(share: number): string {
  return `${decimal.format(share * 100)}%`;
}

/** Signed byte change: +1,204 B / -9,000 B. */
export function fmtBytes(delta: number): string {
  const sign = delta > 0 ? "+" : delta < 0 ? "−" : "";
  return `${sign}${integer.format(Math.abs(delta))} B`;
}

export function fmtSeconds(seconds: number): string {
  if (seconds < 90) return `${decimal.format(seconds)} s`;
  if (seconds < 90 * 60) return `${decimal.format(seconds / 60)} min`;
  return `${decimal.format(seconds / 3600)} h`;
}

export function fmtMs(ms: number): string {
  return ms < 1000 ? `${integer.format(ms)} ms` : `${decimal.format(ms / 1000)} s`;
}

/** 4 Oct 2026, 02:13 UTC */
export function fmtUtc(iso: string): string {
  const d = new Date(iso);
  const date = d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
  return `${date}, ${fmtClock(iso)} UTC`;
}

/** 02:13 (UTC) */
export function fmtClock(iso: string | number): string {
  return new Date(iso).toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
  });
}

/** 4 Oct */
export function fmtDay(iso: string): string {
  return new Date(iso).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    timeZone: "UTC",
  });
}

/** "3 min ago", "5 h ago", "2 days ago". `now` is passed in so server and client agree. */
export function timeAgo(iso: string, now: number): string {
  const seconds = Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000));
  if (seconds < 60) return "just now";
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours} h ago`;
  return `${Math.round(hours / 24)} days ago`;
}

export function isOffline(generatedAt: string, now: number): boolean {
  return now - new Date(generatedAt).getTime() > OFFLINE_AFTER_MS;
}

export const CATEGORY_LABEL = {
  own_brand: "Own brand",
  product: "Product",
  competitor: "Competitor",
} as const;

export const EDITOR_LABEL = {
  registered: "Registered",
  unregistered: "Unregistered",
  bot: "Bot",
} as const;

/** Rule names from docs/v1.md, for places that only carry the rule ID (the digest). */
export const RULE_NAME = {
  R1: "Page deleted or moved",
  R2: "Large removal",
  R3: "Unregistered editor",
  R4: "Protection change",
  R5: "Edit burst",
} as const;

/** "product-comms" -> "Product comms". */
export function teamLabel(slug: string): string {
  const words = slug.replace(/[-_]+/g, " ").trim();
  return words ? words[0]!.toUpperCase() + words.slice(1) : slug;
}

/** Public URL of a page: enwiki + "Ben & Jerry's" -> en.wikipedia.org/wiki/Ben_%26_Jerry%27s. */
export function wikipediaUrl(wiki: string, title: string): string | null {
  const match = /^([a-z]{2,3}(?:-[a-z]+)?)wiki$/.exec(wiki); // language wikis only
  if (!match) return null;
  const path = encodeURIComponent(title.replace(/ /g, "_")).replace(/'/g, "%27");
  return `https://${match[1]}.wikipedia.org/wiki/${path}`;
}
