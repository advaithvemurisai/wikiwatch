"use client";

import { useSyncExternalStore } from "react";

import { fmtUtc } from "@/lib/format";

const noSubscription = () => () => {};

function localText(iso: string): string {
  return new Date(iso).toLocaleString(undefined, {
    day: "numeric",
    month: "short",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

/**
 * A timestamp in the reader's own time zone, with UTC on hover. The server renders UTC
 * (it cannot know the reader's zone) and the browser renders local time, without a
 * hydration mismatch.
 */
export function LocalTime({ iso, className }: { iso: string; className?: string }) {
  const text = useSyncExternalStore(
    noSubscription,
    () => localText(iso),
    () => fmtUtc(iso),
  );
  return (
    <time dateTime={iso} title={fmtUtc(iso)} className={className}>
      {text}
    </time>
  );
}
