import type { Metadata } from "next";

import { Nav } from "@/components/Nav";
import { OfflineBanner } from "@/components/OfflineBanner";
import { loadSnapshot, renderTime } from "@/lib/snapshots";

import "./globals.css";

export const metadata: Metadata = {
  title: { default: "WikiWatch", template: "%s · WikiWatch" },
  description:
    "Real-time monitoring of brand pages on Wikipedia: alerts, platform baseline and pipeline health.",
};

export const revalidate = 300;

export default async function RootLayout({ children }: { children: React.ReactNode }) {
  const health = await loadSnapshot("health");
  const heartbeat = health.ok ? health.data.generated_at : null;
  return (
    <html lang="en">
      <body>
        <Nav />
        <OfflineBanner heartbeat={heartbeat} now={renderTime()} />
        <main className="main">{children}</main>
      </body>
    </html>
  );
}
