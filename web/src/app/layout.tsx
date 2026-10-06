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
        <div className="intro">
          <p className="main" style={{ margin: "0 auto" }}>
            WikiWatch streams every Wikipedia edit and alerts a (fictional) communications team
            when a watched brand page gets a risky edit.{" "}
            <a href="https://github.com/advaithvemurisai/wikiwatch#readme">How it works</a>
          </p>
        </div>
        <OfflineBanner heartbeat={heartbeat} now={renderTime()} />
        <main className="main">{children}</main>
      </body>
    </html>
  );
}
