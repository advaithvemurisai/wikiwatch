"use client";

import "./globals.css";

// Last-resort boundary (the root layout itself failed): generic message only.
export default function GlobalError({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <html lang="en">
      <body>
        <main className="main">
          <div className="card" role="alert">
            <h2>Something went wrong</h2>
            <p className="secondary" style={{ margin: "6px 0 12px" }}>
              WikiWatch could not be shown. Please try again.
            </p>
            <button type="button" onClick={reset}>
              Try again
            </button>
          </div>
        </main>
      </body>
    </html>
  );
}
