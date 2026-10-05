"use client";

// Route error boundary: a generic message, never the error's details.
export default function Error({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <div className="card" role="alert">
      <h2>Something went wrong</h2>
      <p className="secondary" style={{ margin: "6px 0 12px" }}>
        This page could not be shown. Please try again.
      </p>
      <button type="button" onClick={reset}>
        Try again
      </button>
    </div>
  );
}
