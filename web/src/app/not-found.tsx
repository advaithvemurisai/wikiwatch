import Link from "next/link";

export default function NotFound() {
  return (
    <div className="card">
      <h2>Page not found</h2>
      <p className="secondary" style={{ margin: "6px 0 0" }}>
        Try <Link href="/">Alerts</Link>, <Link href="/baseline">Baseline</Link> or{" "}
        <Link href="/health">Pipeline health</Link>.
      </p>
    </div>
  );
}
