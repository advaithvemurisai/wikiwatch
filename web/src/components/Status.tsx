// Status marks: a shape plus a label, so state never depends on color alone.

type Level = "good" | "warning" | "serious" | "critical" | "neutral";

const COLOR: Record<Level, string> = {
  good: "var(--good)",
  warning: "var(--warning)",
  serious: "var(--serious)",
  critical: "var(--critical)",
  neutral: "var(--ink-muted)",
};

function Icon({ level }: { level: Level }) {
  const fill = COLOR[level];
  switch (level) {
    case "critical": // triangle
      return (
        <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
          <path d="M6 1 11 11H1Z" fill={fill} />
        </svg>
      );
    case "serious":
    case "warning": // diamond
      return (
        <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
          <path d="M6 0.5 11.5 6 6 11.5 0.5 6Z" fill={fill} />
        </svg>
      );
    case "good": // check in a circle
      return (
        <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
          <circle cx="6" cy="6" r="6" fill={fill} />
          <path d="M3.3 6.2 5.2 8 8.8 4.2" stroke="#fff" strokeWidth="1.6" fill="none" />
        </svg>
      );
    default: // ring
      return (
        <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
          <circle cx="6" cy="6" r="4.5" stroke={fill} strokeWidth="2" fill="none" />
        </svg>
      );
  }
}

export function Status({ level, children }: { level: Level; children: React.ReactNode }) {
  return (
    <span className="status">
      <Icon level={level} />
      {children}
    </span>
  );
}

const SEVERITY_LEVEL = { high: "critical", medium: "warning", low: "neutral" } as const;

export function SeverityBadge({ severity }: { severity: "high" | "medium" | "low" }) {
  const label = severity[0]!.toUpperCase() + severity.slice(1);
  return <Status level={SEVERITY_LEVEL[severity]}>{label}</Status>;
}
