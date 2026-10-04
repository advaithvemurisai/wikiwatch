// Small pieces shared by the charts: legend, tooltip rows and the table view.

export interface LegendItem {
  label: string;
  color: string;
  shape: "line" | "rect";
}

export function Legend({ items }: { items: LegendItem[] }) {
  return (
    <ul className="legend">
      {items.map((item) => (
        <li key={item.label}>
          <span
            className={item.shape === "line" ? "key-line" : "key-rect"}
            style={{ background: item.color }}
            aria-hidden="true"
          />
          {item.label}
        </li>
      ))}
    </ul>
  );
}

export function TooltipBox({ title, rows }: { title: string; rows: { label: string; value: string; color: string }[] }) {
  return (
    <div className="tooltip">
      <div className="tooltip-title">{title}</div>
      {rows.map((row) => (
        <div className="tooltip-row" key={row.label}>
          <span className="key-line" style={{ background: row.color }} aria-hidden="true" />
          <strong>{row.value}</strong>
          <span className="secondary">{row.label}</span>
        </div>
      ))}
    </div>
  );
}

/** Every charted value, readable without hovering. */
export function TableView({
  caption,
  columns,
  rows,
}: {
  caption: string;
  columns: string[];
  rows: (string | number)[][];
}) {
  return (
    <details style={{ marginTop: 8 }}>
      <summary className="small secondary" style={{ cursor: "pointer" }}>
        Show as table
      </summary>
      <div className="table-wrap" style={{ marginTop: 8, maxHeight: 320, overflowY: "auto" }}>
        <table>
          <caption className="sr-only">{caption}</caption>
          <thead>
            <tr>
              {columns.map((c, i) => (
                <th key={c} scope="col" className={i === 0 ? undefined : "num"}>
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, r) => (
              <tr key={r}>
                {row.map((cell, i) => (
                  <td key={i} className={i === 0 ? "nowrap" : "num"}>
                    {cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  );
}

export const AXIS_TICK = { fill: "var(--ink-muted)", fontSize: 12 };
