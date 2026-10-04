"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import styles from "./Nav.module.css";

const LINKS = [
  { href: "/", label: "Alerts" },
  { href: "/baseline", label: "Baseline" },
  { href: "/health", label: "Pipeline health" },
];

export function Nav() {
  const pathname = usePathname();
  return (
    <header className={styles.header}>
      <div className={styles.inner}>
        <Link href="/" className={styles.brand}>
          WikiWatch
        </Link>
        <nav aria-label="Main">
          <ul className={styles.links}>
            {LINKS.map(({ href, label }) => (
              <li key={href}>
                <Link
                  href={href}
                  className={styles.link}
                  aria-current={pathname === href ? "page" : undefined}
                >
                  {label}
                </Link>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </header>
  );
}
