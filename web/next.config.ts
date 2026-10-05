import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  poweredByHeader: false,
  // The fixture snapshots are read from disk at request time (ISR), so ship them
  // with every server function.
  outputFileTracingIncludes: { "/*": ["./fixtures/**/*"] },
};

export default nextConfig;
