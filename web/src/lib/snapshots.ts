// The one module that reads dashboard snapshots. Server only: the AWS SDK, the bucket
// name and any credentials never reach the browser.
//
// SNAPSHOT_SOURCE picks where snapshots come from:
//   fixtures (default)  web/fixtures/dashboard/v1/ (local dev, Vercel previews, CI)
//   s3                  s3://$SNAPSHOT_BUCKET/dashboard/v1/
//                       - SeaweedFS locally: S3_ENDPOINT + the local S3 keys
//                       - Vercel production: AWS_ROLE_ARN, exchanged for short-lived
//                         credentials through Vercel OIDC (no stored AWS keys)
import "server-only";

import { readFile } from "node:fs/promises";
import path from "node:path";

import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { awsCredentialsProvider } from "@vercel/oidc-aws-credentials-provider";

import type { SnapshotName, SnapshotResult, Snapshots } from "./types";
import { parseSnapshot } from "./validate";

const PREFIX = "dashboard/v1/";

let s3: S3Client | undefined;

function s3Client(): S3Client {
  if (!s3) {
    const endpoint = process.env.S3_ENDPOINT;
    const roleArn = process.env.AWS_ROLE_ARN;
    s3 = new S3Client({
      region: process.env.AWS_REGION ?? "us-east-1",
      ...(endpoint ? { endpoint, forcePathStyle: true } : {}),
      ...(roleArn ? { credentials: awsCredentialsProvider({ roleArn }) } : {}),
    });
  }
  return s3;
}

async function readText(name: SnapshotName): Promise<string> {
  const key = `${PREFIX}${name}.json`;
  if ((process.env.SNAPSHOT_SOURCE ?? "fixtures") === "s3") {
    const bucket = process.env.SNAPSHOT_BUCKET;
    if (!bucket) throw new Error("SNAPSHOT_BUCKET is not set");
    const response = await s3Client().send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    if (!response.Body) throw new Error("empty response body");
    return response.Body.transformToString();
  }
  return readFile(path.join(process.cwd(), "fixtures", key), "utf8");
}

/** Load and validate one snapshot. Failures become a problem code, never an exception. */
export async function loadSnapshot<N extends SnapshotName>(name: N): Promise<SnapshotResult<N>> {
  let text: string;
  try {
    text = await readText(name);
  } catch (error) {
    // Log only the error class: messages can contain bucket names or endpoints.
    console.error(`snapshot ${name}: unavailable (${(error as Error)?.name ?? "Error"})`);
    return { ok: false, problem: "unavailable" };
  }
  const result = parseSnapshot(name, text);
  if (!result.ok) console.error(`snapshot ${name}: ${result.problem}`);
  return result;
}

export type { SnapshotName, SnapshotResult, Snapshots };

/**
 * The time of this server render. Pages are rendered once per revalidation (every 5
 * minutes), so this is the "as of" time of the page; client components receive it as a
 * prop so server and browser compute the same "time ago" text.
 */
export function renderTime(): number {
  return Date.now();
}
