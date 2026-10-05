# Vercel setup (dashboard)

The site in `web/` is a Next.js app on Vercel's free Hobby plan. It never queries Athena:
it reads the four JSON snapshots under `dashboard/v1/` (docs/v1.md, "How data reaches the
app"). No AWS key exists anywhere. Production exchanges a short-lived Vercel OIDC token
for credentials of the `wikiwatch-vercel-dashboard` role, which can only `s3:GetObject`
under `dashboard/` (infra/foundation/iam.tf).

| Environment | Snapshots from | AWS access |
| --- | --- | --- |
| Local (`make web`) | `web/fixtures/dashboard/v1/` | none |
| Local (`make web-s3`) | SeaweedFS, `dashboard/v1/` in the local lake bucket | local SeaweedFS keys from `.env.local` |
| Vercel preview (every PR) | `web/fixtures/` (no env vars set) | none; the role trusts production only |
| Vercel production (`main`) | S3, `dashboard/v1/` in the lake bucket | Vercel OIDC to the read-only role |

## Environment variables (server only)

None of these is a `NEXT_PUBLIC_` variable, and none reaches the browser: the only module
that reads them is `web/src/lib/snapshots.ts`, which imports `server-only`.

| Variable | Production | Preview / Development |
| --- | --- | --- |
| `SNAPSHOT_SOURCE` | `s3` | unset (defaults to `fixtures`) |
| `SNAPSHOT_BUCKET` | the `lake_bucket` Terraform output | unset |
| `AWS_REGION` | the region the lake is in (`us-east-1` by default) | unset |
| `AWS_ROLE_ARN` | the `vercel_role_arn` Terraform output | unset |

Copy the two outputs from CloudShell straight into the Vercel dashboard. Do not paste them
into chat, issues, commits or this repo.

## One-time project setup

1. In Vercel, **Add New > Project** and import the GitHub repository.
2. **Root Directory:** `web`. Framework preset: Next.js. Leave build and install commands
   at their defaults (`npm run build` runs `prebuild`, which copies `schemas/dashboard/`
   into the app).
3. Keep **Include files outside the Root Directory in the Build Step** enabled (the
   default): the build needs `../schemas/dashboard/`. If it is off, the build fails with
   "schemas/dashboard/ is missing", never silently.
4. **Settings > General > Node.js Version:** 24.x (also pinned in `web/package.json`).
5. **Project name** must equal the Terraform variable `vercel_project_name` (default
   `wikiwatch`), because the role's trust policy matches
   `owner:<team>:project:<project>:environment:production`.
6. **Settings > Security > Secure Backend Access with OIDC Federation:** enable it in
   **Team** issuer mode. The issuer is `https://oidc.vercel.com/<team slug>` and the
   audience `https://vercel.com/<team slug>`, which is what `infra/foundation` trusts
   (`TF_VAR_vercel_team_slug`).
7. **Settings > Environment Variables:** add the four variables above for
   **Production only**. Leave Preview and Development empty, so pull request previews
   build on fixtures and cannot reach AWS even by mistake (the role would refuse them anyway).
8. **Settings > Git:** production branch `main`. Previews for pull requests are on by
   default.

No `vercel.json` is needed: the defaults (Next.js preset, ISR) are what the app uses.

## Checks after the first production deploy

- [ ] A pull request gets a preview deployment that builds and shows the fixture data
      (alerts for Ben & Jerry's, Unilever and Marmite; the "Pipeline offline" banner).
- [ ] Production shows the data of the last session. The Pipeline health page's
      "Health checked" time matches the last `freshness_monitor` run.
- [ ] After `demo-down` (compute destroyed), production still loads, and within
      15 minutes of the last `health.json` it shows "Pipeline offline since <time>,
      showing last session".
- [ ] No page shows a bucket name, ARN, account ID, hostname or IP (view the page source too).

## How it fails

- A missing or unreadable snapshot (for example before the first export, or when the role
  is misconfigured) shows "<section> unavailable" with a generic message. The server log
  has only the snapshot name and the error class (for example `NoSuchKey`,
  `AccessDenied`), never a bucket name or endpoint.
- A snapshot with a different `schema_version`, or one that fails its JSON Schema, is not
  shown: the page says the data is in a format the site does not understand yet.
- Pages are static with incremental regeneration every 5 minutes (`revalidate = 300`), so
  page views do not call S3; at most one regeneration per page per 5 minutes does.
