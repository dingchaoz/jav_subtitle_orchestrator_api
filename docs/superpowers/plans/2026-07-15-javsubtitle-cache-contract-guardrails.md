# JavSubtitle Cache Contract Guardrails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make javsubtitle.com use one versioned v4 movie-cache contract and block production deployments that could restore stale legacy subtitle payloads.

**Architecture:** Integrate the currently deployed admin catalog sync into a fresh canonical website task branch, centralize full/light key construction behind a machine-readable v4 contract, and make movie reads fall through to D1 on an active-key miss without trusting legacy keys. Verify D1 after admin sync, expose the schema in health responses, enforce canonical clean-main deploys and non-downgrade preflight, and run exact subtitle-ID canaries after deployment.

**Tech Stack:** TypeScript, Cloudflare Workers, D1, KV, Node test runner, Bash, Wrangler

---

## Execution Boundary

This plan modifies the separate `jav_subtitle_com` repository. Do not implement it in the orchestrator checkout. Start from the canonical website repository and follow `AGENTS.md`, `docs/agent-playbook/project-baseline.md`, and `docs/agent-playbook/release-guardrails.md`.

The currently deployed sync implementation is represented by commits `e316781` and `e700ed2` in `/Users/ytt/Documents/startup/jav_subtitle_com-admin-sync-production`. Integrate those commits into a fresh canonical task branch before applying the v4 contract. Never deploy from that old worktree.

## File Structure

- Create `apps/edge-api/catalog-cache-contract.json`: machine-readable active schema (`v4`).
- Create `apps/edge-api/src/utils/movieCacheKeys.ts`: the only full/light cache-key constructors.
- Modify `apps/edge-api/src/handlers/movie.ts`: v4 reads/writes and D1 fallback behavior.
- Modify `apps/edge-api/src/utils/catalogSync.ts`: v4 writes/invalidation, D1 readback, and response schema.
- Modify `apps/edge-api/src/handlers/adminCatalogSync.ts`: return the strengthened sync result unchanged.
- Modify `apps/edge-api/src/handlers/health.ts`: expose `catalogCacheSchemaVersion`.
- Create `apps/edge-api/test/movieCacheKeyContract.test.mjs`: helper/source-contract coverage.
- Modify `apps/edge-api/test/movieCatalogD1.test.ts`: active-miss/legacy-stale regression.
- Modify `apps/edge-api/test/adminCatalogSync.test.mjs`: D1 readback and v4 response coverage.
- Modify `apps/edge-api/test/health.test.mjs` or create it if absent: health schema coverage.
- Create `deployment/scripts/catalog-cache-schema-check.mjs`: predeploy version comparison.
- Create `deployment/scripts/catalog-cache-schema-check.test.mjs`: initialization/downgrade tests.
- Create `deployment/config/catalog-subtitle-canaries.json`: reviewed exact public subtitle identities.
- Create `deployment/scripts/catalog-subtitle-canary.mjs`: read-only postdeploy canary.
- Create `deployment/scripts/catalog-subtitle-canary.test.mjs`: canary behavior tests.
- Modify `deployment/scripts/deploy-production.sh`: clean canonical-main checks, schema preflight, and canary execution.
- Create `deployment/scripts/deploy-production-context.test.mjs`: shell-context contract tests.
- Modify `docs/agent-playbook/project-baseline.md`: v4 cache invariant.
- Modify `docs/agent-playbook/release-guardrails.md`: initialization, preflight, and canary runbook.

### Task 1: Create a canonical website task branch from the production baseline

**Files:**
- No functional source changes in this task.

- [ ] **Step 1: Confirm canonical repository context**

Run from `/Users/ytt/Documents/startup/jav_subtitle_com`:

```bash
npm run task:context
git status --short --branch
git worktree list --porcelain
```

Expected: context output identifies the canonical repository and shows existing user changes. Do not alter or stage them.

- [ ] **Step 2: Create a fresh isolated task branch/worktree**

Run:

```bash
npm run task:start -- --tool codex --name catalog-cache-contract-v4
```

Expected: a new `task/codex/catalog-cache-contract-v4` branch and isolated worktree based on canonical `main`.

- [ ] **Step 3: Integrate the deployed admin sync commits**

In the new worktree:

```bash
git cherry-pick e316781 e700ed2
```

Expected: the thin admin handler calls `syncSubtitleCatalog`, the sync utility supports subtitle-only catalog entries, and movie cache reads use the deployed unversioned contract before v4 work begins.

- [ ] **Step 4: Verify the production-baseline tests**

Run:

```bash
cd apps/edge-api
npm test
npx tsc --noEmit
```

Expected: PASS. If cherry-pick conflicts expose later canonical changes, resolve by preserving both the deployed sync behavior and the newer canonical behavior, then rerun these commands before continuing.

### Task 2: One machine-readable v4 cache-key contract

**Files:**
- Create: `apps/edge-api/catalog-cache-contract.json`
- Create: `apps/edge-api/src/utils/movieCacheKeys.ts`
- Create: `apps/edge-api/test/movieCacheKeyContract.test.mjs`
- Modify: `apps/edge-api/tsconfig.json` only if JSON imports are not already enabled.

- [ ] **Step 1: Write failing helper and source-contract tests**

Create tests that require:

```javascript
assert.equal(MOVIE_FULL_CACHE_SCHEMA_VERSION, "v4");
assert.equal(fullMovieCacheKey("KTB-111"), "movie:full:v4:ktb-111");
assert.equal(lightMovieCacheKey(" KTB-111 "), "movie:light:ktb-111");
assert.deepEqual(legacyFullMovieCacheKeys("ktb-111"), [
  "movie:full:v3:ktb-111",
  "movie:full:ktb-111",
]);
```

The same test recursively scans `apps/edge-api/src` and rejects the text `movie:full:` outside `utils/movieCacheKeys.ts`. It also rejects `movie:light:` outside that helper.

- [ ] **Step 2: Run tests and confirm missing helper**

Run: `cd apps/edge-api && node --test test/movieCacheKeyContract.test.mjs`

Expected: FAIL because the helper and contract file do not exist.

- [ ] **Step 3: Add the contract file**

Create `apps/edge-api/catalog-cache-contract.json`:

```json
{
  "fullMovieSchemaVersion": "v4",
  "legacyFullMovieSchemaVersions": ["v3", "legacy"],
  "lightMovieSchemaVersion": "legacy"
}
```

- [ ] **Step 4: Implement the only key constructors**

Create:

```typescript
import contract from "../../catalog-cache-contract.json";

export const MOVIE_FULL_CACHE_SCHEMA_VERSION = contract.fullMovieSchemaVersion;

function normalizeCode(code: string): string {
  const normalized = code.trim().toLowerCase();
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)+$/.test(normalized)) {
    throw new Error("movie_cache_code_invalid");
  }
  return normalized;
}

export function fullMovieCacheKey(code: string): string {
  return `movie:full:${MOVIE_FULL_CACHE_SCHEMA_VERSION}:${normalizeCode(code)}`;
}

export function lightMovieCacheKey(code: string): string {
  return `movie:light:${normalizeCode(code)}`;
}

export function legacyFullMovieCacheKeys(code: string): string[] {
  const normalized = normalizeCode(code);
  return [`movie:full:v3:${normalized}`, `movie:full:${normalized}`];
}
```

- [ ] **Step 5: Run helper tests and commit**

Run: `cd apps/edge-api && node --test test/movieCacheKeyContract.test.mjs && npx tsc --noEmit`

Expected: source-contract test still FAILS and lists existing literal sites; helper unit assertions PASS. The source-contract test becomes green in Tasks 3 and 4.

```bash
git add apps/edge-api/catalog-cache-contract.json \
  apps/edge-api/src/utils/movieCacheKeys.ts \
  apps/edge-api/test/movieCacheKeyContract.test.mjs
git diff --quiet -- apps/edge-api/tsconfig.json || \
  git add apps/edge-api/tsconfig.json
git commit -m "feat(api): define v4 movie cache contract"
```

### Task 3: Make movie reads trust only v4 and fall through to D1

**Files:**
- Modify: `apps/edge-api/src/handlers/movie.ts`
- Modify: `apps/edge-api/test/movieCatalogD1.test.ts`
- Modify: `apps/edge-api/test/movieCacheKeyContract.test.mjs`

- [ ] **Step 1: Write the stale-legacy regression test**

Add a test with:

- `movie:full:ktb-111` containing `subtitles: []`;
- `movie:full:v3:ktb-111` containing the expected subtitle;
- no `movie:full:v4:ktb-111` entry;
- D1 containing the expected subtitle.

Assert the handler returns `X-Catalog-Backend: D1`, contains the expected subtitle ID, never reads either legacy key, and schedules a put only to `movie:full:v4:ktb-111`.

```typescript
assert.equal(response.headers.get("X-Catalog-Backend"), "D1");
assert.deepEqual((await response.json()).subtitles.map((row) => row.id), [EXPECTED_ID]);
assert.deepEqual(kv.gets, ["movie:full:v4:ktb-111"]);
assert.deepEqual(kv.puts.map((row) => row.key), ["movie:full:v4:ktb-111"]);
```

- [ ] **Step 2: Run the regression and confirm old-key behavior**

Run: `cd apps/edge-api && node --test --test-name-pattern='stale legacy' test/movieCatalogD1.test.ts`

Expected: FAIL because the handler reads the unversioned key.

- [ ] **Step 3: Replace full/light read and write literals with helper calls**

Import `fullMovieCacheKey` and `lightMovieCacheKey`. Replace the key construction in `readCachedMovie`, `readCachedMovieLight`, `writeCachedMovie`, `writeCachedMovieLight`, and safe-delete paths. Do not call `legacyFullMovieCacheKeys` from the read path.

Required read shape:

```typescript
const key = fullMovieCacheKey(code);
const cached = await env.KV_MOVIE_CACHE.get<unknown>(key, "json");
```

On a v4 miss, retain the existing D1-first hybrid fallback and populate v4 through the existing `executionCtx.waitUntil` path.

- [ ] **Step 4: Run movie and source-contract tests**

Run:

```bash
cd apps/edge-api
node --test test/movieCatalogD1.test.ts test/movieCacheKeyContract.test.mjs
```

Expected: stale-legacy regression PASS; source-contract test may still list sync utility literals only.

- [ ] **Step 5: Commit the reader migration**

```bash
git add apps/edge-api/src/handlers/movie.ts \
  apps/edge-api/test/movieCatalogD1.test.ts \
  apps/edge-api/test/movieCacheKeyContract.test.mjs
git commit -m "fix(api): bypass stale legacy movie cache keys"
```

### Task 4: Make admin sync write v4 and verify D1 identity

**Files:**
- Modify: `apps/edge-api/src/utils/catalogSync.ts`
- Modify: `apps/edge-api/src/handlers/adminCatalogSync.ts`
- Modify: `apps/edge-api/test/adminCatalogSync.test.mjs`
- Modify: `apps/edge-api/test/movieCacheKeyContract.test.mjs`

- [ ] **Step 1: Write failing response and readback tests**

For an ordinary `ktb-111` sync, assert:

```javascript
assert.equal(result.cacheSchemaVersion, "v4");
assert.equal(result.results[0].d1RowsUpdated, 1);
assert.equal(result.results[0].d1Verified, true);
assert.equal(result.results[0].kvAction, "written");
assert.deepEqual(result.results[0].kvKeysTouched, [
  "movie:full:v4:ktb-111",
  "movie:light:ktb-111",
]);
```

Add a fake-D1 variant where write calls succeed but readback returns no expected subtitle ID. Assert the response is unsuccessful with safe error `catalog_d1_verification_failed` and no KV put occurs.

- [ ] **Step 2: Run focused tests and confirm missing fields/readback**

Run: `cd apps/edge-api && node --test --test-name-pattern='v4|readback' test/adminCatalogSync.test.mjs`

Expected: FAIL.

- [ ] **Step 3: Replace sync key literals with shared helpers**

Use `fullMovieCacheKey` and `lightMovieCacheKey` in:

- ordinary dry-run key reporting;
- `writeKvMovieGroup`;
- prune invalidation;
- claim-fence best-effort deletion;
- any alias/variant loop.

Legacy keys may be deleted best-effort after the active v4 write, but they must not be returned as the active contract and a legacy-delete failure must not invalidate a verified active write.

- [ ] **Step 4: Add exact D1 readback**

After `writeD1MovieGroup` and before KV writes, query both `catalog_movies.subtitles_json` and `catalog_subtitle_tracks` for every canonical group. Compare sorted expected subtitle IDs from the mapped Supabase rows with sorted D1 IDs.

Required helper contract:

```typescript
async function verifyD1MovieGroup(
  env: Pick<Env, "CATALOG_DB">,
  movies: MappedMovie[],
): Promise<number> {
  // Return the number of catalog_movies rows whose exact subtitle IDs match.
  // Throw CatalogD1VerificationError on missing, duplicate, malformed, or mismatched IDs.
}
```

Set `d1RowsUpdated` from the verified row count, not `movies.length` before readback.

- [ ] **Step 5: Extend the response types**

Add top-level:

```typescript
cacheSchemaVersion: typeof MOVIE_FULL_CACHE_SCHEMA_VERSION;
```

Add per result:

```typescript
d1Verified: boolean;
kvAction: "written" | "deleted_for_d1_fallback" | "unchanged";
```

Retain `kvKeysDeleted` as a compatibility alias for `kvKeysTouched` during this release so the existing orchestrator remains compatible. Document it as deprecated in code, and keep both arrays identical for active keys.

Coordinate this response change with Task 1 of the orchestrator plan. The
orchestrator validator must be deployed first and accept both the currently
deployed response and this exact strengthened v4 response. Do not deploy the
website response until that compatibility test is green.

- [ ] **Step 6: Run sync and source-contract tests**

Run:

```bash
cd apps/edge-api
node --test test/adminCatalogSync.test.mjs test/movieCacheKeyContract.test.mjs
npx tsc --noEmit
```

Expected: PASS, including no stray full/light key literals.

- [ ] **Step 7: Commit the strengthened sync contract**

```bash
git add apps/edge-api/src/utils/catalogSync.ts \
  apps/edge-api/src/handlers/adminCatalogSync.ts \
  apps/edge-api/test/adminCatalogSync.test.mjs \
  apps/edge-api/test/movieCacheKeyContract.test.mjs
git commit -m "fix(api): verify and publish v4 catalog cache entries"
```

### Task 5: Expose cache schema in health

**Files:**
- Modify: `apps/edge-api/src/handlers/health.ts`
- Create: `apps/edge-api/test/health.test.mjs` if no health test exists.

- [ ] **Step 1: Write a failing health response test**

```javascript
const response = await healthHandler(fakeContext());
const body = await response.json();
assert.equal(body.ok, true);
assert.equal(body.catalogCacheSchemaVersion, "v4");
assert.equal(response.headers.get("Cache-Control"), "no-store");
```

- [ ] **Step 2: Run and confirm the field is absent**

Run: `cd apps/edge-api && node --test test/health.test.mjs`

Expected: FAIL with `undefined !== "v4"`.

- [ ] **Step 3: Add the safe field**

Import `MOVIE_FULL_CACHE_SCHEMA_VERSION` and add:

```typescript
catalogCacheSchemaVersion: MOVIE_FULL_CACHE_SCHEMA_VERSION,
```

Do not expose binding IDs, secrets, commit-local paths, or credentials.

- [ ] **Step 4: Test and commit**

Run: `cd apps/edge-api && node --test test/health.test.mjs && npx tsc --noEmit`

Expected: PASS.

```bash
git add apps/edge-api/src/handlers/health.ts apps/edge-api/test/health.test.mjs
git commit -m "feat(api): expose catalog cache schema version"
```

### Task 6: Enforce canonical clean-main production context

**Files:**
- Modify: `deployment/scripts/deploy-production.sh:82-126`
- Create: `deployment/scripts/deploy-production-context.test.mjs`

- [ ] **Step 1: Write failing shell-context contract tests**

The test copies the script into temporary fake repositories and stubs `git`, `pwd`, and downstream commands. Cover:

- task branch rejected;
- dirty tree rejected;
- linked worktree rejected even when its path lacks `.codex/worktrees`;
- canonical clean `main` accepted through build-only preflight.

Expected stable error codes in output:

```text
production_branch_must_be_main
production_worktree_must_be_clean
production_deploy_requires_primary_worktree
```

- [ ] **Step 2: Run and confirm current checks are insufficient**

Run: `node --test deployment/scripts/deploy-production-context.test.mjs`

Expected: FAIL for branch, dirty tree, and generic linked-worktree cases.

- [ ] **Step 3: Strengthen `check_execution_context`**

After the current root check, require:

```bash
branch="$(git branch --show-current)"
[[ "${branch}" == "main" ]] || {
  error "production_branch_must_be_main"
  exit 1
}

[[ -z "$(git status --porcelain --untracked-files=normal)" ]] || {
  error "production_worktree_must_be_clean"
  exit 1
}

primary_worktree="$(git worktree list --porcelain | awk '/^worktree / {print substr($0,10); exit}')"
[[ "$(cd "${primary_worktree}" && pwd -P)" == "${PROJECT_ROOT}" ]] || {
  error "production_deploy_requires_primary_worktree"
  exit 1
}
```

Retain the existing `pwd -P` root requirement and do not add a normal override flag.

- [ ] **Step 4: Run tests and commit**

Run: `node --test deployment/scripts/deploy-production-context.test.mjs`

Expected: PASS.

```bash
git add deployment/scripts/deploy-production.sh \
  deployment/scripts/deploy-production-context.test.mjs
git commit -m "fix(deploy): require canonical clean main releases"
```

### Task 7: Block cache-schema downgrades and guard one-time v4 initialization

**Files:**
- Create: `deployment/scripts/catalog-cache-schema-check.mjs`
- Create: `deployment/scripts/catalog-cache-schema-check.test.mjs`
- Modify: `deployment/scripts/deploy-production.sh`

- [ ] **Step 1: Write failing version-comparison tests**

Test candidate v4 against production v4 (pass), v5 (reject downgrade), v3 (pass upgrade), missing field without initialization (reject), and missing field with exact `--initialize v4` (pass).

```javascript
assert.deepEqual(
  await checkSchema({ candidate: "v4", production: "v4" }),
  { action: "unchanged", candidate: "v4", production: "v4" },
);
await assert.rejects(
  checkSchema({ candidate: "v4", production: "v5" }),
  /catalog_cache_schema_downgrade_rejected/,
);
```

- [ ] **Step 2: Run and confirm helper is missing**

Run: `node --test deployment/scripts/catalog-cache-schema-check.test.mjs`

Expected: FAIL during import.

- [ ] **Step 3: Implement the schema checker**

The script reads candidate version from `apps/edge-api/catalog-cache-contract.json`, fetches `/api/health` with `Cache-Control: no-cache`, parses only `v<positive integer>`, and compares integers. Support:

```bash
node deployment/scripts/catalog-cache-schema-check.mjs \
  --origin https://javsubtitle.com \
  --initialize v4
```

Initialization is accepted only when production omits the field and the exact candidate equals the explicit initialization value.

- [ ] **Step 4: Wire preflight and the one-time flag**

Add deploy option:

```text
--initialize-catalog-cache-schema v4
```

Pass it only to the schema checker. After production exposes v4, the flag is unnecessary and supplying it when production already has a version is rejected.

Run the checker during API-capable preflight before `wrangler deploy`. Web-only and SEO-only deploys skip it.

- [ ] **Step 5: Test and commit**

Run:

```bash
node --test deployment/scripts/catalog-cache-schema-check.test.mjs \
  deployment/scripts/deploy-production-context.test.mjs
```

Expected: PASS.

```bash
git add deployment/scripts/catalog-cache-schema-check.mjs \
  deployment/scripts/catalog-cache-schema-check.test.mjs \
  deployment/scripts/deploy-production.sh
git commit -m "fix(deploy): block catalog cache schema downgrades"
```

### Task 8: Exact read-only postdeploy subtitle canaries

**Files:**
- Create: `deployment/config/catalog-subtitle-canaries.json`
- Create: `deployment/scripts/catalog-subtitle-canary.mjs`
- Create: `deployment/scripts/catalog-subtitle-canary.test.mjs`
- Modify: `deployment/scripts/deploy-production.sh`

- [ ] **Step 1: Add the reviewed canary fixture**

```json
{
  "schemaVersion": 1,
  "movies": [
    {
      "code": "ktb-111",
      "subtitleId": "fc9bed2a-f432-45a6-b7f9-bf141dd61810"
    },
    {
      "code": "iene-963",
      "subtitleId": "abc955c5-52eb-4d33-a902-7f702523a0f2"
    }
  ]
}
```

- [ ] **Step 2: Write failing canary tests**

Cover exact success, missing ID, duplicate ID, wrong canonical code, HTTP failure, redirect, invalid fixture, and confirmation that the script sends GET only.

- [ ] **Step 3: Run tests and confirm the script is missing**

Run: `node --test deployment/scripts/catalog-subtitle-canary.test.mjs`

Expected: FAIL during import.

- [ ] **Step 4: Implement the read-only canary**

For every fixture entry, GET:

```text
<origin>/api/movie/<code>?cacheNonce=postdeploy-<subtitle-id>
```

Use `redirect: "manual"`, 20-second abort timeout, browser-like user agent, `Cache-Control: no-cache`, exact canonical-code match, and exactly one matching subtitle ID. Print one safe line per movie and exit nonzero on the first failure.

- [ ] **Step 5: Run canaries after API deployment**

Add to `run_health_checks` after `/api/health` and before generic movie smoke:

```bash
node "${PROJECT_ROOT}/deployment/scripts/catalog-subtitle-canary.mjs" \
  --origin "https://${DOMAIN}" \
  --fixture "${PROJECT_ROOT}/deployment/config/catalog-subtitle-canaries.json"
```

Build-only and dry-run print the exact would-run command without network access.

- [ ] **Step 6: Test and commit**

Run:

```bash
node --test deployment/scripts/catalog-subtitle-canary.test.mjs \
  deployment/scripts/deploy-production-context.test.mjs
```

Expected: PASS.

```bash
git add deployment/config/catalog-subtitle-canaries.json \
  deployment/scripts/catalog-subtitle-canary.mjs \
  deployment/scripts/catalog-subtitle-canary.test.mjs \
  deployment/scripts/deploy-production.sh
git commit -m "test(deploy): require exact subtitle visibility canaries"
```

### Task 9: Full verification and knowledge writeback

**Files:**
- Modify: `docs/agent-playbook/project-baseline.md`
- Modify: `docs/agent-playbook/release-guardrails.md`

- [ ] **Step 1: Document stable invariants**

Record:

- active full key is `movie:full:v4:<code>`;
- only `movieCacheKeys.ts` constructs full/light keys;
- legacy full keys are never read;
- active miss falls through to D1;
- sync verifies exact D1 subtitle IDs before touching KV;
- production deploy requires primary clean `main`;
- schema downgrade is blocked;
- KTB-111 and IENE-963 canaries are mandatory after API deploy.

- [ ] **Step 2: Run the full Edge API suite and typecheck**

Run:

```bash
cd apps/edge-api
npm test
npx tsc --noEmit
```

Expected: PASS.

- [ ] **Step 3: Run deployment helper tests and build-only preflight**

From repository root:

```bash
node --test deployment/scripts/*.test.mjs
./deployment/scripts/deploy-production.sh --build-only --yes \
  --initialize-catalog-cache-schema v4
```

Expected: PASS without deploying or mutating production.

- [ ] **Step 4: Checkpoint the task branch**

```bash
npm run task:checkpoint -- --message "fix(api): enforce v4 catalog cache contract"
```

Expected: committed task checkpoint with only reviewed website files.

- [ ] **Step 5: Stop for explicit deployment approval**

Report the branch, commit, build/test evidence, candidate schema `v4`, current production schema observation, and planned canary identities. Do not run a production deploy until the user explicitly approves it.

- [ ] **Step 6: After approval, integrate and deploy from canonical main**

Finish the task through the repository workflow, integrate into canonical `main`, require a clean primary worktree, then run:

```bash
./deployment/scripts/deploy-production.sh --api-only --yes \
  --initialize-catalog-cache-schema v4
```

The initialization flag is used only for the first v4 deployment. Expected postdeploy evidence:

- `/api/health` reports `catalogCacheSchemaVersion: "v4"`;
- KTB-111 exact subtitle canary passes;
- IENE-963 exact subtitle canary passes;
- generic catalog and playback smoke remain green.

- [ ] **Step 7: Run the orchestrator full visibility audit after deployment**

Return to the orchestrator repository and run the GET-only audit from the first plan over all verified ready receipts. Expected: no new repair-eligible mismatches caused by the website deployment.
