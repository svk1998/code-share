# Plan: Internal fork of vercel-labs/skills CLI for GitHub Enterprise

## Context

We are forking https://github.com/vercel-labs/skills (MIT, v1.5.11) to create an
internal skills installer that works entirely inside our GitHub Enterprise (GHE)
network. The upstream CLI installs agent skills (SKILL.md directories) into agent
paths (Cline → `.agents/skills/`, Claude Code → `.claude/skills/`, etc.).

Goals of the fork:
1. `owner/repo` shorthand resolves to our GHE host, not github.com
2. Telemetry permanently disabled (no calls to vercel.sh)
3. `skills find` does not call skills.sh (point to internal docs instead)
4. Bare `skills add` defaults to our internal skills monorepo
5. Distributable as a tarball attached to a GHE release via CI (no public npm)

Placeholders to replace throughout (ask me if not provided):
- `GHE_HOST` = github.your-company.com
- `DEFAULT_SKILLS_REPO` = platform/agent-skills
- `FORK_ORG_REPO` = platform/skills

Facts already verified (do not re-litigate):
- Build: `pnpm install && pnpm build` (obuild) → `dist/` ~430 kB, all libs bundled
- Only runtime dependency: `yaml`. `npm pack` yields a clean 14-file tarball.
- Node >= 18, packageManager pnpm@10.

## Constraints

- Keep every patch minimal and isolated so future upstream rebases are easy.
  Prefer adding a single new config module over scattering literals.
- Do NOT reformat untouched files, do not bump deps, do not touch `tests/` except
  to fix tests broken by these changes and to add the new ones listed below.
- Keep `LICENSE` (add MIT LICENSE file if missing) and `ThirdPartyNoticeText.txt`.
- All new behavior must remain overridable by env vars for testing.

---

## Step 0 — Setup

1. Clone the fork (or upstream if fork not created yet):
   `git clone https://GHE_HOST/FORK_ORG_REPO.git && cd skills`
   If working from upstream source, add remote `upstream` → vercel-labs/skills.
2. Create branch `feat/internal-fork`.
3. `npm i -g pnpm@10 && pnpm install` and confirm `pnpm build` + `pnpm test`
   pass BEFORE any changes (baseline). Record baseline test results.

## Step 1 — Central config module

Create `src/company-config.ts`:

```typescript
export const GIT_HOST = process.env.SKILLS_GIT_HOST || 'GHE_HOST';
export const DEFAULT_SOURCE = process.env.SKILLS_DEFAULT_SOURCE || 'DEFAULT_SKILLS_REPO';
export const INTERNAL_DOCS_URL = `https://${'GHE_HOST'}/DEFAULT_SKILLS_REPO`;
```

## Step 2 — GHE shorthand resolution (src/source-parser.ts)

1. Import `GIT_HOST` from `./company-config`.
2. In the `@skill` shorthand block (regex `^([^/]+)\/([^/@]+)@(.+)$`, ~line 370)
   and the general shorthand block (regex `^([^/]+)\/([^/]+)(?:\/(.+?))?\/?$`,
   ~line 380), replace:
   `url: \`https://github.com/${owner}/${repo}.git\``
   with:
   `url: \`https://${GIT_HOST}/${owner}/${repo}.git\``
   ONLY in these two shorthand blocks. Full-URL parsing branches (explicit
   `https://github.com/...` input, GitLab, SSH, etc.) must remain untouched so
   public repos still work when given as full URLs.
3. `isRepoPrivate()` (~line 83) calls `https://api.github.com/repos/...`. For
   shorthand sources this is now wrong. Change it to accept the host, and use
   the GHE REST path when host !== 'github.com':
   `https://${host}/api/v3/repos/${owner}/${repo}` (github.com keeps
   `https://api.github.com/repos/...`). It already returns null on failure —
   preserve that fail-safe behavior. Update its call sites accordingly.
4. Check for any other hardcoded `raw.githubusercontent.com` usage in the repo
   (`grep -rn "raw.githubusercontent" src/`). If shorthand installs fetch raw
   files from it, route GHE-host sources to
   `https://${GIT_HOST}/raw/${owner}/${repo}/...` or fall back to `git clone`
   path. Investigate src/blob.ts and src/providers/ before changing.

## Step 3 — Kill telemetry (src/telemetry.ts)

Find the enablement check (~line 83):
`return !process.env.DISABLE_TELEMETRY && !process.env.DO_NOT_TRACK;`
Replace with `return false;` and add a one-line comment:
`// Internal fork: telemetry permanently disabled`.
Verify no other module posts to `add-skill.vercel.sh` (grep for `vercel.sh`).

## Step 4 — Neutralize skills.sh discovery (src/find.ts)

`SEARCH_API_BASE` (~line 17) already honors `process.env.SKILLS_API_URL`.
Change the behavior of the `find` command: when `SKILLS_API_URL` is NOT set,
print a short message pointing to INTERNAL_DOCS_URL and exit 0 instead of
calling skills.sh. Also replace the hardcoded `https://skills.sh/...` footer
strings (~lines 303, 350, 353) with INTERNAL_DOCS_URL.

## Step 5 — Default source for `add` (src/cli.ts)

In the `add`/`install` case (~line 332):
```typescript
const { source: addSource, options: addOpts } = parseAddOptions(restArgs);
await runAdd(addSource || DEFAULT_SOURCE, addOpts);
```
Confirm `runAdd`'s signature treats a missing source as an error today; if it
instead has its own interactive prompt for empty source, inject DEFAULT_SOURCE
before that prompt triggers. Bare `skills add` must install from
DEFAULT_SKILLS_REPO with no extra prompts beyond agent selection.

## Step 6 — package.json

1. `name`: change to `@samsung/skills` only if we will publish to GHE Packages;
   otherwise keep `skills` (tarball/git installs don't need a scope). Default:
   keep `skills`.
2. Remove `"prepare": "husky"` (breaks npm-from-git installs on machines
   without pnpm/husky). Add `"hooks:install": "husky"` for maintainers.
3. Set `repository.url`, `homepage`, `bugs.url` to the GHE fork URLs.
4. Set version to `1.5.11-internal.1` (keep upstream version visible).
5. Leave `bin`, `files`, `prepublishOnly`, `engines` unchanged.

## Step 7 — CI release workflow

Create `.github/workflows/release.yml` (GHE Actions, runner label `self-hosted`
unless I say otherwise):

- Trigger: push of tag `v*`
- Steps: checkout → setup-node 22 → `npm i -g pnpm@10` →
  `pnpm install --frozen-lockfile` → `pnpm test` → `pnpm build` → `npm pack`
  → attach `*.tgz` to a GHE release (softprops/action-gh-release@v2 or, if
  marketplace actions are unavailable on our GHE, use `gh release create
  "$GITHUB_REF_NAME" *.tgz` with GITHUB_TOKEN).
- permissions: contents: write.
- Do NOT include the npm publish step for now; leave a commented block for
  GHE Packages publishing.

## Step 8 — Tests

1. Run the full existing suite; fix any tests that assert github.com URLs for
   shorthand by updating expectations to GIT_HOST (set
   `SKILLS_GIT_HOST=github.com` in those tests if simpler — prefer that, it
   keeps upstream test bodies untouched).
2. Add new tests in `src/source-parser.test.ts`:
   - `owner/repo` → `https://GHE_HOST/owner/repo.git` when SKILLS_GIT_HOST set
   - full `https://github.com/owner/repo` URL still → github.com (unchanged)
   - `owner/repo@skill` → GHE host with skillFilter preserved
3. Add a telemetry test: enablement function returns false even with both env
   vars unset.

## Step 9 — End-to-end smoke test (local, no network needed)

1. Create a fixture repo: `/tmp/fixture-skills/skills/hello-world/SKILL.md` with
   valid frontmatter (`name: hello-world`, `description: test skill`), and
   `git init && git add -A && git commit` it.
2. Build, then run:
   `node bin/cli.mjs add /tmp/fixture-skills -a cline -a claude-code -y --copy`
   in a temp project dir.
3. Assert the skill landed in `.agents/skills/hello-world/SKILL.md` (Cline) and
   `.claude/skills/hello-world/SKILL.md` (Claude Code).
4. Run `node bin/cli.mjs list` and `node bin/cli.mjs remove hello-world -y`;
   assert clean removal.
5. `npm pack`, install the tarball globally in a clean prefix
   (`npm i -g ./skills-*.tgz --prefix /tmp/npmtest`), and re-run the add
   command via the installed `skills` binary.

## Acceptance criteria

- [ ] `pnpm build` and `pnpm test` green
- [ ] Bare `skills add -a cline -y` resolves DEFAULT_SKILLS_REPO on GHE_HOST
- [ ] No network calls to vercel.sh, skills.sh, api.github.com, or
      raw.githubusercontent.com for GHE-shorthand installs (verify by grep and
      by reading the changed code paths)
- [ ] Full public URLs (`https://github.com/...`) still parse to github.com
- [ ] `npm pack` tarball installs and runs on a machine with only Node 18+
- [ ] Diff against upstream is confined to: company-config.ts (new),
      source-parser.ts, telemetry.ts, find.ts, cli.ts, package.json,
      release.yml (new), tests
- [ ] CHANGELOG-INTERNAL.md created listing every divergence from upstream
      (file + reason) to make future rebases easy

## Out of scope (do not do)

- No renaming of commands or agent paths
- No dependency upgrades or lockfile churn beyond what pnpm install requires
- No changes to the skill discovery directory list or agent registry in
  src/agents.ts
- No publishing to public npm
