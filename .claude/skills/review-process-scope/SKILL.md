---
name: review-process-scope
description: Review Apache Pinot diffs for process and scope discipline — PR size, single-concern commits, commit message clarity, referenced issues/PRs on reverts, anti-patterns like "add retry to fix flake", labels (backward-incompat), rolling-upgrade notes, and TODO hygiene. Trigger keywords — revert, retry, flake, TODO, backward-incompat, rolling upgrade, PR description.
domain: kb/code-review-principles.md#8-process--scope
triggers:
  - diff is > ~500 changed lines or spans > 4 modules
  - PR title contains "Revert" / "Hotfix"
  - diff adds retries / sleeps to tests
  - diff adds or modifies TODO / FIXME comments
  - diff touches backward-incompat surfaces (requires label + rolling-upgrade note)
license: Apache-2.0
---

# Skill: review-process-scope

You are a specialized reviewer for **Apache Pinot domain 8: Process & Scope**. Read `kb/code-review-principles.md` section 8 and `CLAUDE.md`.

Severity:
- **CRITICAL** — revert without referencing the original PR or explaining the regression; test-retry / sleep added to mask a flake (never fix by retry — investigate root cause); missing rolling-upgrade note on a backward-incompat change.
- **MAJOR** — PR bundles multiple unrelated concerns; commit message doesn't explain WHY; new TODO with no issue link.
- **MINOR** — PR title style; label missing.

## 1. Broad scan

- Diff size + module count. If > 500 lines or > 4 modules, flag for scope review.
- Commit messages (`git log <base>..HEAD`): check each for a WHY clause.
- New `// TODO` / `// FIXME` — confirm each has a linked issue.
- Test retry patterns: `@Test(retryAnalyzer = ...)`, `Thread.sleep` added in tests, `@Flaky` annotations.
- PR title / labels if available.

## 2. Deep analysis

- **C8.x** PR scope: one concern per PR; bundle refactor + test rewrite (that's acceptable) but not refactor + feature.
- **Reverts**: must name the reverted PR number and the reason.
- **No-retry rule**: flaky tests are investigated via `/flaky-analyze`, not retried into submission.
- **Backward-incompat labeling**: any change touching wire formats / APIs / configs that can't roll-forward-and-back needs the `backward-incompat` label and a rolling-upgrade note.
- **TODO hygiene**: every TODO links to an issue.

## 3. Findings

Tag `skill: review-process-scope`, cite `C8.x`, use `[PROC]` for process nits. Most findings MINOR; the retry-to-fix-flake and revert-without-reference cases are CRITICAL.

## When to defer to the developer

- PR is pre-coordinated large refactor with a linked design doc; scope is justified.
- Label was set after PR description was drafted; confirm it's now correct.
