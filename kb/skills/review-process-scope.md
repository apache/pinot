# review-process-scope

Review **Apache Pinot domain 8: Process & Scope**. Read the applicable parts of section 8 in
`kb/code-review-principles.md` and relevant repository conventions not already loaded. Reuse material already read.

Use the canonical severity definitions and Review Delivery rules in `kb/code-review-principles.md`. Assess demonstrated
impact; pattern matches are investigation triggers, not findings or automatic severity assignments.

## 1. Broad scan

- Use diff size and module count to plan coverage. Report scope issues only when the diff includes unrelated concerns,
  not because it crosses a fixed size threshold.
- Commit messages (`git log <base>..HEAD`): check that the purpose of the change is understandable from the available context.
- New `// TODO` / `// FIXME` — confirm each has a linked issue.
- Test retry patterns: `@Test(retryAnalyzer = ...)`, `Thread.sleep` added in tests, `@Flaky` annotations.
- PR title / labels if available.
- Required checks for the reviewed SHA: apply C6.1, classify failures, and report pending or failing merge gates.
  Read-only review does not change Git state or retry CI; authorized remediation addresses attributable failures within scope.

## 2. Deep analysis

- **C8.x** PR scope: one concern per PR; bundle refactor + test rewrite (that's acceptable) but not refactor + feature.
- **Reverts**: must name the reverted PR number and the reason.
- **No-retry rule**: flaky tests are investigated via `/flaky-analyze`, not retried into submission.
- **Backward-incompat labeling**: any change touching wire formats / APIs / configs that can't roll-forward-and-back needs the `backward-incompat` label and a rolling-upgrade note.
- **TODO hygiene**: every TODO links to an issue.

## 3. Findings

Tag `skill: review-process-scope`, cite `C8.x`, use `[PROC]` for process nits. Distinguish missing process context from a
demonstrated production or test-reliability problem, and use the canonical severity definitions.

## When to defer to the developer

- PR is pre-coordinated large refactor with a linked design doc; scope is justified.
- Label was set after PR description was drafted; confirm it's now correct.
