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

Procedure: see [`kb/skills/review-process-scope.md`](../../../kb/skills/review-process-scope.md). Read it first, then follow it.
