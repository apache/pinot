---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
name: flaky-analyze
description: Pull recent GitHub Actions failures for a Pinot test class and analyze whether they share a root cause. Uses the gh CLI. Surfaces stack traces, failure patterns, and a candidate hypothesis — does not auto-fix.
---

# /flaky-analyze

Purpose: when a test is failing intermittently on CI, gather the evidence in one place so the user can decide whether it's a real race, an environmental issue, or a legitimate regression. Principle C6.2 is explicit: the fix is never "add retries" — it's understanding the root cause.

Usage:
- `/flaky-analyze RangeIndexTest` — last ~20 runs on master + PRs.
- `/flaky-analyze RangeIndexTest 50` — look at 50 runs.
- `/flaky-analyze RangeIndexTest --pr 18267` — only that PR's runs.

## Prerequisites

1. `gh` CLI installed and authenticated: `gh auth status` must succeed.
2. Network access to GitHub.
3. The test must be in `apache/pinot`. If the user's remote is a fork, still query `apache/pinot` — that's where CI lives.

If `gh` isn't available, report that and exit. Don't try to fall back to `curl` with tokens.

## Procedure

1. **Parse the argument.** Extract class name (required), optional run count (default 20), optional PR filter.

2. **Find relevant workflow runs.**
   ```
   gh run list --repo apache/pinot --workflow "Pinot Tests" --status failure --limit <N> --json databaseId,displayTitle,headBranch,headSha,createdAt,url
   ```
   If `--pr` is set, filter with `--branch` to the PR's head branch, or use `gh pr view <num> --json headRefName`.

3. **For each failed run, find the failing jobs.** A "Pinot Tests" run has matrix jobs (test sets 1/2 × java 21 × unit/integration). Only some fail.
   ```
   gh run view <run-id> --repo apache/pinot --json jobs
   ```
   Filter to jobs with `conclusion: "failure"`.

4. **Download and grep the logs for the target test.** For each failing job:
   ```
   gh run view --job <job-id> --repo apache/pinot --log
   ```
   Each log line is prefixed by `<step-name>\tUNKNOWN STEP\t<timestamp>`. The log is large (tens of MB); pipe straight into ripgrep with these patterns — they are what Surefire/TestNG/GitHub Actions actually emit:

   - `\[ERROR\] Tests run: \d+, Failures: [1-9]` — the Surefire class-summary line when a test class had failures. The **fully qualified class name** is on the same line after `-- in `.
   - `\[ERROR\] Tests run: \d+, Failures: \d+, Errors: [1-9]` — same, with errors instead of failures.
   - `<<< FAILURE!` / `<<< ERROR!` — the Surefire marker following a failed assertion. Useful to find the exact method; the method and class appear in the line immediately above.
   - `##\[error\]Process completed with exit code` — GitHub Actions' own marker, always present when the job fails for a process-level reason.
   - `BUILD FAILURE` — Maven-level, always present when Maven returns non-zero.

   **Do not grep for raw `ERROR` / `FAILED` / `Exception`** — Pinot's integration tests log these constantly at runtime (Helix rebalancer, consumer setup, etc.) and you'll drown in noise. The patterns above only match actual failure markers.

   If none of those patterns match in a failing job's log, the test didn't fail at the test level — the job died for infrastructure reasons (timeout, OOM, runner cancel). Classify it as "infrastructure failure" and move on.

5. **Extract structured failure records.** For each hit, record:
   - Run id, PR number (if any), commit SHA, JDK version, test set (parseable from the job name like `Pinot Integration Test Set 1 (temurin-21)`).
   - The failing class FQN from the `-- in <FQN>` suffix of the summary line.
   - The failure message (typically the line containing `<<< FAILURE!` or the `AssertionError: ...` line that follows).
   - The top ~5 frames of the stack trace, taken from the ~30 lines following the `<<< FAILURE!` marker.
   - Any preceding log lines that look like test setup problems (timeouts, `Connection refused`, `Address already in use`, `OutOfMemoryError`).

6. **Cluster the failures.** Group by:
   - Identical exception type + top-of-stack frame → likely same root cause.
   - Different stack traces → either multiple bugs or environmental flakiness.
   - Setup/timeout errors with no test code in the stack → likely infrastructure.

7. **Report.** Structure:
   ```
   ## Flaky analysis: <ClassName>
   Runs scanned: N (M with this test failing, K with unrelated failures)

   ### Failure cluster 1 — <exception type> at <top frame> (<count> occurrences)
   Example (PR #<num>, JDK 21, test set 2):
     <short stack trace>
   Commits affected: <list of short SHAs>

   ### Failure cluster 2 — ...

   ### Hypothesis
   <one-paragraph judgment: real bug vs. race vs. env vs. insufficient data>
   <cite relevant KB principle if applicable, e.g. C6.2, C2.2>

   ### Suggested next steps
   - <specific, e.g. "reproduce locally with: /run-test ClassName", or "inspect X.java:123 which is top-of-stack">
   ```

8. **Do not propose a fix.** This skill is investigation, not remediation. End with "Want me to open the source file at the top-of-stack frame?"

## Notes

- `gh run view --log` can be slow (10–60s per run) and returns large payloads. Cap total runs scanned at 50 unless the user asks for more. Run these fetches in parallel where possible; `gh` is rate-limited but stays under the limit for <50 runs.
- Don't write the raw logs to the repo. Stream them through grep and keep only the extracted records in memory.
- `gh run view --log-failed` is not reliable here — it only returns the steps GitHub marked failed, which for Pinot's "Integration Test" step often just contains runner init lines before the actual Maven invocation. Always use `--log` + the patterns above.
- To find failing *jobs* within a run without downloading its full log, use:
  ```
  gh api repos/apache/pinot/actions/runs/<run-id>/jobs --jq '.jobs[] | select(.conclusion=="failure") | {name, id: .databaseId}'
  ```
  Then pass the `id` as `--job <id>`. Avoids pulling all matrix logs.
- If the test doesn't appear in any failure log, report that directly: either the test isn't actually flaky on CI, or the search term is wrong.
- Results depend on log retention (GitHub keeps 90 days by default). Older flakes are invisible here — suggest checking the `surefire-reports-*` artifacts on master for long-term trends.
- The `Pinot Tests` workflow (file: `pinot_tests.yml`) is the right default. Also worth checking `Pinot Compatibility Regression Testing` and `Pinot Multi-Stage Query Engine Compatibility Regression Testing` workflows for integration tests that only run there — ask the user before querying those since they multiply the API calls.
