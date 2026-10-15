# flaky-analyze

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

1. **Parse the argument.** Extract class name (required), optional run count (default 20), optional PR filter. Use the count as a total budget across relevant workflows; cap at 50 unless the user authorized more.

2. **Find relevant workflow runs.** Select workflows from the target test's module and the current definitions under [`.github/workflows`](../../.github/workflows): `pinot_unit_tests.yml`, `pinot_integration_tests.yml`, or `pinot_quickstart_tests.yml` for the corresponding suites. Include compatibility workflows only when relevant (see Notes). Use `gh workflow list --repo apache/pinot` if the remote workflow names differ. Keep the total run budget across all selected workflows.
   ```
   gh run list --repo apache/pinot --workflow <workflow-file> --status failure --limit <remaining-budget> --json databaseId,displayTitle,headBranch,headSha,createdAt,url
   ```
   If `--pr` is set, resolve it with `gh pr view <num> --repo apache/pinot --json headRefName,headRefOid`, filter with `--branch`, and verify the selected runs belong to that PR.

3. **For each failed run, find the failing jobs.** Unit and integration workflows have separate matrix jobs. Read the test set and JDK from each run rather than assuming the current matrix applies to older runs.
   ```
   gh run view <run-id> --repo apache/pinot --json jobs
   ```
   Filter to jobs with `conclusion: "failure"`.

4. **Download and grep the logs for the target test.** For each failing job:
   ```
   gh run view --job <job-id> --repo apache/pinot --log
   ```
   Log lines include step names and timestamps. Logs can be tens of MB; filter with ripgrep and retain enough surrounding context for the stack traces in step 5. Use these failure markers:

   - `\[ERROR\] Tests run: \d+, Failures: [1-9]` — the Surefire class-summary line when a test class had failures. The **fully qualified class name** is on the same line after `-- in `.
   - `\[ERROR\] Tests run: \d+, Failures: \d+, Errors: [1-9]` — same, with errors instead of failures.
   - `<<< FAILURE!` / `<<< ERROR!` — the Surefire marker following a failed assertion. Useful to find the exact method; the method and class appear in the line immediately above.
   - `##\[error\]Process completed with exit code` — GitHub Actions' own marker, always present when the job fails for a process-level reason.
   - `BUILD FAILURE` — Maven-level, always present when Maven returns non-zero.

   **Do not grep for raw `ERROR` / `FAILED` / `Exception`** — Pinot's integration tests log these constantly at runtime (Helix rebalancer, consumer setup, etc.) and you'll drown in noise. The patterns above only match actual failure markers.

   If none of those patterns match, inspect the job conclusion, annotations, and log availability. Classify a timeout, OOM, or runner failure only when evidence supports it; otherwise report an unknown cause or missing logs and move on within the budget.

5. **Extract structured failure records.** For each hit, record:
   - Workflow file/name, run id, PR number (if any), head SHA, JDK version, and test set or lane, as recorded by the job and its logs. Do not infer an older run's JDK or lane from today's workflow matrix.
   - The failing class FQN from the `-- in <FQN>` suffix of the summary line.
   - The failure message (typically the line containing `<<< FAILURE!` or the `AssertionError: ...` line that follows).
   - The top ~5 frames of the stack trace, taken from the ~30 lines following the `<<< FAILURE!` marker.
   - Any preceding log lines that look like test setup problems (timeouts, `Connection refused`, `Address already in use`, `OutOfMemoryError`).

6. **Cluster the failures.** Group by:
   - Identical exception type + top-of-stack frame → likely same root cause.
   - Different stack traces → either multiple bugs or environmental flakiness.
   - Setup/timeout errors with no test code in the stack → likely infrastructure.

7. **Inspect relevant source and report.** Read the failing test and directly implicated source when needed to assess the hypothesis. Use the failing run's commit when source drift matters, and identify any mismatch with the current checkout. Structure:
   ```
   ## Flaky analysis: <ClassName>
   Runs scanned: N (M with this test failing, K with unrelated failures)

   ### Failure cluster 1 — <exception type> at <top frame> (<count> occurrences)
   Example (PR #<num>, JDK <version>, test set <set>):
     <short stack trace>
   Commits affected: <list of short SHAs>

   ### Failure cluster 2 — ...

   ### Hypothesis
   <one-paragraph judgment: real bug vs. race vs. env vs. insufficient data>
   <cite relevant KB principle if applicable, e.g. C6.2, C2.2>

   ### Suggested next steps
   - <specific, e.g. "reproduce locally with: /run-test ClassName", or "inspect X.java:123 which is top-of-stack">
   ```

8. **Finish with the evidence and remaining uncertainty.** This skill is report-only: do not change source or launch reproduction tests without remediation or reproduction being authorized. Do not add a routine confirmation question to read relevant source; that read is part of the investigation.

## Notes

- `gh run view --log` can be slow (10–60s per run) and returns large payloads. Cap total runs scanned at 50 unless the user asks for more. Fetch independent logs with bounded concurrency; handle rate limits within the investigation budget.
- Don't write the raw logs to the repo. Stream them through grep and keep only the extracted records in memory.
- `gh run view --log-failed` is not reliable here — it only returns the steps GitHub marked failed, which for Pinot's "Integration Test" step often just contains runner init lines before the actual Maven invocation. Always use `--log` + the patterns above.
- To find failing *jobs* within a run without downloading its full log, use:
  ```
   gh api --paginate repos/apache/pinot/actions/runs/<run-id>/jobs --jq '.jobs[] | select(.conclusion=="failure") | {name, id}'
  ```
  Then pass the `id` as `--job <id>`. Avoids pulling all matrix logs.
- If the test does not appear in sampled failure logs, report the searched scope and lack of evidence. Check the selector and log availability; do not conclude the test is never flaky.
- State the sampled dates and any unavailable logs. Relevant Surefire artifacts can supplement missing console output if they are still retained; absence of retained evidence does not establish that the test passed.
- Query `pinot_compatibility_tests.yml` or `pinot_multi_stage_query_engine_compatibility_tests.yml` when relevant to the target test, within the same total run budget. Ask only before exceeding that budget.
