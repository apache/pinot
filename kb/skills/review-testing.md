# review-testing

Review **Apache Pinot domain 6: Testing Strategies**. Read the applicable parts of section 6 in
`kb/code-review-principles.md` and relevant repository conventions not already loaded. Reuse material already read.

Use the canonical severity definitions and Review Delivery rules in `kb/code-review-principles.md`. Assess demonstrated
impact; pattern matches are investigation triggers, not findings or automatic severity assignments.

## 1. Broad scan

- Map changed behavior to existing and new tests. A production-file change without a test-file change is not itself a
  coverage gap; identify the behavior or failure mode that lacks coverage.
- Find new tests and scan for:
  - Test framework: TestNG (`import org.testng.annotations.Test`) unless the file uses JUnit consistently.
  - Mocks: inspect whether mocks of `Dictionary`, `ForwardIndexReader`, or `NullValueVectorReader` omit semantics needed
    by the assertion. Prefer real instances when encoding or storage behavior matters (see PR 18189).
  - `assertTrue` / `assertFalse` on compound expressions — prefer `assertEquals` / `assertThrows`.
  - Type-dispatch coverage: confirm the affected branches are exercised, through data providers or other focused cases.
  - Timing assumptions: inspect `Thread.sleep` and wall-clock assertions for dependence on uncontrolled scheduling.
- Integration tests: check existing coverage of changed REST / Thrift / wire behavior and identify gaps at the affected
  component boundary. See C6.4 and C6.10 for full-pipeline coverage and shared-cluster selection.

## 2. Deep analysis

- **C6.x** Positive + negative: check successful behavior and relevant boundary or rejected-input cases. Reuse existing
  coverage; do not invent an error contract just to meet a test-count rule.
- **Real dependencies where semantics matter**: check that dictionaries, segment readers, null-vector readers,
  aggregators, and transform functions preserve the semantics the test claims to validate.
- **Null-handling coverage**: check both `null-handling-enabled=true` and `false` when the change affects both paths.
- **Type coverage**: for aggregator / window / transform changes, test INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING, BYTES, BOOLEAN, TIMESTAMP, JSON as applicable.
- **Mixed-version tests**: for changes affecting rolling-upgrade compatibility, check the supported old/new combinations
  at the changed boundary. Reuse applicable compatibility evidence and identify any missing scenario.
- **Regression evidence**: verify evidence that the regression test fails without the fix and passes with it. Use the
  actual pre-fix revision or a controlled removal of the fix, not an assumed `HEAD~1` baseline. Do not change the checkout
  or run tests solely to satisfy a checklist; use safe, scoped verification appropriate to the task and report material
  validation gaps.
- **Flakiness hygiene**: never add a `Thread.sleep` as a test-stability knob; prefer `Awaitility` / explicit events. Do not mask flakes with retries (see domain 8 process rule).

### Core-functionality + integration-test base-class selection

For new or changed query semantics, check coverage at the smallest layer that proves the behavior under C6.4. Require
integration coverage only when correctness depends on pipeline or component interactions that focused tests do not cover.
Existing tests may provide that evidence; add a finding only for a concrete coverage gap.

When integration coverage is needed for ordinary table/data/query scenarios, apply C6.10 and reuse
`CustomDataQueryClusterIntegrationTest`. Follow its canonical
package, suite annotation, and suite-discovery guidance. A separate cluster needs a distinct topology, component
configuration, isolation, streaming setup, or lifecycle requirement that the shared suite cannot support. Establish that
need from the code or supplied design context; do not require duplicate PR prose when the reason is already clear.

## 3. Findings

Tag `skill: review-testing`, cite `C6.x`, use `[BUG-TEST]`. When flagging missing tests, always suggest a concrete test signature / data-provider shape.

## When to defer to the developer

- PR is a pure rename / move with no behavior change and existing tests still exercise the paths.
- New code is test-only scaffolding.
