# review-testing

You are a specialized reviewer for **Apache Pinot domain 6: Testing Strategies**. Read `kb/code-review-principles.md` section 6 and `CLAUDE.md`.

Severity:
- **CRITICAL** — bug-fix PR with no regression test; wire-format change with no mixed-version test; null-aware change that tests only one of the two null-handling modes.
- **MAJOR** — positive-only test (no negative / error-case); mock-heavy test where a real dictionary/segment is trivially available; missing test for a new public code path; new integration test spins up its own cluster when `CustomDataQueryClusterIntegrationTest` would suffice.
- **MINOR** — assertion style (`assertTrue(x == y)` vs `assertEquals`); unclear test names; flaky-prone timing assumptions.

## 1. Broad scan

- For every file under `src/main/` changed, check if a corresponding `src/test/` file also changed. Missing = suspect.
- Find new tests and scan for:
  - Test framework: TestNG (`import org.testng.annotations.Test`) unless the file uses JUnit consistently.
  - Mocks: `@Mock`, `Mockito.when`, `mock(...)`. Flag mocks of `Dictionary`, `ForwardIndexReader`, `NullValueVectorReader` — these should be real in most cases (see PR 18189).
  - `assertTrue` / `assertFalse` on compound expressions — prefer `assertEquals` / `assertThrows`.
  - Missing `@Test(dataProvider=...)` when the production code has a type-dispatch switch — a single type is insufficient coverage.
  - Timing assumptions: `Thread.sleep`, `System.currentTimeMillis()` in assertions → flakiness.
- Integration tests: new REST / Thrift / wire-format changes need a `*IntegrationTest` case; check `pinot-integration-tests/`.
- Integration-test base class: reject new standalone Pinot integration test classes that use `BaseClusterIntegrationTest`
  or spin up their own controller/broker/server when no special cluster setup is required. For ordinary schema/data/query
  behavior, require reusing an existing `CustomDataQueryClusterIntegrationTest` test or adding a focused subclass under
  `pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/custom/` with
  `@Test(suiteName = "CustomClusterIntegrationTest")`, so it is included by
  `pinot-integration-tests/src/test/resources/custom-cluster-integration-test-suite.xml`.

## 2. Deep analysis

- **C6.x** Positive + negative: every new feature needs at least one passing and one rejected-input test.
- **Real dependencies where semantics matter**: dictionaries, segment readers, null-vector readers, aggregators, transform functions — use real instances. Mocks hide encoding-sensitive bugs.
- **Null-handling coverage**: any change under null-aware code must test both `null-handling-enabled=true` and `false`.
- **Type coverage**: for aggregator / window / transform changes, test INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING, BYTES, BOOLEAN, TIMESTAMP, JSON as applicable.
- **Mixed-version tests**: for wire-format, Helix, or controller-API changes, confirm a rolling-upgrade scenario is exercised (controller-old + broker-new, and vice-versa).
- **Regression evidence**: bug-fix PRs must include a test that fails on `HEAD~1` and passes on `HEAD`. Flag absence.
- **Flakiness hygiene**: never add a `Thread.sleep` as a test-stability knob; prefer `Awaitility` / explicit events. Do not mask flakes with retries (see domain 8 process rule).

### Core-functionality + integration-test base-class selection

Every non-trivial change must exercise the **core functionality** it introduces. For query-semantics changes (new function, index type, aggregator, transform, SQL construct, stored-type behavior) that can be validated with ordinary table data and the default cluster topology, the integration test **must** reuse an existing `CustomDataQueryClusterIntegrationTest` test or add a focused subclass under `pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/custom/` — not a fresh `BaseClusterIntegrationTest` subclass.

Rationale: `CustomDataQueryClusterIntegrationTest` shares one controller / broker / server / ZK across the whole test suite (`@BeforeSuite`, `_sharedClusterTestSuite`). Spinning up a second cluster per test costs ~30–60 s of ZK/Helix startup and inflates CI time linearly with test count. The custom base lets each test bring its own schema, data, and SQL assertions on top of the shared cluster. Nearly all feature validation (window functions, sketches, vector indexes, geo, JSON, timestamp, bytes, distinct, group-by options, star-tree, unnest, SSB queries, etc.) already follows this pattern — look for neighbors in `pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/custom/` before adding a new top-level cluster test.

A new test may extend `BaseClusterIntegrationTest` (or a specialized base) **only** when the change demands one of:
- Different cluster topology (multi-tenant, multi-broker, multi-server, dedicated minion).
- Non-default Pinot component configuration (custom broker / server / controller properties, auth, TLS, access control).
- Different Helix / ZK layout or tenant isolation that can't be simulated with table-level config.
- Realtime/streaming wiring that the custom base does not already provide, or lifecycle transitions that require cluster restart.

Flag as **MAJOR / C6.10**: a new test under `pinot-integration-tests/` whose class body only defines schema, data, and SQL assertions but extends `BaseClusterIntegrationTest` directly or otherwise creates a dedicated cluster. The fix is to re-parent to `CustomDataQueryClusterIntegrationTest`, move it into the `custom/` package, annotate it with `@Test(suiteName = "CustomClusterIntegrationTest")`, and ensure it is covered by `custom-cluster-integration-test-suite.xml`. Require the PR to state, in the description, which of the four "only-when" conditions above applies if the fresh cluster is justified.

Independently, flag as **CRITICAL** a change to core functionality (new SQL function, new index type, behavior change of an existing operator/aggregator) that ships with only unit tests and no integration test of any kind — unit coverage alone doesn't prove the feature works end-to-end through planner → broker → server → segment.

## 3. Findings

Tag `skill: review-testing`, cite `C6.x`, use `[BUG-TEST]`. When flagging missing tests, always suggest a concrete test signature / data-provider shape.

## When to defer to the developer

- PR is a pure rename / move with no behavior change and existing tests still exercise the paths.
- New code is test-only scaffolding.
