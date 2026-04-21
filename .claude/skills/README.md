<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Claude Code skills for Apache Pinot

Project-scoped [Claude Code](https://claude.com/claude-code) skills that automate common Pinot developer workflows. Each skill is invocable as a slash command (`/<skill-name>`) when working in this repo with Claude Code.

Skills are plain Markdown with YAML frontmatter — Claude reads them and follows the procedure. They're shared here so contributors don't each reinvent the same shell invocations. Each skill's `SKILL.md` is the authoritative spec; the summaries below are a navigation aid.

## Skills at a glance

| Skill | Purpose | Rough time |
|---|---|---|
| [`/precommit`](precommit/SKILL.md) | Run the four mandatory pre-commit checks (`spotless:apply`, `license:format`, `checkstyle:check`, `license:check`) on only the modules touched by the diff. | 30–120s warm, up to 5min cold |
| [`/run-test <Class>`](run-test/SKILL.md) | Resolve a test class name to its module and run the single-test Maven invocation. Auto-adds integration-test flags. | 30s–15min depending on test |
| [`/quickstart [mode]`](quickstart/SKILL.md) | Launch a local Pinot quickstart cluster (`batch`, `hybrid`, `streaming`, `upsert-streaming`, `auth`, …) in the background. | ~30s to ready |
| [`/bench-compare <Benchmark> [<ref>]`](bench-compare/SKILL.md) | Run a `pinot-perf` JMH benchmark against a baseline ref and the current tree and diff the JMH tables. Uses a git worktree. | 10min – days (see below) |
| [`/flaky-analyze <TestClass>`](flaky-analyze/SKILL.md) | Pull recent CI failures for a test class, cluster by stack trace, propose a root-cause hypothesis. Investigation only. | 1–10min per 20 runs scanned |

---

## `/precommit`

**What it does.** Detects modified files (staged + unstaged + new untracked `.java` / `.xml` / etc.), maps them to their owning Maven modules by walking up to the nearest `pom.xml`, then runs the four check goals in order on only those modules.

**Scope of each check:**
- `spotless:apply` — imports only (order + unused removal). Does **not** fix whitespace, indentation, or braces. Pinot's config is deliberately narrow; see the `spotless-maven-plugin` block in the root `pom.xml`.
- `license:format` — inserts the ASF header into new files that don't have it. Governed by `HEADER` at repo root.
- `checkstyle:check` — `config/checkstyle.xml`. Top offenders: `LineLength` (120), `AvoidStarImport`, `AvoidStaticImport`, `HideUtilityClassConstructor`, `NeedBraces`.
- `license:check` — final gate confirming every touched file has a header.

**Example scenarios:**

- **Clean tree** → prints `No changed Java/XML files — nothing to do.` and exits. Safe to run anytime.
- **Unused import** → `spotless:check` fails with a coloured diff; `spotless:apply` removes it. *Note:* removal leaves a cosmetic double blank line where the import used to be. Checkstyle does not flag this; the user can clean it up manually if desired.
- **Missing ASF header on new file** → `license:check` fails with `Some files do not have the expected license header`; `license:format` inserts the ASF header at the top of the file. No manual intervention needed.
- **Line >120 chars** → `checkstyle:check` fails with `[WARNING] src/.../File.java:[NN] (sizes) LineLength: Line is longer than 120 characters (found NNN).` Skill reports file:line; user must fix manually.
- **Multi-module diff** → modules are joined with commas into one `-pl <m1>,<m2>,...` argument. All four goals run once per step, scoped to the union.
- **Nested plugin module** (e.g. `pinot-plugins/pinot-input-format/pinot-csv/...`) → skill walks up past the `pinot-input-format/pom.xml` aggregator (it has no `src/`) and stops at `pinot-csv/pom.xml`. That's the correct module.

**Usage:**
- `/precommit` — default, scoped to diff vs. HEAD + untracked.
- `/precommit staged` — staged changes only.
- `/precommit branch` — diff vs. `upstream/master` (useful before a PR).
- `/precommit all` — run on the entire repo; slow (several minutes).

**Known quirks:**
- Never pass `-am`. The check plugins don't need upstream modules built.
- Spotless sometimes reformats files you hadn't touched if they were non-compliant to begin with. Review the auto-fix diff before staging.
- Violations in `pinot-controller/src/main/resources/` (the React UI) are not handled by the Maven plugins — skip that tree.

---

## `/run-test`

**What it does.** Given a test class name (or `Class#method`), finds the source file via glob, walks up to the owning module, and builds the correct `./mvnw -pl <module> -am -Dtest=<Class>[#<method>] -Dsurefire.failIfNoSpecifiedTests=false test` command.

**Why `-Dsurefire.failIfNoSpecifiedTests=false` is always needed:** with `-am`, Maven builds upstream modules and runs Surefire in each one. Upstream modules don't have the target test, so Surefire's default behaviour (fail when the `-Dtest` filter matches nothing) kills the build at the first upstream module. The flag makes "no tests matched in this module" a no-op and lets the build progress to the module that actually contains the test.

**Integration test heuristics** (used only to warn the user about expected runtime, not to change the command): path contains `pinot-integration-tests`, OR filename ends with `IntegrationTest.java` / `IT.java` / `ClusterTest.java` / `EndToEndTest.java`, OR the module is `pinot-integration-tests` / `pinot-compatibility-verifier`.

**Example scenarios:**

- **Unit test** → `/run-test BigDecimalUtilsTest` → resolves to `pinot-spi/src/test/java/.../BigDecimalUtilsTest.java` → runs `./mvnw -pl pinot-spi -am -Dtest=BigDecimalUtilsTest test`. Verified: 5 tests pass in ~6s after the dependency build.
- **Integration test** → `/run-test OfflineClusterIntegrationTest` → path `pinot-integration-tests/...` triggers integration detection → adds `-Dsurefire.failIfNoSpecifiedTests=false`. Runs for 10–20 minutes depending on the test.
- **Method selector** → `/run-test BigDecimalUtilsTest#testRoundTrip` → Maven's `-Dtest=Class#method` form.
- **Ambiguous name** → `/run-test AggregationFunctionColumnPairTest` → matches two files in `pinot-segment-spi` (`.../misc/` and `.../index/startree/`). Skill lists both with their package paths and asks the user to pick. Does not guess.
- **Not found** → `/run-test TypoTest` → glob returns zero hits → skill reports and stops.
- **Abstract base class** → warns that the class has no `@Test` methods and suggests concrete subclasses via grep.

**Known quirks:**
- First run builds all upstream modules via `-am`, which can be 5–15 minutes on a cold tree. Subsequent runs against the same module skip rebuilds.
- Pinot uses TestNG; Surefire's `-Dtest=Class#method` syntax still works.
- Integration tests spin up embedded Helix/ZK/Kafka and bind to localhost ports. Don't run two at once.

---

## `/quickstart`

**What it does.** Finds `quick-start-<mode>.sh` in `build/bin/` (produced by `-Pbin-dist`) or `pinot-tools/target/pinot-tools-pkg/bin/` (produced by a plain `pinot-tools` build), launches it in the background, and reports the controller URL.

**Available modes:** `batch`, `hybrid`, `streaming`, `upsert-streaming`, `partial-upsert-streaming`, `json-index-batch`, `json-index-streaming`, `complex-type-handling-offline`, `complex-type-handling-streaming`, `auth`, `auth-zk`. Listed from the `pinot-tools` package.

**Example scenarios:**

- **Scripts already built** → runs `quick-start-batch.sh` in the background. After ~30s the controller is up at `http://localhost:9000`. Verified with `curl http://localhost:9000/tables` (returns `{"tables":[...]}`) and an SQL query (`SELECT COUNT(*) FROM airlineStats` → 9746 rows in 33ms).
- **No build yet** → skill offers two options: full `-Pbin-dist -Pbuild-shaded-jar` (~10min) or `pinot-tools` only (~3min).
- **Invalid mode** → skill lists the available scripts via `ls`.
- **Port 9000 already taken** → cluster start fails with `BindException`. Skill reports and asks whether to kill the existing process.

**To stop the cluster:** kill the background shell Claude launched (the skill records its id), or `pkill -f QuickStart`.

**Known quirks:**
- All quickstart variants bind the same ports (9000 controller, 8000 broker, 7050/8098/8099 server). Only one can run at a time.
- The `auth` and `auth-zk` quickstarts use credentials `admin / verysecret`.
- Quickstarts run with embedded ZK + server + broker + controller in one JVM. That JVM is not small (~4GB heap) — expect to need a decently-sized machine.

---

## `/bench-compare`

**What it does.** Runs the same JMH benchmark twice — once against a baseline ref in a temporary git worktree, once against the current tree — and diffs the JMH output tables.

**Time reality.** This is the slowest skill and it is not subtle. Pinot's default JMH config is **8 warmup × 60s + 8 measurement × 60s × 5 forks per `@Benchmark` method per parameter combination**. A single method with a few parameters can be a multi-hour run; the full default on something like `BenchmarkDictionary` estimated *7 days* in one test. The skill refuses to run without either explicit `--args` reducing iteration counts, or the user confirming they want the full default.

**A reasonable first pass** for a single-method benchmark: `--args "-wi 1 -i 2 -f 1 -r 5s -w 5s"`. That's 1 warmup × 5s + 2 measurement × 5s × 1 fork ≈ 20s per method post-setup. Setup itself (the `@Setup` phase, which often builds Pinot segments) can still take 1–10 minutes for benchmarks that construct real tables.

**Invocation style** (from validation: skill always uses this rather than the generated `.sh`):
```
java -Xms4G -Xmx8G -cp '<tree>/pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main 'org.apache.pinot.perf.<BenchmarkClass>' \
  -wi 1 -i 2 -f 1 -r 5s -w 5s \
  -jvmArgsAppend='<full add-opens/add-exports set from pinot_tests.yml>'
```

Why `org.openjdk.jmh.Main` instead of the per-benchmark `pinot-<Class>.sh`:
- Benchmark classes have a custom `main()` that builds `OptionsBuilder` directly and ignores CLI args. Going through `org.openjdk.jmh.Main` bypasses it so flags like `-wi`, `-i`, `-r`, `-w`, and crucially `-jvmArgsAppend` actually take effect.
- The generated script hard-codes `-Xms24G -Xmx24G` which OOMs any laptop with less than ~32GB.
- The `--add-opens` / `--add-exports` flags are mandatory for any benchmark that extends `BaseClusterIntegrationTest` on JDK 21 — without them ZK startup dies with `InaccessibleObjectException`. The canonical set lives in `.github/workflows/pinot_tests.yml`.

**Example scenarios:**

- **Typical run** → `/bench-compare BenchmarkDictionary --args "-wi 1 -i 2 -f 1 -r 5s -w 5s"` — worktree at `/tmp/pinot-bench-baseline`, builds `pinot-perf` there, runs via `org.openjdk.jmh.Main`, diffs the JMH tables.
- **Cluster-backed benchmark** (e.g. `BenchmarkEquiJoin`, extends `BaseClusterIntegrationTest`) → skill automatically includes the full JDK-21 add-opens set in `-jvmArgsAppend`.
- **Explicit baseline** → `/bench-compare BenchmarkDictionary release-1.5.0` → same flow against a tagged release.
- **Vector benchmark** → `/bench-compare BenchmarkVectorIndex` → uses the `exec:java` form from `pinot-perf/README.md`; it has its own CLI conventions.

**Known quirks:**

- **Stale-jar trap (the real failure mode).** If the current tree's `pinot-perf/target/pinot-perf-pkg/lib/` was built from a previous ref that pulled in different versions of a transitive dep (e.g. `zookeeper-3.9.4.jar` + `zookeeper-3.9.5.jar`), the classpath glob loads both and you get `NoSuchMethodError` at runtime. Pinot/Helix often swallows this as `ZkTimeoutException: Unable to connect to zookeeper server within timeout: 1000`, which looks like an infrastructure/timing issue but is actually a classpath bug. The skill defends against this by always using `./mvnw -pl pinot-perf clean package -DskipTests -am` on the current tree (the worktree is a fresh checkout so baseline is immune). **If you see `ZkTimeoutException` in a second run, don't tune timeouts — check `lib/` for version duplicates.**
- JMH's `-l` flag doesn't help — Pinot benchmark classes have custom `main()` methods that ignore CLI args. There is no fast sanity check; the first real run is also the first verification.
- The generated `pinot-<Class>.sh` scripts hard-code `-Xms24G -Xmx24G`. Avoid them; use the `java -cp 'lib/*'` form with your own `-Xmx`.
- Only ~21 of ~60 benchmark classes are configured as appassembler programs — not every benchmark has a `.sh`. Direct `java -cp` works for all of them.
- Worktrees require a clean `.git`. Abort if rebase/merge is in progress.
- Benchmarks fork their own JVMs; don't be surprised by multiple `java` processes during the run.

---

## `/flaky-analyze`

**What it does.** Queries GitHub Actions via `gh` CLI for recent failing runs of `Pinot Tests`, identifies the failing jobs, downloads the logs, greps for **real** Surefire/TestNG failure markers, clusters by stack trace, and reports a hypothesis.

**Investigation only.** This skill never proposes a fix — the goal is to help the user decide whether the failure is a real bug, a race, or infrastructure noise. Principle C6.2 in `kb/code-review-principles.md` is explicit: the fix is never "add retries".

**The right grep patterns** (documented after burning the wrong ones in testing):
- `\[ERROR\] Tests run: \d+, Failures: [1-9]` — a Surefire class summary with failures. The FQN follows `-- in `.
- `<<< FAILURE!` / `<<< ERROR!` — marker following a failed assertion. Line above has the method.
- `##\[error\]Process completed with exit code` — GitHub Actions' process-level marker.
- `BUILD FAILURE` — Maven's non-zero exit marker.

Do **not** grep for bare `ERROR` / `FAILED` / `Exception` — Pinot's integration tests log those constantly at runtime (Helix rebalancer, Kafka consumer setup, etc.) and you will drown in noise.

**Example scenarios:**

- **Test is genuinely flaky on master** → skill finds multiple failing runs with the same stack trace → hypothesis: likely real race. Suggests specific source file and line, cites C2.x principles.
- **Test only fails on one matrix combination** (e.g. JDK 21 integration set 1) → likely env-specific → suggests checking JDK-specific behaviour.
- **Failing job has no Surefire markers** → infrastructure failure (timeout, OOM, runner cancel). Skill classifies as non-test-level and moves on.
- **Test name never appears** → either the test isn't actually failing (wrong search term) or GitHub's log retention has aged out the runs. Skill reports which and stops.

**Usage:**
- `/flaky-analyze RangeIndexTest` — last 20 failing runs of `Pinot Tests`.
- `/flaky-analyze RangeIndexTest 50` — last 50.
- `/flaky-analyze RangeIndexTest --pr 18267` — only that PR's runs.

**Known quirks:**
- `gh run view --log-failed` is unreliable here — it returns only the steps marked failed, which for Pinot's "Integration Test" step is often just runner init lines. Always use `--log` + grep.
- `gh run view --log` can return tens of MB per job. Cap runs scanned at 50 unless the user asks for more; fetch in parallel where possible.
- GitHub retains Actions logs for 90 days by default. Older flakes require a different data source (e.g. the `surefire-reports-*` artifacts uploaded on master runs).

---

## Related configuration

- [`.claude/agents/code-reviewer.md`](../agents/code-reviewer.md) — independent code-review agent that reads `kb/code-review-principles.md`.
- [`kb/code-review-principles.md`](../../kb/code-review-principles.md) — 163 Pinot-specific review principles; the skills cite these by `C<chapter>.<id>`.
- [`CLAUDE.md`](../../CLAUDE.md) — project-wide instructions consumed by Claude Code.
- [`AGENTS.md`](../../AGENTS.md) — general agent guidance (for Claude Code and other tools).
- [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md) — overlapping guidance for GitHub Copilot / Cursor.

## Adding a new skill

1. Create `.claude/skills/<skill-name>/SKILL.md`.
2. Start with YAML frontmatter containing `name` and `description`. Put the ASF license header as `#` comments inside the frontmatter block so the frontmatter stays valid.
3. Write the procedure in the body as numbered steps — this is what Claude follows. Prefer concrete commands over prose.
4. Keep the description concrete enough that Claude can decide when to invoke the skill from natural-language requests.
5. List the skill in the table above and add a detail section below.

Skills should be narrow, fast to read, and composable. A skill that "runs X and then does a code review" probably belongs as two separate skills chained by the user.

## Troubleshooting

- **Skill isn't invoked when expected.** Claude may not have loaded `.claude/skills/` in its session context. In a new session, ask Claude to list available skills, or re-type the slash command explicitly.
- **Maven wrapper not found.** All skills assume the repo root has `./mvnw`. If you're invoking from a subdirectory, ask Claude to `cd` first, or run from the repo root.
- **`gh` not authenticated.** `/flaky-analyze` requires `gh auth status` to succeed against github.com. Run `gh auth login` once.
- **Worktree errors in `/bench-compare`.** Check that `/tmp/pinot-bench-baseline` isn't a leftover from a previous aborted run — if so, `git worktree remove --force` it and retry.
