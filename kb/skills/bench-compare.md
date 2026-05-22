# bench-compare

Purpose: when a change claims a performance impact (principle C6.7 — "performance-sensitive changes require benchmark comparisons"), produce the before/after numbers in one command, without making the user manually stash, checkout, run, un-stash, and re-run.

Usage:
- `/bench-compare BenchmarkDictionary` — compares current working tree vs. `merge-base HEAD upstream/master` (falls back to `origin/master` if upstream missing).
- `/bench-compare BenchmarkDictionary <baseline-ref>` — compare against an explicit ref (commit, tag, branch).
- `/bench-compare BenchmarkDictionary --args "-wi 1 -i 2 -f 1 -r 5s -w 5s"` — pass extra JMH args. **Always use short warmup/iteration flags for a first pass**; defaults run for hours or days.

**Time expectations.** Pinot benchmarks are not quick. Default JMH config in `pinot-perf` is 8 warmup × 60s + 8 measurement × 60s × 5 forks per parameter combination — a single benchmark method's `@Benchmark` can report an ETA of multiple days. The skill will refuse to run without either: (a) explicit `--args` that reduce warmup/iteration counts, or (b) the user confirming they really do want the full default run.

## Procedure

1. **Locate the benchmark.** Glob for `pinot-perf/**/<benchmarkName>.java`. If zero or multiple matches, report and stop. The benchmark class must be under `pinot-perf`.

2. **Resolve the baseline ref.**
   - Default: `git merge-base HEAD upstream/master`. If the `upstream` remote isn't defined, fall back to `origin/master`. If neither resolves, ask the user for an explicit ref.
   - If the user passed a ref, validate it with `git rev-parse --verify <ref>`.

3. **Prepare output directory.** `mkdir -p .bench-compare/` and append it to the repo's `.gitignore` if not already there. Produce two files: `baseline-<short-sha>.txt` and `current-<short-sha-or-WIP>.txt`.

4. **Warn and confirm.** Benchmarks take real time. Inspect `--args` — if the user hasn't passed iteration controls, warn that the default suite can take hours to days and suggest a starter like `-wi 1 -i 2 -f 1 -r 5s -w 5s`. Print an estimate of the pair of runs (rough: a 5s-warmup × 5s-measurement × 1 fork run takes ~30–120s per `@Benchmark` method after the Pinot-side `@Setup` completes; `@Setup` alone can run for 1–10 minutes for benchmarks that build segments). Ask the user to confirm.

5. **Build pinot-perf in a baseline worktree.** This avoids touching the working tree:
   ```
   git worktree add /tmp/pinot-bench-baseline <baseline-ref>
   (cd /tmp/pinot-bench-baseline && ./mvnw -pl pinot-perf -am package -DskipTests)
   ```
   The package goal produces the jars, an appassembler-generated launcher (for ~21 blessed benchmark classes) at `pinot-perf/target/pinot-perf-pkg/bin/pinot-<BenchmarkClass>.sh`, and a fat `lib/` directory.

6. **Run the baseline benchmark.** Two invocation styles, in order of preference:

   **Preferred — always use JMH's own Main class:**
   ```
   java -Xms4G -Xmx8G -cp '/tmp/pinot-bench-baseline/pinot-perf/target/pinot-perf-pkg/lib/*' \
     org.openjdk.jmh.Main 'org.apache.pinot.perf.<BenchmarkClass>' \
     -wi 1 -i 2 -f 1 -r 5s -w 5s \
     -jvmArgsAppend='-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true' \
     > .bench-compare/baseline-<short-sha>.txt 2>&1
   ```

   Why not use the generated `pinot-<BenchmarkClass>.sh`?
   - It hard-codes `-Xms24G -Xmx24G` — OOMs on <32GB machines.
   - The benchmark's own `main()` (which the script invokes) typically constructs `OptionsBuilder` directly and **ignores CLI args**, so you can't override warmup/iterations or pass `-jvmArgsAppend`. Going through `org.openjdk.jmh.Main` bypasses the custom main and gets you JMH's standard CLI.
   - The `--add-opens`/`--add-exports` flags are mandatory for any benchmark that extends `BaseClusterIntegrationTest` (i.e., spins up a Pinot cluster) on JDK 21 — without them, ZK startup fails with `InaccessibleObjectException` wrapped as `ExceptionInInitializerError`.

   For the vector suite (`BenchmarkVectorIndex`) use the `exec:java` form from `pinot-perf/README.md`; it has its own quirks.

7. **Build and run the current tree.** Two mandatory gotchas:

   - **Always clean first:** `./mvnw -pl pinot-perf clean package -DskipTests` (note the `clean`, no `-am` — see next bullet). If `pinot-perf/target/pinot-perf-pkg/` already exists from a prior build on a different branch/ref, incremental `package` leaves stale third-party jars in `lib/` when a dependency version changes upstream. Those stale jars sit on the classpath alongside the new ones (e.g. `zookeeper-3.9.4.jar` and `zookeeper-3.9.5.jar`) and cause `NoSuchMethodError` at runtime. Crucially, Helix/Pinot swallows the resulting `ExceptionInInitializerError` in ZK startup and surfaces a misleading `ZkTimeoutException: timeout: 1000` instead — which looks for all the world like a flaky port or timing issue. If you see that exception, **check `lib/` for duplicate versions of `zookeeper-*`, `helix-*`, `netty-*`, etc. first.**

   - **Use `-am` only on the first build.** After the first clean+package, upstream modules are populated; subsequent builds can skip `-am`. The worktree build in step 5 gets a fresh `target/` so doesn't have this problem.

   Invocation is identical to step 6, just against the current tree's `lib/*`:
   ```
   ./mvnw -pl pinot-perf clean package -DskipTests -am
   java -Xms4G -Xmx8G -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
     org.openjdk.jmh.Main 'org.apache.pinot.perf.<BenchmarkClass>' <same JMH + jvmArgsAppend flags> \
     > .bench-compare/current-<sha-or-WIP>.txt 2>&1
   ```

8. **Clean up the worktree.** `git worktree remove /tmp/pinot-bench-baseline --force`. Do this even if steps 6 or 7 failed.

9. **Diff the results.** Parse JMH's table output (the `Benchmark ... Score Error Units` lines) from both files. Produce a table:
   ```
   Benchmark          Baseline (ops/s)    Current (ops/s)    Δ        Δ%
   foo.methodA        1234.5 ± 12.1       1478.2 ± 15.3      +243.7   +19.7%
   foo.methodB         987.6 ±  8.0        992.1 ±  7.2       +4.5     +0.4%
   ```
   Use "ops/s", "ns/op", or whatever unit JMH emits — don't convert.

10. **Report.** Print the table. Flag any benchmark where `|Δ%| > 2×error%` as likely a real change (otherwise probably noise). Include the paths to the raw files so the user can share them in a PR.

## Notes

- **Stale-jar trap is the #1 source of mysterious failures.** Pinot benchmarks that fail on a subsequent run of `/bench-compare` in the same repo almost always fail because of duplicate third-party jars in `pinot-perf/target/pinot-perf-pkg/lib/` — typically `zookeeper-X.jar` + `zookeeper-Y.jar` (or equivalent for helix, netty, guava) from different builds. The failure shows up as a deeply-wrapped `ZkTimeoutException: Unable to connect to zookeeper server within timeout: 1000` (or similar NoSuchMethodError swallowed into an infrastructure-looking error). First diagnostic when a second run fails: `ls pinot-perf/target/pinot-perf-pkg/lib/ | sort | awk -F- '{v=$NF; sub("\\.jar$","",v); k=$0; sub("-"v"\\.jar$","",k); print k}' | sort | uniq -d` to spot duplicates. The fix is always `./mvnw -pl pinot-perf clean package -DskipTests -am`, never `rm` individual jars.
- Worktrees require a clean `.git`. If the repo is in the middle of a rebase/merge, abort with a clear message.
- **JDK 21 needs the full `--add-opens` / `--add-exports` flag set** for any cluster-backed benchmark (extends `BaseClusterIntegrationTest`). Without them, ZK startup fails with `InaccessibleObjectException: ... module java.base does not "opens java.lang"`. Pass via `-jvmArgsAppend=...` to `org.openjdk.jmh.Main`; CI's `pinot_tests.yml` has the canonical list.
- **The generated `pinot-<BenchmarkClass>.sh` scripts hard-code `-Xms24G -Xmx24G`.** Avoid them — use `java -cp 'lib/*' org.openjdk.jmh.Main <FQN>` directly with your own `-Xmx`.
- **Not every benchmark has a generated script.** The appassembler programs list in `pinot-perf/pom.xml` covers ~21 of ~60 benchmark classes. The direct `java -cp` invocation works for any of them.
- JMH's `-l` (list benchmarks) flag **does not help here** — Pinot benchmark classes have custom `main()` entry points that construct `OptionsBuilder` directly, ignore CLI args, and plunge straight into `Runner.run(...)` which in turn kicks off `@Setup`. For `BenchmarkDictionary` this `@Setup` alone burns 5+ minutes building dictionaries. There is no fast sanity-check short of actually running the benchmark through `org.openjdk.jmh.Main` (which at least accepts `-wi 1 -i 1 -r 1s -w 1s` to minimise it).
- Benchmarks must run on the same hardware, same JDK, same OS load. Warn the user if they're on battery power or running other heavy processes.
- Do not `sleep` between runs for "timing" reasons. If a second run fails, it is the stale-jar issue (above), not TIME_WAIT. I spent a long time chasing the timing hypothesis before spotting the classpath mismatch.
- If the benchmark's output format isn't plain JMH (e.g. `BenchmarkVectorIndex` writes a custom report), don't try to parse it — just save both outputs and tell the user where they are, with a note that manual comparison is needed.
- Never use `git stash` instead of a worktree. Stash can be lost if the second build fails and the user doesn't know to pop it.
