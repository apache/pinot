# bench-compare

Purpose: when a change claims a performance impact (principle C6.7 — "performance-sensitive changes require benchmark comparisons"), produce the before/after numbers in one command, without making the user manually stash, checkout, run, un-stash, and re-run.

Usage:
- `/bench-compare BenchmarkDictionary` — compares current working tree vs. `merge-base HEAD upstream/master` (falls back to `origin/master` if upstream missing).
- `/bench-compare BenchmarkDictionary <baseline-ref>` — compare against an explicit ref (commit, tag, branch).
- `/bench-compare BenchmarkDictionary --args "-wi 1 -i 2 -f 1 -r 5s -w 5s"` — pass extra JMH args. Use short warmup/iteration flags for an exploratory first pass unless the user authorized a longer run; defaults can run for hours or days.

**Time expectations.** Pinot benchmarks are not quick. Default JMH config in `pinot-perf` is 8 warmup × 60s + 8 measurement × 60s × 5 forks per parameter combination — a single benchmark method's `@Benchmark` can report an ETA of multiple days. Proceed with explicit bounded arguments or an already authorized time budget. Ask before an unbounded run or extending that budget; do not ask for the same authorization again.

## Procedure

1. **Locate the benchmark.** Glob for `pinot-perf/**/<benchmarkName>.java`. If zero or multiple matches, report and stop. The benchmark class must be under `pinot-perf`.

2. **Resolve the baseline ref.**
   - Default: `git merge-base HEAD upstream/master`. If the `upstream` remote isn't defined, fall back to `origin/master`. If neither resolves, ask the user for an explicit ref.
   - If the user passed a ref, resolve it to a commit with `git rev-parse --verify '<ref>^{commit}'`. Record the resolved baseline SHA so both the build and report use the same commit.

3. **Prepare a unique task directory outside the checkout.** Keep results separate from the temporary baseline worktree; do not modify the repository's `.gitignore` for local benchmark artifacts. The following examples share these variables; replace `<baseline-ref>` with the ref resolved in step 2:
   ```sh
   bench_repo=$(git rev-parse --show-toplevel) || exit 1
   bench_baseline_sha=$(git rev-parse --verify '<baseline-ref>^{commit}') || exit 1
   bench_run_dir=$(mktemp -d "${TMPDIR:-/tmp}/pinot-bench.XXXXXX") || exit 1
   bench_worktree="$bench_run_dir/baseline"
   bench_results_dir="$bench_run_dir/results"
   bench_worktree_created=false
   mkdir "$bench_results_dir" || exit 1
   ```
   Retain build logs, benchmark output, commands, resolved SHAs, and the scope of any uncommitted changes in the results directory. Report its absolute path; cleanup in step 8 removes only the owned baseline worktree.

4. **Check the run budget.** Inspect the selected methods, parameter combinations, forks, warmup, measurement, and setup cost; estimate both builds and both runs. If explicit short arguments or an existing budget cover the work, state the estimate and proceed. With only a time budget, choose bounded arguments that fit it. Otherwise suggest a starter like `-wi 1 -i 2 -f 1 -r 5s -w 5s` and ask for the missing budget. Setup can take 1–10 minutes for benchmarks that build segments. If the estimate or observed runtime exceeds the authorized budget, preserve partial output and ask before extending the run.

5. **Build pinot-perf in the baseline worktree.** Record ownership only after this task successfully creates it. Stop dependent steps if creation fails; do not reuse or delete an existing path:
   ```sh
   if git -C "$bench_repo" worktree add --detach "$bench_worktree" "$bench_baseline_sha"; then
     bench_worktree_created=true
   else
     exit 1
   fi
   (cd "$bench_worktree" && ./mvnw -pl pinot-perf -am package -DskipTests) \
     > "$bench_results_dir/baseline-build.txt" 2>&1
   ```
   The package goal produces the jars, an appassembler-generated launcher (for ~21 blessed benchmark classes) at `pinot-perf/target/pinot-perf-pkg/bin/pinot-<BenchmarkClass>.sh`, and a fat `lib/` directory.

6. **Verify selection and run the baseline benchmark.** Before starting measurements, list the selected methods and parameters without invoking their setup:

   ```sh
   java -cp "$bench_worktree/pinot-perf/target/pinot-perf-pkg/lib/*" \
     org.openjdk.jmh.Main -lp 'org.apache.pinot.perf.<BenchmarkClass>'
   ```
   Use `-l` instead of `-lp` when only method names are needed. Check the list against the budget from step 4; narrow the selector or parameters when needed. Do not invoke the benchmark's custom `main()` for discovery.

   **Preferred — always use JMH's own Main class:**
   ```
   (cd "$bench_worktree" && java -Xms4G -Xmx8G -cp "$bench_worktree/pinot-perf/target/pinot-perf-pkg/lib/*" \
     org.openjdk.jmh.Main 'org.apache.pinot.perf.<BenchmarkClass>' \
     -wi 1 -i 2 -f 1 -r 5s -w 5s \
     -jvmArgsAppend='-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true') \
     > "$bench_results_dir/baseline.txt" 2>&1
   ```

   Why not use the generated `pinot-<BenchmarkClass>.sh`?
   - It hard-codes `-Xms24G -Xmx24G` — OOMs on <32GB machines.
   - The benchmark's own `main()` (which the script invokes) typically constructs `OptionsBuilder` directly and **ignores CLI args**, so you can't override warmup/iterations or pass `-jvmArgsAppend`. Going through `org.openjdk.jmh.Main` bypasses the custom main and gets you JMH's standard CLI.
   - Cluster-backed benchmarks can require `--add-opens`/`--add-exports` on the current JDK. Use the required flags consistently for both versions; diagnose module-access failures from their underlying exception.

   For the vector suite (`BenchmarkVectorIndex`) use the `exec:java` form from `pinot-perf/README.md`; it has its own quirks.

7. **Build and run the current tree.** Verify dependency and packaging state before measuring:

   - Existing appassembler output can retain old dependency jars after version changes. Inspect the build output and runtime classpath if versions changed or errors suggest a mismatch. If stale packaging is confirmed, clean only `pinot-perf` with `./mvnw -pl pinot-perf clean`, then run the package command below. Preserve prior diagnostic output before cleaning.

   - Keep `-am` unless dependency artifacts satisfy the evidence requirements in kb/skills/run-test.md. A prior reactor `package` alone does not install artifacts for a module-only invocation.

   Invocation is identical to step 6, just against the current tree's `lib/*`:
   ```
   (cd "$bench_repo" && ./mvnw -pl pinot-perf -am package -DskipTests) \
     > "$bench_results_dir/current-build.txt" 2>&1
   (cd "$bench_repo" && java -Xms4G -Xmx8G -cp "$bench_repo/pinot-perf/target/pinot-perf-pkg/lib/*" \
     org.openjdk.jmh.Main 'org.apache.pinot.perf.<BenchmarkClass>' <same JMH + jvmArgsAppend flags>) \
     > "$bench_results_dir/current.txt" 2>&1
   ```

8. **Clean up only the worktree created by this run.** After measurements finish or fail, check `bench_worktree_created`, inspect `git -C "$bench_repo" worktree list --porcelain` and `git -C "$bench_worktree" status --porcelain`, and verify the registered path and baseline SHA match this run. Once all its processes have stopped and there are no unexpected changes, remove it with:
   ```sh
   if [ "$bench_worktree_created" = true ]; then
     git -C "$bench_repo" worktree remove "$bench_worktree"
   fi
   ```
   If ownership cannot be verified, unexpected changes exist, or removal refuses, retain the path and report the reason. Do not add `--force`, remove another run's directory, or delete the results directory.

9. **Diff the results.** Parse JMH's table output (the `Benchmark ... Score Error Units` lines) from both files. Produce a table:
   ```
   Benchmark          Baseline (ops/s)    Current (ops/s)    Δ        Δ%
   foo.methodA        1234.5 ± 12.1       1478.2 ± 15.3      +243.7   +19.7%
   foo.methodB         987.6 ±  8.0        992.1 ±  7.2       +4.5     +0.4%
   ```
   Use "ops/s", "ns/op", or whatever unit JMH emits — don't convert.

10. **Report.** Print the table. Flag any benchmark where `|Δ%| > 2×error%` as likely a real change (otherwise probably noise). Include the paths to the raw files so the user can share them in a PR.

## Notes

- Long builds and benchmark runs may execute asynchronously. Preserve logs and each process's exit code, provide meaningful progress, and await completion before comparing results. Keep baseline and current measurements sequential to avoid resource contention.
- Diagnose failures from the full exception chain, build logs, runtime classpath, and resource state. Duplicate dependency jars are one possible cause of linkage errors or wrapped ZK startup failures; a timeout alone does not establish that cause. Rebuild only when the evidence supports it, and retain unexplained failures as unresolved.
- An uncommitted working tree is supported. If an in-progress merge/rebase leaves the intended source state ambiguous, resolve the scope before building; do not abort or alter that Git operation automatically.
- Consult the current [integration-test workflow](../../.github/workflows/pinot_integration_tests.yml) for JVM module-access flags. Pass required benchmark-fork flags via `-jvmArgsAppend` to `org.openjdk.jmh.Main` using the same settings for baseline and current runs.
- **The generated `pinot-<BenchmarkClass>.sh` scripts hard-code `-Xms24G -Xmx24G`.** Avoid them — use `java -cp 'lib/*' org.openjdk.jmh.Main <FQN>` directly with your own `-Xmx`.
- **Not every benchmark has a generated script.** The appassembler programs list in `pinot-perf/pom.xml` covers ~21 of ~60 benchmark classes. The direct `java -cp` invocation works for any of them.
- Benchmarks must run on the same hardware, same JDK, same OS load. Warn the user if they're on battery power or running other heavy processes.
- Do not insert arbitrary sleeps or retries after a failed run; first inspect the failure and verify that its processes and exclusive resources have been released.
- If the benchmark's output format isn't plain JMH (e.g. `BenchmarkVectorIndex` writes a custom report), don't try to parse it — just save both outputs and tell the user where they are, with a note that manual comparison is needed.
- Never use `git stash` instead of a worktree. Stash can be lost if the second build fails and the user doesn't know to pop it.
