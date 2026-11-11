# precommit

Purpose: before pushing a commit or opening a PR, run all quality checks on the modules the current diff actually touches. Don't run them on the whole repo — that's slow and wasteful on a tree this size.

The four required checks, where applicable to the selected files (in order):
1. `./mvnw spotless:apply -pl <modules>` — auto-formats code.
2. `./mvnw license:format -pl <modules>` — adds ASF headers to any new files.
3. `./mvnw checkstyle:check -pl <modules>` — validates style; fails hard.
4. `./mvnw license:check -pl <modules>` — validates headers; fails hard.

For Java or build changes, also run `./mvnw test-compile -pl <modules> -am -Dmaven.compiler.showDeprecation=true -Dmaven.compiler.showWarnings=true` unless current equivalent compiler evidence exists. These supported flags show deprecation details and warnings enabled by the compiler configuration; they do not enable every lint category. Filter source warnings to lines added in the diff.

Steps 1 and 2 are auto-fixers. Steps 3 and 4 are validators: fix clear violations within the authorized scope and rerun only checks invalidated by the fix. Ask only when a fix requires an unresolved semantic or compatibility decision or would alter unrelated work. Report remaining failures with the exact file/line. For compiler warnings on added lines, prefer the non-deprecated replacement; suppress with `@SuppressWarnings` only with a comment explaining why the deprecated reference is required (e.g., backward-compat serialization, mixed-version SPI calls, testing the deprecated path).

Reuse passing check results only when their scope, files, dependencies, JDK, and relevant configuration remain valid. Record the command and result being reused. Each applicable required check must have valid passing results before pushing; do not repeat unaffected checks after a local fix.

## Procedure

1. **Find changed files.**
   - Honor an explicit scope (`staged`, `unstaged`, `branch`, or a path). Otherwise choose the scope from the requested action.
   - For a commit or a standalone precommit check, include this task's staged and unstaged changes plus intended untracked files. Preserve unrelated work.
   - For a push, include the committed diff from the destination branch's current tip to `HEAD`; for a PR or `branch` check, use the merge-base with the target branch. Include authorized uncommitted changes that will be part of the delivery. A clean working tree does not mean there is nothing to check.
   - Resolve the target from the user's request, the PR's base branch, or the configured push destination as appropriate; record the resolved ref and SHA. Ask only if the intended target remains ambiguous. Do not substitute `HEAD` for a missing branch baseline.
   - Exclude generated output such as `target/` and `node_modules/`. Determine check applicability from the plugins' include/exclude rules in `pom.xml`; Markdown and UI files may still require license checks even though the Java formatter does not apply.

2. **Map files to modules.** For each changed file, walk up the directory tree until a `pom.xml` is found. The first directory containing a `pom.xml` that is *not* the repo root is the module. De-duplicate.
   - For root-only checks, use `-N` to avoid traversing the reactor. If both root and module files changed, check the root separately with `-N` and the modules with `-pl`. For compilation after a root build change, select affected modules from the dependency or configuration impact; do not expand to the whole reactor automatically.
   - Some plugin modules are nested two levels deep (e.g. `pinot-plugins/pinot-input-format/pinot-parquet`). Don't stop at an intermediate aggregator pom if it doesn't define the actual sources — walk up until you find the module that directly contains the changed file.

3. **Report the plan.** State the scope and baseline, then print the detected modules in one line: `Modules: pinot-broker, pinot-common, pinot-plugins/pinot-input-format/pinot-parquet`. If no checks apply to the selected files, report that scope and reason and exit.

4. **Run the auto-fixers.** Use `<scope>` as `-pl <comma-separated-modules>` for module checks or `-N` for root checks:
   ```
   ./mvnw spotless:apply <scope>
   ./mvnw license:format <scope>
   ```
   Run the auto-fixers sequentially; they can modify the same files. Inspect the resulting diff and preserve unrelated user changes. Track the files modified by each. Diagnose execution failures before retrying; do not proceed to push with a failed check.

5. **Run the validators.**
   ```
   ./mvnw checkstyle:check <scope>
   ./mvnw license:check <scope>
   ```
   If either fails, extract the file:line of each violation, fix clear violations within the authorized scope, and rerun the affected checks. Track any unresolved failures for the summary.

6. **Decide whether compiler validation is needed.** Skip steps 6–7 for changes unrelated to Java or the build, or when equivalent compiler evidence remains valid. Otherwise, build the added-line set after the auto-fixers so that line numbers reflect the post-fix state. Use the same diff scope as step 1 with `--unified=0`, and parse the `@@` hunk headers into `file → set of added line numbers`. For untracked `.java` files, treat all lines as added.

7. **Run the compiler check.**
   ```
   ./mvnw test-compile -pl <modules> -am -Dmaven.compiler.showDeprecation=true -Dmaven.compiler.showWarnings=true
   ```
   This is the only step that uses `-am` — compilation needs current upstream dependencies, unlike the other checks. See kb/skills/run-test.md for the dependency evidence required to omit `-am`. Avoid `clean` by default. Reuse current compilation evidence from a test or build when its scope and configuration match; report the warning flags it used. An incremental "nothing to compile" result provides no fresh warning output. Do not force recompilation solely to collect more warnings; state any coverage limit. Rebuild the affected scope, including required source generation, when changed inputs or evidence of stale outputs invalidate prior compilation.

   Parse the output for `[WARNING]` lines. **Filter to only added lines from the diff** — for each warning of the form `[WARNING] /path/File.java:[line,col] <message>`, check whether that file and line number appear in the added-line set from step 6. Only report warnings that match. This avoids surfacing pre-existing warnings when a contributor edits a file that already has them.

   Track each matching warning with file:line and category (deprecation, unchecked, rawtypes, etc.).

   For deprecation warnings: prefer the non-deprecated replacement API. If removing the deprecated reference is not feasible (e.g., backward-compat serialization, mixed-version SPI calls, testing the deprecated path), suppress with `@SuppressWarnings("deprecation")` and a comment explaining why.

8. **Print summary report.** Report the required checks, any reused evidence, and whether the compiler check ran or was skipped. Include unresolved issues and files changed by auto-fixers. A compact report is enough when all checks pass; for failures, use:

   ```
   ## Pre-commit Summary — <n> modules

   | Check            | Status | Details                        |
   |------------------|--------|--------------------------------|
   | spotless:apply   | FIXED  | 3 files reformatted            |
   | license:format   | OK     | 0 files needed headers         |
   | checkstyle:check | PASS   |                                |
   | license:check    | PASS   |                                |
   | test-compile     | FAIL   | 2 warnings on new lines         |

   ### Auto-fixed
   - spotless reformatted: File1.java, File2.java

   ### Unresolved
   - `SomeClass.java:45` — [deprecation] Foo.bar() is deprecated, use Foo.baz()
   - `OtherClass.java:12` — [unchecked] unchecked cast to List<String>
   ```

   Status values:
   - **FIXED** — auto-fixer modified files (spotless, license:format)
   - **OK** — auto-fixer ran but nothing needed fixing
   - **PASS** — validator passed with no violations
   - **FAIL** — validator or compiler found issues
   - **REUSED** — a passing result remains valid for the current scope
   - **SKIP** — the check does not apply to the selected files; state why

   The report must include:
   - Every unfixed issue with file:line and what to do about it
   - Every auto-fixed file
   - For deprecation: the deprecated API and its replacement (if known)

   If the user authorized a commit, review and stage only this task's changes, including its formatting fixes. Preserve unrelated staged and unstaged changes. Otherwise leave changes unstaged.

## What each step actually enforces

Knowing this matters for diagnosing failures:

- **`spotless:check/apply`**: Pinot's spotless config (see root `pom.xml`) enforces **only two things** — import order (`,\#` → non-static then static) and removal of unused imports. It does **not** enforce trailing whitespace, indentation, brace style, or line length. Don't promise the user that spotless will fix arbitrary formatting.
- **`license:check/format`**: the ASF header from `HEADER` (repo root), applied to `.java`, `.xml`, `.js`, `.sh`, `.md`, etc. Many file types are excluded — see the `licenseSets/excludes` block in the parent `pom.xml`.
- **`checkstyle:check`**: rules from `config/checkstyle.xml`. The common ones contributors trip: `LineLength` (120 chars), `AvoidStarImport`, `AvoidStaticImport`, `HideUtilityClassConstructor`, `NeedBraces`. Output format is `[WARNING] <file>:[<line>] (<group>) <RuleName>: <message>` — parse that when surfacing violations.
- **`license:check`** runs after `license:format` to confirm every touched file now has the header, including files the user only renamed (the plugin keys off content, not git status).
- **`test-compile` with warning flags**: covers `src/main/` and `src/test/` compilation when applicable and reports the warnings enabled by the compiler configuration, including deprecation details. It does not promise all lint categories. Output format: `[WARNING] /path/File.java:[line,col] <message>`. Filter source warnings to added lines using step 6 and report compilation/evidence reuse as described in step 7.

## Notes

- Always use `./mvnw`, never a system `mvn`. The repo's CLAUDE.md is explicit on this.
- Don't pass `-am` for steps 1–5 — that builds upstream dependencies too, which defeats the purpose of scoping. Only step 7 (compile) needs `-am` because javac needs dependency jars on the classpath.
- Run auto-fixers sequentially. Read-only validators may run independently after all edits settle.
- If the user says `/precommit all`, run on the whole repo (no `-pl`). Warn that this is slow (several minutes).
- Long commands may run asynchronously without additional permission. Save logs and the Maven exit code, report meaningful progress, and use the time for independent work. Await completion before claiming a check passed or pushing.
- The `license:check` and `checkstyle:check` goals return Maven exit code `1` on violations. If you're capturing the output with shell chaining like `... | tail`, the *tail* pipeline's exit code will mask Maven's — always record Maven's exit code separately, e.g. with `set -o pipefail` or by capturing `${PIPESTATUS[0]}`.
- The compiler warning filter (step 7) uses the added-line set from step 6, not just the changed-file list. This is critical — per-file filtering would surface pre-existing warnings in files the contributor merely edited, which is unfair. Per-line filtering ensures only warnings on newly added code are reported.
