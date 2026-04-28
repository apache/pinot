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
name: lint
description: Run all Pinot pre-commit quality checks (formatting, license, style, compiler warnings) on modules affected by the current diff. Auto-fixes what it can, reports what it can't.
---

# /lint

Purpose: before pushing a commit or opening a PR, run all quality checks on the modules the current diff actually touches. Don't run them on the whole repo — that's slow and wasteful on a tree this size.

The five checks (in order):
1. `./mvnw spotless:apply -pl <modules>` — auto-formats code.
2. `./mvnw license:format -pl <modules>` — adds ASF headers to any new files.
3. `./mvnw checkstyle:check -pl <modules>` — validates style; fails hard.
4. `./mvnw license:check -pl <modules>` — validates headers; fails hard.
5. `./mvnw test-compile -pl <modules> -am -Dmaven.compiler.showDeprecation=true -Dmaven.compiler.showWarnings=true '-Dmaven.compiler.compilerArgs=-Xlint:all'` — compiles and checks for deprecation, unchecked casts, raw types, fallthrough, etc. Warnings are filtered to only changed files.

Steps 1 and 2 are auto-fixers. Steps 3 and 4 are validators. Step 5 is a compiler check — if it produces warnings in changed files, report them and stop. Do not try to suppress warnings with `@SuppressWarnings`; fix the underlying issue or ask the user.

## Procedure

1. **Find changed files.**
   - If the user passed an argument (`staged`, `unstaged`, `branch`, or a path), use that as the scope.
   - Default: union of staged + unstaged files vs. HEAD, plus any added-but-untracked `.java` / `.xml` / `.properties` files.
   - Ignore: `target/`, `node_modules/`, generated sources, `**/*.md`, anything under `pinot-controller/src/main/resources/` (UI) unless the user explicitly asks — those aren't covered by the Maven plugins.

2. **Map files to modules.** For each changed file, walk up the directory tree until a `pom.xml` is found. The first directory containing a `pom.xml` that is *not* the repo root is the module. De-duplicate.
   - If the only pom is the repo root, the user is touching top-level config — just run the checks at the root (no `-pl`).
   - Some plugin modules are nested two levels deep (e.g. `pinot-plugins/pinot-input-format/pinot-parquet`). Don't stop at an intermediate aggregator pom if it doesn't define the actual sources — walk up until you find the module that directly contains the changed file.

3. **Report the plan.** Print the list of detected modules in one line: `Modules: pinot-broker, pinot-common, pinot-plugins/pinot-input-format/pinot-parquet`. If there are no modules, say "No changed Java/XML files — nothing to do." and exit.

4. **Run the auto-fixers.** Build a single `-pl` argument with comma-separated modules:
   ```
   ./mvnw spotless:apply -pl <modules>
   ./mvnw license:format -pl <modules>
   ```
   Run each in the foreground. Track the number of files modified by each. If either fails with a non-build error, stop and surface the error.

5. **Run the validators.**
   ```
   ./mvnw checkstyle:check -pl <modules>
   ./mvnw license:check -pl <modules>
   ```
   If either fails, parse the Maven output, extract the file:line of each violation, and track them for the summary. Do not attempt to auto-fix checkstyle violations — they need human judgment.

6. **Run the compiler check.**
   ```
   ./mvnw test-compile -pl <modules> -am -Dmaven.compiler.showDeprecation=true -Dmaven.compiler.showWarnings=true '-Dmaven.compiler.compilerArgs=-Xlint:all'
   ```
   This is the only step that uses `-am` — compilation needs upstream dependencies built, unlike the other steps. Parse the output for `[WARNING]` lines. **Filter to only changed `.java` files** — ignore warnings from upstream modules or unchanged code. Track each warning with file:line and category (deprecation, unchecked, rawtypes, etc.).

   For deprecation warnings specifically: **never suppress** with `@SuppressWarnings("deprecation")`. Find the non-deprecated replacement API. If no replacement exists, report to the user and stop.

7. **Print summary report.** Always print the full report, even when all checks pass:

   ```
   ## Lint Summary — <n> modules

   | Check            | Status | Details                        |
   |------------------|--------|--------------------------------|
   | spotless:apply   | FIXED  | 3 files reformatted            |
   | license:format   | OK     | 0 files needed headers         |
   | checkstyle:check | PASS   |                                |
   | license:check    | PASS   |                                |
   | test-compile -Xlint   | FAIL   | 2 warnings in changed files    |

   ### Auto-fixed (review before staging)
   - spotless reformatted: File1.java, File2.java

   ### Not fixed (requires manual action)
   - `SomeClass.java:45` — [deprecation] Foo.bar() is deprecated, use Foo.baz()
   - `OtherClass.java:12` — [unchecked] unchecked cast to List<String>
   ```

   Status values:
   - **FIXED** — auto-fixer modified files (spotless, license:format)
   - **OK** — auto-fixer ran but nothing needed fixing
   - **PASS** — validator passed with no violations
   - **FAIL** — validator or compiler found issues

   The report must include:
   - The full table for all 5 checks, every time
   - Every unfixed issue with file:line and what to do about it
   - Every auto-fixed file so the user can review before staging
   - For deprecation: the deprecated API and its replacement (if known)

   Do not stage auto-fixed files; that's the user's choice.

## What each step actually enforces

Knowing this matters for diagnosing failures:

- **`spotless:check/apply`**: Pinot's spotless config (see root `pom.xml`) enforces **only two things** — import order (`,\#` → non-static then static) and removal of unused imports. It does **not** enforce trailing whitespace, indentation, brace style, or line length. Don't promise the user that spotless will fix arbitrary formatting.
- **`license:check/format`**: the ASF header from `HEADER` (repo root), applied to `.java`, `.xml`, `.js`, `.sh`, `.md`, etc. Many file types are excluded — see the `licenseSets/excludes` block in the parent `pom.xml`.
- **`checkstyle:check`**: rules from `config/checkstyle.xml`. The common ones contributors trip: `LineLength` (120 chars), `AvoidStarImport`, `AvoidStaticImport`, `HideUtilityClassConstructor`, `NeedBraces`. Output format is `[WARNING] <file>:[<line>] (<group>) <RuleName>: <message>` — parse that when surfacing violations.
- **`license:check`** runs after `license:format` to confirm every touched file now has the header, including files the user only renamed (the plugin keys off content, not git status).
- **`test-compile -Xlint:all`**: uses `test-compile` (not `compile`) so both `src/main/` and `src/test/` sources are checked. The Java compiler flags any reference to `@Deprecated` classes/methods/fields from any dependency (Pinot internal or third-party jars), plus unchecked casts, raw types, fallthrough in switch, and other warning categories. Output format: `[WARNING] /path/File.java:[line,col] <message>`. Warnings are filtered to changed files only to avoid noise from existing code.

## Notes

- Always use `./mvnw`, never a system `mvn`. The repo's CLAUDE.md is explicit on this.
- Don't pass `-am` for steps 1–4 — that builds upstream dependencies too, which defeats the purpose of scoping. Only step 5 (compile) needs `-am` because javac needs dependency jars on the classpath.
- Run sequentially, not in parallel. Spotless and license:format may both modify the same files; ordering matters.
- When `spotless:apply` removes an unused import, it leaves a *leftover blank line* where the import used to be. This is harmless (checkstyle does not flag it), but if the user cares about the cosmetic double-blank, they'll need to hand-clean after the skill runs. Mention this in the report if spotless touched any files.
- If the user says `/lint all`, run on the whole repo (no `-pl`). Warn that this is slow (several minutes).
- Long builds: steps 1–4 are fast (<30s warm). Step 5 (compile with `-am`) is slower (~30–120s depending on module depth and Maven cache state). Use `run_in_background` only if the user explicitly asks — otherwise show progress inline.
- The `license:check` and `checkstyle:check` goals return Maven exit code `1` on violations. If you're capturing the output with shell chaining like `... | tail`, the *tail* pipeline's exit code will mask Maven's — always record Maven's exit code separately, e.g. with `set -o pipefail` or by capturing `${PIPESTATUS[0]}`.
- The compiler warning filter (step 6) must use the set of changed files from step 1. Build a set of absolute paths or filenames from the diff, then match each `[WARNING]` line against that set. This prevents reporting pre-existing warnings in unchanged files.
