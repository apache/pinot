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
name: precommit
description: Run Pinot's mandatory pre-commit checks (spotless, license, checkstyle) on only the modules affected by the current diff. Auto-fixes what it can.
---

# /precommit

Purpose: before pushing a commit or opening a PR, run the four mandatory pre-commit checks from `CLAUDE.md` on the modules the current diff actually touches. Don't run them on the whole repo — that's slow and wasteful on a tree this size.

The four checks (in order):
1. `./mvnw spotless:apply -pl <modules>` — auto-formats code.
2. `./mvnw license:format -pl <modules>` — adds ASF headers to any new files.
3. `./mvnw checkstyle:check -pl <modules>` — validates style; fails hard.
4. `./mvnw license:check -pl <modules>` — validates headers; fails hard.

Steps 1 and 2 are auto-fixers. Steps 3 and 4 are validators — if they fail after the auto-fixers ran, report the failure with the exact offending file/line from the Maven output and stop. Do not try to manually patch style errors; fix the underlying issue or ask the user.

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
   Run each in the foreground. If either fails with a non-build error (not a style error — those go through checkstyle), stop and surface the error.

5. **Run the validators.**
   ```
   ./mvnw checkstyle:check -pl <modules>
   ./mvnw license:check -pl <modules>
   ```
   If either fails, parse the Maven output, extract the file:line of each violation, and report them. Do not attempt to auto-fix checkstyle violations — they need human judgment.

6. **If the auto-fixers changed files, tell the user.** Do not stage them; that's the user's choice. Say: "spotless/license:format modified N files. Run `git diff` to review, then stage and commit."

7. **Final status.** Green: "All four checks passed on <n> modules." Red: list which check failed and how many violations.

## What each step actually enforces

Knowing this matters for diagnosing failures:

- **`spotless:check/apply`**: Pinot's spotless config (see root `pom.xml`) enforces **only two things** — import order (`,\#` → non-static then static) and removal of unused imports. It does **not** enforce trailing whitespace, indentation, brace style, or line length. Don't promise the user that spotless will fix arbitrary formatting.
- **`license:check/format`**: the ASF header from `HEADER` (repo root), applied to `.java`, `.xml`, `.js`, `.sh`, `.md`, etc. Many file types are excluded — see the `licenseSets/excludes` block in the parent `pom.xml`.
- **`checkstyle:check`**: rules from `config/checkstyle.xml`. The common ones contributors trip: `LineLength` (120 chars), `AvoidStarImport`, `AvoidStaticImport`, `HideUtilityClassConstructor`, `NeedBraces`. Output format is `[WARNING] <file>:[<line>] (<group>) <RuleName>: <message>` — parse that when surfacing violations.
- **`license:check`** runs after `license:format` to confirm every touched file now has the header, including files the user only renamed (the plugin keys off content, not git status).

## Notes

- Always use `./mvnw`, never a system `mvn`. The repo's CLAUDE.md is explicit on this.
- Don't pass `-am` — that builds upstream dependencies too, which defeats the purpose of scoping. The Maven lifecycle plugins don't need upstream builds to run.
- Run sequentially, not in parallel. Spotless and license:format may both modify the same files; ordering matters.
- When `spotless:apply` removes an unused import, it leaves a *leftover blank line* where the import used to be. This is harmless (checkstyle does not flag it), but if the user cares about the cosmetic double-blank, they'll need to hand-clean after the skill runs. Mention this in the report if spotless touched any files.
- If the user says `/precommit all`, run on the whole repo (no `-pl`). Warn that this is slow (several minutes).
- Long builds: these commands can take 60–300s on a cold Maven cache, usually <30s when warm. Use `run_in_background` only if the user explicitly asks — otherwise show progress inline.
- The `license:check` and `checkstyle:check` goals return Maven exit code `1` on violations. If you're capturing the output with shell chaining like `... | tail`, the *tail* pipeline's exit code will mask Maven's — always record Maven's exit code separately, e.g. with `set -o pipefail` or by capturing `${PIPESTATUS[0]}`.
