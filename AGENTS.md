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
# Apache Pinot - AGENTS Guide

This file provides quick, practical guidance for coding agents working in this
repo. It is intentionally short and focused on day-to-day work.

## Project overview
- Apache Pinot is a real-time distributed OLAP datastore for low-latency
  analytics over streaming and batch data.
- Core runtime roles: broker (query routing), server (segment storage/execution),
  controller (cluster metadata/management), minion (async tasks).

## Repository layout (high level)
- pinot-broker: broker query planning and scatter-gather.
- pinot-controller: controller APIs, table/segment metadata, Helix management.
- pinot-server: server query execution, segment loading, indexing.
- pinot-minion: background tasks (segment conversion, purge, etc).
- pinot-common / pinot-spi: shared utils, config, and SPI interfaces.
- pinot-segment-local / pinot-segment-spi: segment generation, indexes, storage.
- pinot-query-planner / pinot-query-runtime: multi-stage query (MSQ) engine.
- pinot-connectors: external tooling to connect to Pinot
- pinot-plugins: all pinot plugins.
- pinot-tools: CLI and quickstart scripts.
- pinot-integration-tests: end-to-end validation suites.
- pinot-distribution: packaging artifacts.

When locating a plugin implementation, consult [kb/plugin-modules.md](kb/plugin-modules.md).

## Build and test
- Build JDK: Use JDK 25+ for Pinot services and the default build; client and SPI artifacts still target Java 11 bytecode.
- Runtime JRE: Broker/server/controller/minion run on Java 25+.
- Default to affected modules and their dependencies: `./mvnw -pl <module> -am test`.
- Faster module build: `./mvnw -pl <module> -am verify -Ppinot-fastdev`. This profile skips style and license checks;
  it does not replace applicable pre-push validation.
- Single test example: `./mvnw -pl pinot-segment-local -am -Dtest=RangeIndexTest -Dsurefire.failIfNoSpecifiedTests=false test`.
  See [kb/skills/run-test.md](kb/skills/run-test.md) for dependency availability and evidence reuse.
- Full reactor build, when the task requires it: `./mvnw clean install`.
- Full binary/shaded distribution, when required: `./mvnw clean install -DskipTests -Pbin-dist -Pbuild-shaded-jar`.
- Local cluster startup: follow [kb/skills/quickstart.md](kb/skills/quickstart.md) for the smallest required build and readiness checks.

## Integration tests
- Single integration test example: `./mvnw -pl pinot-integration-tests -am -Dtest=OfflineClusterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`

## Coding conventions and hygiene
- Add class-level Javadoc for new classes; describe behavior and thread-safety.
- Use Javadoc comments with either `/** ... */` or `///` syntax (per JEP-467); service code targets Java 25 by default.
- Keep license headers on all new source files.
- Use `./mvnw license:format` to add headers to new files.
- Preserve backward compatibility across mixed-version broker/server/controller.
- Prefer imports over fully qualified class names (e.g., use `import com.foo.Bar` and refer to `Bar`, not `com.foo.Bar` inline).
- In tests, statically import methods from `Assert` and `Mockito`; do not qualify calls with `Assert.` or `Mockito.`.
- Import `FieldSpec.DataType` and `DataSchema.ColumnDataType` directly and use their simple names at call sites.
- Keep a ternary expression on one line when it fits. Otherwise, put `? <true-expression>` and
  `: <false-expression>` on separate lines.
- Prefer `List.of()`, `Set.of()`, and `Map.of()` for non-null immutable collection literals. Checkstyle blocks
  `Collections.emptyList()`, `Collections.emptySet()`, and `Collections.emptyMap()`; use `List.of()`, `Set.of()`, and
  `Map.of()` instead. Do not add blanket bans for `Collections.singleton*`; use them only when an element/key/value
  argument is intentionally null because `List.of(null)`, `Set.of(null)`, and `Map.of(...)` with null keys or values
  throw `NullPointerException`. Before replacing empty collection factories, check whether the value flows to
  mutating callers. See
  `kb/code-review-principles.md` C7.12.
- Prefer targeted unit tests; use integration tests when behavior crosses roles.

## Commit messages
- Do not include `Co-authored-by` trailers that reference AI tools (e.g., Claude, Copilot).
  - **Why**: These trailers propagate into squash-merge commits on GitHub, making the project history appear AI-authored rather than human-authored.
  - **Fix**: Omit the `Co-authored-by` line entirely when committing.

## Checkstyle config
- Checkstyle rules and related config files live under `config/`.
- Use the Maven wrapper (`./mvnw` on Unix-like systems or `mvnw.cmd` on Windows) to run `spotless:apply` to format code and `checkstyle:check` to validate style.
- Run `./mvnw license:check` to validate license headers.

## Pre-commit checks
Before pushing, follow [kb/skills/precommit.md](kb/skills/precommit.md) for affected-module formatting, license headers,
checkstyle, and license validation. That procedure defines applicability, execution order, evidence reuse, and when
compiler warning checks are needed. Fix failures within the authorized scope; do not push with applicable checks failing.

## Change guidance
- Query changes often touch broker planning and server execution; verify both.
- Segment/index changes usually live under `pinot-segment-local` and
  `pinot-segment-spi`.
- Config or API changes should update relevant configs and docs where applicable.

## Reference docs
- `README.md` for build and quickstart details.
- `CONTRIBUTING.md` for style, licensing, and contribution guidance.

## Knowledge base (tool-neutral)
The `kb/` directory holds AI-optimized procedures and reference material that any
coding agent (Claude Code, Copilot, Cursor, GPT, Qwen, Gemini, etc.) can read.
Claude Code's `.claude/skills/<name>/SKILL.md` and `.claude/agents/<name>.md`
files are thin pointers that delegate to the kb/ procedures — non-Claude agents
should read kb/ directly.

- `kb/skills/` — operational procedures and review checklists. See
  [`kb/skills/README.md`](kb/skills/README.md) for the index. Each file is
  self-contained; read it and follow it when your task matches the skill name.
  - Operations: `precommit`, `run-test`, `quickstart`, `bench-compare`,
    `flaky-analyze`.
  - Review (eight domains, one per file): `review-config-backcompat`,
    `review-concurrency-state`, `review-architecture`, `review-performance`,
    `review-correctness-nulls`, `review-testing`, `review-naming-api`,
    `review-process-scope`.
- `kb/agents/code-reviewer.md` — review procedure that selects relevant domains,
  delegates substantial independent checks, and verifies consolidated findings.
- `kb/code-review-principles.md` — Pinot-specific review principles cited by id
  (e.g. `C2.4`, `C6.1`) from the review skills.
- `kb/claude.md` — kb/ authoring rules (one source of truth, terse, AI-optimized).

**For non-Claude agents:** when a task matches a skill name (e.g. user asks for
a pre-commit check, a benchmark comparison, a flaky-test investigation, or a
code review), read the corresponding `kb/skills/<name>.md` and follow its
procedure. For a full code review, follow `kb/agents/code-reviewer.md` and cover
the domains relevant to the diff; small changes can be reviewed directly.
