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
# CLAUDE.md - Apache Pinot

## What is this project?
Apache Pinot is a real-time distributed OLAP datastore for low-latency analytics over streaming and batch data. Core runtime roles: **broker** (query routing), **server** (segment storage/execution), **controller** (cluster metadata/management), **minion** (async tasks).

## Repository layout
| Directory | Purpose |
|---|---|
| `pinot-broker` | Broker query planning and scatter-gather |
| `pinot-controller` | Controller APIs, table/segment metadata, Helix management |
| `pinot-server` | Server query execution, segment loading, indexing |
| `pinot-minion` | Background tasks (segment conversion, purge, etc.) |
| `pinot-common` / `pinot-spi` | Shared utils, config, and SPI interfaces |
| `pinot-segment-local` / `pinot-segment-spi` | Segment generation, indexes, storage |
| `pinot-query-planner` / `pinot-query-runtime` | Multi-stage query engine (MSQE) |
| `pinot-connectors` | External tooling to connect to Pinot |
| `pinot-plugins` | All Pinot plugins (input formats, filesystems, stream/batch ingestion, metrics, etc.) |
| `pinot-tools` | CLI and quickstart scripts |
| `pinot-integration-tests` | End-to-end validation suites |
| `pinot-distribution` | Packaging artifacts |

When locating a plugin implementation, consult [kb/plugin-modules.md](kb/plugin-modules.md).

## Build commands
Use [AGENTS.md](AGENTS.md#build-and-test) for the build JDK, artifact bytecode targets, and scoped build/test commands.
Use [kb/skills/run-test.md](kb/skills/run-test.md) for targeted tests and
[kb/skills/quickstart.md](kb/skills/quickstart.md) to start and verify a local cluster.

## Code style and formatting
- Run `./mvnw spotless:apply` to auto-format code.
- Run `./mvnw checkstyle:check` to validate style. Checkstyle config is in `config/checkstyle.xml`.
- Run `./mvnw license:format` to add license headers to new files.
- Run `./mvnw license:check` to validate license headers.
- Always use the Maven wrapper (`./mvnw`) rather than a system `mvn`.

## Coding conventions
- Add class-level Javadoc for new classes; describe behavior and thread-safety.
- Use Javadoc syntax supported by the owning module's configured source release; see [AGENTS.md](AGENTS.md#coding-conventions-and-hygiene).
- Keep Apache 2.0 license headers on all new source files.
- Preserve backward compatibility across mixed-version broker/server/controller.
- Prefer imports over fully qualified class names (e.g., use `import com.foo.Bar` and refer to `Bar`, not `com.foo.Bar` inline).
- Prefer `List.of()`, `Set.of()`, and `Map.of()` for non-null immutable collection literals. Checkstyle blocks
  `Collections.emptyList()`, `Collections.emptySet()`, and `Collections.emptyMap()`; use `List.of()`, `Set.of()`, and
  `Map.of()` instead. Do not add blanket bans for `Collections.singleton*`; use them only when an element/key/value
  argument is intentionally null because `List.of(null)`, `Set.of(null)`, and `Map.of(...)` with null keys or values
  throw `NullPointerException`. Before replacing empty collection factories, check whether the value flows to
  mutating callers. See
  `kb/code-review-principles.md` C7.12.
- Prefer targeted unit tests; use integration tests when behavior crosses roles.
- Avoid deprecated APIs in new code. If you must reference one (e.g., for backward-compat serialization or to test the deprecated path), justify it with a comment.

## Commit messages
- Do not include `Co-authored-by` trailers that reference AI tools (e.g., Claude, Copilot).
  - **Why**: These trailers propagate into squash-merge commits on GitHub, making the project history appear AI-authored rather than human-authored.
  - **Fix**: Omit the `Co-authored-by` line entirely when committing.

## Pre-commit checks
Before pushing, follow [kb/skills/precommit.md](kb/skills/precommit.md) for affected-module formatting, license headers,
checkstyle, and license validation. That procedure defines applicability, execution order, evidence reuse, and when
compiler warning checks are needed. Fix failures within the authorized scope; do not push with applicable checks failing.
Claude Code users can invoke `/precommit`.

## Change guidance
- **Query changes** often touch broker planning and server execution; verify both.
- **Segment/index changes** usually live under `pinot-segment-local` and `pinot-segment-spi`.
- **Config or API changes** should update relevant configs and docs where applicable.

## Code review

Review the changed behavior and relevant contracts before declaring implementation complete. Follow
[kb/agents/code-reviewer.md](kb/agents/code-reviewer.md) for requested full reviews and changes that benefit from independent
domain review, especially compatibility, concurrency, security, and distributed state changes. Small changes can be
reviewed directly. Reuse review evidence while the relevant diff and assumptions remain unchanged.
Fix confirmed CRITICAL findings before proceeding; address MAJOR findings or explain why they can be deferred.

## Common gotchas
- This is a large multi-module Maven project. Building the entire project takes a long time — prefer building only the modules you need with `-pl <module> -am`.
- When running tests, use `-Dtest=ClassName` to run a specific test class rather than the full suite.
- Mixed-version compatibility matters — do not break wire protocols or serialization formats without careful consideration.
