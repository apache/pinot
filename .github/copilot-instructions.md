# Project overview
Originally developed at LinkedIn, Apache PinotTM is a real-time distributed OLAP datastore,
purpose-built to provide ultra low-latency analytics at extremely high throughput.

With its distributed architecture and columnar storage, Apache Pinot empowers businesses to gain valuable insights from
real-time data, supporting data-driven decision-making and applications.

This document guides AI coding assistants (Copilot, Cursor, etc.) contributing to this repository.

## Libraries and Frameworks

- Most source code is written in Java 11.
- Code in `pinot-clients` is written in Java 8.
- Pinot UI is a React.js frontend. It is stored in `pinot-controller/src/main/resources/`.
- The code is compiled with Maven and follows a multimodule structure. Use each module's `pom.xml` to
  understand dependencies and configurations.

## Coding Standards

- Include the Apache Software Foundation (ASF) license header in all supported file types (Java, XML, JSON, Markdown).
- Javadoc comments can either be written using the `/** ... */` syntax or the `///` syntax,
  as specified in [JEP-467](https://openjdk.org/jeps/467).
- Methods and parameters are non-null by default unless annotated with `javax.annotation.Nullable`.
- Prefer explicit imports for classes rather than fully qualified names used inline.
- Use SLF4J for logging; do not use `System.out` or `System.err`.
- Propagate exceptions with useful context; avoid blanket `catch (Exception)` and never swallow errors.
- Keep diffs minimal: do not reformat unrelated code and preserve existing whitespace/indentation styles.
- Favor early returns and guard clauses; avoid deep nesting beyond two to three levels.
- Add concise comments that explain "why", not "how"; avoid TODOs—implement instead.

## Repository and Workflow Conventions

- Use SSH Git remotes (`git@github.com:...`) rather than HTTPS.
- Keep changes small and focused; include or update tests when behavior changes.
- Write descriptive commit messages and PR descriptions; link related issues when relevant.
- Prefer absolute file paths in discussions and tool calls for clarity.
- Do not commit generated files, large binaries, or secrets.

## Build and Test

- Build with Maven from the repository root.
- Run unit tests locally before opening a PR; ensure the project compiles cleanly without introducing lint errors.
- Respect the multi-module structure; build order is governed by Maven.

## Domain-specific conventions

- Timezones: When defining or updating `STANDARD_TIMEZONES`, use a three-letter timezone ID with location in the
  format `PDT(America/Los_Angeles)`. Prefer specific timezone IDs over generic UTC offsets, ensure comprehensive
  coverage of common timezones, and sort entries by UTC offset.
- Imports: Favor importing classes at the top of files instead of using fully qualified class names inline.

## Assistant guidance

- When editing, touch only what is necessary; keep changes minimal and localized.
- Preserve the file's existing indentation and whitespace; do not reflow or reformat unrelated code.
- When creating new files, include the ASF license header and all required imports/dependencies.
- Avoid adding extremely long hashes or non-textual content; keep repository contents textual and reviewable.
- If uncertain about where a change belongs, search for similar code in the codebase and follow established patterns.
