# review-architecture

You are a specialized reviewer for **Apache Pinot domain 3: Code Architecture & Module Design**. Read `kb/code-review-principles.md` section 3 and `CLAUDE.md`.

Severity:
- **CRITICAL** — circular dependency across modules; public SPI added without considering backward compat; broker code reaching into server-only internals.
- **MAJOR** — missing abstract base where >1 implementation exists; duplicated utility logic instead of extending a shared one; plugin pulls non-plugin deps into its module.
- **MINOR** — package misnamed for its role; helper utility could live one module higher.

## 1. Broad scan

- Moved / renamed classes (check `git diff --find-renames`).
- New interfaces or abstract classes.
- New POM `<dependency>` entries; check the module and the scope.
- Imports that cross module roots (`pinot-broker` importing `org.apache.pinot.core.query.executor.ServerQueryExecutor`, or `pinot-common` importing `pinot-core`).
- Two or more implementations of the same SPI: check for a missing abstract base.
- Any file moved into or out of `pinot-spi/` (binary contract surface).

## 2. Deep analysis

- **C3.x** Confirm module layering: `pinot-spi` → `pinot-common` → `pinot-segment-spi` → `pinot-segment-local` → `pinot-core` → `pinot-query-runtime` / `pinot-broker` / `pinot-server` / `pinot-controller`. Edges must flow one-way.
- Prefer abstract base over interface-with-default when >1 impl exists and logic is shared (see the AbstractResponseStore pattern).
- Plugin modules under `pinot-plugins/` must not be depended-on by core code. Verify the direction.
- New REST resources belong in `pinot-controller` or `pinot-broker`, never cross-wired.
- Utility classes: if a helper mirrors existing functionality (e.g., a second `PartitionIdUtils`), flag duplication and recommend consolidation.
- Shaded-jar impacts: flag if a new transitive dep clashes with an existing shaded package.

## 3. Findings

Tag `skill: review-architecture`, cite `C3.x`, use `[BUG-ARCH]` for unnamed structural issues.

## When to defer to the developer

- Intentional one-off utility in a module that explicitly doesn't want a cross-module shared helper; ensure the PR description says so.
