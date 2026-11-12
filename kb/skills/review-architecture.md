# review-architecture

Review **Apache Pinot domain 3: Code Architecture & Module Design**. Read the applicable parts of section 3 in
`kb/code-review-principles.md` and relevant repository conventions not already loaded. Reuse material already read.

Use the canonical severity definitions and Review Delivery rules in `kb/code-review-principles.md`. Assess demonstrated
impact; pattern matches are investigation triggers, not findings or automatic severity assignments.

## 1. Broad scan

- Moved / renamed classes (check `git diff --find-renames`).
- New interfaces or abstract classes.
- New POM `<dependency>` entries; check the module and the scope.
- Imports that cross module roots (`pinot-broker` importing `org.apache.pinot.core.query.executor.ServerQueryExecutor`, or `pinot-common` importing `pinot-core`).
- Multiple implementations of the same SPI: check whether shared behavior warrants reuse without coupling distinct semantics.
- Any file moved into or out of `pinot-spi/` (binary contract surface).

## 2. Deep analysis

- **C3.x** Confirm module layering: `pinot-spi` → `pinot-common` → `pinot-segment-spi` → `pinot-segment-local` → `pinot-core` → `pinot-query-runtime` / `pinot-broker` / `pinot-server` / `pinot-controller`. Edges must flow one-way.
- Consider an abstract base or shared helper when implementations share behavior and lifecycle contracts. Keep separate
  implementations when reuse would couple different semantics or introduce unwanted dependencies.
- Plugin modules under `pinot-plugins/` must not be depended-on by core code. Verify the direction.
- New REST resources belong in `pinot-controller` or `pinot-broker`, never cross-wired.
- Utility classes: compare semantics and dependencies before recommending consolidation of similar helpers. Report a
  concrete maintenance or correctness risk, not resemblance alone.
- Shaded-jar impacts: flag if a new transitive dep clashes with an existing shaded package.

## 3. Findings

Tag `skill: review-architecture`, cite `C3.x`, use `[BUG-ARCH]` for unnamed structural issues.

## When to defer to the developer

- Intentional one-off utility in a module that explicitly doesn't want a cross-module shared helper; ensure the PR description says so.
