# review-naming-api

Review **Apache Pinot domain 7: Naming & API Design**. Read the applicable parts of section 7 in
`kb/code-review-principles.md` and relevant repository conventions not already loaded. Reuse material already read.

Use the canonical severity definitions and Review Delivery rules in `kb/code-review-principles.md`. Assess demonstrated
impact; pattern matches are investigation triggers, not findings or automatic severity assignments.

## 1. Broad scan

- New / renamed public classes, interfaces, methods, fields.
- New enum values — check against SQL / Parquet / Arrow conventions (see C1.2).
- New `@Path` routes or `@JsonProperty` names — confirm kebab-case for URL, camelCase for JSON, consistent with neighbors.
- Inline fully-qualified class names — flag (CLAUDE.md convention).
- Qualified `Assert.` or `Mockito.` method calls in test files — apply C7.21.
- `FieldSpec.DataType` or `DataSchema.ColumnDataType` outside import declarations — apply C7.22.
- Multiline ternary expressions — verify C7.23 formatting.
- New public classes without class-level Javadoc — flag (CLAUDE.md convention).
- License headers on new files — flag missing.

## 2. Deep analysis

- **C7.x** Confirm name matches behavior. `get*` should not mutate. `isXxx` / `hasXxx` for booleans. `toXxx` / `fromXxx` for conversions.
- Consistency: consult the most relevant neighbors in the same package / module when the naming convention is unclear.
- Public API surface: if adding a method to an SPI interface, confirm domain-1 backward-compat story (C1.3).
- Javadoc: new public classes must describe behavior and thread-safety.
- Imports: `com.foo.Bar foo = new com.foo.Bar()` → use import.
- Modern collection factories: apply C7.12 to direct calls and method references. Flag any reintroduction of
  Checkstyle-blocked `Collections.emptyList`, `Collections.emptySet`, or `Collections.emptyMap` usage. Do not
  recommend blanket bans for `Collections.singleton*`, but only allow them for explicit null element/key/value
  arguments; check whether empty collections flow to mutating callers before replacement.
- Repository checks: license header, build JDK and bytecode targets from `AGENTS.md`, SLF4J logger pattern.

## 3. Findings

Tag `skill: review-naming-api`, cite `C7.x`, use `[CONV]` for CLAUDE.md violations. Most findings here are MINOR; do not inflate severity.

## When to defer to the developer

- Name is internal-only (package-private) and the team has indicated preference.
- A rename would force a larger refactor already deferred to a follow-up.
