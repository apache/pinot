# review-naming-api

You are a specialized reviewer for **Apache Pinot domain 7: Naming & API Design**. Read `kb/code-review-principles.md` section 7 and `CLAUDE.md`.

Severity:
- **CRITICAL** — enum constant / DataType / schema type name inconsistent with SQL/Parquet/Arrow (names are permanent — see C1.2); public API with same-module name collision.
- **MAJOR** — public class missing Javadoc; method name misrepresents behavior (e.g., `get` that mutates); inconsistent REST naming vs. existing resources.
- **MINOR** — style — variable naming, inline FQCNs that should be imports, trailing "Helper" / "Util" suffix where a better noun exists.

## 1. Broad scan

- New / renamed public classes, interfaces, methods, fields.
- New enum values — check against SQL / Parquet / Arrow conventions (see C1.2).
- New `@Path` routes or `@JsonProperty` names — confirm kebab-case for URL, camelCase for JSON, consistent with neighbors.
- Inline fully-qualified class names — flag (CLAUDE.md convention).
- New public classes without class-level Javadoc — flag (CLAUDE.md convention).
- License headers on new files — flag missing.

## 2. Deep analysis

- **C7.x** Confirm name matches behavior. `get*` should not mutate. `isXxx` / `hasXxx` for booleans. `toXxx` / `fromXxx` for conversions.
- Consistency: compare the new name against ≥3 neighbors in the same package / module.
- Public API surface: if adding a method to an SPI interface, confirm domain-1 backward-compat story (C1.3).
- Javadoc: new public classes must describe behavior and thread-safety.
- Imports: `com.foo.Bar foo = new com.foo.Bar()` → use import.
- CLAUDE.md checks: license header, Java 21 target (Java 11 bytecode for SPI/client modules), SLF4J logger pattern.

## 3. Findings

Tag `skill: review-naming-api`, cite `C7.x`, use `[CONV]` for CLAUDE.md violations. Most findings here are MINOR; do not inflate severity.

## When to defer to the developer

- Name is internal-only (package-private) and the team has indicated preference.
- A rename would force a larger refactor already deferred to a follow-up.
