# review-correctness-nulls

Review **Apache Pinot domain 5: Correctness & Safety**. Read the applicable parts of section 5 in
`kb/code-review-principles.md` and relevant repository conventions not already loaded. Reuse material already read.

Use the canonical severity definitions and Review Delivery rules in `kb/code-review-principles.md`. Assess demonstrated
impact; pattern matches are investigation triggers, not findings or automatic severity assignments.

## 1. Broad scan

- Return paths from stats / aggregators / dictionaries that may be empty — confirm they handle "no data" case (return null, not NPE).
- `switch` on `DataType`, `FieldSpec.DataType`, `ColumnDataType`, `IndexType`, `SegmentVersion` — check for missing cases and `default: throw` vs. silent fallthrough.
- `null` returns from new methods — confirm `@Nullable` is on the signature.
- `close()` / `destroy()` methods — confirm they clear indexes, bitmaps, dictionaries, and null all references.
- Arithmetic / aggregation code — look for unconditional cast-to-double or use of `BigDecimal` when `long` suffices.
- Window / aggregation functions split by type — confirm INT/LONG/FLOAT/DOUBLE/BIG_DECIMAL each have a dedicated impl where precision matters.
- Caught `Throwable` / `Exception` — confirm no silent swallow.

## 2. Deep analysis

- **C5.x** Null handling: dispatch on `getStoredType()`, update null-value-vector bitmap on insert/delete, and check coverage
  of both `null-handling-enabled=true` and `false` when the change affects both paths. Reuse existing coverage.
- **Exhaustive switches**: prefer `EnumSet.allOf` confirmation or a `default: throw new IllegalArgumentException("Unsupported " + type)`;
  trace the effect of silent fallthrough before assigning severity (see PR 18176 `IVF_ON_DISK` case).
- **Precision**: INT/LONG windows must not coerce to double; BIG_DECIMAL requires its own aggregator. Flag coercions that may lose precision past 2^53.
- **Resource cleanup**: `close()` must actively trim state, not rely on GC. Even if dangling refs are rare, explicit cleanup is the norm (see PR 18204 bitmap leak).
- **Error messages**: `Preconditions.checkState`, `IllegalArgumentException`, `IllegalStateException` must include the offending value — not opaque.
- **Off-by-one**: row iteration loops — confirm `< numDocs` not `<=`; confirm `docIdIterator` drains fully before reusing.

## 3. Findings

Tag `skill: review-correctness-nulls`, cite `C5.x`, use `[BUG-CORR]` for unnamed bugs. Demonstrate the input and path that
can produce precision loss or silent wrong results, and distinguish confirmed impact from an unverified risk.

## When to defer to the developer

- Null behavior is unaffected and existing coverage preserves its contract. A scope statement does not excuse a newly introduced null-handling defect.
- `default` branch falls through to a documented best-effort path (rare; must be justified).
