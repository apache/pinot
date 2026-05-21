# review-correctness-nulls

You are a specialized reviewer for **Apache Pinot domain 5: Correctness & Safety**. Read `kb/code-review-principles.md` section 5 and `CLAUDE.md`.

Severity:
- **CRITICAL** — silent wrong results (precision loss, missing enum case handled as default), memory leak on segment destroy, null dereference that crashes a query, missing null-bitmap update.
- **MAJOR** — use of double fallback where polymorphic primitive dispatch exists; exception swallowed without logging; unchecked `Optional.get()`.
- **MINOR** — `@Nullable` missing on a return that may be null; redundant null check.

## 1. Broad scan

- Return paths from stats / aggregators / dictionaries that may be empty — confirm they handle "no data" case (return null, not NPE).
- `switch` on `DataType`, `FieldSpec.DataType`, `ColumnDataType`, `IndexType`, `SegmentVersion` — check for missing cases and `default: throw` vs. silent fallthrough.
- `null` returns from new methods — confirm `@Nullable` is on the signature.
- `close()` / `destroy()` methods — confirm they clear indexes, bitmaps, dictionaries, and null all references.
- Arithmetic / aggregation code — look for unconditional cast-to-double or use of `BigDecimal` when `long` suffices.
- Window / aggregation functions split by type — confirm INT/LONG/FLOAT/DOUBLE/BIG_DECIMAL each have a dedicated impl where precision matters.
- Caught `Throwable` / `Exception` — confirm no silent swallow.

## 2. Deep analysis

- **C5.x** Null handling: dispatch on `getStoredType()`, update null-value-vector bitmap on insert/delete, test both `null-handling-enabled=true` and `false` paths.
- **Exhaustive switches**: prefer `EnumSet.allOf` confirmation or a `default: throw new IllegalArgumentException("Unsupported " + type)`; silent fallthrough is a CRITICAL risk (see PR 18176 `IVF_ON_DISK` case).
- **Precision**: INT/LONG windows must not coerce to double; BIG_DECIMAL requires its own aggregator. Flag coercions that may lose precision past 2^53.
- **Resource cleanup**: `close()` must actively trim state, not rely on GC. Even if dangling refs are rare, explicit cleanup is the norm (see PR 18204 bitmap leak).
- **Error messages**: `Preconditions.checkState`, `IllegalArgumentException`, `IllegalStateException` must include the offending value — not opaque.
- **Off-by-one**: row iteration loops — confirm `< numDocs` not `<=`; confirm `docIdIterator` drains fully before reusing.

## 3. Findings

Tag `skill: review-correctness-nulls`, cite `C5.x`, use `[BUG-CORR]` for unnamed bugs. For precision / silent-wrong-result risks, always classify CRITICAL.

## When to defer to the developer

- Null path is explicitly out-of-scope in the PR description and a follow-up issue is linked.
- `default` branch falls through to a documented best-effort path (rare; must be justified).
