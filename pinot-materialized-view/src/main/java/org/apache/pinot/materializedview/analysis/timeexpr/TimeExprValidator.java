/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.materializedview.analysis.timeexpr;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Opinionated validator for the MV time column SELECT expression. The MV-side column type is
/// strict — it MUST be [DataType#TIMESTAMP] (epoch millis stored canonically). The base side
/// is unconstrained: any base-table dateTime column is acceptable so long as the SELECT
/// expression produces a millis-epoch value the MV can store as TIMESTAMP.
///
/// <h3>Accepted MV time-column SELECT shapes</h3>
///
///   - **Identity passthrough**: a bare identifier equal to the base table's primary time
///       column. Allowed only when the base column is itself TIMESTAMP — otherwise the value
///       would not be a millis-epoch in the MV.
///   - **`DATETRUNC('<unit>', baseTimeCol [, MILLISECONDS [, UTC [, MILLISECONDS]]])`** — the
///       unit literal must match the table's declared `bucketTimePeriod`. Allowed only when
///       the base column is TIMESTAMP. Optional trailing args, if present, must be the
///       defaults; non-default values are rejected to keep the semantic surface minimal.
///   - **Arithmetic scaling**: a chain of multiplications whose only identifier leaf is the
///       base time column, with the remaining operands being positive integer literals. The
///       chain may be nested (`base * 24 * 60 * 60 * 1000`) or flat (`base * 86400000`). This
///       is the recommended form when the base column stores a coarser unit (e.g. days, seconds)
///       and the user wants the MV to hold millis-epoch values. The validator does NOT verify
///       the multiplied constants actually convert the base unit to millis — that responsibility
///       sits with the user, but the MV column being TIMESTAMP gives a clear contract.
///
/// Anything else (DATETIMECONVERT, TODATETIME, mixed addition/subtraction, nested DATETRUNC,
/// non-TIMESTAMP MV column type) is rejected at table-create time with an actionable message.
///
/// <h3>Structure</h3>
///
/// The file is logically two layers:
///
///   1. **Shape recognition** (the {@link #parse} pipeline and the
///      {@link TimeExpression} ADT) — pure structural recognition that emits one of the three
///      variants with literal arguments attached. No policy, no TIMESTAMP-ness check, no
///      bucket-alignment check. {@link ParseException} surfaces structural violations.
///   2. **Policy validation** (the {@link #validate} entry point and its helpers) — maps the
///      recognised shape onto the TIMESTAMP / `bucketTimePeriod` / base-column-type rules
///      above and rethrows {@link IllegalStateException} on any violation so the analyzer has
///      a single exception type to catch.
public final class TimeExprValidator {

  /// `DATETRUNC` units that map to a fixed-size [TimeUnit]. WEEK/MONTH/QUARTER/YEAR are
  /// intentionally absent because their bucket size is calendar-dependent (28/29/30/31 days,
  /// etc.) and cannot be reconciled with the millis-based `bucketTimePeriod` config.
  private static final Map<String, TimeUnit> DATETRUNC_UNIT_TO_TIMEUNIT = Map.ofEntries(
      Map.entry("MILLISECOND", TimeUnit.MILLISECONDS),
      Map.entry("SECOND", TimeUnit.SECONDS),
      Map.entry("MINUTE", TimeUnit.MINUTES),
      Map.entry("HOUR", TimeUnit.HOURS),
      Map.entry("DAY", TimeUnit.DAYS));

  /// Canonical name produced by [FunctionRegistry#canonicalize] for `DATETRUNC` calls.
  /// {@code FunctionRegistry.canonicalize} strips underscores and lower-cases, so
  /// `DATETRUNC`, `date_trunc`, `DateTrunc` all collapse to {@code "datetrunc"}.
  private static final String DATETRUNC_CANONICAL = "datetrunc";

  /// Canonical name for the `*` operator (Calcite's `times` function). Pinot's parser emits
  /// the operator name `times` for binary `*`; chained multiplications surface as nested
  /// `times(...)` calls.
  private static final String TIMES_CANONICAL = "times";

  /// The single value the optional `DATETRUNC` `inputTimeUnit` and `outputTimeUnit`
  /// arguments are allowed to hold. Compared case-insensitively. Non-default values would
  /// change the result's units relative to the MV column contract (millis-epoch) and are
  /// rejected so the inferer cannot produce an MV that the validator will later refuse —
  /// and conversely.
  private static final String DEFAULT_TIME_UNIT = "MILLISECONDS";

  /// The single value the optional `DATETRUNC` `timeZone` argument is allowed to hold.
  /// Compared case-insensitively. Non-UTC truncation would shift bucket boundaries relative
  /// to `bucketTimePeriod` and is rejected.
  private static final String DEFAULT_TIMEZONE = "UTC";

  private TimeExprValidator() {
  }

  // ─── Policy layer ──────────────────────────────────────────────────────────────────────

  /// Validates the MV time-column SELECT expression and the data types on both sides.
  ///
  /// @param sourceExpr         MV SELECT expression producing the MV time column (alias stripped)
  /// @param baseTimeColName    name of the base table's primary time column
  /// @param baseTimeFieldSpec  base table's [DateTimeFieldSpec] for that column
  /// @param viewTimeColName    MV table's primary time column name (for error messages)
  /// @param viewTimeFieldSpec  MV table's [DateTimeFieldSpec] for that column
  /// @param bucketMs           the table's declared `bucketTimePeriod` resolved to millis
  /// @throws IllegalStateException with a user-facing message on any violation
  public static void validate(Expression sourceExpr, String baseTimeColName,
      DateTimeFieldSpec baseTimeFieldSpec, String viewTimeColName,
      DateTimeFieldSpec viewTimeFieldSpec, long bucketMs) {
    Preconditions.checkNotNull(sourceExpr, "sourceExpr");
    Preconditions.checkNotNull(baseTimeColName, "baseTimeColName");
    Preconditions.checkNotNull(baseTimeFieldSpec, "baseTimeFieldSpec");
    Preconditions.checkNotNull(viewTimeColName, "viewTimeColName");
    Preconditions.checkNotNull(viewTimeFieldSpec, "viewTimeFieldSpec");

    Preconditions.checkState(viewTimeFieldSpec.getDataType() == DataType.TIMESTAMP,
        "MV requires TIMESTAMP-typed time columns. MV time column '%s' has data type %s. "
            + "Declare the column with dataType=TIMESTAMP and format='1:MILLISECONDS:TIMESTAMP'.",
        viewTimeColName, viewTimeFieldSpec.getDataType());

    TimeExpression parsed;
    try {
      parsed = parse(sourceExpr);
    } catch (ParseException e) {
      // Translate the parser's domain exception into the validator's IllegalStateException
      // contract so callers (currently only MaterializedViewAnalyzer) keep a single
      // exception type to catch. The parser's message is preserved so it surfaces in the
      // operator-facing error verbatim.
      throw new IllegalStateException(e.getMessage(), e);
    }

    // The parser does NOT verify the identifier matches the base time column — that is
    // policy, surfaced here so the error message is consistent with the validator's
    // historical "must derive from base time column 'X'" wording.
    Preconditions.checkState(baseTimeColName.equals(parsed.baseTimeColumnName()),
        "MV time column must derive from base time column '%s' (identity, DATETRUNC, or "
            + "arithmetic scaling), got identifier '%s'.",
        baseTimeColName, parsed.baseTimeColumnName());

    if (parsed instanceof IdentityPassthrough) {
      validateIdentityPolicy(baseTimeColName, baseTimeFieldSpec);
    } else if (parsed instanceof DateTrunc) {
      validateDateTruncPolicy((DateTrunc) parsed,
          baseTimeColName, baseTimeFieldSpec, bucketMs);
    } else if (parsed instanceof ArithmeticScaling) {
      // Arithmetic scaling is the recommended path when the base column is NOT TIMESTAMP
      // (e.g. INT-days), so unlike identity / DATETRUNC there is no base-type guard here.
      // The parser already verified positivity / overflow / single-identifier; nothing
      // further to enforce.
      return;
    } else {
      // Unreachable: the three concrete subtypes are the only inhabitants of the closed-set
      // ADT. Defensive throw guards against a future ADT extension that forgets a branch.
      throw new IllegalStateException(
          "Internal error: unhandled MV time expression variant " + parsed.getClass().getName());
    }
  }

  private static void validateIdentityPolicy(String baseTimeColName, DateTimeFieldSpec baseTimeFieldSpec) {
    Preconditions.checkState(baseTimeFieldSpec.getDataType() == DataType.TIMESTAMP,
        "Identity passthrough requires the base time column '%s' to be TIMESTAMP-typed. "
            + "Base column data type is %s; use arithmetic scaling (e.g. '%s * 86400000') to "
            + "convert into millis-epoch.",
        baseTimeColName, baseTimeFieldSpec.getDataType(), baseTimeColName);
  }

  private static void validateDateTruncPolicy(DateTrunc dt,
      String baseTimeColName, DateTimeFieldSpec baseTimeFieldSpec, long bucketMs) {
    Preconditions.checkState(baseTimeFieldSpec.getDataType() == DataType.TIMESTAMP,
        "DATETRUNC on the MV time column requires the base time column '%s' to be "
            + "TIMESTAMP-typed. Base column data type is %s; use arithmetic scaling instead.",
        baseTimeColName, baseTimeFieldSpec.getDataType());

    String unitUpper = dt.unit().toUpperCase(Locale.ROOT);
    TimeUnit truncTimeUnit = DATETRUNC_UNIT_TO_TIMEUNIT.get(unitUpper);
    Preconditions.checkState(truncTimeUnit != null,
        "DATETRUNC unit '%s' is not supported by MV. Supported units: %s. WEEK/MONTH/QUARTER/YEAR "
            + "have calendar-variable bucket sizes incompatible with bucketTimePeriod.",
        dt.unit(), DATETRUNC_UNIT_TO_TIMEUNIT.keySet());
    long truncBucketMs = truncTimeUnit.toMillis(1);
    Preconditions.checkState(truncBucketMs == bucketMs,
        "DATETRUNC unit '%s' (%s ms) does not match the declared bucketTimePeriod (%s ms). "
            + "Either change DATETRUNC to a matching unit or update bucketTimePeriod.",
        dt.unit(), truncBucketMs, bucketMs);
  }

  // ─── Structural layer: ADT + parser ────────────────────────────────────────────────────

  /// Closed-set ADT for the three SELECT-list expression shapes a Materialized View accepts
  /// as its time-column expression. Produced by {@link #parse}. Does not carry policy: it
  /// reports the shape and the literal arguments the user wrote.
  public abstract static class TimeExpression {
    private final String _baseTimeColumnName;

    private TimeExpression(String baseTimeColumnName) {
      _baseTimeColumnName = baseTimeColumnName;
    }

    /// The base-table column name the user referenced, exactly as written.
    public final String baseTimeColumnName() {
      return _baseTimeColumnName;
    }
  }

  /// `<baseTimeColumn>` — the SELECT item is the base time column itself.
  public static final class IdentityPassthrough extends TimeExpression {
    public IdentityPassthrough(String baseTimeColumnName) {
      super(baseTimeColumnName);
    }
  }

  /// `DATETRUNC('<unit>', <baseTimeColumn>[, ...])`. The optional trailing args, if present,
  /// were validated by the parser to equal their defaults (MILLISECONDS / UTC / MILLISECONDS);
  /// only `unit` varies. The unit is preserved with the user's casing.
  public static final class DateTrunc extends TimeExpression {
    private final String _unit;

    public DateTrunc(String baseTimeColumnName, String unit) {
      super(baseTimeColumnName);
      _unit = unit;
    }

    public String unit() {
      return _unit;
    }
  }

  /// `<baseTimeColumn> * lit1 * lit2 * ...` — chained or flat multiplication. `scaleFactor`
  /// is the product of all literal multiplicands and is guaranteed by the parser to be
  /// strictly positive and overflow-free.
  public static final class ArithmeticScaling extends TimeExpression {
    private final long _scaleFactor;

    public ArithmeticScaling(String baseTimeColumnName, long scaleFactor) {
      super(baseTimeColumnName);
      _scaleFactor = scaleFactor;
    }

    public long scaleFactor() {
      return _scaleFactor;
    }
  }

  /// Thrown when the input expression does not match any of the three accepted MV
  /// time-column shapes or carries a structural violation (negative literal, non-default
  /// DATETRUNC inputTimeUnit, etc.). Unchecked; {@link #validate} catches this and rethrows
  /// as {@link IllegalStateException} so the analyzer has a single exception type to catch.
  public static class ParseException extends RuntimeException {
    public ParseException(String message) {
      super(message);
    }
  }

  /// Parses the alias-stripped SELECT-list expression for an MV time column into the
  /// {@link TimeExpression} ADT.
  ///
  /// @param sourceExpr the [Expression] tree, with any wrapping {@code AS} already removed
  ///                   by the caller (e.g. via {@code RequestUtils.unwrapAlias}). Must not
  ///                   be {@code null}.
  /// @return the recognised shape with literal arguments attached.
  /// @throws ParseException if the expression is none of the three accepted shapes or
  ///         violates a structural constraint of one of them (non-default DATETRUNC arg,
  ///         non-positive literal, repeated base column reference, etc.).
  /// @throws NullPointerException if {@code sourceExpr} is {@code null}.
  public static TimeExpression parse(Expression sourceExpr) {
    if (sourceExpr == null) {
      throw new NullPointerException("sourceExpr");
    }
    ExpressionType exprType = sourceExpr.getType();
    if (exprType == ExpressionType.IDENTIFIER) {
      return new IdentityPassthrough(sourceExpr.getIdentifier().getName());
    }
    if (exprType == ExpressionType.FUNCTION) {
      Function func = sourceExpr.getFunctionCall();
      if (func == null) {
        throw new ParseException(
            "MV time column expression is a FUNCTION but has no function-call payload.");
      }
      String canonical = FunctionRegistry.canonicalize(func.getOperator());
      if (DATETRUNC_CANONICAL.equals(canonical)) {
        return parseDateTrunc(func);
      }
      if (TIMES_CANONICAL.equals(canonical)) {
        return parseArithmeticScaling(sourceExpr);
      }
      throw new ParseException(
          "MV time column expression uses unsupported function '" + func.getOperator()
              + "'. The MV feature accepts only identity passthrough, DATETRUNC, or arithmetic "
              + "scaling (multiplication of the base time column by integer constants).");
    }
    throw new ParseException(
        "MV time column expression must be an identifier (identity passthrough), a DATETRUNC "
            + "call, or arithmetic scaling. Got expression type: " + exprType);
  }

  /// Recognises the `DATETRUNC` shape and validates the optional trailing arguments equal
  /// their defaults. Note: this method does NOT enforce the unit is among the MILLISECOND /
  /// SECOND / MINUTE / HOUR / DAY whitelist — that is policy and lives in
  /// {@link #validateDateTruncPolicy} (calendar units are rejected because their bucket size
  /// is calendar-dependent and incompatible with `bucketTimePeriod`).
  private static DateTrunc parseDateTrunc(Function func) {
    List<Expression> operands = func.getOperands();
    int operandsSize = operands == null ? 0 : operands.size();
    if (operandsSize < 2 || operandsSize > 5) {
      throw new ParseException(
          "DATETRUNC must be called with 2 to 5 arguments, got " + operandsSize + ".");
    }

    String unit = requireStringLiteral(operands.get(0), "unit");

    // Second arg must be a bare base-column identifier. Nested expressions
    // (DATETRUNC('DAY', ts*1000)) are rejected because they would mix arithmetic scaling
    // and truncation in a way the consumer's bucket-alignment check does not handle.
    if (operands.get(1).getType() != ExpressionType.IDENTIFIER) {
      throw new ParseException(
          "DATETRUNC second argument must be a bare base-table time-column identifier, not a "
              + "nested expression.");
    }
    String baseColumnName = operands.get(1).getIdentifier().getName();

    if (operandsSize >= 3) {
      String inputTimeUnit = requireStringLiteral(operands.get(2), "inputTimeUnit");
      if (!DEFAULT_TIME_UNIT.equalsIgnoreCase(inputTimeUnit)) {
        throw new ParseException(
            "DATETRUNC inputTimeUnit must be " + DEFAULT_TIME_UNIT + " (base column is TIMESTAMP "
                + "/ millis); got '" + inputTimeUnit + "'.");
      }
    }
    if (operandsSize >= 4) {
      String timeZone = requireStringLiteral(operands.get(3), "timeZone");
      if (!DEFAULT_TIMEZONE.equalsIgnoreCase(timeZone)) {
        throw new ParseException(
            "DATETRUNC timeZone must be " + DEFAULT_TIMEZONE + ", got '" + timeZone + "'. "
                + "Non-UTC truncation would shift bucket boundaries relative to "
                + "bucketTimePeriod and is rejected.");
      }
    }
    if (operandsSize == 5) {
      String outputTimeUnit = requireStringLiteral(operands.get(4), "outputTimeUnit");
      if (!DEFAULT_TIME_UNIT.equalsIgnoreCase(outputTimeUnit)) {
        throw new ParseException(
            "DATETRUNC outputTimeUnit must be " + DEFAULT_TIME_UNIT + " (MV column is TIMESTAMP "
                + "/ millis); got '" + outputTimeUnit + "'.");
      }
    }

    return new DateTrunc(baseColumnName, unit);
  }

  /// Walks a (possibly nested) chain of `times(...)` calls and verifies that exactly one
  /// leaf is an identifier (the base time column) and every other leaf is a positive
  /// integer literal. Computes the effective scale factor as the product of all literal
  /// operands and rejects multiplicative overflow.
  private static ArithmeticScaling parseArithmeticScaling(Expression sourceExpr) {
    long[] scaleHolder = {1L};
    String[] baseColumnHolder = {null};
    walkMultiplicationChain(sourceExpr, scaleHolder, baseColumnHolder);
    if (baseColumnHolder[0] == null) {
      throw new ParseException(
          "Arithmetic scaling for the MV time column must reference exactly one base-table "
              + "identifier as one of the multiplicands; got none.");
    }
    if (scaleHolder[0] <= 0) {
      throw new ParseException(
          "Arithmetic scaling for the MV time column must produce a positive scale factor; got "
              + scaleHolder[0] + ".");
    }
    return new ArithmeticScaling(baseColumnHolder[0], scaleHolder[0]);
  }

  /// Recursive helper: walks any node in the `times` chain. Identifier leaves are recorded
  /// as the base column (rejected on second sighting); literal leaves are folded into the
  /// running product (overflow-checked); nested function nodes must be further `times(...)`
  /// calls.
  private static void walkMultiplicationChain(Expression node, long[] scaleHolder, String[] baseColumnHolder) {
    ExpressionType type = node.getType();
    if (type == ExpressionType.IDENTIFIER) {
      String name = node.getIdentifier().getName();
      if (baseColumnHolder[0] != null) {
        throw new ParseException(
            "Arithmetic scaling for the MV time column must reference exactly one base-table "
                + "identifier; saw both '" + baseColumnHolder[0] + "' and '" + name + "'.");
      }
      baseColumnHolder[0] = name;
      return;
    }
    if (type == ExpressionType.LITERAL) {
      long literal = requirePositiveIntegerLiteral(node);
      // Multiplicative overflow guard — `scaleHolder * literal` must stay below
      // Long.MAX_VALUE. Using division avoids the well-known signed-overflow trap of
      // checking `> 0` on the post-multiply result.
      if (scaleHolder[0] > Long.MAX_VALUE / literal) {
        throw new ParseException(
            "Arithmetic scaling for the MV time column overflows; literal operand " + literal
                + " pushes the running product past Long.MAX_VALUE.");
      }
      scaleHolder[0] *= literal;
      return;
    }
    if (type != ExpressionType.FUNCTION) {
      throw new ParseException(
          "Arithmetic scaling for the MV time column accepts only identifier or literal operands "
              + "at the leaves; got expression type: " + type);
    }
    Function func = node.getFunctionCall();
    String canonical = FunctionRegistry.canonicalize(func.getOperator());
    if (!TIMES_CANONICAL.equals(canonical)) {
      throw new ParseException(
          "Arithmetic scaling for the MV time column accepts only chained multiplication; got '"
              + func.getOperator() + "'. Use a single `times` chain such as "
              + "'<baseTimeColumn> * 86400000'.");
    }
    for (Expression operand : func.getOperands()) {
      walkMultiplicationChain(operand, scaleHolder, baseColumnHolder);
    }
  }

  /// Coerces a positive integer literal (INT or LONG) into a `long`. Rejects floating point,
  /// decimal, string, etc., as well as zero / negative values — both because negative
  /// scaling is meaningless for a millis-epoch MV column and because zero would collapse
  /// the range to a constant.
  private static long requirePositiveIntegerLiteral(Expression expr) {
    Literal literal = expr.getLiteral();
    if (literal == null) {
      throw new ParseException(
          "Arithmetic scaling for the MV time column requires positive integer literal operands.");
    }
    long value;
    if (literal.isSetLongValue()) {
      value = literal.getLongValue();
    } else if (literal.isSetIntValue()) {
      value = literal.getIntValue();
    } else {
      throw new ParseException(
          "Arithmetic scaling for the MV time column requires positive integer literal operands; "
              + "non-integer literal: " + literal);
    }
    if (value <= 0) {
      throw new ParseException(
          "Arithmetic scaling for the MV time column requires positive integer literal operands; "
              + "got " + value + ".");
    }
    return value;
  }

  /// Coerces a string-literal expression into its raw `String` value. Rejects any other
  /// literal kind (int / long / boolean / etc.) so DATETRUNC's string-typed arguments cannot
  /// be silently confused with their integer counterparts.
  private static String requireStringLiteral(Expression expr, String argName) {
    if (expr.getType() != ExpressionType.LITERAL) {
      throw new ParseException(
          "DATETRUNC argument '" + argName + "' must be a string literal.");
    }
    Literal literal = expr.getLiteral();
    if (literal == null || !literal.isSetStringValue()) {
      throw new ParseException(
          "DATETRUNC argument '" + argName + "' must be a string literal.");
    }
    return literal.getStringValue();
  }
}
