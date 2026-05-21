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


/// Opinionated validator for the MV time column SELECT expression.  The MV-side column type is
/// strict — it MUST be [DataType#TIMESTAMP] (epoch millis stored canonically).  The base side
/// is unconstrained: any base-table dateTime column is acceptable so long as the SELECT
/// expression produces a millis-epoch value the MV can store as TIMESTAMP.
///
/// ### Accepted MV time-column SELECT shapes
///
///   - **Identity passthrough**: a bare identifier equal to the base table's primary time
///       column.  Allowed only when the base column is itself TIMESTAMP — otherwise the value
///       would not be a millis-epoch in the MV.
///   - **`DATETRUNC('<unit>', baseTimeCol [, MILLISECONDS [, UTC [, MILLISECONDS]]])`** — the
///       unit literal must match the table's declared `bucketTimePeriod`.  Allowed only when
///       the base column is TIMESTAMP.  Optional trailing args, if present, must be the
///       defaults; non-default values are rejected to keep the semantic surface minimal.
///   - **Arithmetic scaling**: a chain of multiplications whose only identifier leaf is the
///       base time column, with the remaining operands being positive integer literals.  The
///       chain may be nested (`base * 24 * 60 * 60 * 1000`) or flat (`base * 86400000`).  This
///       is the recommended form when the base column stores a coarser unit (e.g. days, seconds)
///       and the user wants the MV to hold millis-epoch values.  The validator does NOT verify
///       the multiplied constants actually convert the base unit to millis — that responsibility
///       sits with the user, but the MV column being TIMESTAMP gives a clear contract.
///
/// Anything else (DATETIMECONVERT, TODATETIME, mixed addition/subtraction, nested DATETRUNC,
/// non-TIMESTAMP MV column type) is rejected at table-create time with an actionable message.
public final class TimeExprValidator {

  /// `DATETRUNC` units that map to a fixed-size [TimeUnit].  WEEK/MONTH/QUARTER/YEAR are
  /// intentionally absent because their bucket size is calendar-dependent (28/29/30/31 days,
  /// etc.) and cannot be reconciled with the millis-based `bucketTimePeriod` config.
  private static final Map<String, TimeUnit> DATETRUNC_UNIT_TO_TIMEUNIT = Map.ofEntries(
      Map.entry("MILLISECOND", TimeUnit.MILLISECONDS),
      Map.entry("SECOND", TimeUnit.SECONDS),
      Map.entry("MINUTE", TimeUnit.MINUTES),
      Map.entry("HOUR", TimeUnit.HOURS),
      Map.entry("DAY", TimeUnit.DAYS));

  private static final String DATETRUNC_CANONICAL = "datetrunc";
  private static final String TIMES_CANONICAL = "times";
  private static final String DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS.name();
  private static final String DEFAULT_TIMEZONE = "UTC";

  private TimeExprValidator() {
  }

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

    ExpressionType exprType = sourceExpr.getType();
    if (exprType == ExpressionType.IDENTIFIER) {
      String actual = sourceExpr.getIdentifier().getName();
      Preconditions.checkState(baseTimeColName.equals(actual),
          "MV time column must derive from base time column '%s' (identity, DATETRUNC, or "
              + "arithmetic scaling), got identifier '%s'.",
          baseTimeColName, actual);
      Preconditions.checkState(baseTimeFieldSpec.getDataType() == DataType.TIMESTAMP,
          "Identity passthrough requires the base time column '%s' to be TIMESTAMP-typed. "
              + "Base column data type is %s; use arithmetic scaling (e.g. '%s * 86400000') to "
              + "convert into millis-epoch.",
          baseTimeColName, baseTimeFieldSpec.getDataType(), baseTimeColName);
      return;
    }

    if (exprType == ExpressionType.FUNCTION) {
      Function func = sourceExpr.getFunctionCall();
      Preconditions.checkState(func != null,
          "MV time column expression is a FUNCTION but has no function call payload");
      String canonical = FunctionRegistry.canonicalize(func.getOperator());
      if (DATETRUNC_CANONICAL.equals(canonical)) {
        Preconditions.checkState(baseTimeFieldSpec.getDataType() == DataType.TIMESTAMP,
            "DATETRUNC on the MV time column requires the base time column '%s' to be "
                + "TIMESTAMP-typed. Base column data type is %s; use arithmetic scaling instead.",
            baseTimeColName, baseTimeFieldSpec.getDataType());
        validateDateTrunc(func, baseTimeColName, bucketMs);
        return;
      }
      if (TIMES_CANONICAL.equals(canonical)) {
        validateArithmeticScaling(func, baseTimeColName);
        return;
      }
      throw new IllegalStateException(
          "MV time column expression uses unsupported function '" + func.getOperator()
              + "'. The MV feature accepts only identity passthrough, DATETRUNC, or arithmetic "
              + "scaling (multiplication of the base time column by integer constants).");
    }

    throw new IllegalStateException(
        "MV time column expression must be either the base time column '" + baseTimeColName
            + "' (identity), a DATETRUNC call, or arithmetic scaling. Got expression type: " + exprType);
  }

  private static void validateDateTrunc(Function func, String baseTimeColName, long bucketMs) {
    List<Expression> operands = func.getOperands();
    int operandsSize = operands.size();
    Preconditions.checkState(operandsSize >= 2 && operandsSize <= 5,
        "DATETRUNC must be called with 2 to 5 arguments, got %s.", operandsSize);

    String unit = requireStringLiteral(operands.get(0), "unit");
    requireIdentifier(operands.get(1), baseTimeColName,
        "DATETRUNC second argument must be the base time column");

    if (operandsSize >= 3) {
      String inputTimeUnit = requireStringLiteral(operands.get(2), "inputTimeUnit");
      Preconditions.checkState(DEFAULT_TIME_UNIT.equalsIgnoreCase(inputTimeUnit),
          "DATETRUNC inputTimeUnit must be %s (base column is TIMESTAMP / millis), got '%s'.",
          DEFAULT_TIME_UNIT, inputTimeUnit);
    }
    if (operandsSize >= 4) {
      String timeZone = requireStringLiteral(operands.get(3), "timeZone");
      Preconditions.checkState(DEFAULT_TIMEZONE.equalsIgnoreCase(timeZone),
          "DATETRUNC timeZone must be %s, got '%s'. Non-UTC truncation would shift bucket boundaries "
              + "relative to the bucketTimePeriod and is rejected.",
          DEFAULT_TIMEZONE, timeZone);
    }
    if (operandsSize == 5) {
      String outputTimeUnit = requireStringLiteral(operands.get(4), "outputTimeUnit");
      Preconditions.checkState(DEFAULT_TIME_UNIT.equalsIgnoreCase(outputTimeUnit),
          "DATETRUNC outputTimeUnit must be %s (MV column is TIMESTAMP / millis), got '%s'.",
          DEFAULT_TIME_UNIT, outputTimeUnit);
    }

    String unitUpper = unit.toUpperCase(Locale.ROOT);
    TimeUnit truncTimeUnit = DATETRUNC_UNIT_TO_TIMEUNIT.get(unitUpper);
    Preconditions.checkState(truncTimeUnit != null,
        "DATETRUNC unit '%s' is not supported by MV. Supported units: %s. WEEK/MONTH/QUARTER/YEAR "
            + "have calendar-variable bucket sizes incompatible with bucketTimePeriod.",
        unit, DATETRUNC_UNIT_TO_TIMEUNIT.keySet());
    long truncBucketMs = truncTimeUnit.toMillis(1);
    Preconditions.checkState(truncBucketMs == bucketMs,
        "DATETRUNC unit '%s' (%s ms) does not match the declared bucketTimePeriod (%s ms). "
            + "Either change DATETRUNC to a matching unit or update bucketTimePeriod.",
        unit, truncBucketMs, bucketMs);
  }

  /// Walks a (possibly nested) chain of `times(...)` calls and verifies that exactly one leaf is
  /// the base time column and every other leaf is a positive integer literal.  Computes the
  /// effective scale factor as the product of all literal operands and rejects overflow.
  private static void validateArithmeticScaling(Function root, String baseTimeColName) {
    long[] scaleHolder = {1L};
    boolean[] sawBase = {false};
    walkMultiplicationChain(asExpression(root), baseTimeColName, scaleHolder, sawBase);
    Preconditions.checkState(sawBase[0],
        "Arithmetic scaling for the MV time column must reference the base time column '%s' "
            + "exactly once as one of the multiplicands.", baseTimeColName);
    Preconditions.checkState(scaleHolder[0] > 0,
        "Arithmetic scaling for the MV time column must produce a positive scale factor; "
            + "got %s.", scaleHolder[0]);
  }

  private static Expression asExpression(Function func) {
    Expression expr = new Expression(ExpressionType.FUNCTION);
    expr.setFunctionCall(func);
    return expr;
  }

  private static void walkMultiplicationChain(Expression node, String baseTimeColName,
      long[] scaleHolder, boolean[] sawBase) {
    ExpressionType type = node.getType();
    if (type == ExpressionType.IDENTIFIER) {
      String name = node.getIdentifier().getName();
      Preconditions.checkState(baseTimeColName.equals(name),
          "Arithmetic scaling for the MV time column may only reference the base time column "
              + "'%s'; got identifier '%s'.",
          baseTimeColName, name);
      Preconditions.checkState(!sawBase[0],
          "Arithmetic scaling for the MV time column must reference the base time column '%s' "
              + "exactly once; saw it more than once.",
          baseTimeColName);
      sawBase[0] = true;
      return;
    }
    if (type == ExpressionType.LITERAL) {
      long literal = requirePositiveIntegerLiteral(node);
      // Multiplicative overflow guard — bucketTimePeriod*days*hours*... must stay positive.
      Preconditions.checkState(scaleHolder[0] <= Long.MAX_VALUE / literal,
          "Arithmetic scaling for the MV time column overflows; literal operand %s pushes the "
              + "running product past Long.MAX_VALUE.", literal);
      scaleHolder[0] *= literal;
      return;
    }
    Preconditions.checkState(type == ExpressionType.FUNCTION,
        "Arithmetic scaling for the MV time column accepts only identifier or literal operands "
            + "at the leaves, got expression type: %s",
        type);
    Function func = node.getFunctionCall();
    String canonical = FunctionRegistry.canonicalize(func.getOperator());
    Preconditions.checkState(TIMES_CANONICAL.equals(canonical),
        "Arithmetic scaling for the MV time column accepts only chained multiplication; got '%s'. "
            + "Use a single `times` chain such as `%s * 86400000`.",
        func.getOperator(), baseTimeColName);
    for (Expression operand : func.getOperands()) {
      walkMultiplicationChain(operand, baseTimeColName, scaleHolder, sawBase);
    }
  }

  private static long requirePositiveIntegerLiteral(Expression expr) {
    Literal literal = expr.getLiteral();
    Preconditions.checkState(literal != null,
        "Arithmetic scaling for the MV time column requires positive integer literal operands.");
    long value;
    if (literal.isSetLongValue()) {
      value = literal.getLongValue();
    } else if (literal.isSetIntValue()) {
      value = literal.getIntValue();
    } else {
      throw new IllegalStateException(
          "Arithmetic scaling for the MV time column requires positive integer literal operands; "
              + "non-integer literal: " + literal);
    }
    Preconditions.checkState(value > 0,
        "Arithmetic scaling for the MV time column requires positive integer literal operands; "
            + "got %s.", value);
    return value;
  }

  private static String requireStringLiteral(Expression expr, String argName) {
    Preconditions.checkState(expr.getType() == ExpressionType.LITERAL,
        "DATETRUNC argument '%s' must be a string literal.", argName);
    Literal literal = expr.getLiteral();
    Preconditions.checkState(literal != null && literal.isSetStringValue(),
        "DATETRUNC argument '%s' must be a string literal.", argName);
    return literal.getStringValue();
  }

  private static void requireIdentifier(Expression expr, String baseTimeColName, String contextMessage) {
    Preconditions.checkState(expr.getType() == ExpressionType.IDENTIFIER,
        "%s '%s' as a bare identifier, not a nested expression.",
        contextMessage, baseTimeColName);
    String actual = expr.getIdentifier().getName();
    Preconditions.checkState(baseTimeColName.equals(actual),
        "%s '%s', got '%s'.", contextMessage, baseTimeColName, actual);
  }
}
