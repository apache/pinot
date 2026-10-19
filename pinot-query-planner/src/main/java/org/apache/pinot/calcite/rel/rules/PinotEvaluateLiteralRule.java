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
package org.apache.pinot.calcite.rel.rules;

import com.google.common.base.Preconditions;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.DateString;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.QueryFunctionInvoker;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/// PinotEvaluateLiteralRule that matches the literal only function calls and evaluates them.
///
/// The function gets each literal as the Java value that the runtime passes for it, so that the result is the same as
/// the result of the call at runtime.
public class PinotEvaluateLiteralRule {

  public static class Project extends RelOptRule {
    public static final Project INSTANCE = new Project(PinotRuleUtils.PINOT_REL_FACTORY, null);

    public static Project instanceWithDescription(String description) {
      return new Project(PinotRuleUtils.PINOT_REL_FACTORY, description);
    }

    private Project(RelBuilderFactory factory, @Nullable String description) {
      super(operand(LogicalProject.class, any()), factory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalProject oldProject = call.rel(0);
      RexBuilder rexBuilder = oldProject.getCluster().getRexBuilder();
      LogicalProject newProject = (LogicalProject) oldProject.accept(new EvaluateLiteralShuttle(rexBuilder));
      if (newProject != oldProject) {
        call.transformTo(constructNewProject(oldProject, newProject, rexBuilder));
      }
    }
  }

  /// Constructs a new LogicalProject that matches the type of the old LogicalProject.
  private static LogicalProject constructNewProject(LogicalProject oldProject, LogicalProject newProject,
      RexBuilder rexBuilder) {
    List<RexNode> oldProjects = oldProject.getProjects();
    List<RexNode> newProjects = newProject.getProjects();
    int numProjects = oldProjects.size();
    assert newProjects.size() == numProjects;
    List<RexNode> castedNewProjects = new ArrayList<>(numProjects);
    boolean needCast = false;
    for (int i = 0; i < numProjects; i++) {
      RexNode oldNode = oldProjects.get(i);
      RexNode newNode = newProjects.get(i);
      // Need to cast the result to the original type if the literal type is changed, e.g. VARCHAR literal is typed as
      // CHAR(STRING_LENGTH) in Calcite, but we need to cast it back to VARCHAR.
      if (!oldNode.getType().equals(newNode.getType())) {
        needCast = true;
        newNode = rexBuilder.makeCast(oldNode.getType(), newNode, true);
      }
      castedNewProjects.add(newNode);
    }
    return needCast ? oldProject.copy(oldProject.getTraitSet(), oldProject.getInput(), castedNewProjects,
        oldProject.getRowType()) : newProject;
  }

  public static class Filter extends RelOptRule {
    public static final Filter INSTANCE = new Filter(PinotRuleUtils.PINOT_REL_FACTORY, null);

    public static Filter instanceWithDescription(String description) {
      return new Filter(PinotRuleUtils.PINOT_REL_FACTORY, description);
    }

    private Filter(RelBuilderFactory factory, @Nullable String description) {
      super(operand(LogicalFilter.class, any()), factory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter oldFilter = call.rel(0);
      RexBuilder rexBuilder = oldFilter.getCluster().getRexBuilder();
      LogicalFilter newFilter = (LogicalFilter) oldFilter.accept(new EvaluateLiteralShuttle(rexBuilder));
      if (newFilter != oldFilter) {
        call.transformTo(newFilter);
      }
    }
  }

  /// A RexShuttle that recursively evaluates all the calls with literal only operands.
  private static class EvaluateLiteralShuttle extends RexShuttle {
    final RexBuilder _rexBuilder;

    EvaluateLiteralShuttle(RexBuilder rexBuilder) {
      _rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall visitedCall = (RexCall) super.visitCall(call);
      // Check if all operands are RexLiteral
      if (visitedCall.operands.stream().allMatch(
          operand -> operand instanceof RexLiteral || (operand instanceof RexCall && ((RexCall) operand).getOperands()
              .stream().allMatch(op -> op instanceof RexLiteral)))) {
        return evaluateLiteralOnlyFunction(visitedCall, _rexBuilder);
      } else {
        return visitedCall;
      }
    }
  }

  /// Evaluates the literal only function and returns the result as a RexLiteral if it can be evaluated, or the function
  /// itself (RexCall) if it cannot be evaluated.
  private static RexNode evaluateLiteralOnlyFunction(RexCall rexCall, RexBuilder rexBuilder) {
    List<RexNode> operands = rexCall.getOperands();
    assert operands.stream().allMatch(
        operand -> operand instanceof RexLiteral || (operand instanceof RexCall && ((RexCall) operand).getOperands()
            .stream().allMatch(op -> op instanceof RexLiteral)));

    int numArguments = operands.size();
    ColumnDataType[] argumentTypes = new ColumnDataType[numArguments];
    for (int i = 0; i < numArguments; i++) {
      RexNode rexNode = operands.get(i);
      if (!(rexNode instanceof RexLiteral)) {
        // Function operands cannot be evaluated, skip
        return rexCall;
      }
      argumentTypes[i] = RelToPlanNodeConverter.convertToColumnDataType(rexNode.getType());
    }

    if (isDatetimeIntervalArithmetic(rexCall)) {
      return evaluateDatetimeIntervalArithmetic(rexCall, rexBuilder);
    }
    if (isIntervalArithmetic(rexCall)) {
      return evaluateIntervalArithmetic(rexCall, rexBuilder);
    }
    String castTargetType = null;
    if (rexCall.getKind() == SqlKind.CAST) {
      // Handle separately because the CAST operator only has one operand (the value to be cast) and the type to be cast
      // to is determined by the operator's return type. Pinot's CAST function implementation requires two arguments:
      // the value to be cast and the target type.
      argumentTypes = new ColumnDataType[]{argumentTypes[0], ColumnDataType.STRING};
      castTargetType = RelToPlanNodeConverter.convertToColumnDataType(rexCall.getType()).name();
    }
    String canonicalName = FunctionRegistry.canonicalize(PinotRuleUtils.extractFunctionName(rexCall));
    FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, argumentTypes);
    if (functionInfo == null || !functionInfo.isDeterministic()) {
      // Function cannot be evaluated
      return rexCall;
    }
    // Convert the literals only for a function that can be evaluated, because some operators take literals that are
    // not values, for example the Sarg of a SEARCH
    Object[] arguments = new Object[argumentTypes.length];
    for (int i = 0; i < numArguments; i++) {
      arguments[i] = getArgumentValue((RexLiteral) operands.get(i), argumentTypes[i]);
    }
    if (castTargetType != null) {
      arguments[1] = castTargetType;
    }
    RelDataType rexNodeType = rexCall.getType();
    if (rexNodeType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      rexNodeType = convertDecimalType(rexNodeType, rexBuilder);
    } else if (isUnsignedIntegerType(rexNodeType.getSqlTypeName())) {
      // Pinot has no unsigned storage and Calcite cannot build a RexLiteral of an unsigned type (CALCITE-1466), so
      // fold the constant into its signed-equivalent type. Mirrors the unsigned->signed mapping that
      // RelToPlanNodeConverter#convertToColumnDataType applies everywhere else -- which also means an unsupported
      // UBIGINT literal cast is rejected here too, since that method throws for UBIGINT.
      RelDataType signedType =
          RelToPlanNodeConverter.convertToColumnDataType(rexNodeType).toType(rexBuilder.getTypeFactory());
      rexNodeType = rexBuilder.getTypeFactory().createTypeWithNullability(signedType, rexNodeType.isNullable());
    }
    Object resultValue;
    try {
      QueryFunctionInvoker invoker = new QueryFunctionInvoker(functionInfo);
      if (functionInfo.getMethod().isVarArgs()) {
        resultValue = invoker.invoke(new Object[]{arguments});
      } else {
        invoker.convertTypes(arguments);
        resultValue = invoker.invoke(arguments);
      }
      if (rexNodeType.getSqlTypeName() == SqlTypeName.ARRAY) {
        RelDataType componentType = rexNodeType.getComponentType();
        if (componentType != null) {
          if (Objects.requireNonNull(componentType.getSqlTypeName()) == SqlTypeName.CHAR) {
            // Calcite uses CHAR for STRING, but we need to use VARCHAR for STRING
            rexNodeType = rexBuilder.getTypeFactory()
                .createArrayType(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), -1);
          }
        }
      }
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while invoking method: " + functionInfo.getMethod().getName() + " with arguments: "
              + Arrays.toString(arguments) + ": " + e.getMessage(), e);
    }
    try {
      resultValue = convertResultValue(resultValue, rexNodeType);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while converting result value: " + resultValue + " to type: " + rexNodeType, e);
    }
    if (resultValue == null) {
      return rexBuilder.makeNullLiteral(rexNodeType);
    }
    try {
      if (rexNodeType instanceof ArraySqlType) {
        // Scalar functions may return a primitive array (e.g. generateArray returns long[]), which cannot be cast to
        // Object[]. Read the elements reflectively instead, which boxes them the same way either kind of array does.
        int length = Array.getLength(resultValue);
        RelDataType componentType = rexNodeType.getComponentType();
        List<Object> resultValues = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
          resultValues.add(convertResultValue(Array.get(resultValue, i), componentType));
        }
        return rexBuilder.makeLiteral(resultValues, rexNodeType, false);
      }
      return rexBuilder.makeLiteral(resultValue, rexNodeType, false);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while making literal with value: " + resultValue + " and type: " + rexNodeType, e);
    }
  }

  /// Returns whether the call is `datetime + interval` (Calcite's `DATETIME_PLUS`) or `datetime - interval`
  /// (`MINUS_DATE`) with a `TIMESTAMP` or `DATE` result. Calcite puts the interval second for both.
  private static boolean isDatetimeIntervalArithmetic(RexCall rexCall) {
    SqlKind kind = rexCall.getKind();
    SqlTypeName resultType = rexCall.getType().getSqlTypeName();
    List<RexNode> operands = rexCall.getOperands();
    return (kind == SqlKind.PLUS || kind == SqlKind.MINUS) && (resultType == SqlTypeName.TIMESTAMP
        || resultType == SqlTypeName.DATE) && operands.size() == 2 && SqlTypeUtil.isInterval(operands.get(1).getType());
  }

  /// Evaluates `datetime + interval` or `datetime - interval` with the same semantics as Calcite. It cannot use Pinot's
  /// `plus` / `minus` functions, because Calcite stores a day-time interval as milliseconds but a year-month interval
  /// as months: they would add `INTERVAL '1' MONTH` as 1 millisecond. Months are added with the same calendar
  /// arithmetic as `TIMESTAMPADD(MONTH, ...)`, so `2019-01-31 + INTERVAL '1' MONTH` is `2019-02-28`. Like Calcite, only
  /// the whole days of a day-time interval are added to a `DATE`.
  private static RexNode evaluateDatetimeIntervalArithmetic(RexCall rexCall, RexBuilder rexBuilder) {
    RexLiteral datetime = (RexLiteral) rexCall.getOperands().get(0);
    RexLiteral interval = (RexLiteral) rexCall.getOperands().get(1);
    RelDataType resultType = rexCall.getType();
    if (datetime.isNull() || interval.isNull()) {
      return rexBuilder.makeNullLiteral(resultType);
    }
    Object resultValue;
    try {
      long millis = Objects.requireNonNull(datetime.getValueAs(Calendar.class)).getTimeInMillis();
      // Number of months for a year-month interval, number of milliseconds for a day-time interval
      long intervalValue = Objects.requireNonNull(interval.getValueAs(BigDecimal.class)).longValueExact();
      if (rexCall.getKind() == SqlKind.MINUS) {
        intervalValue = Math.negateExact(intervalValue);
      }
      boolean isDate = resultType.getSqlTypeName() == SqlTypeName.DATE;
      long resultMillis;
      if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(interval.getType().getSqlTypeName())) {
        resultMillis = DateTimeFunctions.timestampAdd("MONTH", intervalValue, millis);
      } else {
        if (isDate) {
          intervalValue = intervalValue / DateTimeUtils.MILLIS_PER_DAY * DateTimeUtils.MILLIS_PER_DAY;
        }
        resultMillis = Math.addExact(millis, intervalValue);
      }
      resultValue = isDate ? DateString.fromDaysSinceEpoch(
          Math.toIntExact(Math.floorDiv(resultMillis, DateTimeUtils.MILLIS_PER_DAY))) : resultMillis;
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while evaluating: " + rexCall + ": " + e.getMessage(), e);
    }
    return rexBuilder.makeLiteral(resultValue, resultType, false);
  }

  /// Returns whether the call is `+`, `-`, `*` or `/` with an interval result and only interval or numeric operands,
  /// e.g. `INTERVAL '1' MONTH * 2`.
  private static boolean isIntervalArithmetic(RexCall rexCall) {
    SqlKind kind = rexCall.getKind();
    List<RexNode> operands = rexCall.getOperands();
    return (kind == SqlKind.PLUS || kind == SqlKind.MINUS || kind == SqlKind.TIMES || kind == SqlKind.DIVIDE)
        && SqlTypeUtil.isInterval(rexCall.getType()) && operands.size() == 2 && operands.stream()
        .allMatch(operand -> SqlTypeUtil.isInterval(operand.getType()) || SqlTypeUtil.isNumeric(operand.getType()));
  }

  /// Evaluates interval arithmetic exactly. Pinot's `times` / `divide` functions compute in DOUBLE, so
  /// `INTERVAL '100' MONTH * 0.29` would be 28.999999999999996 months instead of 29, and `INTERVAL '1' DAY * 0.7` would
  /// be 1 millisecond short.
  private static RexNode evaluateIntervalArithmetic(RexCall rexCall, RexBuilder rexBuilder) {
    RexLiteral left = (RexLiteral) rexCall.getOperands().get(0);
    RexLiteral right = (RexLiteral) rexCall.getOperands().get(1);
    RelDataType resultType = rexCall.getType();
    if (left.isNull() || right.isNull()) {
      return rexBuilder.makeNullLiteral(resultType);
    }
    BigDecimal resultValue;
    try {
      // An interval literal holds its number of months or milliseconds
      BigDecimal leftValue = Objects.requireNonNull(left.getValueAs(BigDecimal.class));
      BigDecimal rightValue = Objects.requireNonNull(right.getValueAs(BigDecimal.class));
      switch (rexCall.getKind()) {
        case PLUS:
          resultValue = leftValue.add(rightValue);
          break;
        case MINUS:
          resultValue = leftValue.subtract(rightValue);
          break;
        case TIMES:
          resultValue = leftValue.multiply(rightValue);
          break;
        default:
          resultValue = leftValue.divide(rightValue, MathContext.DECIMAL128);
          break;
      }
      resultValue = toIntervalValue(resultValue, resultType);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while evaluating: " + rexCall + ": " + e.getMessage(), e);
    }
    return rexBuilder.makeLiteral(resultValue, resultType, false);
  }

  /// Converts a number to the value that Calcite stores for an interval: a whole number of months for a year-month
  /// interval, and milliseconds for a day-time interval (like Calcite, fractions of a millisecond are dropped).
  private static BigDecimal toIntervalValue(BigDecimal value, RelDataType intervalType) {
    if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(intervalType.getSqlTypeName())) {
      // Intentionally stricter than Calcite, which truncates to whole months (INTERVAL '1' MONTH / 2 is 0 months):
      // reject the query instead of silently dropping a fraction of a month
      Preconditions.checkArgument(value.stripTrailingZeros().scale() <= 0,
          "Year-month interval must be a whole number of months, got: %s", value);
    }
    return value.setScale(0, RoundingMode.DOWN);
  }

  private static RelDataType convertDecimalType(RelDataType relDataType, RexBuilder rexBuilder) {
    Preconditions.checkArgument(relDataType.getSqlTypeName() == SqlTypeName.DECIMAL);
    return RelToPlanNodeConverter.convertToColumnDataType(relDataType).toType(rexBuilder.getTypeFactory());
  }

  private static boolean isUnsignedIntegerType(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
      case UTINYINT:
      case USMALLINT:
      case UINTEGER:
      case UBIGINT:
        return true;
      default:
        return false;
    }
  }

  /// Returns the value that the runtime passes to a function for the literal (see `FunctionOperand`): the value from
  /// [RexExpressionUtils#fromRexLiteral], converted with [ColumnDataType#toExternal]. For example, an INTEGER literal
  /// becomes an `Integer` (Calcite stores a `BigDecimal`), and a TIMESTAMP literal becomes a `Timestamp`.
  @Nullable
  private static Object getArgumentValue(RexLiteral rexLiteral, ColumnDataType argumentType) {
    if (argumentType == ColumnDataType.OBJECT && rexLiteral.getTypeName() != SqlTypeName.SYMBOL) {
      // The runtime cannot represent this literal (for example an INTERVAL), so keep the value that Calcite stores
      return rexLiteral.getValue();
    }
    if (canExceedArgumentType(rexLiteral, argumentType)) {
      // Keep the exact value, which the conversion to INT or LONG can wrap around
      return rexLiteral.getValue();
    }
    RexExpression.Literal literal = RexExpressionUtils.fromRexLiteral(rexLiteral);
    Object value = literal.getValue();
    return value != null ? literal.getDataType().toExternal(value) : null;
  }

  /// Returns whether the literal is a DECIMAL whose type can hold numbers beyond the range of its INT or LONG argument
  /// type. The planner gives DECIMAL(p, 0) the type INT or LONG by its precision only, for example INT to
  /// DECIMAL(10, 0), and LONG to an integer literal beyond the BIGINT range. The decision depends on the type, not on
  /// the value, so that the literals of one type (for example the elements of an array) get the same Java class.
  private static boolean canExceedArgumentType(RexLiteral rexLiteral, ColumnDataType argumentType) {
    // NOTE: Check the SQL type, because RexLiteral.getTypeName() is DECIMAL for every exact number, e.g. INTEGER
    RelDataType type = rexLiteral.getType();
    if (type.getSqlTypeName() != SqlTypeName.DECIMAL) {
      return false;
    }
    int precision = type.getPrecision();
    switch (argumentType) {
      case INT:
        // An INT holds every number with up to 9 digits
        return precision > 9;
      case LONG:
        // A LONG holds every number with up to 18 digits
        return precision > 18;
      default:
        return false;
    }
  }

  @Nullable
  private static Object convertResultValue(@Nullable Object resultValue, RelDataType relDataType) {
    if (resultValue == null) {
      return null;
    }
    if (relDataType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
      // Return millis since epoch for TIMESTAMP
      if (resultValue instanceof Timestamp) {
        return ((Timestamp) resultValue).getTime();
      } else if (resultValue instanceof Number) {
        return ((Number) resultValue).longValue();
      } else {
        return TimestampUtils.toMillisSinceEpoch(resultValue.toString());
      }
    }
    // Use BigDecimal for INTEGER / BIGINT literals. (Unsigned integer types are normalized to their signed-equivalent
    // type earlier in convertRexCall, so they never reach here as an unsigned SqlTypeName.)
    if (relDataType.getSqlTypeName() == SqlTypeName.INTEGER || relDataType.getSqlTypeName() == SqlTypeName.BIGINT) {
      return new BigDecimal(((Number) resultValue).longValue());
    }
    if (relDataType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return new BigDecimal(resultValue.toString());
    }
    // Other functions with an interval result, e.g. ABS(INTERVAL '-1' MONTH). Interval arithmetic is evaluated exactly
    // in evaluateIntervalArithmetic().
    if (SqlTypeUtil.isInterval(relDataType)) {
      if (resultValue instanceof BigDecimal) {
        return toIntervalValue((BigDecimal) resultValue, relDataType);
      }
      double value = ((Number) resultValue).doubleValue();
      Preconditions.checkArgument(Double.isFinite(value), "Invalid interval value: %s", value);
      return toIntervalValue(BigDecimal.valueOf(value), relDataType);
    }
    // Use double for FLOAT / DOUBLE literals
    if (relDataType.getSqlTypeName() == SqlTypeName.FLOAT || relDataType.getSqlTypeName() == SqlTypeName.DOUBLE
        || relDataType.getSqlTypeName() == SqlTypeName.REAL) {
      return ((Number) resultValue).doubleValue();
    }
    // Return ByteString for byte[]
    if (resultValue instanceof byte[]) {
      return new ByteString((byte[]) resultValue);
    }
    // TODO: Add more type handling
    return resultValue;
  }
}
