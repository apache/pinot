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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Supplies the explicit data type argument from the type of the data argument:
/// - FIRST_WITH_TIME(dataColumn, timeColumn) -> FIRST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - LAST_WITH_TIME(dataColumn, timeColumn) -> LAST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - ARRAY_AGG(expression[, isDistinct]) -> ARRAY_AGG(expression, 'dataType'[, isDistinct])
/// - MODE(stringColumn) -> MODE(stringColumn, 'MIN', 'STRING')
/// - ANY_VALUE(expr) -> ANY_VALUE(expr, 'dataType'), except by [#WITHOUT_ANY_VALUE]
///
/// The result type of the call is unchanged. The literals are projected directly beneath the aggregate, appended to
/// the existing project when there is one, because [PinotAggregateExchangeNodeInsertRule] and `AggregatePushdownRule`
/// inline literal arguments from the immediate project or the inputs of a union.
///
/// The servers require type arguments for FIRST_WITH_TIME, LAST_WITH_TIME and ARRAY_AGG. `QueryEnvironment` always
/// applies [#WITHOUT_ANY_VALUE], or [#INSTANCE] when `inferAnyValueType` is true, regardless of `skipPlannerRules`.

public class PinotAggregateTypeArgumentRewriteRule extends RelOptRule {
  private static final Set<ColumnDataType> ANY_VALUE_DATA_TYPES =
      EnumSet.of(ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
          ColumnDataType.BIG_DECIMAL, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.STRING,
          ColumnDataType.BYTES, ColumnDataType.UUID);

  public static final PinotAggregateTypeArgumentRewriteRule INSTANCE =
      new PinotAggregateTypeArgumentRewriteRule(true, false, PinotRuleUtils.PINOT_REL_FACTORY, null);
  /// Leaves ANY_VALUE without the data type argument.
  public static final PinotAggregateTypeArgumentRewriteRule WITHOUT_ANY_VALUE =
      new PinotAggregateTypeArgumentRewriteRule(false, false, PinotRuleUtils.PINOT_REL_FACTORY,
          "PinotAggregateTypeArgumentRewriteRuleWithoutAnyValue");

  /// Runs before CASE-to-FILTER without changing other aggregation functions.
  public static final PinotAggregateTypeArgumentRewriteRule ARRAY_AGG_ONLY =
      new PinotAggregateTypeArgumentRewriteRule(false, true, PinotRuleUtils.PINOT_REL_FACTORY,
          "PinotAggregateTypeArgumentRewriteRuleArrayAggOnly");

  private final boolean _arrayAggOnly;
  private final boolean _rewriteAnyValue;

  // Matches with RelOptRule operands, like the other aggregate rewrite rules, instead of a RelRule config.
  @SuppressWarnings("deprecation")
  private PinotAggregateTypeArgumentRewriteRule(boolean rewriteAnyValue, boolean arrayAggOnly,
      RelBuilderFactory factory, @Nullable String description) {
    super(operand(LogicalAggregate.class, any()), factory, description);
    _rewriteAnyValue = rewriteAnyValue;
    _arrayAggOnly = arrayAggOnly;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    // Requiring a call without its data type is also what makes the rule terminate.
    Aggregate aggRel = call.rel(0);
    RelNode input = aggRel.getInput();
    return aggRel.getAggCallList().stream().anyMatch(aggCall -> getLiteralArguments(aggCall, input) != null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    RelNode input = aggRel.getInput();
    RexBuilder rexBuilder = aggRel.getCluster().getRexBuilder();
    int numInputFields = input.getRowType().getFieldCount();
    List<RexNode> literals = new ArrayList<>();

    // Appending keeps the existing field indexes, so the group set and the other calls are unaffected.
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    List<AggregateCall> aggCalls = new ArrayList<>(orgAggCalls.size());
    Map<String, Integer> literalIndexes = new HashMap<>();
    for (AggregateCall aggCall : orgAggCalls) {
      List<String> literalArguments = getLiteralArguments(aggCall, input);
      if (literalArguments == null) {
        aggCalls.add(aggCall);
        continue;
      }
      List<Integer> argList = new ArrayList<>(aggCall.getArgList());
      int argumentPosition = isArrayAgg(aggCall) ? 1 : argList.size();
      for (String literalArgument : literalArguments) {
        int literalIndex = literalIndexes.computeIfAbsent(literalArgument, literal -> {
          literals.add(rexBuilder.makeLiteral(literal));
          return numInputFields + literals.size() - 1;
        });
        argList.add(argumentPosition++, literalIndex);
      }
      // Keeps the original result type, so the rewrite does not change the result schema.
      aggCalls.add(aggCall.withArgList(argList));
    }
    RelNode newInput = PinotRuleUtils.appendAggregateLiterals(input, literals);
    call.transformTo(
        aggRel.copy(aggRel.getTraitSet(), newInput, aggRel.getGroupSet(), aggRel.getGroupSets(), aggCalls));
  }

  /// Returns the literal arguments to append, or `null` when the call already has them or is not such a call.
  @Nullable
  private List<String> getLiteralArguments(AggregateCall aggCall, RelNode input) {
    List<Integer> argList = aggCall.getArgList();
    String name = AggregationFunctionType.getNormalizedAggregationFunctionName(aggCall.getAggregation().getName());
    if (isArrayAgg(aggCall)) {
      if (argList.size() != 1 && (argList.size() != 2
          || getSqlTypeName(input, argList.get(1)) != SqlTypeName.BOOLEAN)) {
        return null;
      }
      RelDataType dataType = input.getRowType().getFieldList().get(argList.get(0)).getType();
      if (dataType.getComponentType() != null) {
        dataType = dataType.getComponentType();
      }
      if (dataType.getSqlTypeName() == SqlTypeName.DECIMAL) {
        return List.of(RelToPlanNodeConverter.convertToColumnDataType(aggCall.getType()).toDataType().name());
      }
      return List.of(getDataTypeArgument(name, dataType.getSqlTypeName()));
    }
    if (_arrayAggOnly) {
      return null;
    }
    if (name.equals(AggregationFunctionType.MODE.name())) {
      // BOOLEAN and TIMESTAMP already return their own type, cast from the DOUBLE result.
      return argList.size() == 1 && SqlTypeName.CHAR_TYPES.contains(getSqlTypeName(input, argList.get(0)))
          ? List.of("MIN", "STRING") : null;
    }
    if (argList.size() == 2 && (name.equals(AggregationFunctionType.FIRSTWITHTIME.name())
        || name.equals(AggregationFunctionType.LASTWITHTIME.name()))) {
      return List.of(getDataTypeArgument(name, getSqlTypeName(input, argList.get(0))));
    }
    if (name.equals(AggregationFunctionType.ANYVALUE.name())) {
      if (argList.size() == 2) {
        checkAnyValueDataTypeArgument(name, argList, input);
      } else if (_rewriteAnyValue) {
        return List.of(getAnyValueDataType(name + "(expr)", getArgumentType(input, argList.get(0))));
      }
    }
    return null;
  }

  private static RelDataType getArgumentType(RelNode input, int argIndex) {
    return input.getRowType().getFieldList().get(argIndex).getType();
  }

  private static SqlTypeName getSqlTypeName(RelNode input, int fieldIndex) {
    return getArgumentType(input, fieldIndex).getSqlTypeName();
  }

  /// Returns the data type argument of FIRST_WITH_TIME / LAST_WITH_TIME. Throws for a data argument without an
  /// explicit form, such as an array from a multi-value column.
  private static String getDataTypeArgument(String name, SqlTypeName dataType) {
    switch (dataType) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return "INT";
      case BIGINT:
        return "LONG";
      case REAL:
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case DECIMAL:
        return "BIG_DECIMAL";
      case CHAR:
      case VARCHAR:
        return "STRING";
      case BOOLEAN:
        return "BOOLEAN";
      case TIMESTAMP:
        return "TIMESTAMP";
      case BINARY:
      case VARBINARY:
        return "BYTES";
      case UUID:
        return "UUID";
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported data type for %s%s: %s", name,
                name.equals(AggregationFunctionType.ARRAYAGG.name()) ? "(expression)" : "(dataColumn, timeColumn)",
                dataType));
    }
  }

  /// Returns the type the multi-stage engine reads the argument as, e.g. DOUBLE for a DECIMAL expression with a scale,
  /// so that the values are read without a conversion that can fail, such as an infinite DOUBLE to BIG_DECIMAL.
  private static String getAnyValueDataType(String call, RelDataType relDataType) {
    ColumnDataType dataType = RelToPlanNodeConverter.convertToColumnDataType(relDataType);
    if (!ANY_VALUE_DATA_TYPES.contains(dataType)) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type for %s: %s", call, relDataType.getSqlTypeName()));
    }
    return dataType.name();
  }

  /// The result type of ANY_VALUE follows its data argument, so an explicit data type argument must be the one this
  /// rule supplies for it.
  private static void checkAnyValueDataTypeArgument(String name, List<Integer> argList, RelNode input) {
    RelDataType relDataType = getArgumentType(input, argList.get(0));
    String dataType = getAnyValueDataType(name + "(expr, 'dataType')", relDataType);
    RelNode unboxedInput = PinotRuleUtils.unboxRel(input);
    RexNode dataTypeArgument =
        unboxedInput instanceof Project ? ((Project) unboxedInput).getProjects().get(argList.get(1)) : null;
    if (!(dataTypeArgument instanceof RexLiteral)) {
      throw new IllegalArgumentException(
          String.format("%s expects a literal data type argument, got: %s", name, dataTypeArgument));
    }
    String explicitDataType = ((RexLiteral) dataTypeArgument).getValueAs(String.class);
    if (!dataType.equalsIgnoreCase(explicitDataType)) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type for %s(expr, 'dataType') with expr of type %s: %s", name,
              relDataType.getSqlTypeName(), explicitDataType != null ? explicitDataType : dataTypeArgument));
    }
  }

  private static boolean isArrayAgg(AggregateCall aggCall) {
    return AggregationFunctionType.getNormalizedAggregationFunctionName(aggCall.getAggregation().getName())
        .equals(AggregationFunctionType.ARRAYAGG.name());
  }
}
