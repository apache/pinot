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
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Supplies the explicit data type argument from the type of the data argument:
/// - FIRST_WITH_TIME(dataColumn, timeColumn) -> FIRST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - LAST_WITH_TIME(dataColumn, timeColumn) -> LAST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - ANY_VALUE(expr) -> ANY_VALUE(expr, 'dataType'), except by [#WITHOUT_ANY_VALUE]
///
/// The result type of the call is unchanged. The literal is projected directly beneath the aggregate, appended to the
/// existing project when there is one, because [PinotAggregateExchangeNodeInsertRule] and `AggregatePushdownRule`
/// inline literal arguments only from the immediate project.
///
/// The servers require the data type argument of FIRST_WITH_TIME / LAST_WITH_TIME, so the rule is required rather than
/// an optimization: `QueryEnvironment` always applies [#INSTANCE], or [#WITHOUT_ANY_VALUE] when the `inferAnyValueType`
/// query option is false, and `skipPlannerRules` cannot turn it off.
public class PinotAggregateTypeArgumentRewriteRule extends RelOptRule {
  // Data types of the explicit forms, by the type the multi-stage engine reads the data argument as
  private static final Set<ColumnDataType> DATA_TYPES =
      EnumSet.of(ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
          ColumnDataType.BIG_DECIMAL, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.STRING,
          ColumnDataType.BYTES, ColumnDataType.UUID);

  public static final PinotAggregateTypeArgumentRewriteRule INSTANCE =
      new PinotAggregateTypeArgumentRewriteRule(true, PinotRuleUtils.PINOT_REL_FACTORY, null);
  /// Leaves ANY_VALUE without the data type argument.
  public static final PinotAggregateTypeArgumentRewriteRule WITHOUT_ANY_VALUE =
      new PinotAggregateTypeArgumentRewriteRule(false, PinotRuleUtils.PINOT_REL_FACTORY,
          "PinotAggregateTypeArgumentRewriteRuleWithoutAnyValue");

  private final boolean _rewriteAnyValue;

  // Matches with RelOptRule operands, like the other aggregate rewrite rules, instead of a RelRule config.
  @SuppressWarnings("deprecation")
  private PinotAggregateTypeArgumentRewriteRule(boolean rewriteAnyValue, RelBuilderFactory factory,
      @Nullable String description) {
    super(operand(LogicalAggregate.class, any()), factory, description);
    _rewriteAnyValue = rewriteAnyValue;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    // Requiring a call without its data type is also what makes the rule terminate.
    Aggregate aggRel = call.rel(0);
    RelNode input = aggRel.getInput();
    return aggRel.getAggCallList().stream().anyMatch(aggCall -> getDataTypeArgument(aggCall, input) != null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    RelNode input = aggRel.getInput();
    RelNode unboxedInput = PinotRuleUtils.unboxRel(input);
    RexBuilder rexBuilder = aggRel.getCluster().getRexBuilder();

    List<RexNode> projects;
    RelNode projectInput;
    List<RelHint> hints;
    if (unboxedInput instanceof Project) {
      Project project = (Project) unboxedInput;
      projects = new ArrayList<>(project.getProjects());
      projectInput = project.getInput();
      hints = project.getHints();
    } else {
      int numFields = input.getRowType().getFieldCount();
      projects = new ArrayList<>(numFields + 1);
      for (int i = 0; i < numFields; i++) {
        projects.add(rexBuilder.makeInputRef(input, i));
      }
      projectInput = input;
      hints = List.of();
    }

    // Appending keeps the existing field indexes, so the group set and the other calls are unaffected.
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    List<AggregateCall> aggCalls = new ArrayList<>(orgAggCalls.size());
    Map<String, Integer> literalIndexByDataType = new HashMap<>();
    for (AggregateCall aggCall : orgAggCalls) {
      String dataType = getDataTypeArgument(aggCall, input);
      if (dataType == null) {
        aggCalls.add(aggCall);
        continue;
      }
      List<Integer> argList = new ArrayList<>(aggCall.getArgList());
      argList.add(literalIndexByDataType.computeIfAbsent(dataType, type -> {
        projects.add(rexBuilder.makeLiteral(type));
        return projects.size() - 1;
      }));
      // Keeps the original result type, so the rewrite does not change the result schema.
      aggCalls.add(aggCall.withArgList(argList));
    }
    RelNode newInput = LogicalProject.create(projectInput, hints, projects, (List<String>) null, Set.of());
    call.transformTo(
        aggRel.copy(aggRel.getTraitSet(), newInput, aggRel.getGroupSet(), aggRel.getGroupSets(), aggCalls));
  }

  /// Returns the data type argument to append, or `null` when the call already has one or is not such a call. Throws
  /// for a data argument without an explicit form, such as an array from a multi-value column.
  @Nullable
  private String getDataTypeArgument(AggregateCall aggCall, RelNode input) {
    String name = AggregationFunctionType.getNormalizedAggregationFunctionName(aggCall.getAggregation().getName());
    List<Integer> argList = aggCall.getArgList();
    String arguments;
    if (name.equals(AggregationFunctionType.FIRSTWITHTIME.name())
        || name.equals(AggregationFunctionType.LASTWITHTIME.name())) {
      if (argList.size() != 2) {
        return null;
      }
      arguments = "dataColumn, timeColumn";
    } else if (name.equals(AggregationFunctionType.ANYVALUE.name())) {
      if (argList.size() == 2) {
        checkAnyValueDataTypeArgument(name, argList, input);
        return null;
      }
      if (!_rewriteAnyValue) {
        return null;
      }
      arguments = "expr";
    } else {
      return null;
    }
    return inferDataType(name, arguments, input.getRowType().getFieldList().get(argList.get(0)).getType());
  }

  private static String inferDataType(String name, String arguments, RelDataType relDataType) {
    ColumnDataType dataType = RelToPlanNodeConverter.convertToColumnDataType(relDataType);
    if (!DATA_TYPES.contains(dataType)) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type for %s(%s): %s", name, arguments, relDataType.getSqlTypeName()));
    }
    return dataType.name();
  }

  /// The result type of ANY_VALUE follows its data argument, so an explicit data type argument must be the one the data
  /// argument is read as.
  private static void checkAnyValueDataTypeArgument(String name, List<Integer> argList, RelNode input) {
    RelDataType relDataType = input.getRowType().getFieldList().get(argList.get(0)).getType();
    String dataType = inferDataType(name, "expr", relDataType);
    RelNode unboxedInput = PinotRuleUtils.unboxRel(input);
    RexNode dataTypeArgument =
        unboxedInput instanceof Project ? ((Project) unboxedInput).getProjects().get(argList.get(1)) : null;
    String explicitDataType =
        dataTypeArgument instanceof RexLiteral ? ((RexLiteral) dataTypeArgument).getValueAs(String.class) : null;
    if (!dataType.equalsIgnoreCase(explicitDataType)) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type for %s(expr, 'dataType') with expr of type %s: %s", name,
              relDataType.getSqlTypeName(), explicitDataType != null ? explicitDataType : dataTypeArgument));
    }
  }
}
