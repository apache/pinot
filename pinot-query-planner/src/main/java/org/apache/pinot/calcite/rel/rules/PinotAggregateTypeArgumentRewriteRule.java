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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Supplies the explicit data type argument from the type of the data argument:
/// - FIRST_WITH_TIME(dataColumn, timeColumn) -> FIRST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - LAST_WITH_TIME(dataColumn, timeColumn) -> LAST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - ARRAY_AGG(expression[, isDistinct]) -> ARRAY_AGG(expression, 'dataType'[, isDistinct])
///
/// The result type of the call is unchanged. The literal is projected directly beneath the aggregate, appended to the
/// existing project when there is one, because [PinotAggregateExchangeNodeInsertRule] and `AggregatePushdownRule`
/// inline literal arguments only from the immediate project.
///
/// The servers require the data type argument, so the rule is required rather than an optimization: `QueryEnvironment`
/// always applies it, and `skipPlannerRules` cannot turn it off.
public class PinotAggregateTypeArgumentRewriteRule extends RelOptRule {
  public static final PinotAggregateTypeArgumentRewriteRule INSTANCE =
      new PinotAggregateTypeArgumentRewriteRule(PinotRuleUtils.PINOT_REL_FACTORY);

  // Matches with RelOptRule operands, like the other aggregate rewrite rules, instead of a RelRule config.
  @SuppressWarnings("deprecation")
  private PinotAggregateTypeArgumentRewriteRule(RelBuilderFactory factory) {
    super(operand(LogicalAggregate.class, any()), factory, null);
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
      int literalIndex = literalIndexByDataType.computeIfAbsent(dataType, type -> {
        projects.add(rexBuilder.makeLiteral(type));
        return projects.size() - 1;
      });
      List<Integer> argList = new ArrayList<>(aggCall.getArgList());
      if (isArrayAgg(aggCall)) {
        argList.add(1, literalIndex);
      } else {
        argList.add(literalIndex);
      }
      // Keeps the original result type, so the rewrite does not change the result schema.
      aggCalls.add(aggCall.withArgList(argList));
    }
    RelNode newInput = LogicalProject.create(projectInput, hints, projects, (List<String>) null, Set.of());
    call.transformTo(
        aggRel.copy(aggRel.getTraitSet(), newInput, aggRel.getGroupSet(), aggRel.getGroupSets(), aggCalls));
  }

  /// Returns the data type argument to add, or `null` when the call already has one or is not such a call. Throws for
  /// a data argument without an explicit form, such as an array from a multi-value column in FIRST_WITH_TIME. The
  /// mapped types match the single-stage `AggregateFunctionRewriteOptimizer`, except for an ARRAY_AGG of a DECIMAL with
  /// scale 0, which only an expression can produce.
  @Nullable
  private static String getDataTypeArgument(AggregateCall aggCall, RelNode input) {
    List<Integer> argList = aggCall.getArgList();
    List<RelDataTypeField> fields = input.getRowType().getFieldList();
    String name = AggregationFunctionType.getNormalizedAggregationFunctionName(aggCall.getAggregation().getName());
    RelDataType dataType;
    String signature;
    if (name.equals(AggregationFunctionType.ARRAYAGG.name())) {
      // ARRAY_AGG(expression) or ARRAY_AGG(expression, isDistinct); a string second argument is the data type
      if (argList.size() != 1
          && (argList.size() != 2 || fields.get(argList.get(1)).getType().getSqlTypeName() != SqlTypeName.BOOLEAN)) {
        return null;
      }
      // A multi-value input has the type of its elements
      dataType = fields.get(argList.get(0)).getType();
      if (dataType.getComponentType() != null) {
        dataType = dataType.getComponentType();
      }
      signature = "(expression)";
    } else {
      if (argList.size() != 2) {
        return null;
      }
      if (!name.equals(AggregationFunctionType.FIRSTWITHTIME.name())
          && !name.equals(AggregationFunctionType.LASTWITHTIME.name())) {
        return null;
      }
      dataType = fields.get(argList.get(0)).getType();
      signature = "(dataColumn, timeColumn)";
    }
    switch (dataType.getSqlTypeName()) {
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
        // The planner types an array of DECIMAL with scale 0 as INT_ARRAY or LONG_ARRAY, see
        // RelToPlanNodeConverter.resolveDecimal
        if (name.equals(AggregationFunctionType.ARRAYAGG.name()) && dataType.getScale() == 0
            && dataType.getPrecision() <= 38) {
          return dataType.getPrecision() <= 10 ? "INT" : "LONG";
        }
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
            String.format("Unsupported data type for %s%s: %s", name, signature, dataType.getSqlTypeName()));
    }
  }

  private static boolean isArrayAgg(AggregateCall aggCall) {
    return AggregationFunctionType.getNormalizedAggregationFunctionName(aggCall.getAggregation().getName())
        .equals(AggregationFunctionType.ARRAYAGG.name());
  }
}
