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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Supplies the explicit data type argument from the type of the data argument:
/// - FIRST_WITH_TIME(dataColumn, timeColumn) -> FIRST_WITH_TIME(dataColumn, timeColumn, 'dataType')
/// - LAST_WITH_TIME(dataColumn, timeColumn) -> LAST_WITH_TIME(dataColumn, timeColumn, 'dataType')
///
/// The result type of the call is unchanged. The literal is projected directly beneath the aggregate, appended to the
/// existing project when there is one, because [PinotAggregateExchangeNodeInsertRule] and `AggregatePushdownRule`
/// inline literal arguments only from the immediate project.
public class PinotAggregateTypeArgumentRewriteRule extends RelOptRule {
  public static final PinotAggregateTypeArgumentRewriteRule INSTANCE =
      new PinotAggregateTypeArgumentRewriteRule(PinotRuleUtils.PINOT_REL_FACTORY, null);

  public static PinotAggregateTypeArgumentRewriteRule instanceWithDescription(String description) {
    return new PinotAggregateTypeArgumentRewriteRule(PinotRuleUtils.PINOT_REL_FACTORY, description);
  }

  // Matches with RelOptRule operands, like the other aggregate rewrite rules, instead of a RelRule config.
  @SuppressWarnings("deprecation")
  private PinotAggregateTypeArgumentRewriteRule(RelBuilderFactory factory, @Nullable String description) {
    super(operand(LogicalAggregate.class, any()), factory, description);
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
    int numAggCalls = orgAggCalls.size();
    int[] literalIndexes = new int[numAggCalls];
    Map<String, Integer> literalIndexByDataType = new HashMap<>();
    for (int i = 0; i < numAggCalls; i++) {
      String dataType = getDataTypeArgument(orgAggCalls.get(i), input);
      literalIndexes[i] = dataType == null ? -1 : literalIndexByDataType.computeIfAbsent(dataType, type -> {
        projects.add(rexBuilder.makeLiteral(type));
        return projects.size() - 1;
      });
    }
    RelNode newInput = LogicalProject.create(projectInput, hints, projects, (List<String>) null, Set.of());

    List<AggregateCall> aggCalls = new ArrayList<>(numAggCalls);
    for (int i = 0; i < numAggCalls; i++) {
      AggregateCall aggCall = orgAggCalls.get(i);
      if (literalIndexes[i] < 0) {
        aggCalls.add(aggCall);
        continue;
      }
      List<Integer> argList = new ArrayList<>(aggCall.getArgList());
      argList.add(literalIndexes[i]);
      // Keeps the original result type, so the rewrite does not change the result schema.
      aggCalls.add(aggCall.withArgList(argList));
    }
    call.transformTo(
        aggRel.copy(aggRel.getTraitSet(), newInput, aggRel.getGroupSet(), aggRel.getGroupSets(), aggCalls));
  }

  /// Returns the data type argument to append, or `null` when the call already has one or is not such a call.
  @Nullable
  private static String getDataTypeArgument(AggregateCall aggCall, RelNode input) {
    if (aggCall.getArgList().size() != 2) {
      return null;
    }
    String name = AggregationFunctionType.getNormalizedAggregationFunctionName(aggCall.getAggregation().getName());
    if (!name.equals(AggregationFunctionType.FIRSTWITHTIME.name())
        && !name.equals(AggregationFunctionType.LASTWITHTIME.name())) {
      return null;
    }
    SqlTypeName dataType =
        input.getRowType().getFieldList().get(aggCall.getArgList().get(0)).getType().getSqlTypeName();
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
        // Rejected during validation; leave the call as it is rather than guess a type.
        return null;
    }
  }
}
