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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Pinot customized version of {@link AggregateReduceFunctionsRule}, which only reduce on SUM and AVG when the produced SUM0 could benefit from rewriting CASE to FILTER. The conditions are copied from {@link AggregateCaseToFilterRule}
 * Mostly we don't want to reduce because Pinot supports all aggregation functions natively,
 * but not REGR_COUNT which can be generated during reduce.
 * But in cases that the below Project contains certain CASE WHEN expression that
 * reducing SUM or AVG to SUM0 could help AggregateCaseToFilterRule to transform
 * CASE WHEN to FILTER, reducing might be a good idea.
 */
public class PinotAggregateReduceFunctionsRule extends AggregateReduceFunctionsRule {

  // only consider reducing when possible to do CaseToFilter
  public static final PinotAggregateReduceFunctionsRule INSTANCE =
      new PinotAggregateReduceFunctionsRule((Config) Config.DEFAULT
          .withOperandSupplier(b0 ->
              b0.operand(Aggregate.class).oneInput(b1 ->
                  b1.operand(Project.class).anyInputs())
          ));
  private PinotAggregateReduceFunctionsRule(Config config) {
    super(config);
  }

  @Override
  public boolean canReduce(AggregateCall call) {
    // This logic is handled in onMatch()
    return true;
  }

  /** The below functions are copied and adapted from {@link AggregateReduceFunctionsRule} */
  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    for (AggregateCall aggCall: aggregate.getAggCallList()) {
      SqlKind kind = aggCall.getAggregation().getKind();
      // only consider reducing SUM and AVG
      if (aggCall.isDistinct() || (kind != SqlKind.SUM && kind != SqlKind.AVG)) {
        continue;
      }
      final int singleArg = soleArgument(aggCall);
      if (singleArg < 0) {
        continue;
      }
      final RexNode rexNode = project.getProjects().get(singleArg);
      if (!isThreeArgCase(rexNode)) {
        continue;
      }

      final RelOptCluster cluster = project.getCluster();
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final RexCall caseCall = (RexCall) rexNode;

      // If one arg is null and the other is not, reverse them and set "flip",
      // which negates the filter.
      final boolean flip = RexLiteral.isNullLiteral(caseCall.operands.get(1))
          && !RexLiteral.isNullLiteral(caseCall.operands.get(2));
      final RexNode arg1 = caseCall.operands.get(flip ? 2 : 1);
      final RexNode arg2 = caseCall.operands.get(flip ? 1 : 2);


      // If one of the below case matches, reducing SUM to SUM0 would be beneficial when
      // used with CoreRules.AGGREGATE_CASE_TO_FILTER:
      // B: SUM0(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
      //   => COUNT() FILTER (x = 'foo')
      // A2: SUM0(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
      //   => SUM0(cnt) FILTER (x = 'foo')
      if ((isIntLiteral(arg1, BigDecimal.ONE) && isIntLiteral(arg2, BigDecimal.ZERO))
        || (isIntLiteral(arg2, BigDecimal.ZERO))) {
        super.onMatch(call);
      }
    }
  }

  /** Returns the argument, if an aggregate call has a single argument,
   * otherwise -1. */
  private static int soleArgument(AggregateCall aggregateCall) {
    return aggregateCall.getArgList().size() == 1
        ? aggregateCall.getArgList().get(0)
        : -1;
  }

  private static boolean isThreeArgCase(final RexNode rexNode) {
    return rexNode.getKind() == SqlKind.CASE
        && ((RexCall) rexNode).operands.size() == 3;
  }

  private static boolean isIntLiteral(RexNode rexNode, BigDecimal value) {
    return rexNode instanceof RexLiteral
        && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName())
        && value.equals(((RexLiteral) rexNode).getValueAs(BigDecimal.class));
  }
}
