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
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;


/**
 * Converts a semi-join with literal only right side to an IN clause.
 *
 * This is required because Calcite compiles IN clause into a semi-join, which is not efficient.
 *
 * Example query plan when reaching this rule:
 * LogicalJoin(condition=[=($1, $2)], joinType=[semi])
 *   LogicalProject(col1=[$0], col2=[$1])
 *     LogicalTableScan(table=[[default, a]])
 *   LogicalValues(tuples=[[{ _UTF-8'a1' }, { _UTF-8'a2' }, { _UTF-8'a3' }]])
 */
public class PinotSemiJoinToInClauseRule extends RelOptRule {
  public static final PinotSemiJoinToInClauseRule INSTANCE =
      new PinotSemiJoinToInClauseRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotSemiJoinToInClauseRule(RelBuilderFactory factory) {
    super(operand(LogicalJoin.class, operand(RelNode.class, any()), operand(LogicalValues.class, any())), factory,
        null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    if (join.getJoinType() != JoinRelType.SEMI) {
      return false;
    }
    // Match single column non-empty LogicalValues
    LogicalValues values = call.rel(2);
    ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();
    if (tuples.isEmpty()) {
      return false;
    }
    return tuples.get(0).size() == 1;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    RelNode left = call.rel(1);
    LogicalValues values = call.rel(2);

    // Extract values from LogicalValues
    ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();
    List<RexNode> inArguments = new ArrayList<>(1 + tuples.size());
    inArguments.add(RexInputRef.of(join.analyzeCondition().leftKeys.get(0), left.getRowType()));
    for (List<RexLiteral> tuple : tuples) {
      if (!tuple.isEmpty()) {
        inArguments.add(tuple.get(0));
      }
    }

    // Build the IN condition
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RexNode inCondition = rexBuilder.makeCall(PinotOperatorTable.PINOT_IN, inArguments);

    // Create a LogicalFilter with the IN condition
    LogicalFilter filter = LogicalFilter.create(left, inCondition);
    call.transformTo(filter);
  }
}
