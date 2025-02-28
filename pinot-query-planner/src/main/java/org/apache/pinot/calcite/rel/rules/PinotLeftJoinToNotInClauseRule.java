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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;


/**
 * Converts a left-join with literal only right side to a NOT IN clause.
 *
 * This is required because Calcite compiles NOT IN clause into a left-join, which is not efficient.
 *
 * Example query plan when reaching this rule:
 * LogicalProject(col1=[$0])
 *   LogicalFilter(condition=[IS NOT TRUE($3)])
 *     LogicalJoin(condition=[=($1, $2)], joinType=[left])
 *       LogicalProject(col1=[$0], col20=[$1])
 *         LogicalTableScan(table=[[default, a]])
 *       LogicalProject(ROW_VALUE=[$0], $f1=[true])
 *         LogicalValues(tuples=[[{ _UTF-8'a1' }, { _UTF-8'a2' }, { _UTF-8'a3' }]])
 */
public class PinotLeftJoinToNotInClauseRule extends RelOptRule {
  public static final PinotLeftJoinToNotInClauseRule INSTANCE =
      new PinotLeftJoinToNotInClauseRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotLeftJoinToNotInClauseRule(RelBuilderFactory factory) {
    super(operand(LogicalProject.class, operand(LogicalFilter.class,
        operand(LogicalJoin.class, operand(RelNode.class, any()),
            operand(LogicalProject.class, operand(LogicalValues.class, any()))))), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    // Match LogicalFilter with single IS NOT TRUE condition
    LogicalFilter filter = call.rel(1);
    RexNode filterCondition = filter.getCondition();
    if (filterCondition.getKind() != SqlKind.IS_NOT_TRUE) {
      return false;
    }
    // Match LogicalJoin with left join type and single EQUALS condition
    LogicalJoin join = call.rel(2);
    if (join.getJoinType() != JoinRelType.LEFT) {
      return false;
    }
    RexNode joinCondition = join.getCondition();
    if (joinCondition.getKind() != SqlKind.EQUALS) {
      return false;
    }
    // Match LogicalProject with 2 columns, the first one is a reference to the LogicalValues, the second one is TRUE
    LogicalProject project = call.rel(4);
    List<RexNode> projects = project.getProjects();
    if (projects.size() != 2) {
      return false;
    }
    RexNode firstProject = projects.get(0);
    if (!(firstProject instanceof RexInputRef) || ((RexInputRef) firstProject).getIndex() != 0) {
      return false;
    }
    RexNode secondProject = projects.get(1);
    if (!(secondProject instanceof RexLiteral) || !Boolean.TRUE.equals(((RexLiteral) secondProject).getValue())) {
      return false;
    }
    // Match single column non-empty LogicalValues
    LogicalValues values = call.rel(5);
    ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();
    if (tuples.isEmpty()) {
      return false;
    }
    return tuples.get(0).size() == 1;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalJoin join = call.rel(2);
    RelNode left = call.rel(3);
    LogicalValues values = call.rel(5);

    // Extract values from LogicalValues
    ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();
    List<RexNode> notInArguments = new ArrayList<>(1 + tuples.size());
    notInArguments.add(RexInputRef.of(join.analyzeCondition().leftKeys.get(0), left.getRowType()));
    for (List<RexLiteral> tuple : tuples) {
      if (!tuple.isEmpty()) {
        notInArguments.add(tuple.get(0));
      }
    }

    // Build the NOT IN condition
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RexNode notInCondition = rexBuilder.makeCall(PinotOperatorTable.PINOT_NOT_IN, notInArguments);

    // Create a LogicalFilter with the NOT IN condition
    LogicalFilter filter = LogicalFilter.create(left, notInCondition);

    // Create a LogicalProject with the NOT IN filter as the input
    LogicalProject project = call.rel(0);
    call.transformTo(LogicalProject.create(filter, project.getHints(), project.getProjects(), project.getRowType(),
        project.getVariablesSet()));
  }
}
