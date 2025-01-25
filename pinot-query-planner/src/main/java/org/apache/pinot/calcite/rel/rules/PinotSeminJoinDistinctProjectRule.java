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

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;


/**
 * Special rule for Pinot, this rule always append a distinct to the
 * {@link org.apache.calcite.rel.logical.LogicalProject} on top of a Semi join
 * {@link org.apache.calcite.rel.core.Join} to ensure the correctness of the query.
 */
public class PinotSeminJoinDistinctProjectRule extends RelOptRule {
  public static final PinotSeminJoinDistinctProjectRule INSTANCE =
      new PinotSeminJoinDistinctProjectRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotSeminJoinDistinctProjectRule(RelBuilderFactory factory) {
    super(operand(LogicalJoin.class, operand(AbstractRelNode.class, any()), operand(LogicalProject.class, any())),
        factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    if (join.getJoinType() != JoinRelType.SEMI) {
      return false;
    }
    // Do not apply this rule if join strategy is explicitly set to something other than dynamic broadcast
    String hintOption = PinotHintStrategyTable.getHintOption(join.getHints(), PinotHintOptions.JOIN_HINT_OPTIONS,
        PinotHintOptions.JoinHintOptions.APPEND_DISTINCT_TO_SEMI_JOIN_PROJECT);
    if (!Boolean.parseBoolean(hintOption)) {
      return false;
    }
    return ((LogicalProject) call.rel(2)).getProjects().size() == 1;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    RelNode newRightProject = insertDistinctToProject(call, call.rel(2));
    call.transformTo(join.copy(join.getTraitSet(), List.of(call.rel(1), newRightProject)));
  }

  private RelNode insertDistinctToProject(RelOptRuleCall call, LogicalProject project) {
    RelBuilder relBuilder = call.builder();
    relBuilder.push(project);
    relBuilder.distinct();
    return relBuilder.build();
  }
}
