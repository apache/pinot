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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * For an aggregate query which doesn't contain any grouping-sets, Calcite doesn't generate a Projection node which
 * leads to the table-scan stage reading all the columns. For this case, this rule adds a LogicalProject on the
 * join-key columns on top of the LogicalJoin node. The projection would later be pushed down by
 * @{link CoreRules.PROJECT_JOIN_TRANSPOSE}.
 *
 * <h3>Example Query:</h3>
 * <pre>
 *   SELECT COUNT(*)
 *     FROM baseballStats_OFFLINE AS A JOIN baseballStats_OFFLINE AS B
 *       ON A.playerID = B.playerID
 * </pre>
 *
 * <h3>Plan before</h3>
 * <pre>
 * LogicalAggregate(group={}, EXPR$0=COUNT())
 *   LogicalJoin(left=..., right=..., condition=EQ($27, $55))
 *     ...
 *       LogicalTableScan(table=baseballStats, rowType.fieldList=[homeRuns, playerStint, numberOfGames, ...])
 *     ...
 *       LogicalTableScan(table=baseballStats, rowType.fieldList=[homeRuns, playerStint, numberOfGames, ...])
 * </pre>
 *
 * <h3>Plan after</h3>
 * <pre>
 * LogicalAggregate(group={}, EXPR$0=COUNT())
 *   LogicalProject(exprs=[$27, $55])
 *     LogicalJoin(left=..., right=..., condition=EQ($27, $55))
 *     ...
 *       LogicalTableScan(table=baseballStats, rowType.fieldList=[homeRuns, playerStint, numberOfGames, ...])
 *     ...
 *       LogicalTableScan(table=baseballStats, rowType.fieldList=[homeRuns, playerStint, numberOfGames, ...])
 * </pre>
 */
public class PinotProjectNodeInsertRule extends RelOptRule {
  public static final PinotProjectNodeInsertRule INSTANCE =
      new PinotProjectNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotProjectNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalAggregate.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Aggregate) {
      Aggregate agg = call.rel(0);
      if (agg.getGroupCount() == 0) {
        RelNode relNode = getRelNode(agg);
        return relNode instanceof LogicalJoin;
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    LogicalJoin join = (LogicalJoin) getRelNode(oldAggRel);
    if (!(join.getCondition() instanceof RexCall)) {
      return;
    }
    // Create a list of projects from the join condition.
    List<RexNode> projects = new ArrayList<>(((RexCall) join.getCondition()).getOperands());
    RelRecordType relRecordType = (RelRecordType) join.getRowType();
    int[] fields = getProjectFields(projects);
    // Continue only if the join condition operands are RexInputRef
    if (fields == null) {
      return;
    }
    List<String> fieldNames = new ArrayList<>();
    for (int field : fields) {
      fieldNames.add(relRecordType.getFieldList().get(field).getName());
    }
    LogicalProject project = LogicalProject.create(
        join,
        ImmutableList.of(),  // no hints
        projects,
        fieldNames);
    Aggregate newAggregate = new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(),
        oldAggRel.getHints(), project, oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());
    call.transformTo(newAggregate);
  }

  private RelNode getRelNode(RelNode relNode) {
    return ((HepRelVertex) relNode.getInput(0)).getCurrentRel();
  }

  @Nullable
  private static int[] getProjectFields(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }
}
