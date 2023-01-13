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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * <h3>Plan before</h3>
 * <pre>
 * LogicalAggregate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])
 *   LogicalJoin()
 *     ...
 *       LogicalTableScan(table=[[scott, EMP]])
 * </pre>
 *
 * <h3>Plan after</h3>
 * <pre>
 * LogicalAggregate()
 *   LogicalProject()
 *     LogicalJoin()
 *       ...
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
      return getRelNode(agg) instanceof Join;
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
    List<RexNode> projects = new ArrayList<>(((RexCall) join.getCondition()).getOperands());
    RelRecordType relRecordType = (RelRecordType) join.getRowType();
    int[] fields = getProjectFields(projects);
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
