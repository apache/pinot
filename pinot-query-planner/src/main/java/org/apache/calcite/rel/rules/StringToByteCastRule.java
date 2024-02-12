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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.PinotSqlTransformFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;



/**
 * A transformation rule that looks for castings from String to Bytes and substitute them with {@code hexToByte()}
 * calls. This is needed because Calcite and Pinot conversion semantic is different and although most of the time the
 * conversion is done by Pinot, at simplification time Calcite may try to apply its own conversion if the converted
 * value is a literal.
 */
// TODO: Verify that all other conversions are compatible
public abstract class StringToByteCastRule extends RelOptRule implements TransformationRule {

  private static final PinotSqlTransformFunction HEX_TO_BYTES =
      new PinotSqlTransformFunction("hexToBytes", SqlKind.OTHER_FUNCTION, null, null, null,
          SqlFunctionCategory.STRING);

  public StringToByteCastRule(Class<? extends AbstractRelNode> relNode) {
    super(operand(relNode, any()), "StringToBinary");
  }

  public abstract void onMatch(RelOptRuleCall call);

  protected RexShuttle getRexShuttle(RexBuilder rexBuilder) {
    return new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        RexNode rexNode = super.visitCall(call);

        if (!call.getOperator().getKind().equals(SqlKind.CAST)) {
          return rexNode;
        }
        String returnTypeName = call.getType().getSqlTypeName().getName();
        if (!returnTypeName.equalsIgnoreCase("VARBINARY") && !returnTypeName.equalsIgnoreCase("BINARY")) {
          return rexNode;
        }
        RexNode operand = call.getOperands().get(0);

        RelDataType sourceType = operand.getType();
        SqlTypeName sqlSourceType = sourceType.getSqlTypeName();
        String sourceTypeName = sqlSourceType.getName();
        if (!sourceTypeName.equalsIgnoreCase("CHAR") && !sourceTypeName.equalsIgnoreCase("VARCHAR")) {
          return rexNode;
        }

        return rexBuilder.makeCall(call.getType(), HEX_TO_BYTES, Collections.singletonList(operand));
      }
    };
  }

  public static class OnProject extends StringToByteCastRule {

    public static final StringToByteCastRule.OnProject INSTANCE = new StringToByteCastRule.OnProject();

    public OnProject() {
      super(LogicalProject.class);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Project project = call.rel(0);

      RelOptCluster cluster = project.getCluster();

      RexShuttle rexShuttle = getRexShuttle(cluster.getRexBuilder());
      List<RexNode> newProjects = project.getProjects().stream()
          .map(rexNode -> rexNode.accept(rexShuttle))
          .collect(Collectors.toList());

      Project newProject = project.copy(project.getTraitSet(), project.getInput(), newProjects, project.getRowType());
      call.transformTo(newProject);
    }
  }

  public static class OnFilter extends StringToByteCastRule {

    public static final StringToByteCastRule.OnFilter INSTANCE = new StringToByteCastRule.OnFilter();

    public OnFilter() {
      super(LogicalFilter.class);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);

      RelOptCluster cluster = filter.getCluster();

      RexNode newCondition = filter.getCondition().accept(getRexShuttle(cluster.getRexBuilder()));

      Filter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
      call.transformTo(newFilter);
    }
  }

  public static class OnJoin extends StringToByteCastRule {

    public static final StringToByteCastRule.OnJoin INSTANCE = new StringToByteCastRule.OnJoin();

    public OnJoin() {
      super(LogicalJoin.class);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalJoin join = call.rel(0);

      RelOptCluster cluster = join.getCluster();

      RexNode newCondition = join.getCondition().accept(getRexShuttle(cluster.getRexBuilder()));

      Join newJoin = join.copy(join.getTraitSet(), newCondition, join.getLeft(), join.getRight(), join.getJoinType(),
          join.isSemiJoinDone());
      call.transformTo(newJoin);
    }
  }
}
