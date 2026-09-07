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
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;


/// Supplies string and timestamp MODE calls with an inferred type argument.
/// This stateless rule keeps the reducer and type as projected literals so distributed stages retain both arguments.
public class PinotModeAggregationFunctionRewriteRule extends RelOptRule {
  public static PinotModeAggregationFunctionRewriteRule instanceWithDescription(String description) {
    return new PinotModeAggregationFunctionRewriteRule(description);
  }

  private PinotModeAggregationFunctionRewriteRule(String description) {
    super(operand(LogicalAggregate.class, any()), PinotRuleUtils.PINOT_REL_FACTORY, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    RelNode input = PinotRuleUtils.unboxRel(aggregate.getInput());
    List<RexNode> projects = new ArrayList<>();
    List<String> names = new ArrayList<>(input.getRowType().getFieldNames());
    if (input instanceof Project) {
      projects.addAll(((Project) input).getProjects());
    } else {
      for (int i = 0; i < names.size(); i++) {
        projects.add(RexInputRef.of(i, input.getRowType()));
      }
    }
    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    List<AggregateCall> originalCalls = aggregate.getAggCallList();
    List<AggregateCall> rewrittenCalls = new ArrayList<>(originalCalls.size());
    boolean changed = false;
    for (AggregateCall originalCall : originalCalls) {
      List<Integer> arguments = originalCall.getArgList();
      List<Integer> rewritten = arguments;
      if (originalCall.getAggregation().getKind() == SqlKind.MODE && !arguments.isEmpty() && arguments.size() < 3) {
        SqlTypeName operandType = input.getRowType().getFieldList().get(arguments.get(0)).getType().getSqlTypeName();
        String type = SqlTypeName.STRING_TYPES.contains(operandType)
            ? "STRING"
            : operandType == SqlTypeName.TIMESTAMP ? "TIMESTAMP" : null;
        if (type != null) {
          rewritten = new ArrayList<>(arguments);
          if (arguments.size() == 1) {
            rewritten.add(addLiteral(rexBuilder, projects, names, "MIN"));
          }
          rewritten.add(addLiteral(rexBuilder, projects, names, type));
          changed = true;
        }
      }
      rewrittenCalls.add(originalCall.withArgList(rewritten));
    }
    if (!changed) {
      return;
    }

    RelNode rewrittenInput;
    if (input instanceof Project) {
      // Extend the existing projection: wrapping it in identity refs would hide the original reducer literals.
      Project project = (Project) input;
      RelDataTypeFactory.Builder rowType = input.getCluster().getTypeFactory().builder();
      for (int i = 0; i < projects.size(); i++) {
        rowType.add(names.get(i), projects.get(i).getType());
      }
      rewrittenInput = project.copy(project.getTraitSet(), project.getInput(), projects, rowType.build());
    } else {
      rewrittenInput = LogicalProject.create(input, List.of(), projects, names);
    }
    call.transformTo(aggregate.copy(aggregate.getTraitSet(), rewrittenInput, aggregate.getGroupSet(),
        aggregate.getGroupSets(), rewrittenCalls));
  }

  private static int addLiteral(RexBuilder rexBuilder, List<RexNode> projects, List<String> names, String value) {
    RexNode literal = rexBuilder.makeLiteral(value);
    int index = projects.indexOf(literal);
    if (index < 0) {
      index = projects.size();
      projects.add(literal);
      names.add("$mode$" + index);
    }
    return index;
  }
}
