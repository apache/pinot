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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;


/**
 * Optimizes queries with common expressions between project and filter operations.
 * Computes shared expressions once instead of duplicating computation.
 */
public class PinotProjectFilterTransposeRule extends RelOptRule {
  public static final PinotProjectFilterTransposeRule INSTANCE =
      new PinotProjectFilterTransposeRule("PinotProjectFilterTranspose");

  public static PinotProjectFilterTransposeRule instanceWithDescription(String description) {
    return new PinotProjectFilterTransposeRule(description);
  }

  private PinotProjectFilterTransposeRule(String description) {
    super(operand(LogicalProject.class, operand(LogicalFilter.class, any())), description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    LogicalFilter filter = call.rel(1);
    RelBuilder relBuilder = call.builder();
    RexBuilder rexBuilder = relBuilder.getRexBuilder();

    // Find common expressions between project and filter
    Set<RexNode> commonExprs = findCommonExpressions(project.getProjects(), filter.getCondition());
    if (commonExprs.isEmpty()) {
      return;
    }

    List<RexNode> commonExprList = new ArrayList<>(commonExprs);

    // Collect columns needed by final project (excluding common expressions)
    Set<Integer> neededColumns = new HashSet<>();
    for (RexNode projectExpr : project.getProjects()) {
      if (!commonExprs.contains(projectExpr)) {
        collectInputRefs(projectExpr, neededColumns);
      }
    }

    // Build intermediate projection: needed columns + common expressions
    List<RexNode> intermediateProjects = new ArrayList<>();
    List<String> intermediateNames = new ArrayList<>();
    Map<Integer, Integer> oldToNewColumnMapping = new HashMap<>();

    RelNode tableInput = filter.getInput();
    for (int oldIndex : neededColumns) {
      int newIndex = intermediateProjects.size();
      intermediateProjects.add(rexBuilder.makeInputRef(tableInput, oldIndex));
      intermediateNames.add(tableInput.getRowType().getFieldNames().get(oldIndex));
      oldToNewColumnMapping.put(oldIndex, newIndex);
    }

    Map<RexNode, Integer> commonExprToIndex = new HashMap<>();
    for (RexNode commonExpr : commonExprList) {
      int index = intermediateProjects.size();
      intermediateProjects.add(commonExpr);
      intermediateNames.add("commonExpr" + (index - neededColumns.size()));
      commonExprToIndex.put(commonExpr, index);
    }

    relBuilder.push(filter.getInput());
    relBuilder.project(intermediateProjects, intermediateNames);
    RelNode newInput = relBuilder.build();

    // Prepare replacement references for common expressions
    List<RexNode> commonExprReplacements = new ArrayList<>();
    for (RexNode commonExpr : commonExprList) {
      int index = commonExprToIndex.get(commonExpr);
      RexNode replacement = rexBuilder.makeInputRef(newInput, index);
      commonExprReplacements.add(replacement);
    }

    // Replace common expressions in filter and project
    RexNode newCondition =
        replaceExpressions(filter.getCondition(), commonExprList, commonExprReplacements, oldToNewColumnMapping);

    List<RexNode> newProjects = project.getProjects().stream()
        .map(expr -> replaceExpressions(expr, commonExprList, commonExprReplacements, oldToNewColumnMapping))
        .collect(Collectors.toList());

    // Build optimized plan
    LogicalFilter newFilter = filter.copy(filter.getTraitSet(), newInput, newCondition);
    LogicalProject newProject = project.copy(project.getTraitSet(), newFilter, newProjects, project.getRowType());
    call.transformTo(newProject);
  }

  private Set<RexNode> findCommonExpressions(List<RexNode> projectExprs, RexNode filterCondition) {
    Set<RexNode> common = new HashSet<>();
    Set<RexNode> filterExprs = new HashSet<>();
    collectAllExpressions(filterCondition, filterExprs);

    for (RexNode pExpr : projectExprs) {
      for (RexNode fExpr : filterExprs) {
        if (pExpr instanceof RexCall && areEqual(pExpr, fExpr)) {
          common.add(pExpr);
        }
      }
    }
    return common;
  }

  private void collectAllExpressions(RexNode node, Set<RexNode> expressions) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      expressions.add(call);
      for (RexNode operand : call.getOperands()) {
        collectAllExpressions(operand, expressions);
      }
    } else {
      expressions.add(node);
    }
  }

  private void collectInputRefs(RexNode expr, Set<Integer> columnRefs) {
    expr.accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        columnRefs.add(inputRef.getIndex());
        return inputRef;
      }
    });
  }

  private RexNode replaceExpressions(RexNode expr, List<RexNode> oldExprs, List<RexNode> newExprs,
      Map<Integer, Integer> columnMapping) {
    return expr.accept(new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        for (int i = 0; i < oldExprs.size(); i++) {
          if (areEqual(oldExprs.get(i), call)) {
            return newExprs.get(i);
          }
        }
        return (RexCall) super.visitCall(call);
      }

      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        for (int i = 0; i < oldExprs.size(); i++) {
          if (RexUtil.eq(oldExprs.get(i), inputRef)) {
            return newExprs.get(i);
          }
        }
        Integer newIndex = columnMapping.get(inputRef.getIndex());
        if (newIndex != null) {
          return new RexInputRef(newIndex, inputRef.getType());
        }
        return inputRef;
      }

      @Override
      public RexNode visitLiteral(RexLiteral literal) {
        for (int i = 0; i < oldExprs.size(); i++) {
          if (RexUtil.eq(oldExprs.get(i), literal)) {
            return newExprs.get(i);
          }
        }
        return literal;
      }
    });
  }

  private boolean areEqual(RexNode node1, RexNode node2) {
    return RexUtil.eq(node1, node2);
  }
}


