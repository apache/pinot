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
package org.apache.pinot.query.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotLogicalTableScan;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;


public class TraitShuttle extends RelShuttleImpl {
  private final Map<String, String> _queryOptions;

  private TraitShuttle(Map<String, String> queryOptions) {
    _queryOptions = queryOptions;
  }

  public static TraitShuttle create(Map<String, String> queryOptions) {
    return new TraitShuttle(queryOptions);
  }

  @Override
  public RelNode visit(TableScan tableScan) {
    Preconditions.checkState(tableScan instanceof PinotLogicalTableScan,
        "Table scan should have been substituted with logical table scan");
    return tableScan;
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    sort = (LogicalSort) super.visit(sort);
    // Current behavior is to always converge to a single server for Sort, so add the SINGLETON trait.
    // Note: This is also required for correctness. In the future, it might make sense to add a hint to run local
    //   order-by for performance in exchange for strict correctness.
    RelNode newInput = sort.getInput();
    newInput = newInput.copy(newInput.getTraitSet().plus(RelDistributions.SINGLETON).plus(sort.collation),
        newInput.getInputs());
    return sort.copy(sort.getTraitSet(), ImmutableList.of(newInput));
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof LogicalWindow) {
      return visitWindow((LogicalWindow) other);
    } else if (other instanceof PinotLogicalAggregate) {
      return visitAggregate((PinotLogicalAggregate) other);
    }
    return super.visit(other);
  }

  @Override public RelNode visit(LogicalJoin join) {
    join = (LogicalJoin) super.visit(join);
    List<RelNode> newInputs = join.getInputs();
    RelNode leftInput = newInputs.get(0);
    RelNode rightInput = newInputs.get(1);
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      // Lookup join expects right input to have project and table-scan nodes exactly.
      // Add broadcast trait to both of them for correctness' sake. Worker assignment will have to handle this
      // explicitly in any case.
      Preconditions.checkState(rightInput instanceof LogicalProject, "Expected project as right input of table scan");
      Preconditions.checkState(rightInput.getInput(0) instanceof PinotLogicalTableScan,
          "Expected table scan under project for right input of lookup join");
      LogicalProject oldProject = (LogicalProject) rightInput;
      PinotLogicalTableScan oldTableScan = (PinotLogicalTableScan) oldProject.getInput(0);
      PinotLogicalTableScan newTableScan =
          (PinotLogicalTableScan) oldTableScan.copy(oldTableScan.getTraitSet().plus(
              RelDistributions.BROADCAST_DISTRIBUTED), Collections.emptyList());
      LogicalProject newProject =
          (LogicalProject) oldProject.copy(oldProject.getTraitSet().plus(RelDistributions.BROADCAST_DISTRIBUTED),
              ImmutableList.of(newTableScan));
      return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, newProject));
    }
    JoinInfo joinInfo = join.analyzeCondition();
    if (join.isSemiJoin() && !_queryOptions.getOrDefault("skipDynamicFilter", "false").equals("true")) {
      if (joinInfo.nonEquiConditions.isEmpty() && joinInfo.leftKeys.size() == 1) {
        if (PinotRuleUtils.canPushDynamicBroadcastToLeaf(join.getLeft())) {
          /*
           * When dynamic broadcast is enabled, push broadcast trait to right input along with the pipeline breaker
           * trait. Use hash trait if a hint is given to indicate that left-input is partitioned.
           */
          Preconditions.checkState(rightInput.getTraitSet().getDistribution() == null,
              "Found existing dist trait on right input of semi-join");
          RelDistribution distribution = RelDistributions.BROADCAST_DISTRIBUTED;
          if (Boolean.TRUE.equals(PinotHintOptions.JoinHintOptions.isColocatedByJoinKeys(join))) {
            distribution = RelDistributions.hash(joinInfo.rightKeys);
          }
          RelTraitSet rightTraitSet = rightInput.getTraitSet().plus(distribution)
              .plus(PinotExecStrategyTrait.PIPELINE_BREAKER);
          rightInput = rightInput.copy(rightTraitSet, rightInput.getInputs());
          return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
        }
      }
    }
    // TODO: only checking rightKeys.isEmpty(). should check left keys too?
    if (!joinInfo.isEqui() || joinInfo.rightKeys.isEmpty()) {
      rightInput = rightInput.copy(rightInput.getTraitSet().plus(RelDistributions.BROADCAST_DISTRIBUTED),
          rightInput.getInputs());
      return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
    }
    // TODO: Support explicit left and right exchange strategies.
    List<Integer> leftKeys = joinInfo.leftKeys;
    List<Integer> rightKeys = joinInfo.rightKeys;
    if (rightInput.getTraitSet().getDistribution() != null) {
      // if right input has a broadcast trait, no trait required on left input.
      RelDistribution rightDistribution = Objects.requireNonNull(rightInput.getTraitSet().getDistribution());
      if (rightDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
        return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
      }
      throw new IllegalStateException("Unexpected distribution trait on right input of join: "
          + rightDistribution.getType());
    }
    Preconditions.checkState(leftInput.getTraitSet().getDistribution() == null,
        "Found distribution trait on left input of join");
    // TODO: Allow default join strategy to be configured.
    leftInput = leftInput.copy(leftInput.getTraitSet().plus(RelDistributions.hash(leftKeys)), leftInput.getInputs());
    rightInput = rightInput.copy(rightInput.getTraitSet().plus(RelDistributions.hash(rightKeys)),
        rightInput.getInputs());
    return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
  }

  private RelNode visitAggregate(PinotLogicalAggregate aggregate) {
    Preconditions.checkState(aggregate.getInput(0).getTraitSet().getDistribution() == null,
        "aggregate input already has distribution trait");
    aggregate = (PinotLogicalAggregate) super.visit(aggregate);
    RelNode newInput = aggregate.getInput(0);
    if (aggregate.getGroupCount() == 0) {
      newInput = newInput.copy(newInput.getTraitSet().plus(RelDistributions.SINGLETON), newInput.getInputs());
    } else {
      newInput = newInput.copy(newInput.getTraitSet().plus(RelDistributions.hash(aggregate.getGroupSet().asList())),
          newInput.getInputs());
    }
    return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(newInput));
  }

  private RelNode visitWindow(LogicalWindow window) {
    Preconditions.checkState(window.groups.size() <= 1,
        "Different partition-by clause not allowed in window function yet");
    window = (LogicalWindow) super.visit(window);
    RelCollation windowGroupCollation = getCollation(window);
    RelNode newInput = window.getInput(0);
    if (window.groups.isEmpty() || window.groups.get(0).keys.isEmpty()) {
      RelTraitSet newTraitSet = newInput.getTraitSet().plus(RelDistributions.SINGLETON);
      if (!windowGroupCollation.getKeys().isEmpty()) {
        if (newInput instanceof Sort) {
          Sort sortInput = (Sort) newInput;
          if (!sortInput.getCollation().equals(windowGroupCollation)) {
            newInput = LogicalSort.create(sortInput, windowGroupCollation, null, null);
            newTraitSet = newInput.getTraitSet().plus(RelDistributions.SINGLETON);
          }
        } else {
          newTraitSet = newTraitSet.plus(windowGroupCollation);
        }
      }
      newInput = newInput.copy(newTraitSet, newInput.getInputs());
    } else {
      Window.Group group = window.groups.get(0);
      List<Integer> partitionKeys = group.keys.asList();
      if (newInput instanceof LogicalSort) {
        LogicalSort inputSort = (LogicalSort) newInput;
        LogicalSort newSort = LogicalSort.create(inputSort, windowGroupCollation, null, null);
        newSort = (LogicalSort) newSort.copy(newSort.getTraitSet().plus(RelDistributions.hash(partitionKeys)).plus(
            newSort.collation), newSort.getInputs());
        newInput = newSort;
      } else {
        RelTraitSet newTraitSet = newInput.getTraitSet().plus(RelDistributions.hash(partitionKeys));
        if (!windowGroupCollation.getKeys().isEmpty()) {
          newTraitSet = newTraitSet.plus(windowGroupCollation);
        }
        newInput = newInput.copy(newTraitSet, newInput.getInputs());
      }
    }
    return window.copy(window.getTraitSet(), ImmutableList.of(newInput));
  }

  private RelCollation getCollation(LogicalWindow window) {
    return window.groups.isEmpty() ? RelCollations.EMPTY : window.groups.get(0).collation();
  }

  private List<Integer> getOrderKeys(LogicalWindow window) {
    return window.groups.isEmpty() ? Collections.emptyList() : window.groups.get(0).orderKeys.getKeys();
  }
}
