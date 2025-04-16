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
package org.apache.pinot.calcite.rel.traits;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Window;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalAggregate;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalJoin;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalProject;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalSort;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalTableScan;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalWindow;


/**
 * Assign trait constraints to the plan. The Physical Planner should ensure that these constraints are met by
 * inserting Exchange wherever required. This operates with Physical RelNodes because Calcite emits Logical RelNodes,
 * many of which drop traits on copy.
 */
public class TraitAssignment {
  private final Supplier<Integer> _planIdGenerator;

  private TraitAssignment(Supplier<Integer> planIdGenerator) {
    _planIdGenerator = planIdGenerator;
  }

  public static PRelNode assign(PRelNode pRelNode, PhysicalPlannerContext physicalPlannerContext) {
    TraitAssignment traitAssignment = new TraitAssignment(physicalPlannerContext.getNodeIdGenerator());
    return traitAssignment.assign(pRelNode);
  }

  @VisibleForTesting
  PRelNode assign(PRelNode pRelNode) {
    // Process inputs first.
    RelNode relNode = pRelNode.unwrap();
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : relNode.getInputs()) {
      newInputs.add(assign((PRelNode) input).unwrap());
    }
    relNode = relNode.copy(relNode.getTraitSet(), newInputs);
    // Process current relNode.
    if (relNode instanceof PhysicalSort) {
      return (PRelNode) assignSort((PhysicalSort) relNode);
    } else if (relNode instanceof PhysicalJoin) {
      return (PRelNode) assignJoin((PhysicalJoin) relNode);
    } else if (relNode instanceof PhysicalAggregate) {
      return (PRelNode) assignAggregate((PhysicalAggregate) relNode);
    } else if (relNode instanceof PhysicalWindow) {
      return (PRelNode) assignWindow((PhysicalWindow) relNode);
    }
    return (PRelNode) relNode;
  }

  /**
   * Sort is always computed by coalescing to a single stream. Hence, we add a SINGLETON trait to the sort input.
   */
  @VisibleForTesting
  RelNode assignSort(PhysicalSort sort) {
    RelNode input = sort.getInput();
    RelTraitSet newTraitSet = input.getTraitSet().plus(RelDistributions.SINGLETON);
    input = input.copy(newTraitSet, input.getInputs());
    return sort.copy(sort.getTraitSet(), ImmutableList.of(input));
  }

  /**
   * Handles lookup and dynamic filter for semi-join case separately.
   * <p>
   *   TODO(mse-physical): Support colocated join hint. See
   *   <a href="https://github.com/apache/pinot/issues/15455">F2</a>).
   *   <br />
   *   TODO(mse-physical): Instead of random exchange on the left, we should simply skip exchange.
   *     See <a href="https://github.com/apache/pinot/issues/15455">F3</a>.
   * </p>
   */
  @VisibleForTesting
  RelNode assignJoin(PhysicalJoin join) {
    // Case-1: Handle lookup joins.
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      return assignLookupJoin(join);
    }
    // Case-2: Handle dynamic filter for semi joins.
    JoinInfo joinInfo = join.analyzeCondition();
    if (join.isSemiJoin() && joinInfo.nonEquiConditions.isEmpty() && joinInfo.leftKeys.size() == 1) {
      if (PinotRuleUtils.canPushDynamicBroadcastToLeaf(join.getLeft())) {
        return assignDynamicFilterSemiJoin(join);
      }
    }
    // Case-3: Default case.
    RelDistribution leftDistribution = joinInfo.leftKeys.isEmpty() ? RelDistributions.RANDOM_DISTRIBUTED
        : RelDistributions.hash(joinInfo.leftKeys);
    RelDistribution rightDistribution = joinInfo.rightKeys.isEmpty() ? RelDistributions.BROADCAST_DISTRIBUTED
        : RelDistributions.hash(joinInfo.rightKeys);
    // left-input
    RelNode leftInput = join.getInput(0);
    RelTraitSet leftTraitSet = leftInput.getTraitSet().plus(leftDistribution);
    leftInput = leftInput.copy(leftTraitSet, leftInput.getInputs());
    // right-input
    RelNode rightInput = join.getInput(1);
    RelTraitSet rightTraitSet = rightInput.getTraitSet().plus(rightDistribution);
    rightInput = rightInput.copy(rightTraitSet, rightInput.getInputs());
    return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
  }

  /**
   * When group-by keys are empty, we can use SINGLETON distribution. Otherwise, we use hash distribution on the
   * group-by keys.
   */
  @VisibleForTesting
  RelNode assignAggregate(PhysicalAggregate aggregate) {
    RelNode input = aggregate.getInput(0);
    if (aggregate.getGroupCount() == 0) {
      RelTraitSet newTraitSet = input.getTraitSet().plus(RelDistributions.SINGLETON);
      input = input.copy(newTraitSet, input.getInputs());
    } else {
      RelTraitSet newTraitSet = input.getTraitSet().plus(RelDistributions.hash(aggregate.getGroupSet().asList()));
      input = input.copy(newTraitSet, input.getInputs());
    }
    return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
  }

  /**
   * Assigns traits to the input of window, accounting for partition-by and order-by clauses.
   */
  @VisibleForTesting
  RelNode assignWindow(PhysicalWindow window) {
    Preconditions.checkState(window.groups.size() <= 1,
        "Different partition-by clause not allowed in window functions yet");
    RelCollation windowGroupCollation = window.groups.isEmpty() ? RelCollations.EMPTY
        : window.groups.get(0).collation();
    RelNode input = window.getInput(0);
    if (window.groups.isEmpty() || window.groups.get(0).keys.isEmpty()) {
      // Case-1: No partition by clause in Window function.
      if (!windowGroupCollation.getKeys().isEmpty()) {
        // Push collation trait.
        if (input instanceof PhysicalSort) {
          // If input is sort with a different collation, add another sort.
          PhysicalSort sort = (PhysicalSort) input;
          if (!sort.getCollation().equals(windowGroupCollation)) {
            RelTraitSet traitSetOfNewSort = RelTraitSet.createEmpty().plus(windowGroupCollation)
                .plus(RelDistributions.SINGLETON);
            input = new PhysicalSort(sort.getCluster(), traitSetOfNewSort, List.of() /* hints */,
                windowGroupCollation, null /* offset */, null /* fetch */, sort, _planIdGenerator.get(),
                null /* pinot data distribution */, false /* leaf stage */);
          } else {
            input = input.copy(input.getTraitSet().plus(RelDistributions.SINGLETON), input.getInputs());
          }
        } else {
          RelTraitSet newTraitSet = input.getTraitSet().plus(RelDistributions.SINGLETON)
              .plus(windowGroupCollation);
          input = input.copy(newTraitSet, input.getInputs());
        }
      } else {
        input = input.copy(input.getTraitSet().plus(RelDistributions.SINGLETON), input.getInputs());
      }
    } else {
      // Case-2: Partition-by clause present in window.
      Window.Group group = window.groups.get(0);
      List<Integer> partitionKeys = group.keys.asList();
      RelDistribution newHashDistTrait = RelDistributions.hash(partitionKeys);
      if (!windowGroupCollation.getKeys().isEmpty()) {
        if (input instanceof PhysicalSort && !windowGroupCollation.equals(((PhysicalSort) input).getCollation())) {
          // If input is sort with a different collation, add another sort.
          PhysicalSort sort = (PhysicalSort) input;
          RelTraitSet traitSetOfNewSort = RelTraitSet.createEmpty().plus(windowGroupCollation)
              .plus(newHashDistTrait);
          input = new PhysicalSort(sort.getCluster(), traitSetOfNewSort, List.of() /* hints */,
              windowGroupCollation, null /* offset */, null /* fetch */, sort, _planIdGenerator.get(),
              null /* pinot data distribution */, false /* leaf stage */);
        } else {
          RelTraitSet newTraitSet = input.getTraitSet().plus(newHashDistTrait).plus(windowGroupCollation);
          input = input.copy(newTraitSet, input.getInputs());
        }
      } else {
        input = input.copy(input.getTraitSet().plus(newHashDistTrait), input.getInputs());
      }
    }
    return window.copy(window.getTraitSet(), ImmutableList.of(input));
  }

  private RelNode assignLookupJoin(PhysicalJoin join) {
    /*
     * Lookup join expects right input to have project and table-scan nodes exactly. Moreover, lookup join is used
     * with Dimension tables only. Given this, we expect the entire right input to be available in all workers
     * selected for the left input. For now, we will assign broadcast trait to the entire right input. Worker
     * assignment will have to handle this explicitly regardless.
     */
    RelNode leftInput = join.getInputs().get(0);
    RelNode rightInput = join.getInputs().get(1);
    Preconditions.checkState(rightInput instanceof PhysicalProject, "Expected project as right input of table scan");
    Preconditions.checkState(rightInput.getInput(0) instanceof PhysicalTableScan,
        "Expected table scan under project for right input of lookup join");
    PhysicalProject oldProject = (PhysicalProject) rightInput;
    PhysicalTableScan oldTableScan = (PhysicalTableScan) oldProject.getInput(0);
    PhysicalTableScan newTableScan =
        (PhysicalTableScan) oldTableScan.copy(oldTableScan.getTraitSet().plus(
            RelDistributions.BROADCAST_DISTRIBUTED), Collections.emptyList());
    PhysicalProject newProject =
        (PhysicalProject) oldProject.copy(oldProject.getTraitSet().plus(RelDistributions.BROADCAST_DISTRIBUTED),
            ImmutableList.of(newTableScan));
    return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, newProject));
  }

  private RelNode assignDynamicFilterSemiJoin(PhysicalJoin join) {
    /*
     * When dynamic broadcast is enabled, push broadcast trait to right input along with the pipeline breaker
     * trait. Use hash trait if a hint is given to indicate that the left-input is partitioned.
     */
    RelNode leftInput = join.getInput(0);
    RelNode rightInput = join.getInput(1);
    JoinInfo joinInfo = join.analyzeCondition();
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
