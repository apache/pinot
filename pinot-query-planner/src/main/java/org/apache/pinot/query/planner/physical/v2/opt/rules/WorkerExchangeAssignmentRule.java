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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.physical.v2.HashDistributionDesc;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.mapping.DistMappingGenerator;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalJoin;
import org.apache.pinot.query.planner.physical.v2.opt.PRelNodeTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <h1>Overview</h1>
 * Assigns workers to the plan tree. Leaf stage should already have workers assigned before this Rule is run.
 * This rule also adds Exchanges when either of the following conditions are met:
 * <ul>
 *   <li>When the data distribution does not match the trait constraints.</li>
 *   <li>When the data is not sorted by the required RelCollation.</li>
 *   <li>When the workers don't match. E.g. if the workers are same but in different orders, then we'll need to
 *   add an exchange</li>
 * </ul>
 * <h1>Features</h1>
 * <ul>
 *   <li>If data is partitioned in the same way across the same number of workers, then Identity Exchange would be
 *   used.</li>
 *   <li>Can simplify Exchanges for arbitrarily long plans. E.g. you can have any number of joins on the partitioning
 *   key, and this Rule would still be able to use Identity Exchange for the entire plan.</li>
 * </ul>
 */
public class WorkerExchangeAssignmentRule implements PRelNodeTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerExchangeAssignmentRule.class);
  private final PhysicalPlannerContext _physicalPlannerContext;
  private static final String DEFAULT_HASH_FUNCTION = KeySelector.DEFAULT_HASH_ALGORITHM;

  public WorkerExchangeAssignmentRule(PhysicalPlannerContext context) {
    _physicalPlannerContext = context;
  }

  @Override
  public PRelNode execute(PRelNode currentNode) {
    return executeInternal(currentNode, null);
  }

  public PRelNode executeInternal(PRelNode currentNode, @Nullable PRelNode parent) {
    if (currentNode.isLeafStage() && !isLeafStageBoundary(currentNode, parent)) {
      return currentNode;
    }
    if (currentNode.getPRelInputs().isEmpty()) {
      return processCurrentNode(currentNode, parent);
    }
    if (currentNode.getPRelInputs().size() == 1) {
      List<PRelNode> newInputs = List.of(executeInternal(currentNode.getPRelInput(0), currentNode));
      currentNode = currentNode.with(newInputs);
      return processCurrentNode(currentNode, parent);
    }
    // Process first input.
    List<PRelNode> newInputs = new ArrayList<>();
    newInputs.add(executeInternal(currentNode.getPRelInput(0), currentNode));
    newInputs.addAll(currentNode.getPRelInputs().subList(1, currentNode.getPRelInputs().size()));
    currentNode = currentNode.with(newInputs);
    // Process current node.
    currentNode = processCurrentNode(currentNode, parent);
    // Process remaining inputs.
    if (currentNode instanceof PhysicalExchange) {
      PhysicalExchange exchange = (PhysicalExchange) currentNode;
      currentNode = exchange.getPRelInput(0);
      for (int index = 1; index < currentNode.getPRelInputs().size(); index++) {
        newInputs.set(index, executeInternal(currentNode.getPRelInput(index), currentNode));
      }
      currentNode = currentNode.with(newInputs);
      currentNode = inheritDistDescFromInputs(currentNode);
      return exchange.with(List.of(currentNode));
    }
    for (int index = 1; index < currentNode.getPRelInputs().size(); index++) {
      newInputs.set(index, executeInternal(currentNode.getPRelInput(index), currentNode));
    }
    currentNode = currentNode.with(newInputs);
    currentNode = inheritDistDescFromInputs(currentNode);
    return currentNode;
  }

  PRelNode processCurrentNode(PRelNode currentNode, @Nullable PRelNode parentNode) {
    // Step-1: Initialize variables.
    boolean isLeafStageBoundary = isLeafStageBoundary(currentNode, parentNode);
    // Step-2: Get current node's distribution. If the current node already has a distribution attached, use that.
    //         Otherwise, compute it using DistMappingGenerator.
    PinotDataDistribution currentNodeDistribution = computeCurrentNodeDistribution(currentNode, parentNode);
    currentNode = currentNode.with(currentNode.getPRelInputs(), currentNodeDistribution);
    // Step-3: Add an optional exchange to meet unmet distribution trait constraint, if it exists. This also takes care
    //         of different workers when the parent already has workers assigned to it (when parent is not a SingleRel).
    PRelNode currentNodeExchange = meetDistributionConstraint(currentNode, currentNodeDistribution, parentNode);
    // Step-4: Meet ordering requirement on output streams.
    currentNodeExchange = meetCollationConstraint(currentNode, currentNodeExchange, currentNodeDistribution);
    if (currentNodeExchange != null) {
      // Update current node with its distribution, and update currentNodeExchange to point to the new current node.
      currentNode = currentNode.with(currentNode.getPRelInputs(), currentNodeDistribution);
      currentNodeExchange = currentNodeExchange.with(ImmutableList.of(currentNode),
          currentNodeExchange.getPinotDataDistributionOrThrow());
      return currentNodeExchange;
    }
    if (isLeafStageBoundary && parentNode != null) {
      currentNode = currentNode.with(currentNode.getPRelInputs(), currentNodeDistribution);
      // Update current node with its distribution, and since this is a leaf stage boundary, add an identity exchange.
      return new PhysicalExchange(nodeId(), currentNode,
          currentNode.getPinotDataDistribution(), Collections.emptyList(), ExchangeStrategy.IDENTITY_EXCHANGE,
          null, PinotExecStrategyTrait.getDefaultExecStrategy());
    }
    // When no exchange, simply update current node with the distribution.
    return currentNode.with(currentNode.getPRelInputs(), currentNodeDistribution);
  }

  PRelNode inheritDistDescFromInputs(PRelNode currentNode) {
    // Inherit distribution trait from inputs (except left-most input, which is already inherited).
    if (currentNode.getPRelInputs().size() <= 1
        || currentNode.getPinotDataDistributionOrThrow().getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
      return currentNode;
    }
    PinotDataDistribution currentDistribution = currentNode.getPinotDataDistributionOrThrow();
    Set<HashDistributionDesc> newDistributionSet =
        new HashSet<>(currentNode.getPinotDataDistributionOrThrow().getHashDistributionDesc());
    List<RelNode> leadingSiblings = new ArrayList<>();
    for (int inputIndex = 1; inputIndex < currentNode.getPRelInputs().size(); inputIndex++) {
      leadingSiblings.add(currentNode.unwrap().getInput(inputIndex - 1));
      PinotDataDistribution inputDistribution = currentNode.getPRelInput(inputIndex).getPinotDataDistributionOrThrow();
      if (inputDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
        PinotDataDistribution inheritedDist = inputDistribution.apply(DistMappingGenerator.compute(
            currentNode.unwrap().getInput(inputIndex), currentNode.unwrap(), leadingSiblings));
        if (inheritedDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
          newDistributionSet.addAll(inheritedDist.getHashDistributionDesc());
        }
      }
    }
    PinotDataDistribution finalDist = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
        currentDistribution.getWorkers(), currentDistribution.getWorkerHash(), newDistributionSet,
        currentDistribution.getCollation());
    return currentNode.with(currentNode.getPRelInputs(), finalDist);
  }

  @Nullable
  @VisibleForTesting
  PRelNode meetDistributionConstraint(PRelNode currentNode, PinotDataDistribution derivedDistribution,
      @Nullable PRelNode parent) {
    RelDistribution relDistribution = coalesceDistribution(currentNode.unwrap().getTraitSet().getDistribution());
    PinotDataDistribution parentDistribution = parent == null ? null : parent.getPinotDataDistribution();
    boolean isDistributionSatisfied = derivedDistribution.satisfies(relDistribution);
    boolean forcePartitioned = forcePartitioned(parent);
    PRelNode currentNodeExchange = null;
    if (forcePartitioned) {
      if (!isDistributionSatisfied) {
        LOGGER.warn("Forced partitioning info, even though inferred distribution does not satisfy traits");
      }
      if (parentDistribution != null && parentDistribution.getWorkerHash() != derivedDistribution.getWorkerHash()) {
        throw new IllegalStateException("Attempted to forcefully skip exchange even though workers different in "
            + "parent");
      }
    } else if (isDistributionSatisfied) {
      if (parentDistribution != null) {
        // currentNode is right sibling of another node, and since workers for the top-level node are already fixed,
        // we need to make sure that the current node's data-distribution aligns with that.
        // e.g. if parent is an inner-join with servers (S0, S1) with 16 partitions of data, then the right join must
        // also have the same servers and number of partitions, merely being hash-distributed is not enough.
        currentNodeExchange = meetParentEnforcedDistributionConstraint(currentNode, relDistribution, parent,
            derivedDistribution);
      }
    } else {
      if (parentDistribution == null) {
        currentNodeExchange = meetDistributionConstraintNoParent(currentNode, derivedDistribution,
            relDistribution);
      } else {
        currentNodeExchange = meetParentEnforcedDistributionConstraint(currentNode, relDistribution, parent,
            derivedDistribution);
      }
    }
    return currentNodeExchange;
  }

  @VisibleForTesting
  @Nullable
  PRelNode meetCollationConstraint(PRelNode currentNode, @Nullable PRelNode currentNodeExchange,
      PinotDataDistribution derivedDistribution) {
    RelCollation relCollation = coalesceCollation(currentNode.unwrap().getTraitSet().getCollation());
    if (currentNodeExchange == null) {
      if (!derivedDistribution.satisfies(relCollation)) {
        // TODO(mse-physical): We can simply use the Sort operator here. That would avoid creation of another plan
        //   fragment too.
        // Add new identity exchange for sort.
        PinotDataDistribution newDataDistribution = derivedDistribution.withCollation(relCollation);
        currentNodeExchange = new PhysicalExchange(nodeId(), currentNode,
            newDataDistribution, Collections.emptyList(), ExchangeStrategy.IDENTITY_EXCHANGE, relCollation,
            PinotExecStrategyTrait.getDefaultExecStrategy());
      }
    } else {
      if (!relCollation.getKeys().isEmpty()) {
        // Update existing exchange and add sort.
        PhysicalExchange oldExchange = (PhysicalExchange) currentNodeExchange.unwrap();
        PinotDataDistribution newDataDistribution = currentNodeExchange.getPinotDataDistributionOrThrow();
        currentNodeExchange = new PhysicalExchange(_physicalPlannerContext.getNodeIdGenerator().get(),
            oldExchange.getPRelInput(0), newDataDistribution.withCollation(relCollation),
            oldExchange.getDistributionKeys(), oldExchange.getExchangeStrategy(), relCollation,
            PinotExecStrategyTrait.getDefaultExecStrategy());
      }
    }
    return currentNodeExchange;
  }

  /**
   * There's no parent distribution and given distribution is not satisfied with default assignment.
   * <b>Assumption:</b> Since no parent distribution, implies current node is single child and hence workers will
   *   be same.
   */
  private PRelNode meetDistributionConstraintNoParent(PRelNode currentNode,
      PinotDataDistribution currentNodeDistribution, RelDistribution distributionConstraint) {
    Preconditions.checkState(!currentNodeDistribution.satisfies(distributionConstraint),
        "Method should only be called when constraint is not met");
    if (distributionConstraint.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(
          RelDistribution.Type.BROADCAST_DISTRIBUTED, currentNodeDistribution.getWorkers(),
          currentNodeDistribution.getWorkerHash(), null, null);
      return new PhysicalExchange(nodeId(), currentNode, pinotDataDistribution, List.of(),
          ExchangeStrategy.BROADCAST_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy());
    }
    if (distributionConstraint.getType() == RelDistribution.Type.SINGLETON) {
      List<String> newWorkers = currentNodeDistribution.getWorkers().subList(0, 1);
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(RelDistribution.Type.SINGLETON,
          newWorkers, newWorkers.hashCode(), null, null);
      return new PhysicalExchange(nodeId(), currentNode, pinotDataDistribution, List.of(),
          ExchangeStrategy.SINGLETON_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy());
    }
    if (distributionConstraint.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      HashDistributionDesc desc = new HashDistributionDesc(
          distributionConstraint.getKeys(), DEFAULT_HASH_FUNCTION, currentNodeDistribution.getWorkers().size());
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(
          RelDistribution.Type.HASH_DISTRIBUTED, currentNodeDistribution.getWorkers(),
          currentNodeDistribution.getWorkerHash(), ImmutableSet.of(desc), null);
      return new PhysicalExchange(nodeId(), currentNode, pinotDataDistribution, distributionConstraint.getKeys(),
          ExchangeStrategy.PARTITIONING_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy());
    }
    throw new IllegalStateException("Distribution constraint not met: " + distributionConstraint.getType());
  }

  @Nullable
  private PRelNode meetParentEnforcedDistributionConstraint(PRelNode currentNode, RelDistribution relDistribution,
      PRelNode parent, PinotDataDistribution assumedDistribution) {
    PinotDataDistribution parentDistribution = parent.getPinotDataDistributionOrThrow();
    boolean parentHasSameWorkers = parentDistribution.getWorkerHash() == assumedDistribution.getWorkerHash();
    if (parentDistribution.getWorkers().size() == 1) {
      relDistribution = RelDistributions.SINGLETON;
    }
    if (relDistribution.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED
        || relDistribution.getType() == RelDistribution.Type.ANY) {
      // Since distribution trait constraints are no-op, we just need to check workers.
      // TODO: Think if we need to treat random distribution as a constraint.
      if (parentHasSameWorkers) {
        return null;
      }
      // If parent has different workers, do a random exchange.
      // TODO: Can optimize this to reduce fan out?
      PinotDataDistribution newDistribution = new PinotDataDistribution(RelDistribution.Type.RANDOM_DISTRIBUTED,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null);
      return new PhysicalExchange(nodeId(), currentNode, newDistribution, List.of(), ExchangeStrategy.RANDOM_EXCHANGE,
          null, PinotExecStrategyTrait.getDefaultExecStrategy());
    } else if (relDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      if (assumedDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
        if (parentHasSameWorkers) {
          return null;
        }
        // TODO: Add broadcast to broadcast exchange.
        throw new IllegalStateException("Can't do broadcast to broadcast exchange yet");
      }
      PinotDataDistribution newDistribution = new PinotDataDistribution(RelDistribution.Type.BROADCAST_DISTRIBUTED,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null);
      return new PhysicalExchange(nodeId(), currentNode, newDistribution, List.of(),
          ExchangeStrategy.BROADCAST_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy());
    } else if (relDistribution.getType() == RelDistribution.Type.SINGLETON) {
      if (parentHasSameWorkers) {
        return null;
      }
      Preconditions.checkState(parentDistribution.getWorkers().size() == 1,
          "Singleton constraint but parent has %s workers", parentDistribution.getWorkers().size());
      PinotDataDistribution newDistribution = new PinotDataDistribution(RelDistribution.Type.SINGLETON,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null);
      return new PhysicalExchange(nodeId(), currentNode, newDistribution, List.of(),
          ExchangeStrategy.SINGLETON_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy());
    }
    Preconditions.checkState(relDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED,
        "Unexpected distribution constraint: %s", relDistribution.getType());
    Preconditions.checkState(parent instanceof PhysicalJoin, "Expected parent to be join. Found: %s", parent);
    PhysicalJoin parentJoin = (PhysicalJoin) parent;
    // TODO(mse-physical): add support for sub-partitioning and coalescing exchange.
    HashDistributionDesc hashDistToMatch = getLeftInputHashDistributionDesc(parentJoin).orElseThrow();
    if (assumedDistribution.satisfies(relDistribution)) {
      if (parentDistribution.getWorkers().size() == assumedDistribution.getWorkers().size()) {
        List<Integer> distKeys = relDistribution.getKeys();
        HashDistributionDesc currentNodeDesc = assumedDistribution.getHashDistributionDesc().stream().filter(
            desc -> desc.getKeys().equals(distKeys)).findFirst().orElseThrow();
        if (currentNodeDesc.getHashFunction().equals(hashDistToMatch.getHashFunction())) {
          boolean canSkipExchange = false;
          if (currentNodeDesc.getNumPartitions() == hashDistToMatch.getNumPartitions()) {
            canSkipExchange = true;
          } else if (complicatedButColocated(currentNodeDesc.getNumPartitions(), hashDistToMatch.getNumPartitions(),
              parentDistribution.getWorkers().size())) {
            canSkipExchange = true;
          }
          if (canSkipExchange && parentHasSameWorkers) {
            return null;
          } else if (canSkipExchange) {
            PinotDataDistribution newDistribution = new PinotDataDistribution(assumedDistribution.getType(),
                parentDistribution.getWorkers(), parentDistribution.getWorkerHash(),
                assumedDistribution.getHashDistributionDesc(), assumedDistribution.getCollation());
            return new PhysicalExchange(nodeId(), currentNode, newDistribution, List.of(),
                ExchangeStrategy.IDENTITY_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy());
          }
        }
      }
      // TODO: Add support for sub-partitioning or coalescing exchange here.
    }
    // Re-partition.
    int numberOfPartitions = hashDistToMatch.getNumPartitions();
    String hashFunction = hashDistToMatch.getHashFunction();
    HashDistributionDesc newDesc = new HashDistributionDesc(relDistribution.getKeys(), hashFunction,
        numberOfPartitions);
    PinotDataDistribution newDistribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
        parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), ImmutableSet.of(newDesc),
        null);
    return new PhysicalExchange(nodeId(), currentNode, newDistribution, relDistribution.getKeys(),
        ExchangeStrategy.PARTITIONING_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy());
  }

  private boolean complicatedButColocated(int partitionOne, int partitionTwo, int numStreams) {
    int minP = Math.min(partitionOne, partitionTwo);
    int maxP = Math.max(partitionOne, partitionTwo);
    return minP > 0 && maxP % minP == 0 && minP % numStreams == 0;
  }

  private int nodeId() {
    return _physicalPlannerContext.getNodeIdGenerator().get();
  }

  private Optional<HashDistributionDesc> getLeftInputHashDistributionDesc(PhysicalJoin join) {
    List<Integer> leftKeys = join.analyzeCondition().leftKeys;
    return join.getPRelInput(0).getPinotDataDistributionOrThrow().getHashDistributionDesc().stream()
        .filter(desc -> desc.getKeys().equals(leftKeys))
        .findFirst();
  }

  /**
   * Computes the PinotDataDistribution of the given node from the input node. This assumes that all traits of the
   * input node are already satisfied.
   */
  private static PinotDataDistribution computeCurrentNodeDistribution(PRelNode currentNode, @Nullable PRelNode parent) {
    if (currentNode.getPinotDataDistribution() != null) {
      Preconditions.checkState(isLeafStageBoundary(currentNode, parent),
          "current node should not have assigned data distribution unless it's a boundary");
      return currentNode.getPinotDataDistributionOrThrow();
    }
    PinotDataDistribution inputDistribution = currentNode.getPRelInput(0).getPinotDataDistributionOrThrow();
    return inputDistribution.apply(DistMappingGenerator.compute(
        currentNode.unwrap().getInput(0), currentNode.unwrap(), null));
  }

  private static boolean isLeafStageBoundary(PRelNode currentNode, @Nullable PRelNode parentNode) {
    if (currentNode.isLeafStage()) {
      return parentNode == null || !parentNode.isLeafStage();
    }
    return false;
  }

  private static boolean forcePartitioned(@Nullable PRelNode parent) {
    // TODO: Setup explicit metadata to force assume distribution constraint met.
    if (parent instanceof Aggregate) {
      Aggregate aggRel = (Aggregate) parent.unwrap();
      Map<String, String> hintOptions =
          PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      hintOptions = hintOptions == null ? Map.of() : hintOptions;
      boolean hasGroupBy = !aggRel.getGroupSet().isEmpty();
      return hasGroupBy && Boolean.parseBoolean(
          hintOptions.get(PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS));
    }
    return false;
  }

  private static RelDistribution coalesceDistribution(@Nullable RelDistribution distribution) {
    return distribution == null ? RelDistributions.ANY : distribution;
  }

  private static RelCollation coalesceCollation(@Nullable RelCollation collation) {
    return collation == null ? RelCollations.EMPTY : collation;
  }
}
