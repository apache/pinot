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
package org.apache.pinot.query.planner.physical.v2.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.pinot.calcite.rel.ExchangeStrategy;
import org.apache.pinot.calcite.rel.HashDistributionDesc;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.logical.PinotPhysicalExchange;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.MappingGen;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Does the following:
 * 1. Assigns workers and adds exchanges. (done)
 * 2. If is_partitioned_by_group_by_keys present and aggregation is above leaf stage, makes aggregation part of leaf
 *    stage.
 */
public class WorkerExchangeAssignmentRule extends PRelOptRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerExchangeAssignmentRule.class);
  private final PhysicalPlannerContext _physicalPlannerContext;

  public WorkerExchangeAssignmentRule(PhysicalPlannerContext context) {
    _physicalPlannerContext = context;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PRelNode currentNode = call._currentNode;
    PinotDataDistribution parentDistribution = getParentDistribution(call);
    boolean isLeafStageBoundary = isLeafStageBoundary(call);
    RelDistribution relDistribution = coalesceDistribution(currentNode.getRelNode().getTraitSet().getDistribution());
    RelCollation relCollation = coalesceCollation(currentNode.getRelNode().getTraitSet().getCollation());
    PinotDataDistribution derivedDistribution = getAssumedDistribution(call);
    if (call._currentNode.isLeafStage() && !isLeafStageBoundary) {
      /* Preconditions.checkState(derivedDistribution.satisfies(relDistribution),
          "Leaf stage distribution (non-boundary) should satisfy dist constraint: %s",
          currentNode.getRelNode().explain()); */
      Preconditions.checkState(derivedDistribution.satisfies(relCollation),
          "Leaf stage distribution (non-boundary) should satisfy collation constraint: %s",
          currentNode.getRelNode().explain());
      return call._currentNode.withPinotDataDistribution(derivedDistribution);
    }
    boolean isDistributionSatisfied = derivedDistribution.satisfies(relDistribution);
    boolean forcePartitioned = forcePartitioned(call);
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
        currentNodeExchange = meetParentEnforcedDistributionConstraintNew(currentNode, relDistribution,
            parentDistribution, derivedDistribution);
      }
    } else {
      if (parentDistribution == null) {
        currentNodeExchange = meetDistributionConstraint(currentNode, derivedDistribution,
            relDistribution);
      } else {
        currentNodeExchange = meetParentEnforcedDistributionConstraintNew(currentNode, relDistribution,
            parentDistribution, derivedDistribution);
      }
    }
    // Step-4: Meet ordering requirement on output streams.
    if (currentNodeExchange == null) {
      if (!derivedDistribution.satisfies(relCollation)) {
        // Add new identity exchange for sort.
        PinotPhysicalExchange newExchange = new PinotPhysicalExchange(currentNode.getInput(0).getRelNode(),
            Collections.emptyList(), ExchangeStrategy.IDENTITY_EXCHANGE, relCollation);
        PinotDataDistribution newDataDistribution = derivedDistribution.withCollation(relCollation);
        currentNodeExchange = new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), newExchange,
            newDataDistribution);
      }
    } else {
      if (!relCollation.getKeys().isEmpty()) {
        // Update existing exchange and add sort.
        PinotPhysicalExchange oldExchange = (PinotPhysicalExchange) currentNodeExchange.getRelNode();
        PinotPhysicalExchange newExchange = new PinotPhysicalExchange(oldExchange.getInput(), oldExchange.getKeys(),
            oldExchange.getExchangeStrategy(), relCollation);
        PinotDataDistribution newDataDistribution = currentNodeExchange.getPinotDataDistributionOrThrow();
        currentNodeExchange = new PRelNode(currentNodeExchange.getNodeId(), newExchange,
            newDataDistribution.withCollation(relCollation));
      }
    }
    if (currentNodeExchange != null) {
      PRelNode currentNodeWithNewInputs = currentNode.withNewInputs(currentNode.getNodeId(), currentNode.getInputs(),
          derivedDistribution);
      currentNodeExchange = currentNodeExchange.withNewInputs(
          currentNodeExchange.getNodeId(), ImmutableList.of(currentNodeWithNewInputs),
          currentNodeExchange.getPinotDataDistributionOrThrow());
      return currentNodeExchange;
    }
    if (isLeafStageBoundary) {
      currentNode = currentNode.withNewInputs(currentNode.getNodeId(), currentNode.getInputs(), derivedDistribution);
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), ExchangeStrategy.IDENTITY_EXCHANGE);
      return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), physicalExchange,
          currentNode.getPinotDataDistributionOrThrow(), ImmutableList.of(currentNode));
    }
    return currentNode.withPinotDataDistribution(derivedDistribution);
  }

  @Override
  public PRelNode onDone(PRelNode currentNode) {
    // Inherit distribution trait from inputs (except left-most input, which is already inherited).
    if (currentNode.getInputs().size() <= 1
        || currentNode.getPinotDataDistributionOrThrow().getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
      return currentNode;
    }
    PinotDataDistribution currentDistribution = currentNode.getPinotDataDistributionOrThrow();
    Set<HashDistributionDesc> newDistributionSet =
        new HashSet<>(currentNode.getPinotDataDistributionOrThrow().getHashDistributionDesc());
    List<RelNode> leadingSiblings = new ArrayList<>();
    for (int inputIndex = 1; inputIndex < currentNode.getInputs().size(); inputIndex++) {
      leadingSiblings.add(currentNode.getRelNode().getInput(inputIndex - 1));
      PinotDataDistribution inputDistribution = currentNode.getInput(inputIndex).getPinotDataDistributionOrThrow();
      if (inputDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
        PinotDataDistribution inheritedDist = inputDistribution.apply(MappingGen.compute(
            currentNode.getRelNode().getInput(inputIndex), currentNode.getRelNode(), leadingSiblings));
        if (inheritedDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
          newDistributionSet.addAll(inheritedDist.getHashDistributionDesc());
        }
      }
    }
    PinotDataDistribution finalDist = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
        currentDistribution.getWorkers(), currentDistribution.getWorkerHash(), newDistributionSet,
        currentDistribution.getCollation());
    return currentNode.withPinotDataDistribution(finalDist);
  }

  @Nullable
  private static PinotDataDistribution getParentDistribution(PRelOptRuleCall call) {
    if (!call._parents.isEmpty() && call._parents.getLast().hasPinotDataDistribution()) {
      return call._parents.getLast().getPinotDataDistributionOrThrow();
    }
    return null;
  }

  private static boolean isLeafStageBoundary(PRelOptRuleCall call) {
    if (call._parents.isEmpty()) {
      return call._currentNode.isLeafStage();
    }
    return call._currentNode.isLeafStage() && !call._parents.getLast().isLeafStage();
  }

  private static PinotDataDistribution getAssumedDistribution(PRelOptRuleCall call) {
    PRelNode currentNode = call._currentNode;
    if (currentNode.hasPinotDataDistribution()) {
      return currentNode.getPinotDataDistributionOrThrow();
    }
    PinotDataDistribution inputDistribution = call._currentNode.getInput(0).getPinotDataDistributionOrThrow();
    return inputDistribution.apply(MappingGen.compute(
        currentNode.getRelNode().getInput(0), currentNode.getRelNode(), null));
  }

  private static boolean forcePartitioned(PRelOptRuleCall call) {
    if (!call._parents.isEmpty() && call._parents.getLast().getRelNode() instanceof Aggregate) {
      Aggregate aggRel = (Aggregate) call._parents.getLast().getRelNode();
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

  /**
   * There's no parent distribution and given distribution is not satisfied with default assignment.
   * <b>Assumption:</b> Since no parent distribution, implies current node is single child and hence workers will
   *   be same.
   * TODO: With parallelism, that constraint should also be pushed down from parent to input.
   */
  private PRelNode meetDistributionConstraint(PRelNode currentNode, PinotDataDistribution currentNodeDistribution,
      RelDistribution distributionConstraint) {
    Preconditions.checkState(!currentNodeDistribution.satisfies(distributionConstraint),
        "Method should only be called when constraint is not met");
    if (distributionConstraint.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      PinotPhysicalExchange physicalExchange = PinotPhysicalExchange.broadcast(currentNode.getRelNode());
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(
          RelDistribution.Type.BROADCAST_DISTRIBUTED, currentNodeDistribution.getWorkers(),
          currentNodeDistribution.getWorkerHash(), null, null);
      return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), physicalExchange, pinotDataDistribution);
    }
    if (distributionConstraint.getType() == RelDistribution.Type.SINGLETON) {
      PinotPhysicalExchange physicalExchange = PinotPhysicalExchange.singleton(currentNode.getRelNode());
      List<String> newWorkers = currentNodeDistribution.getWorkers().subList(0, 1);
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(RelDistribution.Type.SINGLETON,
          newWorkers, newWorkers.hashCode(), null, null);
      return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), physicalExchange, pinotDataDistribution);
    }
    if (distributionConstraint.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          distributionConstraint.getKeys(), ExchangeStrategy.PARTITIONING_EXCHANGE);
      HashDistributionDesc desc = new HashDistributionDesc(
          distributionConstraint.getKeys(), "murmur", currentNodeDistribution.getWorkers().size());
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(
          RelDistribution.Type.HASH_DISTRIBUTED, currentNodeDistribution.getWorkers(),
          currentNodeDistribution.getWorkerHash(), ImmutableSet.of(desc), null);
      return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), physicalExchange, pinotDataDistribution);
    }
    throw new IllegalStateException("Distribution constraint not met: " + distributionConstraint.getType());
  }

  @Nullable
  private PRelNode meetParentEnforcedDistributionConstraintNew(PRelNode currentNode, RelDistribution relDistribution,
      PinotDataDistribution parentDistribution, PinotDataDistribution assumedDistribution) {
    if (relDistribution.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED
        || relDistribution.getType() == RelDistribution.Type.ANY) {
      // Only need to use the parent's workers.
      if (parentDistribution.getWorkerHash() == assumedDistribution.getWorkerHash()) {
        return null;
      }
      // TODO: Can optimize this to reduce fanout by random sub-partitioning.
      List<Integer> distributionKeys = ImmutableList.of(0);
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          distributionKeys, ExchangeStrategy.PARTITIONING_EXCHANGE);
      PinotDataDistribution newDistribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(),
          ImmutableSet.of(new HashDistributionDesc(distributionKeys, "murmur",
              parentDistribution.getWorkers().size())), null);
      return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), physicalExchange, newDistribution);
    } else if (relDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      if (assumedDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
        if (assumedDistribution.getWorkerHash() == parentDistribution.getWorkerHash()) {
          // If workers are same and broadcast already, nothing to do.
          return null;
        }
        // TODO: Add broadcast to broadcast exchange.
        throw new IllegalStateException("Can't do broadcast to broadcast yet");
      }
      if (assumedDistribution.getType() == RelDistribution.Type.SINGLETON) {
        if (assumedDistribution.getWorkerHash() == parentDistribution.getWorkerHash()) {
          // If single worker and workers are same, no exchange necessary.
          return null;
        }
      }
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), ExchangeStrategy.BROADCAST_EXCHANGE);
      PinotDataDistribution newDistribution = new PinotDataDistribution(RelDistribution.Type.BROADCAST_DISTRIBUTED,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null);
      return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), physicalExchange, newDistribution);
    } else if (relDistribution.getType() == RelDistribution.Type.SINGLETON) {
      if (assumedDistribution.getWorkerHash() == parentDistribution.getWorkerHash()) {
        return null;
      }
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), ExchangeStrategy.SINGLETON_EXCHANGE);
      PinotDataDistribution newDistribution = new PinotDataDistribution(RelDistribution.Type.SINGLETON,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null);
      return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), physicalExchange, newDistribution);
    }
    Preconditions.checkState(relDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED);
    if (parentDistribution.getWorkerHash() == assumedDistribution.getWorkerHash()) {
      if (assumedDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
        if (parentDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
          // If parent is also hash distributed already, then we need to match partition count and hash function.
          HashDistributionDesc parentDesc = parentDistribution.getHashDistributionDesc()
              .iterator().next();
          HashDistributionDesc currentDesc = assumedDistribution.getHashDistributionDesc()
              .iterator().next();
          int parentNumPartitions = parentDesc.getNumPartitions();
          String parentHash = parentDesc.getHashFunction();
          int currentNumPartitions = currentDesc.getNumPartitions();
          String currentHash = currentDesc.getHashFunction();
          if (parentNumPartitions == currentNumPartitions && parentHash.equals(currentHash)) {
            return null;
          }
        } else {
          return null;
        }
      }
    }
    // Re-partition.
    // TODO: Can do 1:1 mapping still if number of partitions, number of workers and hash function are same.
    // TODO: Can do a lot more here.
    int numberOfPartitions = parentDistribution.getWorkers().size();
    if (parentDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      numberOfPartitions = parentDistribution.getHashDistributionDesc().iterator().next().getNumPartitions();
    }
    PinotPhysicalExchange pinotPhysicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
        relDistribution.getKeys(), ExchangeStrategy.PARTITIONING_EXCHANGE, null);
    HashDistributionDesc newDesc = new HashDistributionDesc(
        relDistribution.getKeys(), "murmur", numberOfPartitions);
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
        parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), ImmutableSet.of(newDesc), null);
    return new PRelNode(_physicalPlannerContext.getNodeIdGenerator().get(), pinotPhysicalExchange,
        pinotDataDistribution);
  }
}
