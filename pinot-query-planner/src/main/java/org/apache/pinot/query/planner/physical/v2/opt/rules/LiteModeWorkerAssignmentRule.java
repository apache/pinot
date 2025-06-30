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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Sort;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.mapping.DistMappingGenerator;
import org.apache.pinot.query.planner.physical.v2.mapping.PinotDistMapping;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalSort;
import org.apache.pinot.query.planner.physical.v2.opt.PRelNodeTransformer;


/**
 * Lite mode uses a single worker for all stages except the leaf stage. Since leaf stage assignment is done before this
 * rule is called, we simply need to sample a random worker from the leaf stage, and assign it to all the non-leaf
 * plan nodes.
 */
public class LiteModeWorkerAssignmentRule implements PRelNodeTransformer {
  private static final Random RANDOM = new Random();
  private final PhysicalPlannerContext _context;
  private final boolean _runInBroker;

  public LiteModeWorkerAssignmentRule(PhysicalPlannerContext context) {
    _context = context;
    _runInBroker = QueryOptionsUtils.isRunInBroker(context.getQueryOptions());
  }

  @Override
  public PRelNode execute(PRelNode currentNode) {
    Set<String> workerSet = new HashSet<>();
    List<String> workers;
    if (_runInBroker) {
      workers = List.of(String.format("0@%s", _context.getInstanceId()));
    } else {
      accumulateWorkers(currentNode, workerSet);
      workers = List.of(sampleWorker(new ArrayList<>(workerSet)));
    }
    return addExchangeAndWorkers(currentNode, null, workers);
  }

  public PRelNode addExchangeAndWorkers(PRelNode currentNode, @Nullable PRelNode parent, List<String> liteModeWorkers) {
    if (currentNode.isLeafStage()) {
      if (parent == null) {
        // This is because the Root Exchange is added by the RootExchangeInsertRule.
        return currentNode;
      }
      return computeLeafExchange(currentNode, liteModeWorkers);
    }
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : currentNode.getPRelInputs()) {
      newInputs.add(addExchangeAndWorkers(input, currentNode, liteModeWorkers));
    }
    PinotDataDistribution currentNodePDD = inferPDD(currentNode, newInputs, liteModeWorkers);
    currentNode = currentNode.with(newInputs, currentNodePDD);
    if (!currentNode.areTraitsSatisfied()) {
      RelCollation collation = currentNode.unwrap().getTraitSet().getCollation();
      Preconditions.checkState(collation != null && !collation.getFieldCollations().isEmpty(),
          "Expected non-null collation since traits are not satisfied");
      PinotDataDistribution sortedPDD = new PinotDataDistribution(
          RelDistribution.Type.SINGLETON, liteModeWorkers, liteModeWorkers.hashCode(), null, collation);
      return new PhysicalSort(currentNode.unwrap().getCluster(), RelTraitSet.createEmpty(), List.of(), collation,
          null, null, currentNode, nodeId(), sortedPDD, false);
    }
    return currentNode;
  }

  /**
   * Stores workers assigned to the leaf stage nodes into the provided Set. Note that each worker has an integer prefix
   * which denotes the "workerId". We remove that prefix before storing them in the set.
   */
  @VisibleForTesting
  static void accumulateWorkers(PRelNode currentNode, Set<String> workerSink) {
    if (currentNode.isLeafStage()) {
      workerSink.addAll(currentNode.getPinotDataDistributionOrThrow().getWorkers().stream()
          .map(LiteModeWorkerAssignmentRule::stripIdPrefixFromWorker).collect(Collectors.toList()));
      return;
    }
    for (PRelNode input : currentNode.getPRelInputs()) {
      accumulateWorkers(input, workerSink);
    }
  }

  /**
   * Samples a worker from the given list.
   */
  @VisibleForTesting
  static String sampleWorker(List<String> instanceIds) {
    Preconditions.checkState(!instanceIds.isEmpty(), "No workers in leaf stage");
    return String.format("0@%s", instanceIds.get(RANDOM.nextInt(instanceIds.size())));
  }

  @VisibleForTesting
  static String stripIdPrefixFromWorker(String worker) {
    return worker.split("@")[1];
  }

  /**
   * Infers Exchange to be added on top of the leaf stage.
   */
  private PhysicalExchange computeLeafExchange(PRelNode leafStageRoot, List<String> liteModeWorkers) {
    RelCollation collation = leafStageRoot.unwrap().getTraitSet().getCollation();
    PinotDataDistribution pdd;
    if (collation != null) {
      // If the leaf stage root has a collation trait, then we will use a sorted receive in the exchange, so we can
      // add the collation to the PDD.
      pdd = new PinotDataDistribution(
          RelDistribution.Type.SINGLETON, liteModeWorkers, liteModeWorkers.hashCode(), null, collation);
    } else {
      pdd = new PinotDataDistribution(
          RelDistribution.Type.SINGLETON, liteModeWorkers, liteModeWorkers.hashCode(), null, null);
    }
    return new PhysicalExchange(nodeId(), leafStageRoot, pdd, Collections.emptyList(),
        ExchangeStrategy.SINGLETON_EXCHANGE, collation, PinotExecStrategyTrait.getDefaultExecStrategy());
  }

  /**
   * Infers distribution for the current node based on its inputs and node-type. Can also add collation to the PDD
   * automatically (e.g. if the current node is a Sort or the input is sorted and this node does not drop collation).
   */
  private static PinotDataDistribution inferPDD(PRelNode currentNode, List<PRelNode> newInputs,
      List<String> liteModeWorkers) {
    if (currentNode instanceof Sort) {
      Sort sort = (Sort) currentNode.unwrap();
      return new PinotDataDistribution(RelDistribution.Type.SINGLETON, liteModeWorkers,
          liteModeWorkers.hashCode(), null, sort.getCollation());
    }
    if (newInputs.isEmpty()) {
      // Can happen for Values node.
      return new PinotDataDistribution(RelDistribution.Type.SINGLETON, liteModeWorkers,
          liteModeWorkers.hashCode(), null, null);
    }
    return newInputs.get(0).getPinotDataDistributionOrThrow().apply(
        DistMappingGenerator.compute(newInputs.get(0).unwrap(), currentNode.unwrap(), null),
        PinotDistMapping.doesDropCollation(currentNode.unwrap()) /* dropCollation */);
  }

  private int nodeId() {
    return _context.getNodeIdGenerator().get();
  }
}
