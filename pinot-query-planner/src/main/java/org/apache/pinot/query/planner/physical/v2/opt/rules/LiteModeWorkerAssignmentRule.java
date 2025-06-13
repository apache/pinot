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
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
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
    PinotDataDistribution pdd = new PinotDataDistribution(RelDistribution.Type.SINGLETON, workers, workers.hashCode(),
        null, null);
    return addExchangeAndWorkers(currentNode, null, pdd);
  }

  public PRelNode addExchangeAndWorkers(PRelNode currentNode, @Nullable PRelNode parent, PinotDataDistribution pdd) {
    if (currentNode.isLeafStage()) {
      if (parent == null) {
        return currentNode;
      }
      return new PhysicalExchange(nodeId(), currentNode, pdd, Collections.emptyList(),
          ExchangeStrategy.SINGLETON_EXCHANGE, currentNode.unwrap().getTraitSet().getCollation(),
          PinotExecStrategyTrait.getDefaultExecStrategy());
    }
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : currentNode.getPRelInputs()) {
      newInputs.add(addExchangeAndWorkers(input, currentNode, pdd));
    }
    currentNode = currentNode.with(newInputs, pdd);
    if (!currentNode.areTraitsSatisfied()) {
      RelCollation collation = currentNode.unwrap().getTraitSet().getCollation();
      Preconditions.checkState(collation != null && !collation.getFieldCollations().isEmpty(),
          "Expected non-null collation since traits are not satisfied");
      PinotDataDistribution sortedPDD = new PinotDataDistribution(
          RelDistribution.Type.SINGLETON, pdd.getWorkers(), pdd.getWorkerHash(), null, collation);
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

  private int nodeId() {
    return _context.getNodeIdGenerator().get();
  }
}
