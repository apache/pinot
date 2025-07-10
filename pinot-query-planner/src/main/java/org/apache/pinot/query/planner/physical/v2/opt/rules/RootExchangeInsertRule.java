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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.opt.PRelNodeTransformer;


/**
 * Adds an exchange node at the root of the plan if the root node is not already located as a singleton on the broker.
 * This is because the entire data needs to be returned by the broker to the client.
 */
public class RootExchangeInsertRule implements PRelNodeTransformer {
  private final PhysicalPlannerContext _context;

  public RootExchangeInsertRule(PhysicalPlannerContext context) {
    _context = context;
  }

  @Override
  public PRelNode execute(PRelNode currentNode) {
    PinotDataDistribution rootDataDistribution = currentNode.getPinotDataDistributionOrThrow();
    List<String> workers = List.of(brokerWorkerId());
    if (rootDataDistribution.getWorkers().equals(workers)) {
      // If the root node is already distributed to the broker, no need to insert an exchange.
      return currentNode;
    }
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(RelDistribution.Type.SINGLETON,
        workers, workers.hashCode(), null, inferCollation(currentNode));
    return new PhysicalExchange(nodeId(), currentNode, pinotDataDistribution, List.of(),
        ExchangeStrategy.SINGLETON_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy(),
        _context.getDefaultHashFunction());
  }

  private String brokerWorkerId() {
    return String.format("0@%s", _context.getInstanceId());
  }

  private int nodeId() {
    return _context.getNodeIdGenerator().get();
  }

  /**
   * If the current node is distributed to a single worker, inherit the collation trait from it. Otherwise, return null.
   */
  @Nullable
  private RelCollation inferCollation(PRelNode currentNode) {
    // Infer collation from the current node if needed.
    if (currentNode.getPinotDataDistributionOrThrow().getWorkers().size() != 1) {
      return null;
    }
    return currentNode.getPinotDataDistributionOrThrow().getCollation();
  }
}
