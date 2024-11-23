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
package org.apache.pinot.calcite.rel.logical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories.JoinFactory;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.JoinNode.JoinStrategy;


/**
 * Factory for {@link PinotLogicalJoin} which contains {@link JoinStrategy} extracted from hint.
 */
public class PinotLogicalJoinFactory implements JoinFactory {

  @Override
  public RelNode createJoin(RelNode left, RelNode right, List<RelHint> hints, RexNode condition,
      Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone) {
    String joinStrategyHint = PinotHintStrategyTable.getHintOption(hints, PinotHintOptions.JOIN_HINT_OPTIONS,
        PinotHintOptions.JoinHintOptions.JOIN_STRATEGY);
    JoinNode.JoinStrategy joinStrategy = null;
    if (joinStrategyHint != null) {
      switch (joinStrategyHint.toLowerCase()) {
        case PinotHintOptions.JoinHintOptions.HASH_JOIN_STRATEGY:
          joinStrategy = JoinNode.JoinStrategy.HASH;
          break;
        case PinotHintOptions.JoinHintOptions.LOOKUP_JOIN_STRATEGY:
          joinStrategy = JoinNode.JoinStrategy.LOOKUP;
          break;
        case PinotHintOptions.JoinHintOptions.DYNAMIC_BROADCAST_JOIN_STRATEGY:
          joinStrategy = JoinNode.JoinStrategy.DYNAMIC_BROADCAST;
          break;
        default:
          break;
      }
    }

    RelOptCluster cluster = left.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new PinotLogicalJoin(cluster, traitSet, hints, left, right, condition, variablesSet, joinType, semiJoinDone,
        ImmutableList.of(), joinStrategy);
  }
}
