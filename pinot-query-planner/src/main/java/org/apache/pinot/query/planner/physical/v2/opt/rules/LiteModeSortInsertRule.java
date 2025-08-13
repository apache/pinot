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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalAggregate;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalSort;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
import org.apache.pinot.query.type.TypeFactory;


/**
 * Lite mode sets a hard limit on the number of rows that the leaf stage is allowed to return. This rule ensures the
 * same. This is done by adding a Sort in the leaf stage if one doesn't exist already.
 * <p>
 *  When the leaf stage has an aggregation and no Sort, then we can add the limit to the aggregate itself to enable
 *  server-level group trimming, which achieves the same purpose as adding a Sort. If the aggregation limit is higher
 *  than the hard-limit, then this Rule will throw an error.
 * </p>
 * <p>
 *   When the leaf stage has a Sort already, we verify that the limit doesn't exceed the configured hard limit.
 * </p>
 */
public class LiteModeSortInsertRule extends PRelOptRule {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  private final PhysicalPlannerContext _context;

  public LiteModeSortInsertRule(PhysicalPlannerContext context) {
    _context = context;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return isLeafBoundary(call);
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    int serverStageLimit = _context.getLiteModeServerStageLimit();
    RexNode newFetch = REX_BUILDER.makeLiteral(serverStageLimit, TYPE_FACTORY.createSqlType(
        SqlTypeName.INTEGER));
    if (call._currentNode instanceof PhysicalSort) {
      // When current node is a Sort, if it has a fetch already, verify it is less than the hard limit. Otherwise,
      // set the configured hard limit within the same Sort.
      PhysicalSort sort = (PhysicalSort) call._currentNode;
      if (sort.fetch != null) {
        int currentFetch = RexExpressionUtils.getValueAsInt(sort.fetch);
        Preconditions.checkState(currentFetch <= serverStageLimit,
            "Attempted to stream %s records from server which exceed limit %s", currentFetch,
            serverStageLimit);
        return sort;
      }
      return sort.withFetch(newFetch);
    }
    if (call._currentNode instanceof PhysicalAggregate) {
      // When current node is aggregate, add the limit to the Aggregate itself and skip adding the Sort.
      PhysicalAggregate aggregate = (PhysicalAggregate) call._currentNode;
      Preconditions.checkState(aggregate.getLimit() <= serverStageLimit,
          "Group trim limit={} exceeds server stage limit={}", aggregate.getLimit(), serverStageLimit);
      int limit = aggregate.getLimit() > 0 ? aggregate.getLimit() : serverStageLimit;
      return aggregate.withLimit(limit);
    }
    RelCollation relCollation = RelCollations.EMPTY;
    if (!call._parents.isEmpty()) {
      // Pass collation from the Exchange above if it exists.
      PRelNode parent = call._parents.getLast();
      if (parent.unwrap() instanceof PhysicalExchange) {
        PhysicalExchange physicalExchange = (PhysicalExchange) parent.unwrap();
        if (physicalExchange.getRelCollation() != null) {
          relCollation = physicalExchange.getRelCollation();
        }
      }
    }
    PRelNode input = call._currentNode;
    return new PhysicalSort(input.unwrap().getCluster(), RelTraitSet.createEmpty(), List.of(),
        relCollation, null /* offset */, newFetch, input, nodeId(), input.getPinotDataDistributionOrThrow(),
        true);
  }

  private int nodeId() {
    return _context.getNodeIdGenerator().get();
  }
}
