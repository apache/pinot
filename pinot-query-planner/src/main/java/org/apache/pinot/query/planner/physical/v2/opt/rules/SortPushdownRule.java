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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalSort;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
import org.apache.pinot.query.type.TypeFactory;


/**
 * <h1>Overview</h1>
 * When a Sort node is on top of an Exchange, it may make sense to add a copy of the Sort under the Exchange too for
 * performance reasons. E.g. if the Sort has a fetch of 50 rows, then it makes sense to trim the amount of rows
 * sent across the Exchange too.
 */
public class SortPushdownRule extends PRelOptRule {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  private final PhysicalPlannerContext _context;

  public SortPushdownRule(PhysicalPlannerContext context) {
    _context = context;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return call._currentNode.unwrap() instanceof Sort
        && call._currentNode.unwrap().getInput(0) instanceof Exchange;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    // Push down sort when there's a fetch. Don't push down offset, since we can't compute it without total order.
    PhysicalSort sort = (PhysicalSort) call._currentNode.unwrap();
    RexNode newSortFetch = computeEffectiveFetch(sort.fetch, sort.offset);
    if (newSortFetch == null) {
      return call._currentNode;
    }
    // Old: Sort (o0) > Exchange (o1) > Input (o2)
    // New: Sort (n0) > Exchange (n1) > Sort (n2) > Input (o2)
    PRelNode o0 = call._currentNode;
    PhysicalExchange o1 = (PhysicalExchange) o0.getPRelInput(0);
    PRelNode o2 = o1.getPRelInput(0);
    if (o2 instanceof Sort) {
      return call._currentNode;
    }
    // TODO(mse-physical): Preserve collation in PinotDataDistribution.
    PhysicalSort n2 = new PhysicalSort(sort.getCluster(), RelTraitSet.createEmpty(), List.of(), sort.collation,
        null /* offset */, newSortFetch, o2, nodeId(), o2.getPinotDataDistributionOrThrow(), o2.isLeafStage());
    PhysicalExchange n1 = new PhysicalExchange(nodeId(), n2, o1.getPinotDataDistributionOrThrow(),
        o1.getDistributionKeys(), o1.getExchangeStrategy(), o1.getRelCollation(), o1.getExecStrategy());
    return new PhysicalSort(sort.getCluster(), sort.getTraitSet(), sort.getHints(), sort.getCollation(),
        sort.offset, sort.fetch, n1, sort.getNodeId(), sort.getPinotDataDistributionOrThrow(), false);
  }

  @Nullable
  @VisibleForTesting
  static RexNode computeEffectiveFetch(@Nullable RexNode fetch, @Nullable RexNode offset) {
    RexNode result;
    if (fetch == null) {
      result = null;
    } else if (offset == null) {
      result = fetch;
    } else {
      int total = RexExpressionUtils.getValueAsInt(fetch) + RexExpressionUtils.getValueAsInt(offset);
      result = REX_BUILDER.makeLiteral(total, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    }
    return result;
  }

  @VisibleForTesting
  static RexNode createLiteral(int value) {
    return REX_BUILDER.makeLiteral(value, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
  }

  private int nodeId() {
    return _context.getNodeIdGenerator().get();
  }
}
