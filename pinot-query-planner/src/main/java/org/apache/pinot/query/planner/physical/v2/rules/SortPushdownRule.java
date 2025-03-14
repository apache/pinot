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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;
import org.apache.pinot.query.type.TypeFactory;


/**
 * <h1>Overview</h1>
 * When a Sort node is on top of an Exchange, it may make sense to add a copy of the Sort under the Exchange too for
 * performance reasons. E.g. if the Sort has a fetch of 50 rows, then it makes sense to trim the amount of rows
 * sent across the Exchange too.
 * <h1>Handling Offsets</h1>
 */
public class SortPushdownRule extends PRelOptRule {
  public static final SortPushdownRule INSTANCE = new SortPushdownRule();

  private static final int DEFAULT_SORT_EXCHANGE_COPY_THRESHOLD = 10_000;
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  private static final RexLiteral REX_ZERO = REX_BUILDER.makeLiteral(0,
      TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return call._currentNode.getRelNode() instanceof Sort
        && call._currentNode.getRelNode().getInput(0) instanceof Exchange;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    // TODO: Make it as smart as existing PinotSortExchangeCopyRule, which tracks max amount of rows. However, the
    //   existing rule leverages RelMetadataQuery.
    // TODO: Some other condition checking is missing here like comparing collations.
    Sort sort = (Sort) call._currentNode.getRelNode();
    final RexNode fetch;
    if (sort.fetch == null) {
      fetch = null;
    } else if (sort.offset == null) {
      fetch = sort.fetch;
    } else {
      int total = RexExpressionUtils.getValueAsInt(sort.fetch) + RexExpressionUtils.getValueAsInt(sort.offset);
      fetch = REX_BUILDER.makeLiteral(total, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    }
    if (fetch == null) {
      return call._currentNode;
    }
    final Exchange exchange = (Exchange) sort.getInput();
    final RelNode newExchangeInput = sort.copy(sort.getTraitSet(), exchange.getInput(), sort.collation, null, fetch);
    final RelNode newExchange = exchange.copy(exchange.getTraitSet(), ImmutableList.of(newExchangeInput));
    final RelNode newTopLevelSort = sort.copy(
        sort.getTraitSet(), newExchange, sort.collation, sort.offset == null ? REX_ZERO : sort.offset, sort.fetch);
    // Old: Sort (o0) > Exchange (o1) > Input (o2)
    // New: Sort (n0) > Exchange (n1) > Sort (n2) > Input (o2)
    PRelNode o0 = call._currentNode;
    PRelNode o1 = o0.getInput(0);
    PRelNode o2 = o1.getInput(0);
    PRelNode n2 = new PRelNode(call._physicalPlannerContext.getNodeIdGenerator().get(), newExchangeInput,
        o2.getPinotDataDistribution(), ImmutableList.of(o2), o2.isLeafStage(), o2.getTableScanMetadata());
    PRelNode n1 = new PRelNode(o1.getNodeId(), newExchange, o1.getPinotDataDistribution(), ImmutableList.of(n2),
        o1.isLeafStage(), o2.getTableScanMetadata());
    // return n0
    return new PRelNode(o0.getNodeId(), newTopLevelSort, o0.getPinotDataDistribution(), ImmutableList.of(n1),
        false, null);
  }
}
