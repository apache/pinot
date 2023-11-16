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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;


public class PinotRuleUtils {
  private static final RelBuilder.Config DEFAULT_CONFIG =
      RelBuilder.Config.DEFAULT.withAggregateUnique(true).withPushJoinCondition(true);

  public static final RelBuilderFactory PINOT_REL_FACTORY =
      RelBuilder.proto(Contexts.of(RelFactories.DEFAULT_STRUCT, DEFAULT_CONFIG));

  private PinotRuleUtils() {
    // do not instantiate.
  }

  public static RelNode unboxRel(RelNode rel) {
    if (rel instanceof HepRelVertex) {
      return ((HepRelVertex) rel).getCurrentRel();
    } else {
      return rel;
    }
  }

  public static boolean isExchange(RelNode rel) {
    return unboxRel(rel) instanceof Exchange;
  }

  public static boolean isProject(RelNode rel) {
    return unboxRel(rel) instanceof Project;
  }

  public static boolean isJoin(RelNode rel) {
    return unboxRel(rel) instanceof Join;
  }

  public static boolean isAggregate(RelNode rel) {
    return unboxRel(rel) instanceof Aggregate;
  }

  /**
   * utility logic to determine if a JOIN can be pushed down to the leaf-stage execution and leverage the
   * segment-local info (indexing and others) to speed up the execution.
   *
   * <p>The logic here is that the "row-representation" of the relation must not have changed. E.g. </p>
   * <ul>
   *   <li>`RelNode` that are single-in, single-out are possible (Project/Filter/)</li>
   *   <li>`Join` can be stacked on top if we only consider SEMI-JOIN</li>
   *   <li>`Window` should be allowed but we dont have impl for Window on leaf, so not yet included.</li>
   *   <li>`Sort` should be allowed but we need to reorder Sort and Join first, so not yet included.</li>
   * </ul>
   */
  public static boolean canPushDynamicBroadcastToLeaf(RelNode relNode) {
    // TODO 1: optimize this part out as it is not efficient to scan the entire subtree for exchanges;
    //    we should cache the stats in the node (potentially using Trait, e.g. marking LeafTrait & IntermediateTrait)
    // TODO 2: this part is similar to how ServerPlanRequestVisitor determines leaf-stage boundary;
    //    we should refactor and merge both logic
    // TODO 3: for JoinNode, currently this only works towards left-side;
    //    we should support both left and right.
    // TODO 4: for JoinNode, currently this only works for SEMI-JOIN, INNER-JOIN can bring in rows from both sides;
    //    we should check only the non-pipeline-breaker side columns are accessed.
    relNode = PinotRuleUtils.unboxRel(relNode);

    if (relNode instanceof TableScan) {
      // reaching table means it is plan-able.
      return true;
    } else if (relNode instanceof Project || relNode instanceof Filter) {
      // reaching single-in, single-out RelNode means we can continue downward.
      return canPushDynamicBroadcastToLeaf(relNode.getInput(0));
    } else if (relNode instanceof Join) {
      // always check only the left child for dynamic broadcast
      return canPushDynamicBroadcastToLeaf(((Join) relNode).getLeft());
    } else {
      // for all others we don't allow dynamic broadcast
      return false;
    }
  }

  public static String extractFunctionName(RexCall function) {
    SqlKind funcSqlKind = function.getOperator().getKind();
    return funcSqlKind == SqlKind.OTHER_FUNCTION ? function.getOperator().getName() : funcSqlKind.name();
  }
}
