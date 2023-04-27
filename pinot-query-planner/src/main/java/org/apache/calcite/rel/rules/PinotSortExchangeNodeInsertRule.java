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

import java.util.Collections;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Rewrite any sort into a relation that adds an exchange and pushes down the collation
 * to the rest of the tree. This happens for two reasons:
 * <ol>
 *   <li>Sort needs to be a distributed operation, if there are multiple nodes that are
 *   scanning data the sort ordering must be applied globally.</li>
 *   <li>It is ideal to push down the sort ordering as far as possible. If upstream nodes
 *   can send data in sorted order, then we can apply N-way merge sort and early terminate
 *   once all nodes have sent data that is no longer in the top OFFSET+LIMIT.</li>
 * </ol>
 */
public class PinotSortExchangeNodeInsertRule extends RelOptRule {
  public static final PinotSortExchangeNodeInsertRule INSTANCE =
      new PinotSortExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotSortExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalSort.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Sort) {
      Sort sort = call.rel(0);
      return !PinotRuleUtils.isExchange(sort.getInput());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Sort sort = call.rel(0);
    // TODO: Assess whether sorting is needed on both sender and receiver side or only receiver side. Potentially add
    //       SqlHint support to determine this. For now setting sort only on receiver side as sender side sorting is
    //       not yet implemented.
    PinotLogicalSortExchange exchange = PinotLogicalSortExchange.create(
        sort.getInput(),
        RelDistributions.hash(Collections.emptyList()),
        sort.getCollation(),
        false,
        !sort.getCollation().getKeys().isEmpty());
    call.transformTo(LogicalSort.create(exchange, sort.getCollation(), sort.offset, sort.fetch));
  }
}
