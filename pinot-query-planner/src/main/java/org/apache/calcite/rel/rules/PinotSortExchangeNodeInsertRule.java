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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, this rule is fixed to always insert exchange after JOIN node.
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
      return sort.getCollation().getFieldCollations().size() > 0 && !PinotRuleUtils.isExchange(sort.getInput());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Sort sort = call.rel(0);
    // TODO: this is a single value
    LogicalExchange exchange = LogicalExchange.create(sort.getInput(), RelDistributions.hash(Collections.emptyList()));
    call.transformTo(LogicalSort.create(exchange, sort.getCollation(), sort.offset, sort.fetch));
  }

  private static boolean isExchange(RelNode rel) {
    RelNode reference = rel;
    if (reference instanceof HepRelVertex) {
      reference = ((HepRelVertex) reference).getCurrentRel();
    }
    return reference instanceof Exchange;
  }
}
