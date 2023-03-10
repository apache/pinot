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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.tools.RelBuilderFactory;


public class PinotFilterExchangeTransposeRule extends RelOptRule {

  public static final PinotFilterExchangeTransposeRule INSTANCE = new PinotFilterExchangeTransposeRule(
      PinotRuleUtils.PINOT_REL_FACTORY);

  protected PinotFilterExchangeTransposeRule(RelBuilderFactory factory) {
    super(operand(LogicalFilter.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Filter) {
      Filter filter = call.rel(0);
      return PinotRuleUtils.isExchange(filter.getInput());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter origFilter = call.rel(0);
    RelNode inputRelNode = origFilter.getInput();
    if (inputRelNode instanceof HepRelVertex) {
      inputRelNode = ((HepRelVertex) inputRelNode).getCurrentRel();
    }
    Exchange origExchange = (Exchange) inputRelNode;
    Filter newFilter = LogicalFilter.create(origExchange.getInput(), origFilter.getCondition());
    Exchange newExchange = LogicalExchange.create(newFilter, origExchange.getDistribution());
    call.transformTo(newExchange);
  }
}
