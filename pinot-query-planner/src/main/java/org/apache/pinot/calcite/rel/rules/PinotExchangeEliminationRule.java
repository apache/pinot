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
package org.apache.pinot.calcite.rel.rules;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;


/**
 * Special rule for Pinot, this rule eliminates {@link PinotLogicalExchange} when {@link RelDistribution} traits
 * are the same at this exchange node and at the node prior to this exchange node.
 */
public class PinotExchangeEliminationRule extends RelOptRule {
  public static final PinotExchangeEliminationRule INSTANCE =
      new PinotExchangeEliminationRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotExchangeEliminationRule(RelBuilderFactory factory) {
    super(operand(Exchange.class, some(operand(Exchange.class, some(operand(RelNode.class, any()))))), factory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Exchange exchange0 = call.rel(0);
    RelNode input = call.rel(2);
    // convert the call to skip the exchange.
    call.transformTo(exchange0.copy(input.getTraitSet(), List.of(input)));
  }
}
