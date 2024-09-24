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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;


/**
 * Special rule for Pinot, this rule is fixed to always insert exchange after SetOp node.
 */
public class PinotSetOpExchangeNodeInsertRule extends RelOptRule {
  public static final PinotSetOpExchangeNodeInsertRule INSTANCE =
      new PinotSetOpExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotSetOpExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(SetOp.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    SetOp setOp = call.rel(0);
    return !PinotRuleUtils.isExchange(setOp.getInput(0));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SetOp setOp = call.rel(0);
    List<RelNode> inputs = setOp.getInputs();
    List<RelNode> newInputs = new ArrayList<>(inputs.size());
    for (RelNode input : inputs) {
      RelNode exchange = PinotLogicalExchange.create(input,
          RelDistributions.hash(ImmutableIntList.range(0, setOp.getRowType().getFieldCount())));
      newInputs.add(exchange);
    }
    call.transformTo(setOp.copy(setOp.getTraitSet(), newInputs));
  }
}
