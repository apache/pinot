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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.tools.RelBuilderFactory;


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
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof SetOp) {
      SetOp setOp = call.rel(0);
      for (RelNode input : setOp.getInputs()) {
        if (PinotRuleUtils.isExchange(input)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SetOp setOp = call.rel(0);
    List<RelNode> newInputs = new ArrayList<>();
    List<Integer> hashFields =
        IntStream.range(0, setOp.getRowType().getFieldCount()).boxed().collect(Collectors.toCollection(ArrayList::new));
    for (RelNode input : setOp.getInputs()) {
      RelNode exchange = PinotLogicalExchange.create(input, RelDistributions.hash(hashFields));
      newInputs.add(exchange);
    }
    SetOp newSetOpNode;
    if (setOp instanceof LogicalUnion) {
      newSetOpNode = new LogicalUnion(setOp.getCluster(), setOp.getTraitSet(), newInputs, setOp.all);
    } else if (setOp instanceof LogicalIntersect) {
      newSetOpNode = new LogicalIntersect(setOp.getCluster(), setOp.getTraitSet(), newInputs, setOp.all);
    } else if (setOp instanceof LogicalMinus) {
      newSetOpNode = new LogicalMinus(setOp.getCluster(), setOp.getTraitSet(), newInputs, setOp.all);
    } else {
      throw new UnsupportedOperationException("Unsupported set op node: " + setOp);
    }
    call.transformTo(newSetOpNode);
  }
}
