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
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
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
    // When the colocation hint is set, force a pre-partitioned (direct, no-shuffle) exchange on every input; otherwise
    // leave it null so the planner auto-detects pre-partitioning from the inputs' distribution. See
    // PinotHintOptions.SetOpHintOptions.IS_COLOCATED_BY_SET_OP_KEYS for the correctness contract.
    Boolean prePartitioned = resolveColocationHint(setOp);
    List<RelNode> newInputs = new ArrayList<>(inputs.size());
    for (RelNode input : inputs) {
      RelNode exchange = PinotLogicalExchange.create(input,
          RelDistributions.hash(ImmutableIntList.range(0, setOp.getRowType().getFieldCount())), prePartitioned);
      newInputs.add(exchange);
    }
    call.transformTo(setOp.copy(setOp.getTraitSet(), newInputs));
  }

  /// Resolves the colocation hint for a set operation. Calcite attaches the hint to the set operation itself only when
  /// the query wraps it in an outer {@code SELECT} that carries the hint; in the natural inline form (the hint on the
  /// first {@code SELECT} of a {@code UNION}/{@code INTERSECT}/{@code EXCEPT}) it lands on the first branch instead.
  /// Precedence: the set operation's own hint wins, otherwise the first input carrying the hint wins. The resolved
  /// value is applied to all inputs, so conflicting per-branch values are not supported.
  @Nullable
  private static Boolean resolveColocationHint(SetOp setOp) {
    Boolean fromSetOp = PinotHintOptions.SetOpHintOptions.isColocatedBySetOpKeys(setOp.getHints());
    if (fromSetOp != null) {
      return fromSetOp;
    }
    for (RelNode input : setOp.getInputs()) {
      RelNode unboxed = PinotRuleUtils.unboxRel(input);
      if (unboxed instanceof Hintable) {
        Boolean fromInput = PinotHintOptions.SetOpHintOptions.isColocatedBySetOpKeys(((Hintable) unboxed).getHints());
        if (fromInput != null) {
          return fromInput;
        }
      }
    }
    return null;
  }
}
