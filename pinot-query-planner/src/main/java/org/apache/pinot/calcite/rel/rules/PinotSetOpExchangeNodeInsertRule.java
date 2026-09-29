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
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;


/// Special rule for Pinot, this rule is fixed to always insert exchange after SetOp node.
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
    // leave it null so the planner auto-detects pre-partitioning from the inputs' distribution. On a UNION ALL only
    // 'false' has an effect, because its inputs already get a local exchange either way. See
    // PinotHintOptions.SetOpHintOptions.IS_COLOCATED_BY_SET_OP_KEYS for the correctness contract.
    Boolean prePartitioned = resolveColocationHint(setOp);
    // UNION ALL only concatenates its inputs, so any row-to-worker mapping produces correct results and no
    // redistribution is required. Use a local (SINGLETON) exchange so the union stage inherits its inputs' worker
    // assignment and the rows are handed over in place, with no shuffle and no network hop when sender and receiver
    // land on the same server.
    // The projected columns are still attached as keys. They are unused while the exchange stays local, but they let
    // the mailbox layer promote it to a real HASH distribution when the inputs do not resolve to the same workers --
    // the same idiom PinotJoinExchangeNodeInsertRule uses for distribution_type='local'. Keeping keys also means a
    // UNION ALL input is never a KEYLESS local exchange, so the "local exchange with parallelism requires keys"
    // guard continues to protect the colocated semi-join build side untouched.
    // An explicit is_colocated_by_set_op_keys='false' hint opts out and restores the full-row shuffle.
    List<Integer> keys = ImmutableIntList.range(0, setOp.getRowType().getFieldCount());
    boolean useLocalExchange = setOp instanceof Union && ((Union) setOp).all && !Boolean.FALSE.equals(prePartitioned);
    List<RelNode> newInputs = new ArrayList<>(inputs.size());
    for (RelNode input : inputs) {
      RelNode exchange = useLocalExchange
          ? PinotLogicalExchange.create(input, RelDistributions.SINGLETON, keys, null)
          : PinotLogicalExchange.create(input, RelDistributions.hash(keys), prePartitioned);
      newInputs.add(exchange);
    }
    call.transformTo(setOp.copy(setOp.getTraitSet(), newInputs));
  }

  /// Resolves the colocation hint for a set operation. Calcite attaches the hint to the set operation itself only when
  /// the query wraps it in an outer `SELECT` that carries the hint; in the natural inline form (the hint on the
  /// first `SELECT` of a `UNION`/`INTERSECT`/`EXCEPT`) it lands on the first branch instead.
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
