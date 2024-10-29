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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;


public class PinotFilterExpandSearchRule extends RelOptRule {
  public static final PinotFilterExpandSearchRule INSTANCE =
      new PinotFilterExpandSearchRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotFilterExpandSearchRule(RelBuilderFactory factory) {
    super(operand(Filter.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    return containsRangeSearch(filter.getCondition());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    RexNode newCondition = RexUtil.expandSearch(filter.getCluster().getRexBuilder(), null, filter.getCondition());
    call.transformTo(filter.copy(filter.getTraitSet(), filter.getInput(), newCondition));
  }

  private boolean containsRangeSearch(RexNode condition) {
    switch (condition.getKind()) {
      case AND:
      case OR:
        for (RexNode operand : ((RexCall) condition).getOperands()) {
          if (containsRangeSearch(operand)) {
            return true;
          }
        }
        return false;
      case SEARCH:
        return true;
      default:
        return false;
    }
  }
}
