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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Sarg;


public class PinotFilterExpandSearchRule extends RelOptRule {
  public static final PinotFilterExpandSearchRule INSTANCE =
      new PinotFilterExpandSearchRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotFilterExpandSearchRule(RelBuilderFactory factory) {
    super(operand(LogicalFilter.class, any()), factory, null);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Filter) {
      Filter filter = call.rel(0);
      return shouldBeSimplified(filter.getCondition());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    RexNode newCondition = RexUtil.expandSearch(filter.getCluster().getRexBuilder(), null, filter.getCondition());
    call.transformTo(LogicalFilter.create(filter.getInput(), newCondition));
  }

  private boolean shouldBeSimplified(RexNode condition) {
    switch (condition.getKind()) {
      case AND:
      case OR:
        for (RexNode operand : ((RexCall) condition).getOperands()) {
          if (shouldBeSimplified(operand)) {
            return true;
          }
        }
        return false;
      case SEARCH: {
        RexCall call = (RexCall) condition;
        // Should always be 2. In case it isn't, lets optimistically consider it a range
        if (call.getOperands().size() != 2) {
          return true;
        }
        RexNode secondOperand = call.getOperands().get(1);
        if (secondOperand.isA(SqlKind.LITERAL) && ((RexLiteral) secondOperand).getValue() instanceof Sarg) {
          Sarg<?> sarg = (Sarg<?>) ((RexLiteral) secondOperand).getValue();
          // returns false for IN (sarg) where Sarg is very large.
          // Otherwise it would be translated into OR(input0 = arg1, input0 = arg2, ...)
          // That may be problematic because Calcite has inefficiencies when optimizing very large ORs
          return !sarg.isPoints() || sarg.pointCount <= 20;
        }
        return true;
      }
      default:
        return false;
    }
  }
}
