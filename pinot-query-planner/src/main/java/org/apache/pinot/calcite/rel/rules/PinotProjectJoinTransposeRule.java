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

import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.sql.fun.ArrayToMvFunctionName;


/// Similar to [ProjectJoinTransposeRule] but do not transpose project into right side of lookup join.
///
/// When the MV aggregate is on (see [PinotRuleUtils#isMvAggregateEnabled]), an expression that contains `ARRAY_TO_MV`
/// stays above the join; only the input references it uses are pushed down. `GROUP BY ARRAY_TO_MV(col)` above a join
/// is planned as an MV aggregate only when the group expression is in the Project under the aggregate (see
/// [ArrayToMvGroupKeys]). Below the join, the expression would be in a leaf-stage Project, where `ARRAY_TO_MV` is not
/// supported. When the MV aggregate is off, the rule pushes such an expression down as before the MV aggregate, so
/// that the plan does not change.
///
/// TODO: Allow transposing project into left side of lookup join.
public class PinotProjectJoinTransposeRule extends ProjectJoinTransposeRule {
  private static final Config MV_AGGREGATE_CONFIG = Config.DEFAULT.withPreserveExprCondition(
      expr -> Config.DEFAULT.preserveExprCondition().test(expr) && !containsArrayToMv(expr));

  public static final PinotProjectJoinTransposeRule INSTANCE =
      new PinotProjectJoinTransposeRule(MV_AGGREGATE_CONFIG, new PinotProjectJoinTransposeRule(Config.DEFAULT, null));

  public static PinotProjectJoinTransposeRule instanceWithDescription(String description) {
    return new PinotProjectJoinTransposeRule((Config) MV_AGGREGATE_CONFIG.withDescription(description),
        new PinotProjectJoinTransposeRule((Config) Config.DEFAULT.withDescription(description), null));
  }

  // The rule as it was before the MV aggregate. It runs while the MV aggregate is off.
  @Nullable
  private final PinotProjectJoinTransposeRule _plainRule;

  private PinotProjectJoinTransposeRule(Config config, @Nullable PinotProjectJoinTransposeRule plainRule) {
    super(config);
    _plainRule = plainRule;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(1);
    return !PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (_plainRule != null && !PinotRuleUtils.isMvAggregateEnabled(call)) {
      _plainRule.onMatch(call);
    } else {
      super.onMatch(call);
    }
  }

  private static boolean containsArrayToMv(RexNode expr) {
    if (!(expr instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) expr;
    if (ArrayToMvFunctionName.matches(call.getOperator().getName())) {
      return true;
    }
    for (RexNode operand : call.getOperands()) {
      if (containsArrayToMv(operand)) {
        return true;
      }
    }
    return false;
  }
}
