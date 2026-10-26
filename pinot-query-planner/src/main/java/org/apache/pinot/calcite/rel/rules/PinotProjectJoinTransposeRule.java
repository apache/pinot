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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.sql.fun.ArrayToMvFunctionName;


/// Similar to [ProjectJoinTransposeRule] but do not transpose project into right side of lookup join.
///
/// An expression that contains `ARRAY_TO_MV` stays above the join; only the input references it uses are pushed down.
/// `GROUP BY ARRAY_TO_MV(col)` above a join is planned as an MV aggregate only when the group expression is in the
/// Project under the aggregate (see [ArrayToMvGroupKeys]). Below the join, the expression would be in a leaf-stage
/// Project, where `ARRAY_TO_MV` is not supported.
///
/// TODO: Allow transposing project into left side of lookup join.
public class PinotProjectJoinTransposeRule extends ProjectJoinTransposeRule {
  private static final Config DEFAULT_CONFIG = Config.DEFAULT.withPreserveExprCondition(
      expr -> Config.DEFAULT.preserveExprCondition().test(expr) && !containsArrayToMv(expr));

  public static final PinotProjectJoinTransposeRule INSTANCE = new PinotProjectJoinTransposeRule(DEFAULT_CONFIG);

  public static PinotProjectJoinTransposeRule instanceWithDescription(String description) {
    return new PinotProjectJoinTransposeRule((Config) DEFAULT_CONFIG.withDescription(description));
  }

  private PinotProjectJoinTransposeRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(1);
    return !PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join);
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
