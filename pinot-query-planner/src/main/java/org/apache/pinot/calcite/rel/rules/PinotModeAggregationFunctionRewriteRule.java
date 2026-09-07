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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;


/// Rewrites string and timestamp MODE calls to typed implementations after an explicit rollout opt-in.
/// Numeric MODE and the separate MIN, MAX and SUM rewrite rule are unaffected.
public class PinotModeAggregationFunctionRewriteRule extends RelOptRule {
  public static PinotModeAggregationFunctionRewriteRule instanceWithDescription(String description) {
    return new PinotModeAggregationFunctionRewriteRule(description);
  }

  private PinotModeAggregationFunctionRewriteRule(String description) {
    super(operand(LogicalAggregate.class, any()), PinotRuleUtils.PINOT_REL_FACTORY, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    RelNode input = aggregate.getInput();
    List<AggregateCall> originalCalls = aggregate.getAggCallList();
    List<AggregateCall> rewrittenCalls = new ArrayList<>(originalCalls.size());
    boolean changed = false;
    for (AggregateCall originalCall : originalCalls) {
      AggregateCall rewrittenCall = maybeRewriteAggCall(originalCall, input, aggregate.getGroupCount());
      changed |= rewrittenCall != originalCall;
      rewrittenCalls.add(rewrittenCall);
    }
    if (changed) {
      call.transformTo(aggregate.copy(aggregate.getTraitSet(), input, aggregate.getGroupSet(), aggregate.getGroupSets(),
          rewrittenCalls));
    }
  }

  private static AggregateCall maybeRewriteAggCall(AggregateCall call, RelNode input, int numGroups) {
    SqlAggFunction aggregation = call.getAggregation();
    List<Integer> arguments = call.getArgList();
    if (aggregation.getKind() != SqlKind.MODE || arguments.isEmpty()) {
      return call;
    }
    SqlTypeName operandType = input.getRowType().getFieldList().get(arguments.get(0)).getType().getSqlTypeName();
    String functionName;
    if (SqlTypeName.STRING_TYPES.contains(operandType)) {
      functionName = "MODESTRING";
    } else if (operandType == SqlTypeName.TIMESTAMP) {
      functionName = "MODETIMESTAMP";
    } else {
      return call;
    }
    SqlAggFunction rewrittenAggregation = new PinotSqlAggFunction(functionName, SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(call.getType()), aggregation.getOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    return AggregateCall.create(rewrittenAggregation, call.isDistinct(), call.isApproximate(), call.ignoreNulls(),
        arguments, call.filterArg, call.distinctKeys, call.getCollation(), numGroups, input, call.getType(),
        call.getName());
  }
}
