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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;


/**
 * Rewrites certain aggregation functions based on operand types to support polymorphic aggregations.
 *
 * Currently supported rewrites:
 * - MIN(STRING) -> MINSTRING
 * - MAX(STRING) -> MAXSTRING
 */
public class PinotAggregateFunctionRewriteRule extends RelOptRule {
  public static final PinotAggregateFunctionRewriteRule INSTANCE =
      new PinotAggregateFunctionRewriteRule(PinotRuleUtils.PINOT_REL_FACTORY, null);

  public static PinotAggregateFunctionRewriteRule instanceWithDescription(String description) {
    return new PinotAggregateFunctionRewriteRule(PinotRuleUtils.PINOT_REL_FACTORY, description);
  }

  private PinotAggregateFunctionRewriteRule(RelBuilderFactory factory, String description) {
    super(operand(LogicalAggregate.class, any()), factory, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    RelNode input = aggRel.getInput();
    List<AggregateCall> originalCalls = aggRel.getAggCallList();

    boolean changed = false;
    List<AggregateCall> rewrittenCalls = new ArrayList<>(originalCalls.size());
    for (AggregateCall aggCall : originalCalls) {
      AggregateCall newCall = maybeRewriteMinMaxOnString(aggCall, input, aggRel.getGroupCount());
      if (newCall != aggCall) {
        changed = true;
      }
      rewrittenCalls.add(newCall);
    }

    if (!changed) {
      return;
    }

    call.transformTo(aggRel.copy(aggRel.getTraitSet(), input, aggRel.getGroupSet(), aggRel.getGroupSets(),
        rewrittenCalls));
  }

  /**
   * If the call is MIN or MAX over a STRING input, rewrite it to MINSTRING/MAXSTRING.
   */
  private static AggregateCall maybeRewriteMinMaxOnString(AggregateCall call, RelNode input, int numGroups) {
    SqlAggFunction aggFunction = call.getAggregation();
    SqlKind kind = aggFunction.getKind();
    if (kind != SqlKind.MIN && kind != SqlKind.MAX) {
      return call;
    }

    List<Integer> argList = call.getArgList();
    if (argList.isEmpty()) {
      return call;
    }

    int argIndex = argList.get(0);
    RelDataTypeField field = input.getRowType().getFieldList().get(argIndex);
    RelDataType fieldType = field.getType();
    SqlTypeName sqlTypeName = fieldType.getSqlTypeName();
    if (!SqlTypeName.STRING_TYPES.contains(sqlTypeName)) {
      return call;
    }

    String targetName = (kind == SqlKind.MIN) ? "MINSTRING" : "MAXSTRING";
    SqlAggFunction stringAgg =
        new PinotSqlAggFunction(targetName, SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(call.getType()),
            aggFunction.getOperandTypeChecker(), SqlFunctionCategory.USER_DEFINED_FUNCTION);

    return AggregateCall.create(stringAgg, call.isDistinct(), call.isApproximate(), call.ignoreNulls(), argList,
        call.filterArg, call.distinctKeys, call.getCollation(), numGroups, input, call.getType(), call.getName());
  }
}
