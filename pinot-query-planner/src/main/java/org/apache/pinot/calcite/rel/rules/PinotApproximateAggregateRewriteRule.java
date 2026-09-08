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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;


/// Rewrites exact aggregations into their threshold-based approximate counterparts, so that one cluster config can
/// stop unbounded per-group accumulators from taking servers down:
/// - `DISTINCT_COUNT(x)` and `COUNT(DISTINCT x)` -> `DISTINCT_COUNT_SMART_HLL(x)`
/// - `PERCENTILE(x, p)` -> `PERCENTILE_SMART_TDIGEST(x, p)`
///
/// This is the multi-stage counterpart of `BaseSingleStageBrokerRequestHandler.handleApproximateFunctionOverride`,
/// gated by the same resolved setting, read from [QueryEnvironment.Config#useApproximateFunction()]. The rewrite keeps
/// the original return type, so switching the config on does not change the result schema.
///
/// It must run before [PinotAggregateExchangeNodeInsertRule], which derives the leaf-to-final intermediate result
/// format from the function name, hence `Phase.BASIC` rather than that rule's `POST_LOGICAL`. That is also why the
/// runtime is not a valid place to do this rewrite.
public class PinotApproximateAggregateRewriteRule extends RelOptRule {
  public static final PinotApproximateAggregateRewriteRule INSTANCE =
      new PinotApproximateAggregateRewriteRule(PinotRuleUtils.PINOT_REL_FACTORY);

  private PinotApproximateAggregateRewriteRule(RelBuilderFactory factory) {
    super(operand(LogicalAggregate.class, any()), factory, PlannerRuleNames.APPROXIMATE_AGGREGATE_REWRITE);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    QueryEnvironment.Config envConfig = envConfig(call);
    if (envConfig == null || !envConfig.useApproximateFunction()) {
      return false;
    }
    // Requiring a rewritable call is also what makes the rule terminate: the rewritten names no longer match.
    Aggregate aggRel = call.rel(0);
    return aggRel.getAggCallList().stream().anyMatch(aggCall -> targetOf(aggCall) != null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    QueryEnvironment.Config envConfig = Objects.requireNonNull(envConfig(call));
    RelNode input = aggRel.getInput();
    int numInputFields = input.getRowType().getFieldCount();

    boolean hasDistinctCount = false;
    boolean hasPercentile = false;
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      AggregationFunctionType target = targetOf(aggCall);
      hasDistinctCount |= target == AggregationFunctionType.DISTINCTCOUNTSMARTHLL;
      hasPercentile |= target == AggregationFunctionType.PERCENTILESMARTTDIGEST;
    }
    String distinctCountParams = hasDistinctCount ? envConfig.approximateFunctionDistinctCountParams() : "";
    String percentileParams = hasPercentile ? envConfig.approximateFunctionPercentileParams() : "";

    // The parameters must be input fields, because an aggregate call holds field indices rather than inline literals,
    // so they are projected underneath the aggregate. Placing them in the immediate project is what lets
    // PinotAggregateExchangeNodeInsertRule inline them back into the pushed-down call. With no parameters configured
    // the plan keeps the shape it had before, with no extra project.
    RelNode newInput = input;
    int distinctCountParamsIndex = -1;
    int percentileParamsIndex = -1;
    if (!distinctCountParams.isEmpty() || !percentileParams.isEmpty()) {
      RexBuilder rexBuilder = aggRel.getCluster().getRexBuilder();
      List<RexNode> projects = new ArrayList<>(numInputFields + 2);
      for (int i = 0; i < numInputFields; i++) {
        projects.add(rexBuilder.makeInputRef(input, i));
      }
      if (!distinctCountParams.isEmpty()) {
        distinctCountParamsIndex = projects.size();
        projects.add(rexBuilder.makeLiteral(distinctCountParams));
      }
      if (!percentileParams.isEmpty()) {
        percentileParamsIndex = projects.size();
        projects.add(rexBuilder.makeLiteral(percentileParams));
      }
      newInput = LogicalProject.create(input, List.of(), projects, (List<String>) null);
    }

    List<AggregateCall> rewrittenAggCalls = new ArrayList<>(aggRel.getAggCallList().size());
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      AggregationFunctionType target = targetOf(aggCall);
      if (target == null) {
        rewrittenAggCalls.add(aggCall);
        continue;
      }
      int paramsIndex = target == AggregationFunctionType.DISTINCTCOUNTSMARTHLL
          ? distinctCountParamsIndex : percentileParamsIndex;
      List<Integer> argList = aggCall.getArgList();
      if (paramsIndex >= 0) {
        argList = new ArrayList<>(argList);
        argList.add(paramsIndex);
      }
      rewrittenAggCalls.add(rewrite(aggCall, target, argList, newInput, aggRel.getGroupCount()));
    }

    PlannerContext plannerContext = call.getPlanner().getContext().unwrap(PlannerContext.class);
    if (plannerContext != null) {
      plannerContext.setApproximateFunctionApplied();
    }
    call.transformTo(
        aggRel.copy(aggRel.getTraitSet(), newInput, aggRel.getGroupSet(), aggRel.getGroupSets(), rewrittenAggCalls));
  }

  /// Returns the approximate function this call should become, or `null` when the call must be left alone.
  @Nullable
  private static AggregationFunctionType targetOf(AggregateCall aggCall) {
    SqlAggFunction aggFunction = aggCall.getAggregation();
    if (aggCall.isDistinct()) {
      // COUNT(DISTINCT x) is the standard SQL spelling of DISTINCT_COUNT, and PinotAggregateExchangeNodeInsertRule
      // only renames it in POST_LOGICAL, after this rule, so it has to be matched on the kind here. Multi-argument
      // COUNT(DISTINCT a, b) means something else and is left alone.
      return aggFunction.getKind() == SqlKind.COUNT && aggCall.getArgList().size() == 1
          ? AggregationFunctionType.DISTINCTCOUNTSMARTHLL : null;
    }
    // The smart functions take multi-valued input themselves, so the MV spellings map onto the same targets, which
    // keeps this in step with the single-stage rewrite.
    String name = AggregationFunctionType.getNormalizedAggregationFunctionName(aggFunction.getName());
    if (name.equals(AggregationFunctionType.DISTINCTCOUNT.name())
        || name.equals(AggregationFunctionType.DISTINCTCOUNTMV.name())) {
      return AggregationFunctionType.DISTINCTCOUNTSMARTHLL;
    }
    if (name.equals(AggregationFunctionType.PERCENTILE.name())
        || name.equals(AggregationFunctionType.PERCENTILEMV.name())) {
      return AggregationFunctionType.PERCENTILESMARTTDIGEST;
    }
    return null;
  }

  private static AggregateCall rewrite(AggregateCall aggCall, AggregationFunctionType target, List<Integer> argList,
      RelNode input, int groupCount) {
    // Pinning the original type keeps the rewrite invisible to the caller: PERCENTILE infers ARG0 while
    // PERCENTILE_SMART_TDIGEST infers DOUBLE, and a silent cluster-wide rewrite must not change the result schema.
    SqlAggFunction newAggFunction = new PinotSqlAggFunction(target.name(), SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(aggCall.getType()), target.getOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    return AggregateCall.create(newAggFunction, false, aggCall.isApproximate(), aggCall.ignoreNulls(), argList,
        aggCall.filterArg, aggCall.distinctKeys, aggCall.getCollation(), groupCount, input, aggCall.getType(),
        aggCall.getName());
  }

  @Nullable
  private static QueryEnvironment.Config envConfig(RelOptRuleCall call) {
    Context context = call.getPlanner().getContext();
    return context != null ? context.unwrap(QueryEnvironment.Config.class) : null;
  }
}
