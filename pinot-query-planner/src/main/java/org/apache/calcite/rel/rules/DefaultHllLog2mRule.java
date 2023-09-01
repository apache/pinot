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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.CompositeList;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.immutables.value.Value;


/**
 * Sets HyperLogLog log2m for DistinctCountHLL functions if not explicitly set for the given query.
 */
@Value.Enclosing
public class DefaultHllLog2mRule extends RelRule<DefaultHllLog2mRule.Config> {

  private final int _defaultHllLog2m;
  private final BigDecimal _defaultHllLog2mBigDecimal;

  private DefaultHllLog2mRule(DefaultHllLog2mRule.Config config) {
    super(config);
    _defaultHllLog2m = config.defaultHllLog2m();
    _defaultHllLog2mBigDecimal = BigDecimal.valueOf(_defaultHllLog2m);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (_defaultHllLog2m <= 0) {
      return false;
    }
    if (call.rels.length < 1) {
      return false;
    }
    final Aggregate aggregate = call.rel(0);
    final List<AggregateCall> originalAggCalls = aggregate.getAggCallList();

    for (AggregateCall originalCall : originalAggCalls) {
      SqlAggFunction originalFunction = originalCall.getAggregation();
      if (isAffectedFunctionCall(originalFunction.getName(), originalCall.getArgList().size())) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall ruleCall) {
    Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new aggregate function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      reduceAgg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
    }

    final int extraArgCount =
        inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
    if (extraArgCount > 0) {
      relBuilder.project(inputExprs,
          CompositeList.of(
              relBuilder.peek().getRowType().getFieldNames(),
              Collections.nCopies(extraArgCount, null)));
      // New plan is absolutely better than old plan.
      // In fact old plan is semantically incorrect
      ruleCall.getPlanner().prune(oldAggRel);
    }
    newAggregateRel(relBuilder, oldAggRel, newCalls);
    RelNode build = relBuilder.build();
    ruleCall.transformTo(build);
  }

  private void reduceAgg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    SqlAggFunction oldFunc = oldCall.getAggregation();

    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    // Check if the aggregation function and arguments match your criteria
    RelNode oldInput = oldAggRel.getInput();
    int nGroups = oldAggRel.getGroupCount();
    if (isAffectedFunctionCall(oldFunc.getName(), oldCall.getArgList().size())) {
      Integer originalFieldIndex = oldCall.getArgList().get(0);

      RexLiteral log2mRexLiteral = rexBuilder.makeExactLiteral(_defaultHllLog2mBigDecimal);
      int log2mIdx = lookupOrAdd(inputExprs, log2mRexLiteral);

      AggregateCall newCall = AggregateCall.create(oldFunc,
          oldCall.isDistinct(),
          oldCall.isApproximate(),
          oldCall.ignoreNulls(),
          ImmutableList.of(originalFieldIndex, log2mIdx),
          oldCall.filterArg,
          oldCall.distinctKeys,
          oldCall.collation,
          nGroups,
          oldAggRel,
          null,
          null);

      rexBuilder.addAggCall(newCall, nGroups, newCalls, aggCallMapping, oldAggRel::fieldIsNullable);
    } else {
      // anything else:  preserve original call
      rexBuilder.addAggCall(oldCall, nGroups, newCalls, aggCallMapping, oldInput::fieldIsNullable);
    }
  }

  private static <T> int lookupOrAdd(List<T> list, T element) {
    int ordinal = list.indexOf(element);
    if (ordinal == -1) {
      ordinal = list.size();
      list.add(element);
    }
    return ordinal;
  }

  protected void newAggregateRel(RelBuilder relBuilder,
      Aggregate oldAggregate,
      List<AggregateCall> newCalls) {
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets()),
        newCalls);
  }

  private boolean isAffectedFunctionCall(String functionName, int argSize) {
    switch (functionName.toLowerCase()) {
      case "distinctcounthll":
      case "distinctcounthllmv":
      case "distinctcountrawhll":
      case "distinctcountrawhllmv":
        return argSize == 1;
      default:
        return false;
    }
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    @Override
    @Value.Default
    default OperandTransform operandSupplier() {
      return (OperandBuilder b0) -> b0.operand(LogicalAggregate.class).anyInputs();
    }

    @Override
    default DefaultHllLog2mRule toRule() {
      return new DefaultHllLog2mRule(this);
    }

    /** Returns defaultHllLog2m. */
    @Value.Default
    default int defaultHllLog2m() {
      return CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M;
    }

    Config withDefaultHllLog2m(int newDefault);

    static Config fromPinotConfig(PinotConfiguration config) {
      int defaultLog2m = config.getProperty(
          CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY,
          CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
      return ImmutableDefaultHllLog2mRule.Config.builder().withDefaultHllLog2m(defaultLog2m).build();
    }
  }
}
