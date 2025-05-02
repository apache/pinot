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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.mapping.DistMappingGenerator;
import org.apache.pinot.query.planner.physical.v2.mapping.PinotDistMapping;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalAggregate;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Does the following:
 * 1. Converts agg calls to their proper forms.
 * 2. Adds aggregate under Exchange, if exchange is an input.
 * 3. Handles leafReturnFinalResult thing.
 */
public class AggregatePushdownRule extends PRelOptRule {
  private final PhysicalPlannerContext _context;

  public AggregatePushdownRule(PhysicalPlannerContext context) {
    _context = context;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return call._currentNode instanceof Aggregate;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    Aggregate aggRel = (Aggregate) call._currentNode;
    Preconditions.checkState(aggRel instanceof PhysicalAggregate, "Expected PhysicalAggregate, got %s", aggRel);
    boolean hasGroupBy = !aggRel.getGroupSet().isEmpty();
    RelCollation withinGroupCollation = extractWithinGroupCollation(aggRel);
    Map<String, String> hintOptions =
        PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);
    if (hintOptions == null) {
      hintOptions = Map.of();
    }
    boolean isInputExchange = call._currentNode.unwrap().getInput(0) instanceof Exchange;
    if (!isInputExchange || withinGroupCollation != null || (hasGroupBy && Boolean.parseBoolean(
        hintOptions.get(PinotHintOptions.AggregateOptions.IS_SKIP_LEAF_STAGE_GROUP_BY)))) {
      return skipPartialAggregate(call._currentNode);
    }
    return addPartialAggregate((PhysicalAggregate) call._currentNode, hintOptions, _context.getNodeIdGenerator());
  }

  private static PRelNode skipPartialAggregate(PRelNode aggPRelNode) {
    PhysicalAggregate aggRel = (PhysicalAggregate) aggPRelNode.unwrap();
    List<AggregateCall> newAggCalls = buildAggCalls(aggRel, AggType.DIRECT, false);
    return new PhysicalAggregate(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), aggRel.getGroupSet(),
        aggRel.groupSets, newAggCalls, aggRel.getNodeId(), aggRel.getPRelInput(0),
        aggRel.getPinotDataDistributionOrThrow(), aggRel.isLeafStage(), AggType.DIRECT,
        false /* leaf return final agg */, aggRel.getCollations(), aggRel.getLimit());
  }

  private static PRelNode addPartialAggregate(PhysicalAggregate aggPRelNode, Map<String, String> hintOptions,
      Supplier<Integer> idGenerator) {
    // Old: Aggregate (o0) > Exchange (o1) > Input (o2)
    // New: Aggregate (n0) > Exchange (n1) > Aggregate (n2) > Input (o2)
    boolean leafReturnFinalResult =
        Boolean.parseBoolean(hintOptions.get(PinotHintOptions.AggregateOptions.IS_LEAF_RETURN_FINAL_RESULT));
    // init old PRelNodes
    PhysicalAggregate o0 = aggPRelNode;
    PhysicalExchange o1 = (PhysicalExchange) o0.getPRelInput(0);
    PRelNode o2 = o1.getPRelInput(0);
    // Create n2
    PhysicalAggregate n2 = new PhysicalAggregate(o0.getCluster(), RelTraitSet.createEmpty(), List.of() /* hints */,
        o0.getGroupSet(), o0.groupSets, buildAggCalls(o0, AggType.LEAF, leafReturnFinalResult), idGenerator.get(),
        o2, null /* data dist */, o2.isLeafStage(), AggType.LEAF, leafReturnFinalResult, aggPRelNode.getCollations(),
        aggPRelNode.getLimit());
    PinotDistMapping mapFromInputToPartialAgg = DistMappingGenerator.compute(o2.unwrap(), n2, null);
    PinotDataDistribution leafAggDataDistribution = o2.getPinotDataDistributionOrThrow().apply(
        mapFromInputToPartialAgg);
    n2 = (PhysicalAggregate) n2.with(n2.getPRelInputs(), leafAggDataDistribution);
    // Create n1.
    List<Integer> newDistKeys = mapFromInputToPartialAgg.getMappedKeys(o1.getDistributionKeys()).get(0);
    RelCollation newCollation = o1.getRelCollation() == null ? null
        : PinotDistMapping.apply(o1.getRelCollation(), mapFromInputToPartialAgg);
    PhysicalExchange n1 = new PhysicalExchange(o1.getNodeId(), n2,
        o1.getPinotDataDistributionOrThrow().apply(mapFromInputToPartialAgg), newDistKeys, o1.getExchangeStrategy(),
        newCollation, PinotExecStrategyTrait.getDefaultExecStrategy());
    return convertAggFromIntermediateInput(aggPRelNode, n1, AggType.FINAL, leafReturnFinalResult,
        PinotDistMapping.apply(RelCollations.of(o0.getCollations()), mapFromInputToPartialAgg).getFieldCollations(),
        aggPRelNode.getLimit(), idGenerator);
  }

  // TODO: Currently it only handles one WITHIN GROUP collation across all AggregateCalls.
  @Nullable
  private static RelCollation extractWithinGroupCollation(Aggregate aggRel) {
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      RelCollation collation = aggCall.getCollation();
      if (!collation.getFieldCollations().isEmpty()) {
        return collation;
      }
    }
    return null;
  }

  private static PhysicalAggregate convertAggFromIntermediateInput(PhysicalAggregate physicalAggregate,
      PhysicalExchange exchange, AggType aggType, boolean leafReturnFinalResult,
      @Nullable List<RelFieldCollation> collations, int limit, Supplier<Integer> nodeId) {
    Aggregate aggRel = (Aggregate) physicalAggregate.unwrap();
    RelNode input = aggRel.getInput();
    List<RexNode> projects = findImmediateProjects(input);
    // Create new AggregateCalls from exchange input. Exchange produces results with group keys followed by intermediate
    // aggregate results.
    int groupCount = aggRel.getGroupCount();
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    int numAggCalls = orgAggCalls.size();
    List<AggregateCall> aggCalls = new ArrayList<>(numAggCalls);
    for (int i = 0; i < numAggCalls; i++) {
      AggregateCall orgAggCall = orgAggCalls.get(i);
      List<Integer> argList = orgAggCall.getArgList();
      int index = groupCount + i;
      RexInputRef inputRef = RexInputRef.of(index, aggRel.getRowType());
      // Generate rexList from argList and replace literal reference with literal. Keep the first argument as is.
      int numArguments = argList.size();
      List<RexNode> rexList;
      if (numArguments <= 1) {
        rexList = ImmutableList.of(inputRef);
      } else {
        rexList = new ArrayList<>(numArguments);
        rexList.add(inputRef);
        for (int j = 1; j < numArguments; j++) {
          int argument = argList.get(j);
          if (projects != null && projects.get(argument) instanceof RexLiteral) {
            rexList.add(projects.get(argument));
          } else {
            // Replace all the input reference in the rexList to the new input reference.
            rexList.add(inputRef);
          }
        }
      }
      aggCalls.add(buildAggCall(exchange, orgAggCall, rexList, groupCount, aggType, leafReturnFinalResult));
    }
    ImmutableBitSet.Builder groupSetBuilder = ImmutableBitSet.builder();
    for (int i = 0; i < groupCount; i++) {
      groupSetBuilder.set(i);
    }
    ImmutableBitSet groupSet = groupSetBuilder.build();
    List<ImmutableBitSet> groupSets = null;
    if (!groupSet.isEmpty()) {
      groupSets = List.of(groupSet);
    }
    return new PhysicalAggregate(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), groupSet,
        groupSets, aggCalls, nodeId.get(), exchange, physicalAggregate.getPinotDataDistributionOrThrow(),
        false, aggType, leafReturnFinalResult, collations, limit);
  }

  public static List<AggregateCall> buildAggCalls(Aggregate aggRel, AggType aggType, boolean leafReturnFinalResult) {
    RelNode input = aggRel.getInput();
    List<RexNode> projects = findImmediateProjects(input);
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    List<AggregateCall> aggCalls = new ArrayList<>(orgAggCalls.size());
    for (AggregateCall orgAggCall : orgAggCalls) {
      // Generate rexList from argList and replace literal reference with literal. Keep the first argument as is.
      List<Integer> argList = orgAggCall.getArgList();
      int numArguments = argList.size();
      List<RexNode> rexList;
      if (numArguments == 0) {
        rexList = ImmutableList.of();
      } else if (numArguments == 1) {
        rexList = ImmutableList.of(RexInputRef.of(argList.get(0), input.getRowType()));
      } else {
        rexList = new ArrayList<>(numArguments);
        rexList.add(RexInputRef.of(argList.get(0), input.getRowType()));
        for (int i = 1; i < numArguments; i++) {
          int argument = argList.get(i);
          if (projects != null && projects.get(argument) instanceof RexLiteral) {
            rexList.add(projects.get(argument));
          } else {
            rexList.add(RexInputRef.of(argument, input.getRowType()));
          }
        }
      }
      aggCalls.add(buildAggCall(input, orgAggCall, rexList, aggRel.getGroupCount(), aggType, leafReturnFinalResult));
    }
    return aggCalls;
  }

  // TODO: Revisit the following logic:
  //   - DISTINCT is resolved here
  //   - argList is replaced with rexList
  private static AggregateCall buildAggCall(RelNode input, AggregateCall orgAggCall, List<RexNode> rexList,
      int numGroups, AggType aggType, boolean leafReturnFinalResult) {
    SqlAggFunction orgAggFunction = orgAggCall.getAggregation();
    String functionName = orgAggFunction.getName();
    SqlKind kind = orgAggFunction.getKind();
    SqlFunctionCategory functionCategory = orgAggFunction.getFunctionType();
    if (orgAggCall.isDistinct()) {
      if (kind == SqlKind.COUNT) {
        functionName = "DISTINCTCOUNT";
        kind = SqlKind.OTHER_FUNCTION;
        functionCategory = SqlFunctionCategory.USER_DEFINED_FUNCTION;
      } else if (kind == SqlKind.LISTAGG) {
        rexList.add(input.getCluster().getRexBuilder().makeLiteral(true));
      }
    }
    SqlReturnTypeInference returnTypeInference = null;
    RelDataType returnType = null;
    // Override the intermediate result type inference if it is provided
    if (aggType.isOutputIntermediateFormat()) {
      AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
      returnTypeInference = leafReturnFinalResult ? functionType.getFinalReturnTypeInference()
          : functionType.getIntermediateReturnTypeInference();
    }
    // When the output is not intermediate format, or intermediate result type inference is not provided (intermediate
    // result type the same as final result type), use the explicit return type
    if (returnTypeInference == null) {
      returnType = orgAggCall.getType();
      returnTypeInference = ReturnTypes.explicit(returnType);
    }
    SqlOperandTypeChecker operandTypeChecker =
        aggType.isInputIntermediateFormat() ? OperandTypes.ANY : orgAggFunction.getOperandTypeChecker();
    SqlAggFunction sqlAggFunction =
        new PinotSqlAggFunction(functionName, kind, returnTypeInference, operandTypeChecker, functionCategory);
    return AggregateCall.create(sqlAggFunction, false, orgAggCall.isApproximate(), orgAggCall.ignoreNulls(), rexList,
        ImmutableList.of(), aggType.isInputIntermediateFormat() ? -1 : orgAggCall.filterArg, orgAggCall.distinctKeys,
        orgAggCall.collation, numGroups, input, returnType, null);
  }

  @Nullable
  private static List<RexNode> findImmediateProjects(RelNode relNode) {
    relNode = PinotRuleUtils.unboxRel(relNode);
    if (relNode instanceof Project) {
      return ((Project) relNode).getProjects();
    } else if (relNode instanceof Union) {
      return findImmediateProjects(relNode.getInput(0));
    }
    return null;
  }
}
