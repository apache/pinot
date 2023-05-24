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

import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.Util;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Note: This class copies the logic for reducing SUM and AVG from {@link AggregateReduceFunctionsRule}
 * Planner rule that reduces aggregate functions in
 * {@link org.apache.calcite.rel.core.Aggregate}s to simpler forms.
 *
 * <p>Rewrites:
 * <ul>
 *
 * <li>AVG(x) &rarr; SUM(x) / COUNT(x)
 *
 * </ul>
 *
 * <p>Since many of these rewrites introduce multiple occurrences of simpler
 * forms like {@code COUNT(x)}, the rule gathers common sub-expressions as it
 * goes.
 *
 * @see CoreRules#AGGREGATE_REDUCE_FUNCTIONS
 */
public class PinotAvgSumAggregateReduceFunctionsRule
    extends RelOptRule {

  public static final PinotAvgSumAggregateReduceFunctionsRule INSTANCE =
      new PinotAvgSumAggregateReduceFunctionsRule(PinotRuleUtils.PINOT_REL_FACTORY);
  //~ Static fields/initializers ---------------------------------------------

  protected PinotAvgSumAggregateReduceFunctionsRule(RelBuilderFactory factory) {
    super(operand(Aggregate.class, any()), factory, null);
  }

  private static boolean isValid(SqlKind function) {
    return SqlKind.AVG_AGG_FUNCTIONS.contains(function)
        || function == SqlKind.SUM;
  }

  private final Set<SqlKind> _functionsToReduce = ImmutableSet.<SqlKind>builder().addAll(SqlKind.AVG_AGG_FUNCTIONS)
      .add(SqlKind.SUM).build();;

  //~ Constructors -----------------------------------------------------------



  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    if (!super.matches(call)) {
      return false;
    }
    Aggregate oldAggRel = (Aggregate) call.rels[0];
    return containsAvgStddevVarCall(oldAggRel.getAggCallList());
  }

  @Override public void onMatch(RelOptRuleCall ruleCall) {
    Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    reduceAggs(ruleCall, oldAggRel);
  }

  /**
   * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
   *
   * @param aggCallList List of aggregate calls
   */
  private boolean containsAvgStddevVarCall(List<AggregateCall> aggCallList) {
    return aggCallList.stream().anyMatch(this::canReduce);
  }

  /** Returns whether this rule can reduce a given aggregate function call. */
  public boolean canReduce(AggregateCall call) {
    return _functionsToReduce.contains(call.getAggregation().getKind());
  }

  /**
   * Reduces calls to functions AVG, SUM, STDDEV_POP, STDDEV_SAMP, VAR_POP,
   * VAR_SAMP, COVAR_POP, COVAR_SAMP, REGR_SXX, REGR_SYY if the function is
   * present in {@link PinotAvgSumAggregateReduceFunctionsRule#_functionsToReduce}
   *
   * <p>It handles newly generated common subexpressions since this was done
   * at the sql2rel stage.
   */
  private void reduceAggs(
      RelOptRuleCall ruleCall,
      Aggregate oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int groupCount = oldAggRel.getGroupCount();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    final List<RexNode> projList = new ArrayList<>();

    // pass through group key
    for (int i = 0; i < groupCount; i++) {
      projList.add(rexBuilder.makeInputRef(oldAggRel, i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new aggregate function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      projList.add(
          reduceAgg(
              oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
    }

    final int extraArgCount =
        inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
    if (extraArgCount > 0) {
      relBuilder.project(inputExprs,
          CompositeList.of(
              relBuilder.peek().getRowType().getFieldNames(),
              Collections.nCopies(extraArgCount, null)));
    }
    newAggregateRel(relBuilder, oldAggRel, newCalls);
    newCalcRel(relBuilder, oldAggRel.getRowType(), projList);
    ruleCall.transformTo(relBuilder.build());
  }

  private RexNode reduceAgg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    if (canReduce(oldCall)) {
      final Integer y;
      final Integer x;
      final SqlKind kind = oldCall.getAggregation().getKind();
      switch (kind) {
        case SUM:
          // replace original SUM(x) with
          // case COUNT(x) when 0 then null else SUM0(x) end
          return reduceSum(oldAggRel, oldCall, newCalls, aggCallMapping);
        case AVG:
          // replace original AVG(x) with SUM(x) / COUNT(x)
          return reduceAvg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
        default:
          throw Util.unexpected(kind);
      }
    } else {
      // anything else:  preserve original call
      RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
      final int nGroups = oldAggRel.getGroupCount();
      return rexBuilder.addAggCall(oldCall,
          nGroups,
          newCalls,
          aggCallMapping,
          oldAggRel.getInput()::fieldIsNullable);
    }
  }

  private static RexNode reduceAvg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      @SuppressWarnings("unused") List<RexNode> inputExprs) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    AggregationFunctionType functionTypeSum = AggregationFunctionType.SUM;
    SqlAggFunction sumAggFunc = new PinotSqlAggFunction(functionTypeSum.getName().toUpperCase(),
        functionTypeSum.getSqlIdentifier(), functionTypeSum.getSqlKind(), functionTypeSum.getSqlReturnTypeInference(),
        functionTypeSum.getSqlOperandTypeInference(), functionTypeSum.getSqlOperandTypeChecker(),
        functionTypeSum.getSqlFunctionCategory());

    final AggregateCall sumCall =
        AggregateCall.create(sumAggFunc,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);
    final AggregateCall countCall =
        AggregateCall.create(SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode numeratorRef =
        rexBuilder.addAggCall(sumCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);
    final RexNode denominatorRef =
        rexBuilder.addAggCall(countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);

    final RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    final RelDataType avgType = typeFactory.createTypeWithNullability(
        oldCall.getType(), numeratorRef.getType().isNullable());
    numeratorRef = rexBuilder.ensureType(avgType, numeratorRef, true);

    AggregationFunctionType type =
        AggregationFunctionType.getAggregationFunctionType(oldCall.getAggregation().getName());
    SqlFunction function = new SqlFunction(type.getReduceFunctionName(), SqlKind.OTHER_FUNCTION,
        type.getSqlReduceReturnTypeInference(), null, type.getSqlReduceOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    List<RexNode> functionArgs = Arrays.asList(numeratorRef, denominatorRef);

    // Use our own reducer instead of divide for null/0 count handling
    final RexNode reduceRef = rexBuilder.makeCall(function, functionArgs);
    return rexBuilder.makeCast(oldCall.getType(), reduceRef);
  }

  private static RexNode reduceSum(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    AggregationFunctionType functionTypeSum = AggregationFunctionType.SUM0;
    SqlAggFunction sumAggFunc = new PinotSqlAggFunction(functionTypeSum.getName().toUpperCase(),
        functionTypeSum.getSqlIdentifier(), functionTypeSum.getSqlKind(), functionTypeSum.getSqlReturnTypeInference(),
        functionTypeSum.getSqlOperandTypeInference(), functionTypeSum.getSqlOperandTypeChecker(),
        functionTypeSum.getSqlFunctionCategory());

    final AggregateCall sumZeroCall =
        AggregateCall.create(sumAggFunc, oldCall.isDistinct(),
            oldCall.isApproximate(), oldCall.ignoreNulls(),
            oldCall.getArgList(), oldCall.filterArg, oldCall.distinctKeys,
            oldCall.collation, oldAggRel.getGroupCount(), oldAggRel.getInput(),
            null, oldCall.name);
    final AggregateCall countCall =
        AggregateCall.create(SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel,
            null,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode sumZeroRef =
        rexBuilder.addAggCall(sumZeroCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);
    if (!oldCall.getType().isNullable()) {
      // If SUM(x) is not nullable, the validator must have determined that
      // nulls are impossible (because the group is never empty and x is never
      // null). Therefore we translate to SUM0(x).
      return sumZeroRef;
    }
    RexNode countRef =
        rexBuilder.addAggCall(countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            oldAggRel.getInput()::fieldIsNullable);
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            countRef, rexBuilder.makeExactLiteral(BigDecimal.ZERO)),
        rexBuilder.makeNullLiteral(sumZeroRef.getType()),
        sumZeroRef);
  }

  /**
   * Does a shallow clone of oldAggRel and updates aggCalls. Could be refactored
   * into Aggregate and subclasses - but it's only needed for some
   * subclasses.
   *
   * @param relBuilder Builder of relational expressions; at the top of its
   *                   stack is its input
   * @param oldAggregate LogicalAggregate to clone.
   * @param newCalls  New list of AggregateCalls
   */
  protected void newAggregateRel(RelBuilder relBuilder,
      Aggregate oldAggregate,
      List<AggregateCall> newCalls) {
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets()),
        newCalls);
  }

  /**
   * Adds a calculation with the expressions to compute the original aggregate
   * calls from the decomposed ones.
   *
   * @param relBuilder Builder of relational expressions; at the top of its
   *                   stack is its input
   * @param rowType The output row type of the original aggregate.
   * @param exprs The expressions to compute the original aggregate calls
   */
  protected void newCalcRel(RelBuilder relBuilder,
      RelDataType rowType,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowType.getFieldNames());
  }
}
