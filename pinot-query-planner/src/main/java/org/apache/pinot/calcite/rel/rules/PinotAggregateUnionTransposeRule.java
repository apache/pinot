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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;


/**
 * Pinot variant of Calcite's {@code AggregateUnionTransposeRule} that pushes an {@link Aggregate} past a non-distinct
 * {@link Union}.
 *
 * <p>Calcite's rule decides which aggregate functions are splittable by looking up the function's <i>class</i> in a
 * small allow-list. Pinot ships its own subclasses of {@link SqlAggFunction} (e.g. {@code PinotSumFunction},
 * {@code PinotMinMaxFunction}) that do not extend Calcite's {@code SqlSumAggFunction} or {@code SqlMinMaxAggFunction},
 * so the upstream rule never fires on Pinot's standard SUM/MIN/MAX. This variant matches on {@link SqlKind} instead so
 * the rule also fires for Pinot's custom aggregate functions that share the same semantics.
 *
 * <p>Must run in the logical-planning phase (alongside {@code AggregateUnionAggregateRule}) before
 * {@code LogicalAggregate} is rewritten to {@code PinotLogicalAggregate}; the operand pattern is fixed to
 * {@link LogicalAggregate} / {@link LogicalUnion}. The emitted plain {@link Aggregate} nodes are subsequently split
 * into LEAF / FINAL pairs by {@code PinotAggregateExchangeNodeInsertRule}.
 *
 * <p>This class is stateless and is safe to share across planners.
 */
public final class PinotAggregateUnionTransposeRule extends RelOptRule implements TransformationRule {

  private static final EnumSet<SqlKind> SUPPORTED_KINDS = EnumSet.of(
      SqlKind.SUM, SqlKind.SUM0, SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX,
      SqlKind.ANY_VALUE, SqlKind.BIT_AND, SqlKind.BIT_OR, SqlKind.BIT_XOR);

  public static PinotAggregateUnionTransposeRule instanceWithDescription(String description) {
    return new PinotAggregateUnionTransposeRule(
        operand(LogicalAggregate.class, operand(LogicalUnion.class, any())),
        PinotRuleUtils.PINOT_REL_FACTORY, description);
  }

  private PinotAggregateUnionTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
      String description) {
    super(operand, relBuilderFactory, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    Union union = call.rel(1);

    if (!union.all) {
      // Only valid for UNION ALL: pushing aggregation past UNION DISTINCT would change semantics.
      return;
    }

    int groupCount = aggRel.getGroupSet().cardinality();

    List<AggregateCall> transformedAggCalls = transformAggCalls(aggRel, groupCount);
    if (transformedAggCalls == null) {
      // At least one aggregate function is not splittable (e.g. DISTINCT or AVG).
      return;
    }

    RelMetadataQuery mq = call.getMetadataQuery();
    boolean hasUniqueKeyInAllInputs = true;
    for (RelNode input : union.getInputs()) {
      if (!RelMdUtil.areColumnsDefinitelyUnique(mq, input, aggRel.getGroupSet())) {
        hasUniqueKeyInAllInputs = false;
        break;
      }
    }
    if (hasUniqueKeyInAllInputs) {
      // Every union branch is already unique on the group key, so pushing down the aggregate would be a no-op and
      // could loop forever inside the planner.
      return;
    }

    RelBuilder relBuilder = call.builder();
    RelDataType origUnionType = union.getRowType();
    for (RelNode input : union.getInputs()) {
      List<AggregateCall> childAggCalls = new ArrayList<>(aggRel.getAggCallList());
      RelDataType inputRowType = input.getRowType();
      for (int i = 0; i < childAggCalls.size(); i += 1) {
        AggregateCall origCall = aggRel.getAggCallList().get(i);
        if (origCall.getAggregation().getKind() == SqlKind.COUNT) {
          // COUNT has no argument-nullability issue and is handled below by rolling up via SUM0.
          continue;
        }
        if (origCall.getArgList().size() != 1) {
          continue;
        }
        int field = origCall.getArgList().get(0);
        if (origUnionType.getFieldList().get(field).getType().isNullable()
            != inputRowType.getFieldList().get(field).getType().isNullable()) {
          // Calcite re-creates the call so the inferred return type matches this branch's input nullability.
          AggregateCall newCall =
              AggregateCall.create(origCall.getParserPosition(), origCall.getAggregation(), origCall.isDistinct(),
                  origCall.isApproximate(), origCall.ignoreNulls(), origCall.rexList, origCall.getArgList(), -1,
                  origCall.distinctKeys, origCall.collation, groupCount, input, null, origCall.getName());
          childAggCalls.set(i, newCall);
        }
      }
      relBuilder.push(input);
      relBuilder.aggregate(relBuilder.groupKey(aggRel.getGroupSet()), childAggCalls);
    }

    // Build the new top-level Union over the branch aggregates, then wrap with the rolled-up top aggregate.
    relBuilder.union(true, union.getInputs().size());

    ImmutableBitSet groupSet = aggRel.getGroupSet();
    Mapping topGroupMapping =
        Mappings.create(MappingType.INVERSE_SURJECTION, union.getRowType().getFieldCount(), aggRel.getGroupCount());
    for (int i = 0; i < groupSet.cardinality(); i += 1) {
      topGroupMapping.set(groupSet.nth(i), i);
    }
    ImmutableBitSet topGroupSet = Mappings.apply(topGroupMapping, groupSet);
    ImmutableList<ImmutableBitSet> topGroupSets = Mappings.apply2(topGroupMapping, aggRel.getGroupSets());

    relBuilder.aggregate(relBuilder.groupKey(topGroupSet, topGroupSets), transformedAggCalls);
    call.transformTo(relBuilder.build());
  }

  /**
   * Builds the rolled-up aggregate calls for the top aggregate, with arg indexes shifted into the new union output and
   * COUNT replaced by SUM0 (counts roll up by summation). Returns {@code null} if any call is not splittable.
   */
  private static @Nullable List<AggregateCall> transformAggCalls(Aggregate aggRel, int groupCount) {
    List<AggregateCall> origCalls = aggRel.getAggCallList();
    List<AggregateCall> newCalls = new ArrayList<>(origCalls.size());
    for (int i = 0; i < origCalls.size(); i += 1) {
      AggregateCall origCall = origCalls.get(i);
      if (origCall.isDistinct() || !SUPPORTED_KINDS.contains(origCall.getAggregation().getKind())) {
        return null;
      }
      SqlAggFunction aggFun;
      RelDataType aggType;
      if (origCall.getAggregation().getKind() == SqlKind.COUNT) {
        // Per-branch COUNT becomes a top-level SUM0 to sum the partial counts.
        aggFun = SqlStdOperatorTable.SUM0;
        aggType = null;
      } else {
        aggFun = origCall.getAggregation();
        aggType = origCall.getType();
      }
      // Pass aggRel itself so AggregateCall.create can infer return types from the (groupCols + aggCols) row type that
      // the new top-of-union union will expose, not from the original union's pre-aggregation row type.
      AggregateCall newCall = AggregateCall.create(origCall.getParserPosition(), aggFun, origCall.isDistinct(),
          origCall.isApproximate(), origCall.ignoreNulls(), origCall.rexList, ImmutableList.of(groupCount + i), -1,
          origCall.distinctKeys, origCall.collation, groupCount, aggRel, aggType, origCall.getName());
      newCalls.add(newCall);
    }
    return newCalls;
  }
}
