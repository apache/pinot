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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.query.planner.plannode.MvAggregateNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Runs an `MvAggregateNode`: a group by where some group keys are multi-value (MV). Each input row contributes to
/// the group of every combination of its MV key elements, with the single-stage engine semantics: one group per
/// element, a repeated element counts once per copy, an empty array contributes to no group, a `null` array forms
/// the `null` group, and several MV keys give the cartesian product. The operator computes the group ids of each row
/// directly from the arrays and calls `AggregationFunction.aggregateGroupByMV`, so it never materializes one row per
/// element.
///
/// The output is the same as the one of [AggregateOperator] for a plain group by: `[group keys..., aggregations...]`,
/// with scalar values for the MV keys. The MV keys only change the input, so the operator only accepts raw input
/// (LEAF and DIRECT aggregations); the FINAL side of a split aggregation is a plain [AggregateOperator].
///
/// It shares the [AggregateOperator.StatKey] stats, the group trim, the groups limit and the group-by flow with
/// [AggregateOperator] through [BaseAggregateOperator], and reports the stats as
/// [MultiStageOperator.Type#MV_AGGREGATE].
///
/// Like every [MultiStageOperator], an instance is not thread-safe: one op chain thread drives it.
public class MvAggregateOperator extends BaseAggregateOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MvAggregateOperator.class);
  private static final String EXPLAIN_NAME = "MV_AGGREGATE_OPERATOR";

  private final List<Integer> _mvKeyIndexes;

  public MvAggregateOperator(OpChainExecutionContext context, MultiStageOperator input, MvAggregateNode node) {
    super(context, input, node.getDataSchema(), node.getAggCalls(), node.getFilterArgs(), node.getNodeHint(),
        node.getLimit(), node.getCollations());
    List<Integer> groupKeys = node.getGroupKeys();
    List<Integer> mvKeyIndexes = node.getMvKeyIndexes();
    Preconditions.checkArgument(!groupKeys.isEmpty(), "MV aggregate requires group keys");
    Preconditions.checkArgument(!mvKeyIndexes.isEmpty(), "MV aggregate requires at least one MV key");
    _mvKeyIndexes = mvKeyIndexes;

    int[] mvKeyIndexArray = new int[mvKeyIndexes.size()];
    for (int i = 0; i < mvKeyIndexArray.length; i++) {
      mvKeyIndexArray[i] = mvKeyIndexes.get(i);
    }
    _groupByExecutor =
        new MultistageGroupByExecutor(getGroupKeyIds(groupKeys), mvKeyIndexArray, _aggFunctions,
            _filterArgIds, _maxFilterArgId, node.getAggType(), node.isLeafReturnFinalResult(), _resultSchema,
            context.getOpChainMetadata(), node.getNodeHint());
  }

  @Override
  public Type getOperatorType() {
    return Type.MV_AGGREGATE;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /// Adds `mvKeys`: the positions of the MV keys in the group keys, as in the plan node.
  @Override
  protected Map<String, Plan.ExplainNode.AttributeValue> getExplainAttributes() {
    List<String> mvKeys = new ArrayList<>(_mvKeyIndexes.size());
    for (Integer mvKeyIndex : _mvKeyIndexes) {
      mvKeys.add(mvKeyIndex.toString());
    }
    // mvKeys lists positions in the group keys, not input column indexes.
    return new ExplainAttributeBuilder().putStringList("mvKeys", mvKeys).build();
  }

  @Override
  protected MseBlock.Eos consumeInput() {
    return consumeGroupBy();
  }

  @Override
  protected MseBlock produceAggregatedBlock() {
    return produceGroupByBlock();
  }
}
