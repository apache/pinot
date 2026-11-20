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
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.MvAggregateNode;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/// Runs an `MvAggregateNode`: a group by where some group keys are multi-value (MV). Each input row contributes to
/// the group of every combination of its MV key elements, with the single-stage engine semantics: one group per
/// element, a repeated element counts once per copy, an empty array contributes to no group, a `null` array forms
/// the `null` group, and several MV keys give the cartesian product. The operator computes the group ids of each row
/// directly from the arrays and calls `AggregationFunction.aggregateGroupByMV`, so it never materializes one row per
/// element.
///
/// It is an [AggregateOperator] whose group-by executor expands the MV keys. The output is the same as for a plain
/// group by: `[group keys..., aggregations...]`, with scalar values for the MV keys. The MV keys only change the input,
/// so only LEAF and DIRECT aggregates run here. The FINAL side of a split aggregate is a plain [AggregateOperator].
///
/// The operator reports its stats as [MultiStageOperator.Type#AGGREGATE], so that a server that predates the MV
/// aggregate can read them.
///
/// Like every [MultiStageOperator], an instance is not thread-safe: one op chain thread drives it.
public class MvAggregateOperator extends AggregateOperator {
  private static final String EXPLAIN_NAME = "MV_AGGREGATE_OPERATOR";

  private final List<Integer> _mvKeyIndexes;

  public MvAggregateOperator(OpChainExecutionContext context, MultiStageOperator input, MvAggregateNode node) {
    super(context, input, toAggregateNode(node), toArray(node.getMvKeyIndexes()));
    _mvKeyIndexes = node.getMvKeyIndexes();
  }

  private static AggregateNode toAggregateNode(MvAggregateNode node) {
    Preconditions.checkArgument(!node.getGroupKeys().isEmpty(), "MV aggregate requires group keys");
    Preconditions.checkArgument(!node.getMvKeyIndexes().isEmpty(), "MV aggregate requires at least one MV key");
    return new AggregateNode(node.getStageId(), node.getDataSchema(), node.getNodeHint(), node.getInputs(),
        node.getAggCalls(), node.getFilterArgs(), node.getGroupKeys(), node.getAggType(),
        node.isLeafReturnFinalResult(), node.getCollations(), node.getLimit());
  }

  private static int[] toArray(List<Integer> values) {
    int[] array = new int[values.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = values.get(i);
    }
    return array;
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
    return new ExplainAttributeBuilder().putStringList("mvKeys", mvKeys).build();
  }
}
