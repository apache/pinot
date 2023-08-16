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
package org.apache.pinot.core.query.aggregation.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DummyAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DummyGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Child aggregation function is used for a result placeholder during the query processing,
 * It holds the position of the original aggregation function in the query
 * and use its name to denote which parent aggregation function it belongs to.
 * The name also serves as the key to retrieve the result from the parent aggregation function
 * result holder.
 * Please look at getResultColumnName() for the detailed format of the name.
 * Please look at ExprMinMaxRewriter as an example of how a child aggregation function is created.
 */
public abstract class ChildAggregationFunction implements AggregationFunction<Long, Long> {

  private static final int CHILD_AGGREGATION_FUNCTION_ID_OFFSET = 0;
  private static final int CHILD_AGGREGATION_FUNCTION_COLUMN_KEY_OFFSET = 1;
  private final ExpressionContext _childFunctionKeyInParent;
  private final List<ExpressionContext> _resultNameOperands;
  private final ExpressionContext _childFunctionID;

  ChildAggregationFunction(List<ExpressionContext> operands) {
    _childFunctionID = operands.get(CHILD_AGGREGATION_FUNCTION_ID_OFFSET);
    _childFunctionKeyInParent = operands.get(CHILD_AGGREGATION_FUNCTION_COLUMN_KEY_OFFSET);
    _resultNameOperands = operands.subList(CHILD_AGGREGATION_FUNCTION_COLUMN_KEY_OFFSET + 1, operands.size());
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    ArrayList<ExpressionContext> expressionContexts = new ArrayList<>();
    expressionContexts.add(_childFunctionID);
    expressionContexts.add(_childFunctionKeyInParent);
    expressionContexts.addAll(_resultNameOperands);
    return expressionContexts;
  }

  @Override
  public final AggregationResultHolder createAggregationResultHolder() {
    return new DummyAggregationResultHolder();
  }

  @Override
  public final GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new DummyGroupByResultHolder();
  }

  @Override
  public final void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
  }

  @Override
  public final void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
  }

  @Override
  public final void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
  }

  @Override
  public final Long extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return 0L;
  }

  @Override
  public final Long extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return 0L;
  }

  @Override
  public final Long merge(Long intermediateResult1, Long intermediateResult2) {
    return 0L;
  }

  @Override
  public final DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public final DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.UNKNOWN;
  }

  @Override
  public final Long extractFinalResult(Long longValue) {
    return 0L;
  }

  /**
   * The name of the column as follows:
   * CHILD_AGGREGATION_NAME_PREFIX + actual function type + operands + CHILD_AGGREGATION_SEPERATOR
   * + actual function type + parent aggregation function id + CHILD_KEY_SEPERATOR + column key in parent function
   * e.g. if the child aggregation function is "exprmax(0,a,b,x)", the name of the column is
   * "pinotchildaggregationepxrmax(a,b,x)@argmax0_x"
   */
  @Override
  public final String getResultColumnName() {
    String type = getType().getName().toLowerCase();
    return CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX
        // above is the prefix for all child aggregation functions

        + type + "(" + _resultNameOperands.stream().map(ExpressionContext::toString)
        .collect(Collectors.joining(",")) + ")"
        // above is the actual child aggregation function name we want to return to the user

        + CommonConstants.RewriterConstants.CHILD_AGGREGATION_SEPERATOR
        + type
        + _childFunctionID.getLiteral().getStringValue()
        + CommonConstants.RewriterConstants.CHILD_KEY_SEPERATOR
        + _childFunctionKeyInParent.toString();
    // above is the column key in the parent aggregation function
  }

  @Override
  public final String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX)
        .append("_").append(getType().getName()).append('(');
    int numArguments = getInputExpressions().size();
    if (numArguments > 0) {
      stringBuilder.append(getInputExpressions().get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }
}
