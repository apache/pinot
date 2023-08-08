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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.local.customobject.SerializedQuantileDigest;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * The {@code PercentileRawEstAggregationFunction} returns the serialized {@code QuantileDigest} data structure of the
 * {@code PercentileEstAggregationFunction}.
 */
public class PercentileRawEstAggregationFunction
    extends BaseSingleInputAggregationFunction<QuantileDigest, SerializedQuantileDigest> {
  private final PercentileEstAggregationFunction _percentileEstAggregationFunction;

  public PercentileRawEstAggregationFunction(ExpressionContext expressionContext, double percentile) {
    this(expressionContext, new PercentileEstAggregationFunction(expressionContext, percentile));
  }

  public PercentileRawEstAggregationFunction(ExpressionContext expressionContext, int percentile) {
    this(expressionContext, new PercentileEstAggregationFunction(expressionContext, percentile));
  }

  protected PercentileRawEstAggregationFunction(ExpressionContext expression,
      PercentileEstAggregationFunction percentileEstAggregationFunction) {
    super(expression);
    _percentileEstAggregationFunction = percentileEstAggregationFunction;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILERAWEST;
  }

  @Override
  public String getResultColumnName() {
    final double percentile = _percentileEstAggregationFunction._percentile;
    final int version = _percentileEstAggregationFunction._version;
    final String type = getType().getName().toLowerCase();

    return version == 0 ? type + (int) percentile + "(" + _expression + ")"
        : type + "(" + _expression + ", " + percentile + ")";
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _percentileEstAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _percentileEstAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _percentileEstAggregationFunction.aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _percentileEstAggregationFunction.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _percentileEstAggregationFunction
        .aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public QuantileDigest extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _percentileEstAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public QuantileDigest extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _percentileEstAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public QuantileDigest merge(QuantileDigest intermediateResult1, QuantileDigest intermediateResult2) {
    return _percentileEstAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return _percentileEstAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public SerializedQuantileDigest extractFinalResult(QuantileDigest intermediateResult) {
    return new SerializedQuantileDigest(intermediateResult, _percentileEstAggregationFunction._percentile);
  }
}
