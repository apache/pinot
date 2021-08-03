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

import com.tdunning.math.stats.TDigest;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * The {@code PercentileRawTDigestAggregationFunction} returns the serialized {@code TDigest} data structure of the
 * {@code PercentileEstAggregationFunction}.
 */
public class PercentileRawTDigestAggregationFunction extends BaseSingleInputAggregationFunction<TDigest, String> {
  private final PercentileTDigestAggregationFunction _percentileRawTDigestAggregationFunction;

  public PercentileRawTDigestAggregationFunction(ExpressionContext expressionContext, int percentile) {
    this(expressionContext, new PercentileTDigestAggregationFunction(expressionContext, percentile));
  }

  public PercentileRawTDigestAggregationFunction(ExpressionContext expressionContext, double percentile) {
    this(expressionContext, new PercentileTDigestAggregationFunction(expressionContext, percentile));
  }

  protected PercentileRawTDigestAggregationFunction(ExpressionContext expression,
      PercentileTDigestAggregationFunction percentileRawTDigestAggregationFunction) {
    super(expression);
    _percentileRawTDigestAggregationFunction = percentileRawTDigestAggregationFunction;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILERAWTDIGEST;
  }

  @Override
  public String getColumnName() {
    final double percentile = _percentileRawTDigestAggregationFunction._percentile;
    final int version = _percentileRawTDigestAggregationFunction._version;
    final String type = getType().getName();

    return version == 0 ? type + (int) percentile + "_" + _expression : type + percentile + "_" + _expression;
  }

  @Override
  public String getResultColumnName() {
    final double percentile = _percentileRawTDigestAggregationFunction._percentile;
    final int version = _percentileRawTDigestAggregationFunction._version;
    final String type = getType().getName().toLowerCase();

    return version == 0 ? type + (int) percentile + "(" + _expression + ")"
        : type + "(" + _expression + ", " + percentile + ")";
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _percentileRawTDigestAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _percentileRawTDigestAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _percentileRawTDigestAggregationFunction.aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _percentileRawTDigestAggregationFunction
        .aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _percentileRawTDigestAggregationFunction
        .aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public TDigest extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _percentileRawTDigestAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public TDigest extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _percentileRawTDigestAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public TDigest merge(TDigest intermediateResult1, TDigest intermediateResult2) {
    return _percentileRawTDigestAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return _percentileRawTDigestAggregationFunction.isIntermediateResultComparable();
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return _percentileRawTDigestAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(TDigest intermediateResult) {
    return BytesUtils.toHexString(CustomSerDeUtils.TDIGEST_SER_DE.serialize(intermediateResult));
  }
}
