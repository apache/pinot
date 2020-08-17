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

import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.datasketches.theta.Sketch;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.utils.ByteArray;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BYTES;


/**
 * A variation of the {@link DistinctCountThetaSketchAggregationFunction} that returns the serialized bytes
 * of the theta-sketch, as opposed to the actual distinct value.
 *
 * Note: It would have been natural for this class to extend the {@link DistinctCountThetaSketchAggregationFunction},
 * except that the return type for this class is a String, as opposed to Integer for the former, due to which the
 * extension is not possible.
 */
public class DistinctCountRawThetaSketchAggregationFunction implements AggregationFunction<Map<String, Sketch>, ByteArray> {
  private final DistinctCountThetaSketchAggregationFunction _thetaSketchAggregationFunction;

  public DistinctCountRawThetaSketchAggregationFunction(List<ExpressionContext> arguments)
      throws SqlParseException {
    _thetaSketchAggregationFunction = new DistinctCountThetaSketchAggregationFunction(arguments);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWTHETASKETCH;
  }

  @Override
  public String getColumnName() {
    return _thetaSketchAggregationFunction.getColumnName();
  }

  @Override
  public String getResultColumnName() {
    return _thetaSketchAggregationFunction.getResultColumnName();
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return _thetaSketchAggregationFunction.getInputExpressions();
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _thetaSketchAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _thetaSketchAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _thetaSketchAggregationFunction.aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _thetaSketchAggregationFunction.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _thetaSketchAggregationFunction.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public Map<String, Sketch> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _thetaSketchAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public Map<String, Sketch> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _thetaSketchAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public Map<String, Sketch> merge(Map<String, Sketch> intermediateResult1, Map<String, Sketch> intermediateResult2) {
    return _thetaSketchAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return _thetaSketchAggregationFunction.isIntermediateResultComparable();
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return _thetaSketchAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return BYTES;
  }

  @Override
  public ByteArray extractFinalResult(Map<String, Sketch> intermediateResult) {
    Sketch finalSketch = _thetaSketchAggregationFunction.extractFinalSketch(intermediateResult);
    return new ByteArray(finalSketch.compact().toByteArray());
  }
}
