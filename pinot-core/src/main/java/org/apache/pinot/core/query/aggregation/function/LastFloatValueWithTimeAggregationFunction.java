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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.FloatValueTimePair;
import org.apache.pinot.segment.local.customobject.ValueTimePair;

/**
 * This function is used for LastWithTime calculations for data column with float type.
 * <p>The function can be used as LastWithTime(dataExpression, timeExpression, 'float')
 * <p>Following arguments are supported:
 * <ul>
 *   <li>dataExpression: expression that contains the float data column to be calculated last on</li>
 *   <li>timeExpression: expression that contains the column to be used to decide which data is last, can be any
 *   Numeric column</li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class LastFloatValueWithTimeAggregationFunction extends LastWithTimeAggregationFunction<Float> {

  private final static ValueTimePair<Float> DEFAULT_VALUE_TIME_PAIR = new FloatValueTimePair(Float.NaN, Long.MIN_VALUE);

  public LastFloatValueWithTimeAggregationFunction(
          ExpressionContext dataCol,
          ExpressionContext timeCol,
          ObjectSerDeUtils.ObjectSerDe<? extends ValueTimePair<Float>> objectSerDe) {
    super(dataCol, timeCol, objectSerDe);
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return Arrays.asList(_expression, _timeCol, ExpressionContext.forLiteral("Long"));
  }

  @Override
  public ValueTimePair<Float> constructValueTimePair(Float value, long time) {
    return new FloatValueTimePair(value, time);
  }

  @Override
  public ValueTimePair<Float> getDefaultValueTimePair() {
    return DEFAULT_VALUE_TIME_PAIR;
  }

  @Override
  public void updateResultWithRawData(int length, AggregationResultHolder aggregationResultHolder,
                                             BlockValSet blockValSet, BlockValSet timeValSet) {
    ValueTimePair<Float> defaultValueTimePair = getDefaultValueTimePair();
    Float lastData = defaultValueTimePair.getValue();
    long lastTime = defaultValueTimePair.getTime();
    float [] floatValues = blockValSet.getFloatValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();
    for (int i = 0; i < length; i++) {
      float data = floatValues[i];
      long time = timeValues[i];
      if (time >= lastTime) {
        lastTime = time;
        lastData = data;
      }
    }
    setAggregationResult(aggregationResultHolder, lastData, lastTime);
  }

  @Override
  public void updateGroupResultWithRawDataSv(int length,
                                             int[] groupKeyArray,
                                             GroupByResultHolder groupByResultHolder,
                                             BlockValSet blockValSet,
                                             BlockValSet timeValSet) {
    float[] floatValues = blockValSet.getFloatValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();
    for (int i = 0; i < length; i++) {
      float data = floatValues[i];
      long time = timeValues[i];
      setGroupByResult(groupKeyArray[i], groupByResultHolder, data, time);
    }
  }

  @Override
  public void updateGroupResultWithRawDataMv(int length,
                                             int[][] groupKeysArray,
                                             GroupByResultHolder groupByResultHolder,
                                             BlockValSet blockValSet,
                                             BlockValSet timeValSet) {
    float[] floatValues = blockValSet.getFloatValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();
    for (int i = 0; i < length; i++) {
      float value = floatValues[i];
      long time = timeValues[i];
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupKey, groupByResultHolder, value, time);
      }
    }
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression + "," + _timeCol + ", Float)";
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.FLOAT;
  }
}
