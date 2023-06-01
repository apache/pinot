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
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class PercentileKLLMVAggregationFunction extends PercentileKLLAggregationFunction {

  public PercentileKLLMVAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType valueType = valueSet.getValueType();
    KllDoublesSketch sketch = getOrCreateSketch(aggregationResultHolder);

    if (valueType == DataType.BYTES) {
      // Assuming the column contains serialized data sketches
      KllDoublesSketch[] deserializedSketches = deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
      for (int i = 0; i < length; i++) {
        sketch.merge(deserializedSketches[i]);
      }
    } else {
      double[][] values = valueSet.getDoubleValuesMV();
      for (int i = 0; i < length; i++) {
        for (double val : values[i]) {
          sketch.update(val);
        }
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType valueType = valueSet.getValueType();

    if (valueType == DataType.BYTES) {
      // serialized sketch
      KllDoublesSketch[] deserializedSketches = deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
      for (int i = 0; i < length; i++) {
        KllDoublesSketch sketch = getOrCreateSketch(groupByResultHolder, groupKeyArray[i]);
        sketch.merge(deserializedSketches[i]);
      }
    } else {
      double[][] values = valueSet.getDoubleValuesMV();
      for (int i = 0; i < length; i++) {
        KllDoublesSketch sketch = getOrCreateSketch(groupByResultHolder, groupKeyArray[i]);
        for (double val : values[i]) {
          sketch.update(val);
        }
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet valueSet = blockValSetMap.get(_expression);
    DataType valueType = valueSet.getValueType();

    if (valueType == DataType.BYTES) {
      // serialized sketch
      KllDoublesSketch[] deserializedSketches = deserializeSketches(blockValSetMap.get(_expression).getBytesValuesSV());
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          KllDoublesSketch sketch = this.getOrCreateSketch(groupByResultHolder, groupKey);
          sketch.merge(deserializedSketches[i]);
        }
      }
    } else {
      double[][] values = valueSet.getDoubleValuesMV();
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          KllDoublesSketch sketch = getOrCreateSketch(groupByResultHolder, groupKey);
          for (double val : values[i]) {
            sketch.update(val);
          }
        }
      }
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILEKLLMV;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.PERCENTILEKLLMV.getName().toLowerCase() + "(" + _expression + ", " + _percentile
        + ")";
  }
}
