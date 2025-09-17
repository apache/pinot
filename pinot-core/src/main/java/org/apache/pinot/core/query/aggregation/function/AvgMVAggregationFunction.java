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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class AvgMVAggregationFunction extends AvgAggregationFunction {

  public AvgMVAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "AVG_MV"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVGMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() == DataType.BYTES) {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      AvgPair avgPair = new AvgPair();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
          avgPair.apply(value);
        }
      });
      if (avgPair.getCount() != 0) {
        updateAggregationResult(aggregationResultHolder, avgPair.getSum(), avgPair.getCount());
      }
      return;
    }

    double[][] valuesArray = blockValSet.getDoubleValuesMV();
    AvgPair avgPair = new AvgPair();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (double value : valuesArray[i]) {
          avgPair.apply(value);
        }
      }
    });
    // Only set the aggregation result when there is at least one non-null input value
    if (avgPair.getCount() != 0) {
      updateAggregationResult(aggregationResultHolder, avgPair.getSum(), avgPair.getCount());
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() != DataType.BYTES) {
      double[][] valuesArray = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          aggregateOnGroupKey(groupKeyArray[i], groupByResultHolder, valuesArray[i]);
        }
      });
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
          updateGroupByResult(groupKeyArray[i], groupByResultHolder, avgPair.getSum(), avgPair.getCount());
        }
      });
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() != DataType.BYTES) {
      double[][] valuesArray = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          double[] values = valuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            aggregateOnGroupKey(groupKey, groupByResultHolder, values);
          }
        }
      });
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            updateGroupByResult(groupKey, groupByResultHolder, avgPair.getSum(), avgPair.getCount());
          }
        }
      });
    }
  }

  private void aggregateOnGroupKey(int groupKey, GroupByResultHolder groupByResultHolder, double[] values) {
    double sum = 0.0;
    for (double value : values) {
      sum += value;
    }
    long count = values.length;
    if (count != 0) {
      updateGroupByResult(groupKey, groupByResultHolder, sum, count);
    }
  }
}
