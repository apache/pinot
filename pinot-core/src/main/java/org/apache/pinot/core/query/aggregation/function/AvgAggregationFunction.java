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
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class AvgAggregationFunction extends NullableSingleInputAggregationFunction<AvgPair, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  public AvgAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "AVG"), nullHandlingEnabled);
  }

  protected AvgAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVG;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() == DataType.BYTES) {
      aggregateSerialized(blockValSet, length, aggregationResultHolder);
    } else {
      if (blockValSet.isSingleValue()) {
        aggregateSV(blockValSet, length, aggregationResultHolder);
      } else {
        aggregateMV(blockValSet, length, aggregationResultHolder);
      }
    }
  }

  protected void aggregateSerialized(BlockValSet blockValSet, int length,
      AggregationResultHolder aggregationResultHolder) {
    // Serialized AvgPair
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    AvgPair avgPair = new AvgPair();
    for (int i = 0; i < length; i++) {
      AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
      avgPair.apply(value);
    }
    // Only set the aggregation result when there is at least one non-null input value
    if (avgPair.getCount() != 0) {
      updateAggregationResult(aggregationResultHolder, avgPair.getSum(), avgPair.getCount());
    }
  }

  protected void aggregateSV(BlockValSet blockValSet, int length, AggregationResultHolder aggregationResultHolder) {
    double[] doubleValues = blockValSet.getDoubleValuesSV();
    AvgPair avgPair = new AvgPair();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        avgPair.apply(doubleValues[i], 1);
      }
    });
    // Only set the aggregation result when there is at least one non-null input value
    if (avgPair.getCount() != 0) {
      updateAggregationResult(aggregationResultHolder, avgPair.getSum(), avgPair.getCount());
    }
  }

  protected void aggregateMV(BlockValSet blockValSet, int length, AggregationResultHolder aggregationResultHolder) {
    double[][] valuesArray = blockValSet.getDoubleValuesMV();
    AvgPair avgPair = new AvgPair();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (double value : valuesArray[i]) {
          avgPair.apply(value);
        }
      }
    });
    if (avgPair.getCount() != 0) {
      updateAggregationResult(aggregationResultHolder, avgPair.getSum(), avgPair.getCount());
    }
  }

  protected void updateAggregationResult(AggregationResultHolder aggregationResultHolder, double sum, long count) {
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      aggregationResultHolder.setValue(new AvgPair(sum, count));
    } else {
      avgPair.apply(sum, count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() == DataType.BYTES) {
      aggregateGroupBySVSerialized(blockValSet, length, groupKeyArray, groupByResultHolder);
    } else {
      if (blockValSet.isSingleValue()) {
        aggregateSVGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
      } else {
        aggregateMVGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
      }
    }
  }

  protected void aggregateGroupBySVSerialized(BlockValSet blockValSet, int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder) {
    // Serialized AvgPair
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    for (int i = 0; i < length; i++) {
      AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
      updateGroupByResult(groupKeyArray[i], groupByResultHolder, avgPair.getSum(), avgPair.getCount());
    }
  }

  protected void aggregateSVGroupBySV(BlockValSet blockValSet, int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder) {
    double[] doubleValues = blockValSet.getDoubleValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        updateGroupByResult(groupKeyArray[i], groupByResultHolder, doubleValues[i], 1L);
      }
    });
  }

  protected void aggregateMVGroupBySV(BlockValSet blockValSet, int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder) {
    double[][] valuesArray = blockValSet.getDoubleValuesMV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        aggregateOnGroupKey(groupKeyArray[i], groupByResultHolder, valuesArray[i]);
      }
    });
  }

  protected void aggregateOnGroupKey(int groupKey, GroupByResultHolder groupByResultHolder, double[] values) {
    double sum = 0.0;
    for (double value : values) {
      sum += value;
    }
    long count = values.length;
    if (count != 0) {
      updateGroupByResult(groupKey, groupByResultHolder, sum, count);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() == DataType.BYTES) {
      aggregateGroupByMVSerialized(blockValSet, length, groupKeysArray, groupByResultHolder);
    } else {
      if (blockValSet.isSingleValue()) {
        aggregateSVGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
      } else {
        aggregateMVGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
      }
    }
  }

  protected void aggregateGroupByMVSerialized(BlockValSet blockValSet, int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder) {
    // Serialized AvgPair
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    for (int i = 0; i < length; i++) {
      AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
      for (int groupKey : groupKeysArray[i]) {
        updateGroupByResult(groupKey, groupByResultHolder, avgPair.getSum(), avgPair.getCount());
      }
    }
  }

  protected void aggregateSVGroupByMV(BlockValSet blockValSet, int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder) {
    double[] doubleValues = blockValSet.getDoubleValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int groupKey : groupKeysArray[i]) {
          updateGroupByResult(groupKey, groupByResultHolder, doubleValues[i], 1L);
        }
      }
    });
  }

  protected void aggregateMVGroupByMV(BlockValSet blockValSet, int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder) {
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        double[] values = valuesArray[i];
        for (int groupKey : groupKeysArray[i]) {
          aggregateOnGroupKey(groupKey, groupByResultHolder, values);
        }
      }
    });
  }

  protected void updateGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double sum, long count) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      groupByResultHolder.setValueForKey(groupKey, new AvgPair(sum, count));
    } else {
      avgPair.apply(sum, count);
    }
  }

  @Override
  public AvgPair extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      return _nullHandlingEnabled ? null : new AvgPair();
    }
    return avgPair;
  }

  @Override
  public AvgPair extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      return _nullHandlingEnabled ? null : new AvgPair();
    }
    return avgPair;
  }

  @Override
  public AvgPair merge(AvgPair intermediateResult1, AvgPair intermediateResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateResult1 == null) {
        return intermediateResult2;
      }
      if (intermediateResult2 == null) {
        return intermediateResult1;
      }
    }
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(AvgPair avgPair) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.AvgPair.getValue(),
        ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair));
  }

  @Override
  public AvgPair deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(AvgPair intermediateResult) {
    if (intermediateResult == null) {
      return null;
    }
    long count = intermediateResult.getCount();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      return intermediateResult.getSum() / count;
    }
  }
}
