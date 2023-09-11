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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
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
import org.roaringbitmap.RoaringBitmap;


public class AvgAggregationFunction extends BaseSingleInputAggregationFunction<AvgPair, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;
  private final boolean _nullHandlingEnabled;

  public AvgAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifyArguments(arguments), nullHandlingEnabled);
  }

  private static ExpressionContext verifyArguments(List<ExpressionContext> arguments) {
    Preconditions.checkArgument(arguments.size() == 1, "AVG expects 1 argument, got: %s", arguments.size());
    return arguments.get(0);
  }

  protected AvgAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
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
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateNullHandlingEnabled(length, aggregationResultHolder, blockValSet, nullBitmap);
        return;
      }
    }

    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      double sum = 0.0;
      for (int i = 0; i < length; i++) {
        sum += doubleValues[i];
      }
      setAggregationResult(aggregationResultHolder, sum, length);
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      double sum = 0.0;
      long count = 0L;
      for (int i = 0; i < length; i++) {
        AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
        sum += value.getSum();
        count += value.getCount();
      }
      setAggregationResult(aggregationResultHolder, sum, count);
    }
  }

  private void aggregateNullHandlingEnabled(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      if (nullBitmap.getCardinality() < length) {
        double sum = 0.0;
        long count = 0L;
        // TODO: need to update the for-loop terminating condition to: i < length & i < doubleValues.length?
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            sum += doubleValues[i];
            count++;
          }
        }
        setAggregationResult(aggregationResultHolder, sum, count);
      }
      // Note: when all input values re null (nullBitmap.getCardinality() == values.length), avg is null. As a result,
      // we don't call setAggregationResult.
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      if (nullBitmap.getCardinality() < length) {
        double sum = 0.0;
        long count = 0L;
        // TODO: need to update the for-loop terminating condition to: i < length & i < bytesValues.length?
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
            sum += value.getSum();
            count += value.getCount();
          }
        }
        setAggregationResult(aggregationResultHolder, sum, count);
      }
    }
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double sum, long count) {
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
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateGroupBySVNullHandlingEnabled(length, groupKeyArray, groupByResultHolder, blockValSet, nullBitmap);
        return;
      }
    }

    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        setGroupByResult(groupKeyArray[i], groupByResultHolder, doubleValues[i], 1L);
      }
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
        setGroupByResult(groupKeyArray[i], groupByResultHolder, avgPair.getSum(), avgPair.getCount());
      }
    }
  }

  private void aggregateGroupBySVNullHandlingEnabled(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      // TODO: need to update the for-loop terminating condition to: i < length & i < valueArray.length?
      if (nullBitmap.getCardinality() < length) {
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            int groupKey = groupKeyArray[i];
            setGroupByResult(groupKey, groupByResultHolder, doubleValues[i], 1L);
          }
        }
      }
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      // TODO: need to update the for-loop terminating condition to: i < length & i < valueArray.length?
      if (nullBitmap.getCardinality() < length) {
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            int groupKey = groupKeyArray[i];
            AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
            setGroupByResult(groupKey, groupByResultHolder, avgPair.getSum(), avgPair.getCount());
          }
        }
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        double value = doubleValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, 1L);
        }
      }
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
        double sum = avgPair.getSum();
        long count = avgPair.getCount();
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, sum, count);
        }
      }
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double sum, long count) {
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
      return _nullHandlingEnabled ? null : new AvgPair(0.0, 0L);
    }
    return avgPair;
  }

  @Override
  public AvgPair extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      return _nullHandlingEnabled ? null : new AvgPair(0.0, 0L);
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
