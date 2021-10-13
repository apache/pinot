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
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountHLLSketchAggregationFunction extends BaseSingleInputAggregationFunction<HllSketch, Long> {
  // This is log-base2 of k, so k = 4096. lgK can be from 4 to 21
  protected final int _log2k;
  protected final TgtHllType _tgtHllType;

  public DistinctCountHLLSketchAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
    // This function expects 1 or 2 arguments.
    Preconditions
        .checkArgument(numExpressions <= 2, "DistinctCountHLLSketch expects 1 or 2 arguments, got: %s", numExpressions);
    if (arguments.size() == 2) {
      _log2k = Integer.parseInt(arguments.get(1).getLiteral());
    } else {
      _log2k = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_SKETCH_LOG2K;
    }
    _tgtHllType = HllSketch.DEFAULT_HLL_TYPE;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTHLLSKETCH;
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
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType != DataType.BYTES) {
      HllSketch hyperLogLog = getDefaultHllSketch(aggregationResultHolder);
      switch (storedType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.update(intValues[i]);
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.update(longValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.update(floatValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.update(doubleValues[i]);
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.update(stringValues[i]);
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal data type for DISTINCT_COUNT_HLL_SKETCH aggregation function: " + storedType);
      }
    } else {
      // Serialized HllSketch
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      Union union;
      try {
        HllSketch hyperLogLog = aggregationResultHolder.getResult();
        union = Union.heapify(hyperLogLog.toUpdatableByteArray());
        if (hyperLogLog != null) {
          for (int i = 0; i < length; i++) {
            union.update(ObjectSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.deserialize(bytesValues[i]));
          }
        } else {
          hyperLogLog = ObjectSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.deserialize(bytesValues[0]);
          union = Union.heapify(hyperLogLog.toUpdatableByteArray());
          for (int i = 1; i < length; i++) {
            union.update(ObjectSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.deserialize(bytesValues[i]));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HllSketchs", e);
      }
      aggregationResultHolder.setValue(union.getResult(_tgtHllType));
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHllSketch(groupByResultHolder, groupKeyArray[i]).update(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHllSketch(groupByResultHolder, groupKeyArray[i]).update(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHllSketch(groupByResultHolder, groupKeyArray[i]).update(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHllSketch(groupByResultHolder, groupKeyArray[i]).update(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHllSketch(groupByResultHolder, groupKeyArray[i]).update(stringValues[i]);
        }
        break;
      case BYTES:
        // Serialized HllSketch
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HllSketch value = ObjectSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.deserialize(bytesValues[i]);
            int groupKey = groupKeyArray[i];
            HllSketch hyperLogLog = groupByResultHolder.getResult(groupKey);
            if (hyperLogLog != null) {
              Union union = Union.heapify(hyperLogLog.toUpdatableByteArray());
              union.update(value);
              groupByResultHolder.setValueForKey(groupKey, union.getResult(_tgtHllType));
            } else {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HllSketchs", e);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_SKETCH aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          int value = intValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHllSketch(groupByResultHolder, groupKey).update(value);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          long value = longValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHllSketch(groupByResultHolder, groupKey).update(value);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          float value = floatValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHllSketch(groupByResultHolder, groupKey).update(value);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = doubleValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHllSketch(groupByResultHolder, groupKey).update(value);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHllSketch(groupByResultHolder, groupKey).update(value);
          }
        }
        break;
      case BYTES:
        // Serialized HllSketch
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HllSketch value = ObjectSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.deserialize(bytesValues[i]);
            for (int groupKey : groupKeysArray[i]) {
              HllSketch hyperLogLog = groupByResultHolder.getResult(groupKey);
              if (hyperLogLog != null) {
                Union union = Union.heapify(hyperLogLog.toUpdatableByteArray());
                union.update(value);
                groupByResultHolder.setValueForKey(groupKey, union.getResult(_tgtHllType));
              } else {
                // Create a new HllSketch for the group
                groupByResultHolder
                    .setValueForKey(groupKey, ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HllSketchs", e);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_SKETCH aggregation function: " + storedType);
    }
  }

  @Override
  public HllSketch extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    HllSketch hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      return new HllSketch(_log2k);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HllSketch extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    HllSketch hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      return new HllSketch(_log2k);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HllSketch merge(HllSketch intermediateResult1, HllSketch intermediateResult2) {
    try {
      intermediateResult1.update(intermediateResult2.toUpdatableByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HllSketchs", e);
    }
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(HllSketch intermediateResult) {
    return (long) intermediateResult.getEstimate();
  }

  /**
   * Returns the HllSketch from the result holder or creates a new one with default log2k if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return HllSketch from the result holder
   */
  protected HllSketch getDefaultHllSketch(AggregationResultHolder aggregationResultHolder) {
    HllSketch hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      hyperLogLog = new HllSketch(_log2k);
      aggregationResultHolder.setValue(hyperLogLog);
    }
    return hyperLogLog;
  }

  /**
   * Returns the HllSketch for the given group key if exists, or creates a new one with default log2k.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the HllSketch
   * @return HllSketch for the group key
   */
  protected HllSketch getDefaultHllSketch(GroupByResultHolder groupByResultHolder, int groupKey) {
    HllSketch hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      hyperLogLog = new HllSketch(_log2k);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
    }
    return hyperLogLog;
  }
}
