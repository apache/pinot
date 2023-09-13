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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
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
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Use {@link DistinctCountHLLAggregationFunction} on byte[] for serialized HyperLogLog.
 */
@Deprecated
public class FastHLLAggregationFunction extends BaseSingleInputAggregationFunction<HyperLogLog, Long> {
  public static final int DEFAULT_LOG2M = 8;
  private static final int BYTE_TO_CHAR_OFFSET = 129;

  public FastHLLAggregationFunction(List<ExpressionContext> arguments) {
    super(verifySingleArgument(arguments, "FAST_HLL"));
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FASTHLL;
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
    String[] values = blockValSetMap.get(_expression).getStringValuesSV();
    try {
      HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
      if (hyperLogLog != null) {
        for (int i = 0; i < length; i++) {
          hyperLogLog.addAll(convertStringToHLL(values[i]));
        }
      } else {
        hyperLogLog = convertStringToHLL(values[0]);
        aggregationResultHolder.setValue(hyperLogLog);
        for (int i = 1; i < length; i++) {
          hyperLogLog.addAll(convertStringToHLL(values[i]));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    String[] values = blockValSetMap.get(_expression).getStringValuesSV();
    try {
      for (int i = 0; i < length; i++) {
        HyperLogLog value = convertStringToHLL(values[i]);
        int groupKey = groupKeyArray[i];
        HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
        if (hyperLogLog != null) {
          hyperLogLog.addAll(value);
        } else {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    String[] values = blockValSetMap.get(_expression).getStringValuesSV();
    try {
      for (int i = 0; i < length; i++) {
        HyperLogLog value = convertStringToHLL(values[i]);
        for (int groupKey : groupKeysArray[i]) {
          HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
          if (hyperLogLog != null) {
            hyperLogLog.addAll(value);
          } else {
            // Create a new HyperLogLog for the group
            groupByResultHolder.setValueForKey(groupKey, convertStringToHLL(values[i]));
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
    }
  }

  @Override
  public HyperLogLog extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      return new HyperLogLog(DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLog extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      return new HyperLogLog(DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLog merge(HyperLogLog intermediateResult1, HyperLogLog intermediateResult2) {
    // Can happen when aggregating serialized HyperLogLog with non-default log2m
    if (intermediateResult1.sizeof() != intermediateResult2.sizeof()) {
      if (intermediateResult1.cardinality() == 0) {
        return intermediateResult2;
      } else {
        Preconditions.checkState(intermediateResult2.cardinality() == 0,
            "Cannot merge HyperLogLogs of different sizes");
        return intermediateResult1;
      }
    }
    try {
      intermediateResult1.addAll(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLog", e);
    }
    return intermediateResult1;
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
  public Long extractFinalResult(HyperLogLog intermediateResult) {
    return intermediateResult.cardinality();
  }

  private static HyperLogLog convertStringToHLL(String value) {
    char[] chars = value.toCharArray();
    int length = chars.length;
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (chars[i] - BYTE_TO_CHAR_OFFSET);
    }
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytes);
  }
}
