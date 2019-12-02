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
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.startree.hll.HllUtil;
import org.apache.pinot.startree.hll.HllConstants;


public class FastHLLAggregationFunction implements AggregationFunction<HyperLogLog, Long> {
  private int _log2m = HllConstants.DEFAULT_LOG2M;

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FASTHLL;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.FASTHLL.getName() + "_" + column;
  }

  public void setLog2m(int log2m) {
    _log2m = log2m;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    String[] valueArray = blockValSets[0].getStringValuesSV();
    HyperLogLog hyperLogLog = getHyperLogLog(aggregationResultHolder);
    try {
      for (int i = 0; i < length; i++) {
        hyperLogLog.addAll(HllUtil.convertStringToHll(valueArray[i]));
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while aggregating HyperLogLog", e);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    String[] valueArray = blockValSets[0].getStringValuesSV();
    try {
      for (int i = 0; i < length; i++) {
        HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKeyArray[i]);
        hyperLogLog.addAll(HllUtil.convertStringToHll(valueArray[i]));
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while aggregating HyperLogLog", e);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    String[] valueArray = blockValSets[0].getStringValuesSV();
    try {
      for (int i = 0; i < length; i++) {
        HyperLogLog value = HllUtil.convertStringToHll(valueArray[i]);
        for (int groupKey : groupKeysArray[i]) {
          HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
          hyperLogLog.addAll(value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while aggregating HyperLogLog", e);
    }
  }

  @Override
  public HyperLogLog extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      return new HyperLogLog(_log2m);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLog extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      return new HyperLogLog(_log2m);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLog merge(HyperLogLog intermediateResult1, HyperLogLog intermediateResult2) {
    try {
      intermediateResult1.addAll(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLog", e);
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
  public Long extractFinalResult(HyperLogLog intermediateResult) {
    return intermediateResult.cardinality();
  }

  /**
   * Returns the HyperLogLog from the result holder or creates a new one if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return HyperLogLog from the result holder
   */
  private HyperLogLog getHyperLogLog(AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(_log2m);
      aggregationResultHolder.setValue(hyperLogLog);
    }
    return hyperLogLog;
  }

  /**
   * Returns the HyperLogLog for the given group key. If one does not exist, creates a new one and returns that.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the HyperLogLog
   * @return HyperLogLog for the group key
   */
  private HyperLogLog getHyperLogLog(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(_log2m);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
    }
    return hyperLogLog;
  }
}
