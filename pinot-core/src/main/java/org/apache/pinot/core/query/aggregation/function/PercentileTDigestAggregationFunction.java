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
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * TDigest based Percentile aggregation function.
 */
public class PercentileTDigestAggregationFunction extends BaseSingleInputAggregationFunction<TDigest, Double> {
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;

  protected final int _percentile;

  public PercentileTDigestAggregationFunction(ExpressionContext expression, int percentile) {
    super(expression);
    _percentile = percentile;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILETDIGEST;
  }

  @Override
  public String getColumnName() {
    return AggregationFunctionType.PERCENTILETDIGEST.getName() + _percentile + "_" + _expression;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.PERCENTILETDIGEST.getName().toLowerCase() + _percentile + "(" + _expression + ")";
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
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      TDigest tDigest = getDefaultTDigest(aggregationResultHolder);
      for (int i = 0; i < length; i++) {
        tDigest.add(doubleValues[i]);
      }
    } else {
      // Serialized TDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      TDigest tDigest = aggregationResultHolder.getResult();
      if (tDigest != null) {
        for (int i = 0; i < length; i++) {
          tDigest.add(ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]));
        }
      } else {
        tDigest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[0]);
        aggregationResultHolder.setValue(tDigest);
        for (int i = 1; i < length; i++) {
          tDigest.add(ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]));
        }
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        getDefaultTDigest(groupByResultHolder, groupKeyArray[i]).add(doubleValues[i]);
      }
    } else {
      // Serialized TDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        TDigest value = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]);
        int groupKey = groupKeyArray[i];
        TDigest tDigest = groupByResultHolder.getResult(groupKey);
        if (tDigest != null) {
          tDigest.add(value);
        } else {
          groupByResultHolder.setValueForKey(groupKey, value);
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
          getDefaultTDigest(groupByResultHolder, groupKey).add(value);
        }
      }
    } else {
      // Serialized QuantileDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        TDigest value = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]);
        for (int groupKey : groupKeysArray[i]) {
          TDigest tDigest = groupByResultHolder.getResult(groupKey);
          if (tDigest != null) {
            tDigest.add(value);
          } else {
            // Create a new TDigest for the group
            groupByResultHolder.setValueForKey(groupKey, ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]));
          }
        }
      }
    }
  }

  @Override
  public TDigest extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      return TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
    } else {
      return tDigest;
    }
  }

  @Override
  public TDigest extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    TDigest tDigest = groupByResultHolder.getResult(groupKey);
    if (tDigest == null) {
      return TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
    } else {
      return tDigest;
    }
  }

  @Override
  public TDigest merge(TDigest intermediateResult1, TDigest intermediateResult2) {
    if (intermediateResult1.size() == 0L) {
      return intermediateResult2;
    }
    if (intermediateResult2.size() == 0L) {
      return intermediateResult1;
    }
    intermediateResult1.add(intermediateResult2);
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
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(TDigest intermediateResult) {
    return intermediateResult.quantile(_percentile / 100.0);
  }

  /**
   * Returns the TDigest from the result holder or creates a new one with default compression if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return TDigest from the result holder
   */
  protected static TDigest getDefaultTDigest(AggregationResultHolder aggregationResultHolder) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      tDigest = TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
      aggregationResultHolder.setValue(tDigest);
    }
    return tDigest;
  }

  /**
   * Returns the TDigest for the given group key if exists, or creates a new one with default compression.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the TDigest
   * @return TDigest for the group key
   */
  protected static TDigest getDefaultTDigest(GroupByResultHolder groupByResultHolder, int groupKey) {
    TDigest tDigest = groupByResultHolder.getResult(groupKey);
    if (tDigest == null) {
      tDigest = TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
      groupByResultHolder.setValueForKey(groupKey, tDigest);
    }
    return tDigest;
  }
}
