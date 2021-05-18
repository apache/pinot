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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class PercentileEstAggregationFunction extends BaseSingleInputAggregationFunction<QuantileDigest, Long> {
  public static final double DEFAULT_MAX_ERROR = 0.05;

  //version 0 functions specified in the of form PERCENTILEEST<2-digits>(column)
  //version 1 functions of form PERCENTILEEST(column, <2-digits>.<16-digits>)
  protected final int _version;
  protected final double _percentile;

  public PercentileEstAggregationFunction(ExpressionContext expression, int percentile) {
    super(expression);
    _version = 0;
    _percentile = percentile;
  }

  public PercentileEstAggregationFunction(ExpressionContext expression, double percentile) {
    super(expression);
    _version = 1;
    _percentile = percentile;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILEEST;
  }

  @Override
  public String getColumnName() {
    return _version == 0 ? AggregationFunctionType.PERCENTILEEST.getName() + (int) _percentile + "_" + _expression
        : AggregationFunctionType.PERCENTILEEST.getName() + _percentile + "_" + _expression;
  }

  @Override
  public String getResultColumnName() {
    return _version == 0 ? AggregationFunctionType.PERCENTILEEST.getName().toLowerCase() + (int) _percentile + "("
        + _expression + ")"
        : AggregationFunctionType.PERCENTILEEST.getName().toLowerCase() + "(" + _expression + ", " + _percentile + ")";
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
      long[] longValues = blockValSet.getLongValuesSV();
      QuantileDigest quantileDigest = getDefaultQuantileDigest(aggregationResultHolder);
      for (int i = 0; i < length; i++) {
        quantileDigest.add(longValues[i]);
      }
    } else {
      // Serialized QuantileDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      QuantileDigest quantileDigest = aggregationResultHolder.getResult();
      if (quantileDigest != null) {
        for (int i = 0; i < length; i++) {
          quantileDigest.merge(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]));
        }
      } else {
        quantileDigest = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[0]);
        aggregationResultHolder.setValue(quantileDigest);
        for (int i = 1; i < length; i++) {
          quantileDigest.merge(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]));
        }
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      long[] longValues = blockValSet.getLongValuesSV();
      for (int i = 0; i < length; i++) {
        getDefaultQuantileDigest(groupByResultHolder, groupKeyArray[i]).add(longValues[i]);
      }
    } else {
      // Serialized QuantileDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        QuantileDigest value = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]);
        int groupKey = groupKeyArray[i];
        QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
        if (quantileDigest != null) {
          quantileDigest.merge(value);
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
      long[] longValues = blockValSet.getLongValuesSV();
      for (int i = 0; i < length; i++) {
        long value = longValues[i];
        for (int groupKey : groupKeysArray[i]) {
          getDefaultQuantileDigest(groupByResultHolder, groupKey).add(value);
        }
      }
    } else {
      // Serialized QuantileDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        QuantileDigest value = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]);
        for (int groupKey : groupKeysArray[i]) {
          QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
          if (quantileDigest != null) {
            quantileDigest.merge(value);
          } else {
            // Create a new QuantileDigest for the group
            groupByResultHolder
                .setValueForKey(groupKey, ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]));
          }
        }
      }
    }
  }

  @Override
  public QuantileDigest extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    QuantileDigest quantileDigest = aggregationResultHolder.getResult();
    if (quantileDigest == null) {
      return new QuantileDigest(DEFAULT_MAX_ERROR);
    } else {
      return quantileDigest;
    }
  }

  @Override
  public QuantileDigest extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
    if (quantileDigest == null) {
      return new QuantileDigest(DEFAULT_MAX_ERROR);
    } else {
      return quantileDigest;
    }
  }

  @Override
  public QuantileDigest merge(QuantileDigest intermediateResult1, QuantileDigest intermediateResult2) {
    if (intermediateResult1.getCount() == 0.0) {
      return intermediateResult2;
    }
    if (intermediateResult2.getCount() == 0.0) {
      return intermediateResult1;
    }
    intermediateResult1.merge(intermediateResult2);
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
  public Long extractFinalResult(QuantileDigest intermediateResult) {
    return intermediateResult.getQuantile(_percentile / 100.0);
  }

  /**
   * Returns the QuantileDigest from the result holder or creates a new one with default max error if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return QuantileDigest from the result holder
   */
  protected static QuantileDigest getDefaultQuantileDigest(AggregationResultHolder aggregationResultHolder) {
    QuantileDigest quantileDigest = aggregationResultHolder.getResult();
    if (quantileDigest == null) {
      quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
      aggregationResultHolder.setValue(quantileDigest);
    }
    return quantileDigest;
  }

  /**
   * Returns the QuantileDigest for the given group key if exists, or creates a new one with default max error.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the QuantileDigest
   * @return QuantileDigest for the group key
   */
  protected static QuantileDigest getDefaultQuantileDigest(GroupByResultHolder groupByResultHolder, int groupKey) {
    QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
    if (quantileDigest == null) {
      quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
      groupByResultHolder.setValueForKey(groupKey, quantileDigest);
    }
    return quantileDigest;
  }
}
